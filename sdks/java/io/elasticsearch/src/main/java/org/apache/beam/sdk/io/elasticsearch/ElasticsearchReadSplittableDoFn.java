/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.elasticsearch;

import static org.apache.beam.sdk.io.elasticsearch.Utils.getBackendVersion;
import static org.apache.beam.sdk.io.elasticsearch.Utils.parseResponse;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.ConnectionConfiguration;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.BoundedPerElement;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterators;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@BoundedPerElement
class ElasticsearchReadSplittableDoFn extends DoFn<String, String> {

  private static final Integer DEFAULT_MAX_SLICES = 4;

  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchReadSplittableDoFn.class);

  private final ConnectionConfiguration connectionConfiguration;
  private final boolean isWithMetadata;
  private final long batchSize;
  private final String scrollKeepalive;
  private final Integer maxSlices;

  @Nullable private final String shardPreference;
  @Nullable private Integer numSlices;

  /* Because we can be scrolling over a long period of time then the scrollId needs to be resilient.
   *
   * With Elasticsearch, you can't use the same scrollId multiple times and get the same result set [1].
   * If we keep track of the scrollId and the total number of records that have been output for the slice,
   * it would be possible to recover state and restart output from where it left off by skipping the number of
   * records that have succeeded.  This strategy has the potential to be brittle and is best left as a future optimization.
   *
   * [1] https://discuss.elastic.co/t/do-unique-reusable--scroll-ids-exist/12280/3
   */

  private transient RestClient restClient;

  private transient int backendVersion;

  ElasticsearchReadSplittableDoFn(
      ConnectionConfiguration connectionConfiguration,
      boolean isWithMetadata,
      long batchSize,
      String scrollKeepalive,
      @Nullable String shardPreference,
      @Nullable Integer numSlices,
      @Nullable Integer maxSlices) {

    this.connectionConfiguration = connectionConfiguration;
    this.isWithMetadata = isWithMetadata;
    this.batchSize = batchSize;
    this.scrollKeepalive = scrollKeepalive;

    this.shardPreference = shardPreference;
    this.numSlices = numSlices;
    this.maxSlices = maxSlices == null ? DEFAULT_MAX_SLICES : maxSlices;
  }

  private String getPreparedQuery(String query, @Nullable Long sliceId) {
    if (query == null || query.length() == 0) {
      query = "{\"query\": { \"match_all\": {} }}";
    }

    if ((backendVersion == 5 || backendVersion == 6)
        && numSlices != null
        && numSlices > 1
        && sliceId != null) {
      // If there is more than one slice, add the slice to the user query
      String sliceQuery = String.format("\"slice\": {\"id\": %s,\"max\": %s}", sliceId, numSlices);
      query = query.replaceFirst("\\{", "{" + sliceQuery + ",");
    }

    return query;
  }

  private static JsonNode getStats(
      ElasticsearchIO.ConnectionConfiguration connectionConfiguration, boolean shardLevel)
      throws IOException {
    HashMap<String, String> params = new HashMap<>();
    if (shardLevel) {
      params.put("level", "shards");
    }
    String endpoint = String.format("/%s/_stats", connectionConfiguration.getIndex());
    try (RestClient restClient = connectionConfiguration.createClient()) {
      return parseResponse(restClient.performRequest("GET", endpoint, params).getEntity());
    }
  }

  private static int getShardCount(ConnectionConfiguration connectionConfiguration)
      throws IOException {

    JsonNode statsJson = getStats(connectionConfiguration, true);
    JsonNode shardsJson =
        statsJson.path("indices").path(connectionConfiguration.getIndex()).path("shards");

    return Iterators.size(shardsJson.elements());
  }

  @GetInitialRestriction
  public OffsetRange getInitialRange(String element) {
    /* The range is the number of slices which will default to tne number of shards in the target index.
     *
     * Maybe we can keep track of the number of records and shard count once we make the first query.
     * That all said, we're using a long running cursor for the processing of elements
     * and we want to ensure that the output doesn't create a hot bundle(window,key)
     * Each incoming query can be sent to a different worker.
     *
     */
    // There can be only one worker pulling data concurrently with Elasticsearch <5
    if (backendVersion < 5) {
      return new OffsetRange(0L, 1L);
    }
    return new OffsetRange(0L, numSlices);
  }

  @SplitRestriction
  public void splitRestriction(
      String element, OffsetRange restriction, OutputReceiver<OffsetRange> receiver) {

    long elements = restriction.getTo() - restriction.getFrom();

    // Attempt to split the restriction in half
    restriction.split(elements / 2, 1).forEach(receiver::output);
  }

  @ProcessElement
  public ProcessContinuation processElement(ProcessContext c, OffsetRangeTracker tracker, OutputReceiver<String> receiver)
      throws IOException {

    LOG.info("Processing OffsetRangeTracker: {}", tracker);
    // Elasticsearch supports parallelism of requests based on a concept called slices.
    // Slicing can be done in parallel, scrolling must be done in serial for each slice.

    if (tracker.currentRestriction().getFrom() == tracker.currentRestriction().getTo()) {
      // TODO: Verify correct usage of the RestrictionTracker
      // This is a weird edge case because when the shards are < 4 we get called with
      // OffsetRangeTracker{range=[1, 1) which should never happen.
      // If we call tryClaim it seems to fix the state machine within the DirectRunner.
      // Need to identify if this will do something else
      if (tracker.tryClaim(tracker.currentRestriction().getFrom())) {
        LOG.info("Tried claiming the first item and it succeeded....");
      }
      return ProcessContinuation.stop();
    }

    for (long sliceId = tracker.currentRestriction().getFrom();
        sliceId < tracker.currentRestriction().getTo();
        ++sliceId) {

      if (!tracker.tryClaim(sliceId)) {
        // Quit processing the element and exit with stop()
        LOG.info("Failed to claim {}", sliceId);
        break;
      }
      LOG.info("Successfully Claimed: {}", sliceId);

      String query = this.getPreparedQuery(c.element(), sliceId);

      this.executeQueryWithOutput(query, receiver);

      /* TODO: Determine the best course of action here.
        Should we just blindly loop and see if the runner splits the restriction or return ProcessContinuation.resume()?
        The right answer is likely predicated on whether tryClaim should be called before or after work has been done.
      */
      LOG.info("Completed outputting results of offset: {}", sliceId);
    }
    LOG.info(
        "Completed processing {}: {} Returning ProcessContinuation.stop()", c.element(), tracker);
    return ProcessContinuation.stop();
  }

  private void executeQueryWithOutput(String query, OutputReceiver<String> receiver)
      throws IOException {
    JsonNode result = executeSearchQuery(query);
    String scrollId = result.path("_scroll_id").asText();
    try {
      while (result.path("hits").path("hits").size() > 0) {

        JsonNode hits = result.path("hits").path("hits");
        if (isWithMetadata) {
          hits.forEach(hit -> receiver.output(hit.toString()));
        } else {
          hits.forEach(hit -> receiver.output(hit.path("_source").toString()));
        }
        scrollId = result.path("_scroll_id").asText();

        result = executeScrollQuery(scrollId);
      }
    }
    finally {
      deleteScroll(scrollId);
    }
  }
  private JsonNode executeScrollQuery(String scrollId) throws IOException {
    String requestBody =
        String.format("{\"scroll\" : \"%s\",\"scroll_id\" : \"%s\"}", scrollKeepalive, scrollId);
    HttpEntity scrollEntity = new NStringEntity(requestBody, ContentType.APPLICATION_JSON);
    Request request = new Request("GET", "/_search/scroll");
    request.setEntity(scrollEntity);

    Response response = restClient.performRequest(request);
    return parseResponse(response.getEntity());
  }

  private void deleteScroll(String scrollId) throws IOException {
    if (scrollId == null) {
      return;
    }

    String requestBody = String.format("{\"scroll_id\" : [\"%s\"]}", scrollId);
    HttpEntity entity = new NStringEntity(requestBody, ContentType.APPLICATION_JSON);

    Request request = new Request("DELETE", "/_search/scroll");
    request.setEntity(entity);
    restClient.performRequest(request);
  }

  private JsonNode executeSearchQuery(String query) throws IOException {

    String endPoint =
        String.format(
            "/%s/%s/_search",
            connectionConfiguration.getIndex(), connectionConfiguration.getType());

    Request request = new Request("GET", endPoint);

    request.addParameter("scroll", scrollKeepalive);
    request.addParameter("size", String.valueOf(batchSize));
    if (backendVersion == 2 && shardPreference != null) {
      request.addParameter("preference", "_shards:" + shardPreference);
    }

    LOG.info("Executing: {}\n With Parameters: {}", query, request);
    HttpEntity queryEntity = new NStringEntity(query, ContentType.APPLICATION_JSON);
    request.setEntity(queryEntity);

    Response response = restClient.performRequest(request);
    return parseResponse(response.getEntity());
  }

  @Setup
  public void setup() throws IOException {
    restClient = connectionConfiguration.createClient();
    backendVersion = getBackendVersion(connectionConfiguration);

    // We can only use slices if the backend version is >=5
    if (backendVersion >= 5 && this.numSlices == null) {
      numSlices = Math.max(getShardCount(connectionConfiguration), maxSlices);
    }
  }

  @Teardown
  public void teardown() throws IOException {
    //deleteScroll();

    if (restClient != null) {
      restClient.close();
    }
  }
}
