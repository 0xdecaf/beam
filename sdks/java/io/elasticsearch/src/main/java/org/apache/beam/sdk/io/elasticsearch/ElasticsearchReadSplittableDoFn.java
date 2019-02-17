package org.apache.beam.sdk.io.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.ConnectionConfiguration;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.BoundedPerElement;
import org.apache.beam.sdk.transforms.splittabledofn.Backlog;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterators;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.beam.sdk.io.elasticsearch.Utils.getBackendVersion;
import static org.apache.beam.sdk.io.elasticsearch.Utils.parseResponse;

@BoundedPerElement
class ElasticsearchReadSplittableDoFn extends DoFn<String, String> {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchReadSplittableDoFn.class);

  private final ConnectionConfiguration connectionConfiguration;
  private final boolean isWithMetadata;
  private final long batchSize;
  private final String scrollKeepalive;

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

  private transient String scrollId;

  private transient int backendVersion;

  ElasticsearchReadSplittableDoFn(
      ConnectionConfiguration connectionConfiguration,
      boolean isWithMetadata,
      long batchSize,
      String scrollKeepalive,
      @Nullable String shardPreference,
      @Nullable Integer numSlices) {

    this.connectionConfiguration = connectionConfiguration;
    this.isWithMetadata = isWithMetadata;
    this.batchSize = batchSize;
    this.scrollKeepalive = scrollKeepalive;

    this.shardPreference = shardPreference;
    this.numSlices = numSlices;
  }

  private String getPreparedQuery(ProcessContext c, @Nullable Long sliceId) {

    String query = c.element();
    if (query == null) {
      query = "{\"query\": { \"match_all\": {} }}";
    }

    if ((backendVersion == 5 || backendVersion == 6) && numSlices != null && numSlices > 1 && sliceId != null) {
      //if there is more than one slice, add the slice to the user query
      String sliceQuery =
          String.format("\"slice\": {\"id\": %s,\"max\": %s}", sliceId, numSlices);
      query = query.replaceFirst("\\{", "{" + sliceQuery + ",");
    }

    return query;
  }

  private static JsonNode getStats(
      ElasticsearchIO.ConnectionConfiguration connectionConfiguration, boolean shardLevel) throws IOException {
    HashMap<String, String> params = new HashMap<>();
    if (shardLevel) {
      params.put("level", "shards");
    }
    String endpoint = String.format("/%s/_stats", connectionConfiguration.getIndex());
    try (RestClient restClient = connectionConfiguration.createClient()) {
      return parseResponse(restClient.performRequest("GET", endpoint, params).getEntity());
    }
  }

  private static int getShardCount(ConnectionConfiguration connectionConfiguration) throws IOException {

    JsonNode statsJson = getStats(connectionConfiguration, true);
    JsonNode shardsJson =
        statsJson.path("indices").path(connectionConfiguration.getIndex()).path("shards");

    return Iterators.size(shardsJson.elements());
    // A hack to temporarily get around an issue with google commons not available by vendored package.
//    int numSlices = 0;
//    Iterator<?> e = shardsJson.elements();
//    while(e.hasNext() && e.next() != null)
//      numSlices += 1;
//
//    return numSlices;
  }

  @GetInitialRestriction
  public OffsetRange getInitialRange(String element) throws IOException {
    /* The range is the number of slices which will default to tne number of shards in the target index.
     *
     * Maybe we can keep track of the number of records and shard count once we make the first query.
     * That all said, we're using a long running cursor for the processing of elements
     * and we want to ensure that the output doesn't create a hot bundle(window,key)
     * Each incoming query can be sent to a different worker.
     *
     */
    // There can be only one worker pulling data concurrently with Elasticsearch <5 so there is only one
    if (backendVersion < 5) {
      return new OffsetRange(0L, 1L);
    }
    return new OffsetRange(0L, numSlices);
  }

  @SplitRestriction
  public void splitRestriction(String element, OffsetRange restriction, Backlog backlog, OutputReceiver<OffsetRange> receiver) {
    // Statically split into a restriction representing a single shard and another for the rest.
    // If we output one restriction per shard and the amount of data is very small we could create
    // more connections than needed to Elasticsearch.

    // This allows the runner to split the restriction further if there are a lot of results.

    LOG.debug("splitRestriction({}, {})", restriction, backlog);

    LOG.debug("\t Outputting Restriction -> {}", new OffsetRange(restriction.getFrom(), restriction.getFrom() + 1));
    receiver.output(new OffsetRange(restriction.getFrom(), restriction.getFrom() + 1));

    if (restriction.getTo() - restriction.getFrom() > 1) {
      LOG.debug("\t Outputting Restriction -> {}", new OffsetRange(restriction.getFrom() + 1, restriction.getTo()));
      receiver.output(new OffsetRange(restriction.getFrom() + 1, restriction.getTo()));
    }
  }

  @ProcessElement
  public ProcessContinuation processElement(ProcessContext c, OffsetRangeTracker tracker) throws IOException {

    LOG.debug("Processing OffsetRangeTracker: {}", tracker);
    // Elasticsearch supports parallelism of requests based on a concept called slices.
    // Slicing can be done in parallel, scrolling must be done in serial for each slice.

    if(tracker.currentRestriction().getFrom() == tracker.currentRestriction().getTo()) {
      // This is a weird edge case because when the shards are < 4 we get called with
      // OffsetRangeTracker{range=[1, 1) which should never happen.
      // If we call tryClaim it seems to fix the state machine within the DirectRunner.
      // Need to identify if this will do something else
      if(tracker.tryClaim(tracker.currentRestriction().getFrom())) {
        LOG.debug("Tried claiming the first item and it succeeded....");
      }
      return ProcessContinuation.stop();
    }

    for (long sliceId = tracker.currentRestriction().getFrom();
         sliceId < tracker.currentRestriction().getTo();
         ++sliceId) {

      // Cleanup any existing scroll state.
      deleteScroll();

      if(!tracker.tryClaim(sliceId)) {
        LOG.debug("Failed to claim {}", sliceId);
        break;
      }
      LOG.debug("Successfully Claimed: {}", sliceId);

      String query = getPreparedQuery(c, sliceId);

      long i, outputCount = 0;
      do {
        JsonNode searchResult = this.executeQueryOrAdvance(query);

        i = outputResults(c, searchResult);

        outputCount += i;

        if(LOG.isDebugEnabled()) { // TODO: Look into Source Metrics
          int totalCount = searchResult.path("hits").path("total").asInt();
          LOG.debug("Element: {} Shard: {} Total: {}/{} Last Page: {} ", c.element(), sliceId, outputCount, totalCount, i);
        }

      } while (i > 0);

        /* TODO: Determine the best course of action here.
           Should we just blindly loop and see if the runner splits the restriction or return ProcessContinuation.resume()?
           The right answer is likely predicated on whether tryClaim should be called before or after work has been done.
         */
      LOG.debug("Completed outputting results of offset: {}", sliceId);
    }
    LOG.debug("Completed processing {}: {} Returning ProcessContinuation.stop()", c.element(), tracker);
    return ProcessContinuation.stop();
  }

  private void deleteScroll() {
    // remove the scroll
    if (scrollId == null)
      return;

    String requestBody = String.format("{\"scroll_id\" : [\"%s\"]}", scrollId);
    HttpEntity entity = new NStringEntity(requestBody, ContentType.APPLICATION_JSON);

    try {
      restClient.performRequest("DELETE", "/_search/scroll", Collections.emptyMap(), entity);
    } catch (IOException ignore) { }
    scrollId = null;
  }

  private void updateScrollId(JsonNode searchResult) {
    scrollId = searchResult.path("_scroll_id").asText();
  }

  private JsonNode executeSearchQuery(String query) throws IOException {

    String endPoint =
        String.format(
            "/%s/%s/_search",
            connectionConfiguration.getIndex(), connectionConfiguration.getType());

    Map<String, String> params = new HashMap<>();
    params.put("scroll", scrollKeepalive);
    params.put("size", String.valueOf(batchSize));
    if (backendVersion == 2 && shardPreference != null) {
      params.put("preference", "_shards:" + shardPreference);
    }

    LOG.debug("Executing: {}\n With Parameters: {}", query, params);
    HttpEntity queryEntity = new NStringEntity(query, ContentType.APPLICATION_JSON);
    Response response = restClient.performRequest("GET", endPoint, params, queryEntity);
    return parseResponse(response.getEntity());
  }

  private JsonNode advance() throws IOException {

    String requestBody =
        String.format("{\"scroll\" : \"%s\",\"scroll_id\" : \"%s\"}", scrollKeepalive, scrollId);
    HttpEntity scrollEntity = new NStringEntity(requestBody, ContentType.APPLICATION_JSON);
    Response response =
        restClient.performRequest("GET", "/_search/scroll", Collections.emptyMap(), scrollEntity);
    return parseResponse(response.getEntity());
  }

  private JsonNode executeQueryOrAdvance(String query) throws IOException {
    JsonNode result = scrollId == null ? executeSearchQuery(query) : advance();
    updateScrollId(result);
    return result;
  }

  private int outputResults(ProcessContext c, JsonNode searchResult) {
    JsonNode hits = searchResult.path("hits").path("hits");

    for (JsonNode hit : hits) {
      if (isWithMetadata) {
        c.output(hit.toString());
      } else {
        c.output(hit.path("_source").toString());
      }
    }
    updateScrollId(searchResult);
    return hits.size();
  }

  @Setup
  public void setup() throws IOException {
    restClient = connectionConfiguration.createClient();
    backendVersion = getBackendVersion(connectionConfiguration);

    // We can only use slices if the backend version is >=5
    if (backendVersion >= 5 && this.numSlices == null) {
      // BUG: When working with the same number of slices as shards some tests aren't succeeding.
      // Tests work when numSlices == 1
      // If numslices == 3 the third shard is never retrieved.
      // TODO: This is an artificial limit, should it exist?
      numSlices = Math.max(getShardCount(connectionConfiguration), 8);
    }
  }

  @Teardown
  public void teardown() throws IOException {
    deleteScroll();

    if (restClient != null) {
      restClient.close();
    }
  }


}

