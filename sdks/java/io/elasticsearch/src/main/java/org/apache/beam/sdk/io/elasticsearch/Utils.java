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

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.apache.beam.vendor.guava.v20_0.com.google.common.annotations.VisibleForTesting;
import org.apache.http.HttpEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

class Utils {
  private static final ObjectMapper mapper = new ObjectMapper();

  @VisibleForTesting
  static JsonNode parseResponse(HttpEntity responseEntity) throws IOException {
    return mapper.readValue(responseEntity.getContent(), JsonNode.class);
  }

  static int getBackendVersion(ElasticsearchIO.ConnectionConfiguration connectionConfiguration) {
    try (RestClient restClient = connectionConfiguration.createClient()) {
      Response response = restClient.performRequest("GET", "");
      JsonNode jsonNode = parseResponse(response.getEntity());
      int backendVersion =
          Integer.parseInt(jsonNode.path("version").path("number").asText().substring(0, 1));
      checkArgument(
          (backendVersion == 2 || backendVersion == 5 || backendVersion == 6),
          "The Elasticsearch version to connect to is %s.x. "
              + "This version of the ElasticsearchIO is only compatible with "
              + "Elasticsearch v6.x, v5.x and v2.x",
          backendVersion);
      return backendVersion;

    } catch (IOException e) {
      throw new IllegalArgumentException("Cannot get Elasticsearch version", e);
    }
  }
}
