/*
 * Licensed to Luca Cavanna (the "Author") under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.river.solr.support;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.TimeZone;

public class SolrIndexer {

    private final CloseableHttpClient httpClient;
    private final String solrUrl;

    private final ObjectMapper objectMapper;

    public SolrIndexer(String solrUrl) {
        this.httpClient = HttpClients.createDefault();
        this.solrUrl = solrUrl + "/update";
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        objectMapper.setDateFormat(dateFormat);
    }

    public void indexDocuments(Map<String, Map<String, Object>> documents) throws IOException {
        StringBuilder jsonBuilder = new StringBuilder("[");
        int i=0;
        for (Map<String, Object> doc : documents.values()) {
            jsonBuilder.append(objectMapper.writeValueAsString(doc));

            if (i < documents.values().size() - 1) {
                jsonBuilder.append(",");
            }
            i++;
        }
        jsonBuilder.append("]");

        HttpPost httpPost = new HttpPost(solrUrl + "?commit=true");
        httpPost.setHeader("Content-type", "application/json");
        httpPost.setEntity(new StringEntity(jsonBuilder.toString()));

        CloseableHttpResponse response = httpClient.execute(httpPost);
        try {
            EntityUtils.consume(response.getEntity());
            if (response.getStatusLine().getStatusCode() != 200) {
                throw new RuntimeException("documents were not properly indexed");
            }
        } finally {
            EntityUtils.consume(response.getEntity());
            response.close();
        }
    }

    public void clearDocuments() throws IOException {
        HttpPost httpPost = new HttpPost(solrUrl + "?commit=true");
        httpPost.setHeader("Content-type", "application/xml");
        httpPost.setEntity(new StringEntity("<delete><query>*:*</query></delete>"));

        CloseableHttpResponse response = httpClient.execute(httpPost);
        try {
            if (response.getStatusLine().getStatusCode() != 200) {
                throw new RuntimeException("documents were not properly cleared");
            }
        } finally {
            EntityUtils.consume(response.getEntity());
            response.close();
        }
    }
}
