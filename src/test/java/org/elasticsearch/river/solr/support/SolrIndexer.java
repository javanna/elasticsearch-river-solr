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

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.Map;

public class SolrIndexer {

    private final SolrServer solrServer;

    public SolrIndexer(SolrServer solrServer) {
        this.solrServer = solrServer;
    }

    public void indexDocuments(Map<String, Iterable<Field>> documents) throws IOException, SolrServerException {
        for (Iterable<Field> fields : documents.values()) {
            solrServer.add(buildSolrInputDocument(fields));
        }
        solrServer.commit();
    }

    private SolrInputDocument buildSolrInputDocument(Iterable<Field> fields) {
        SolrInputDocument solrInputDocument = new SolrInputDocument();
        for (Field<? extends Object> field : fields) {
            solrInputDocument.setField(field.getName(), field.getValue());
        }
        return solrInputDocument;
    }

    public void clearDocuments() throws IOException, SolrServerException {
        solrServer.deleteByQuery("*:*");
        solrServer.commit();
    }
}
