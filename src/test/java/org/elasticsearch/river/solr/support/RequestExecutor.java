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

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestExecutor {

    private static final Logger logger = LoggerFactory.getLogger(RequestExecutor.class);

    private static final long DEFAULT_MAX_WAIT_TIME = 5000;

    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder> Response
        tryExecute(ActionRequestBuilder<Request, Response, RequestBuilder> actionRequestBuilder,
               CheckResponseCallback<Response> checkResponseCallback) {
        return tryExecute(actionRequestBuilder, DEFAULT_MAX_WAIT_TIME, checkResponseCallback);
    }

    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder> Response
            tryExecute(ActionRequestBuilder<Request, Response, RequestBuilder> actionRequestBuilder,
                    long maxWaitTime,
                    CheckResponseCallback<Response> checkResponseCallback) {

        int retries = 0;
        long start = System.currentTimeMillis();
        while(true) {
            try {
                logger.debug("Sending {} request [retry {}]", actionRequestBuilder.request().getClass().getSimpleName(), retries);
                Response response = actionRequestBuilder.execute().actionGet();
                long waitTime = System.currentTimeMillis() - start;
                retries++;
                logger.debug("Checking response  [retry {}, waitTime {}]", retries, waitTime);
                if (checkResponseCallback.checkResponse(response)
                        || waitTime>maxWaitTime) {
                    return response;
                }
            } catch(Exception e) {
                logger.debug("Error while checking the response [retry {}]", retries, e);
            }

            try {
                //we wait 200 ms between a retry and the next one
                Thread.sleep(200);
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }
}
