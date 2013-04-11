Solr River Plugin for ElasticSearch [![Build Status](https://buildhive.cloudbees.com/job/javanna/job/elasticsearch-river-solr/badge/icon)](https://buildhive.cloudbees.com/job/javanna/job/elasticsearch-river-solr/) [![Build Status](https://travis-ci.org/javanna/elasticsearch-river-solr.png)](https://travis-ci.org/javanna/elasticsearch-river-solr) [![Build Status](https://drone.io/github.com/javanna/elasticsearch-river-solr/status.png)](https://drone.io/github.com/javanna/elasticsearch-river-solr/latest)
==================================

The Solr River plugin allows to import data from [Apache Solr](http://lucene.apache.org/solr) to [elasticsearch](http://www.elasticsearch.org).

In order to install the latest version of the plugin, simply run: `bin/plugin install river-solr -url http://bit.ly/12rmrSN`.
You can copy paste the url of a specific version from the table below, depending on the elasticsearch version you're running.


Versions
--------

<table>
	<thead>
		<tr>
			<td>Solr River Plugin</td>
			<td>ElasticSearch</td>
		</tr>
	</thead>
	<tbody>
	   <tr>
        	<td>master</td>
            <td>0.90.0</td>
        </tr>
	    <tr>
    	    <td><a href="http://bit.ly/12rmrSN">1.0.3</a></td>
    		<td>0.20.0 -> 0.20.6</td>
        </tr>
		<tr>
			<td><a href="http://bit.ly/15cCMIB">1.0.2</a></td>
			<td>0.20.0 -> 0.20.6</td>
		</tr>
		<tr>
            <td><a href="http://bit.ly/Yo61UW">1.0.1</a></td>
            <td>0.19.3 -> 0.19.12</td>
        </tr>
		<tr>
            <td><a href="http://bit.ly/XUl2LZ">1.0.0</a></td>
        	<td>0.19.3 -> 0.19.12</td>
        </tr>
	</tbody>
</table>


You might be able to use the river with older versions of elasticsearch, but the tests included with the project run successfully only with version 0.19.3 or higher, the first version using Lucene 3.6.

Getting Started
===============

The Solr River allows to query a running Solr instance and index the returned documents in elasticsearch.
It uses the [Solrj](http://wiki.apache.org/solr/Solrj) library to communicate with Solr.

It's recommended that the solrj version used is the same as the solr version installed on the server that the river is querying.
The Solrj version in use and distributed with the plugin is 3.6.1. Anyway, it's possible to query other Solr versions.
The default format used is in fact [javabin](http://wiki.apache.org/solr/javabin) but you can solve compatibility issues just switching to the xml format using the wt parameter.

All the [common query parameters](http://wiki.apache.org/solr/CommonQueryParameters) are supported.

The solr river is not meant to keep solr and elasticsearch in sync, that's why it automatically deletes itself on completion, so that the river doesn't start up again at every node restart.
This is the default behaviour, which can be disabled through the close_on_completion parameter.


Installation
------------

Here is how you can easily create the river and index data from Solr, just providing the solr url and the query to execute:

```sh
curl -XPUT localhost:9200/_river/solr_river/_meta -d '
{
    "type" : "solr",
    "solr" : {
        "url" : "http://localhost:8080/solr/",
        "q" : "*:*"
    }
}'
```

All supported parameters are optional. The following example request contains all the parameters that are supported together with the corresponding default values applied when not present.

```javascript
{
    "type" : "solr",
    "close_on_completion" : "true",
    "solr" : {
        "url" : "http://localhost:8983/solr/",
        "q" : "*:*",
        "fq" : "",
        "fl" : "",
        "wt" : "javabin",
        "qt" : "",
        "uniqueKey" : "id",
        "rows" : 10
    },
    "index" : {
        "index" : "solr",
        "type" : "import",
        "bulk_size" : 100,
        "max_concurrent_bulk" : 10,
        "mapping" : "",
        "settings": ""
    }
}
```

The fq and fl parameters can be provided as either an array or a single value.

You can provide your own mapping while creating the river, as well as the index settings, which will be used when creating the new index if needed.

The index is created when not already existing, otherwise the documents are added to the existing one with the configured name.

The documents are indexed using the [bulk api](http://www.elasticsearch.org/guide/reference/java-api/bulk.html).
You can control the size of each bulk (default 100) and the maximum number of concurrent bulk operations (default is 10).
Once the limit is reached the indexing will slow down, waiting for one of the bulk operations to finish its work; no documents will be lost.

Transform documents
------------
Since version 1.0.3 it's possible to transform the documents via scripting. The feature works exactly as the [update api](http://www.elasticsearch.org/guide/reference/api/update.html). The needed parameters can be specified within the transform section while registering the river, like this:

```javascript
{
    "type" : "solr",
    "solr" : {
        "url" : "http://localhost:8983/solr/",
        "q" : "*:*",
    },
    "index" : {
        "index" : "solr",
        "type" : "import",
    },
    "transform" : {
        "script" : "ctx._source.counter += count",
        "params" : {
            "count" : 4
        }
    }
}
```

The example above increments by 4 the content of the counter field for every document right before the indexing process in elasticsearch.

Limitations
------------

* only stored fields can be retrieved from Solr, therefore indexed in elasticsearch
* the river is not meant to keep elasticsearch in sync with Solr, but only to import data once. It's possible to register the river multiple times in order to import different sets of documents though, even from different solr instances.
* it's recommended to create the [mapping](http://www.elasticsearch.org/guide/reference/mapping/index.html) given the existing solr schema in order to apply the correct text analysis while importing the documents. In the future there might be an option to auto generating it from the Solr schema.

License
=======

```
This software is licensed under the Apache 2 license, quoted below.

Copyright 2012 Luca Cavanna

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
```
