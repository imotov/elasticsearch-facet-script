curl -XDELETE http://localhost:9200/test-idx
echo
curl -XPUT http://localhost:9200/test-idx -d '{
    "settings" : {
        "index" : {
            "number_of_shards" : 1,
            "number_of_replicas" : 0
        }
    },
    "mappings" : {
        "rec" : {
            "_source" : { "enabled" : false },
            "properties" : {
                "message": { "type": "string", "store": "yes"}
            }
        }
    }
}'
echo
curl -XPUT http://localhost:9200/test-idx/rec/1 -d '{
    "message": "Trying out elasticsearch, so far so good?"
}'
echo
curl -XPUT http://localhost:9200/test-idx/rec/2 -d '{
    "message": "You know, for Search"
}'
echo
curl -XPOST http://localhost:9200/test-idx/_refresh
echo
curl -XGET 'http://localhost:9200/test-idx/rec/_search?pretty=true&search_type=count' -d '{
    "query": {
        "query_string": {
            "query": "*:*"
        }
    },
    "facets": {
        "facet1": {
            "script": {
                "init_script" : "charfreq_init",
                "map_script": "charfreq_map",
                "reduce_script" : "charfreq_reduce",
                "params" : {
                    "facet" : [],
                    "field" : "message"
                }
            }
        }
    }
}
'