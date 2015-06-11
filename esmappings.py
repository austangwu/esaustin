from elasticsearch import Elasticsearch

es = Elasticsearch()

#build json with "headingname" : {"type": "string", "index" : "not_analyzed"}
headings = open("headings.txt", "r").read().splitlines()
mappings = {};

#create new index with headings
if es.indices.exists(index = "doctors"):
    print("Index already exists")
else:
    for heading in headings:
        mappings[heading] = { "type" : "string", "index": "analyzed"}
    es.indices.create(index = "doctors", body = {
        "mappings":{
            "type1" : {
                "_source" : { "enabled" : "true" },
                "properties" : mappings
            }
        }
    })






