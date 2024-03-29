from smart_open import open
from elasticsearch import Elasticsearch, helpers
import json
from lib.utils import read_elastic_pwd


def get_query_from_file(f):
    with open(f, 'r') as reader:
        return json.loads(reader.read())


def write_json_output(data, outfile):
    with open(outfile, 'w') as jsonf:
        json.dump(data, jsonf, ensure_ascii=False)


def write_paginated_results(paginated_generator, outputpath, item_count=1000):
    outputdata = list()
    iter = 0
    for item in paginated_generator:
        outputdata.append(item)
        if len(outputdata) == item_count:
            write_json_output(outputdata, outputpath + "/es_results_" + str(iter) + ".json")
            iter += 1
            outputdata = list()
    if len(outputdata) > 0:
        write_json_output(outputdata, outputpath + "/es_results_" + str(iter) + ".json")


ELASTIC_PASSWORD = read_elastic_pwd("./secrets.txt")
# Create the client instance
# test env client
# client = Elasticsearch("https://ds-es.rahtiapp.fi:443",
#                        basic_auth=("elastic", ELASTIC_PASSWORD), request_timeout=60)

client = Elasticsearch("https://ds-es.rahtiapp.fi:443",
                       basic_auth=("legentic_viewer", "leg-view"), request_timeout=60)

index_name = "legentic"
query_f = 'test_code/testquery.json'

# Load the query saved in Kibana.
q_json = get_query_from_file(query_f)

# Query the Elasticsearch search API with the above query.
try:
    search_res = client.search(index=index_name, body=q_json, request_timeout=120)
except Exception as error:
    print("Elasticsearch Client Error:", error)

# Results. The default query only returns 500 hits, see below:
results = search_res['hits']['hits']

# To retrieve more than the number of results specified by the "size" key in the search query, you will need to:
# 1. Increase the size parameter. Maximum value here is 10000. To do this, modify the size -parameter in the json file,
#    or remove it and add a relevant parameter to the wrapper search request. E.g.:

# Query with size as parameter.
q_json = get_query_from_file('test_code/testquery-s10000.json')
try:
    search_res = client.search(index=index_name, body=q_json, request_timeout=120, size=10000)
except Exception as error:
    print("Elasticsearch Client Error:", error)

# 2. If there are over 10000 records in the you will need to paginate the results. See:
# https://www.elastic.co/guide/en/elasticsearch/reference/current/paginate-search-results.html
# The paginated results need two parameters specified: Sorting, and from which index to retrieve results.
# Alternatively, just use the scan -helper in the Python wrapper. The query json needs small modifications again:
# remove the "size", "track_total_hits", and "sort" -keys.

q_json = get_query_from_file('test_code/testquery-paginated.json')

try:
    scan_search_res = helpers.scan(client,
    body=q_json,
    index=index_name,
    request_timeout=120)
except Exception as error:
    print("Elasticsearch Client Error:", error)

write_paginated_results(paginated_generator=scan_search_res, outputpath='test_code/testres', item_count=1000)
