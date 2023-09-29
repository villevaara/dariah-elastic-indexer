from datetime import datetime
from elasticsearch import Elasticsearch, helpers
from glob import glob
import json
import ndjson
from datetime import datetime
from tqdm import tqdm
from lib.utils import read_elastic_pwd


def read_ndjson(ndjson_file):
    with open(ndjson_file, 'r') as jsonfile:
        data = ndjson.load(jsonfile)
    return data


def add_bulk_values(inputdata):
    for item in inputdata:
        item['_id'] = item['file_id'].replace("_", "-").replace(".", "-")
    return inputdata


# Password for the 'elastic' user generated by Elasticsearch
ELASTIC_PASSWORD = read_elastic_pwd("./secrets.txt")
# Create the client instance
client = Elasticsearch("https://ds-es.rahtiapp.fi:443",
                       basic_auth=("elastic", ELASTIC_PASSWORD), request_timeout=60)


mapping = {
    "properties": {
        'estc_id': {"type": "keyword"},
        'eebo_id': {"type": "keyword"},
        'file_id': {"type": "keyword"},
        'language': {"type": "keyword"},
        'title': {"type": "text"},
        'pagecount': {"type": "integer"},
        'subject_topic': {"type": "keyword"},
        'publisher': {"type": "text"},
        'publication_place': {"type": "keyword"},
        'author': {"type": "keyword"},
        'publication_year': {"type": "date", "format": "yyyy"},
        'document_type': {"type": "keyword"},
        'content': {"type": "text"},
        'part_type': {"type": "keyword"},
  }
}


index_name = "eebo"
# client.indices.create(index=index_name, mappings=mapping)

# read data
data_json = glob("../dariah-elastic-data/work/eebo/*.ndjson")

# index doc range - this gives more detailed error messages than the bulk API
# test_data = read_ndjson(data_json[0])
# for i in range(0, 100):
# doc = inputdata[0]
# resp = client.index(index=index_name, id=doc["_id"], document=doc)
#     print(str(i) + " " + resp['result'])


# index bulk
for ndjson_file in tqdm(data_json):
    # print(ndjson_file)
    inputdata = read_ndjson(ndjson_file)
    inputdata = add_bulk_values(inputdata)
    helpers.bulk(client, inputdata, index=index_name)
