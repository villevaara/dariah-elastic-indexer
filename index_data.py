from datetime import datetime
from elasticsearch import Elasticsearch, helpers
from glob import glob
import json
import ndjson
from datetime import datetime
from lib.utils import read_elastic_pwd


def reshape_data(data_entry):
    reshaped = dict()
    for k in data_entry.keys():
        k_has_digit = any(c.isdigit() for c in k)
        v = data_entry[k]
        if v == "":
            continue
        elif v is None:
            continue
        elif k_has_digit:
            continue
        elif k == "timestamp":
            timestamp = int(v) / 1000
            # v = datetime.fromtimestamp(timestamp)
            v = timestamp
        elif v.isdigit():
            v = int(v)
        elif v == 'False':
            v = False
        elif v == 'True':
            v = True
        reshaped[k] = v
    return reshaped


def read_ndjson(ndjson_file):
    retlist = list()
    with open(ndjson_file, 'r') as jsonfile:
        data = ndjson.load(jsonfile)
    for item in data:
        retlist.append(reshape_data(item))
    return retlist


# Password for the 'elastic' user generated by Elasticsearch
ELASTIC_PASSWORD = read_elastic_pwd("./secrets.txt")
# Create the client instance
client = Elasticsearch("https://nlf-es.rahtiapp.fi:443",
                       basic_auth=("elastic", ELASTIC_PASSWORD))

# test client
resp = client.info()


# create index
mapping = {
    "properties": {
        "action_type": {"type": "keyword"},
        "channel_id": {"type": "integer"},
        "author.name": {"type": "keyword"},
        "author.id": {"type": "keyword"},
        "author.display_name": {"type": "text"},
        "author.is_moderator": {"type": "boolean"},
        "author.is_subscriber": {"type": "boolean"},
        "author.is_turbo": {"type": "boolean"},
        "author.target_id": {"type": "integer"},
        "ban_duration": {"type": "keyword"},
        "ban_type": {"type": "keyword"},
        "banned_user": {"type": "keyword"},
        "client_nonce": {"type": "keyword"},
        "colour": {"type": "keyword"},
        "emote_only": {"type": "boolean"},
        "flags": {"type": "keyword"},
        "follower_only": {"type": "boolean"},
        "in_reply_to.author.display_name": {"type": "keyword"},
        "in_reply_to.author.id": {"type": "keyword"},
        "in_reply_to.author.name": {"type": "keyword"},
        "in_reply_to.message": {"type": "text"},
        "in_reply_to.message_id": {"type": "keyword"},
        "is_first_message": {"type": "boolean"},
        "message": {"type": "text"},
        "message_id": {"type": "keyword"},
        "message_type": {"type": "keyword"},
        "minutes_to_follow_before_chatting": {"type": "integer"},
        "r9k_mode": {"type": "boolean"},
        "rituals_enabled": {"type": "boolean"},
        "seconds_to_wait": {"type": "integer"},
        "slow_mode": {"type": "boolean"},
        "subscriber_only": {"type": "boolean"},
        "target_message_id": {"type": "keyword"},
        "timestamp": {"type": "date"},
        "user_type": {"type": "keyword"}
    }
}

index_name = "yle_eurheilu"
client.indices.create(index=index_name, mappings=mapping)

# read data
data_json = glob("./data/work/*.json")

# index single doc
# test_data = read_ndjson(data_json[0])
# doc = test_data[0]
# resp = client.index(index=index_name, id=1, document=doc)
# print(resp['result'])

# index bulk
# helpers.bulk(client, test_data, index=index_name)
for ndjson_file in data_json:
    inputdata = read_ndjson(ndjson_file)
    helpers.bulk(client, inputdata, index=index_name)

# # test search API
# resp = client.search(index="twitter", query={"match_all": {}})
# print("Got %d Hits:" % resp['hits']['total']['value'])
# for hit in resp['hits']['hits']:
#     print("%(author_name)s: %(message)s" % hit["_source"])

# TODO
# - hankalampi aineisto (Turku NLP, morfologisesti jäsennetty)
#   - custom analyzer, joka ottaa sisään valmiiksi pureskellun aineston
#   - indeksiin samalla offsetillä kuin itse sana
# - Kytkeminen airflowhun
# - testataan pärjääkö elasticsearchin omalla searchAPI:lla ja sen paljastamisella
# - datasettikohtainen shinyapp jos Kibana ei toimi?
#   - aineiston yleiskatselmukseen, ei tarkkaan analyysiin
# - kuinka tähän saa laajennuksia?

# - use https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-custom-analyzer.html
#   -- mapping character filter to add data to tokens?