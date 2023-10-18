from smart_open import open
from elasticsearch import Elasticsearch, helpers
import json
from lib.utils import read_elastic_pwd, log_line, read_indexed_log
import argparse
import csv


def get_allas_url_ndjson(allas_url, add_id=True):
    with open(allas_url, encoding='utf-8') as rawfile:
        rawdata = rawfile.read()
        jsondata = json.loads(rawdata)
        docs = jsondata['documents']
        retdocs = list()
        for doc in docs:
            thisdoc = doc['fields']
            if add_id:
                thisdoc['_id'] = doc['fields']['url'].replace("https://", "").replace("http://", "").replace("/", "_").replace(":", "")
            retdocs.append(thisdoc)
    return retdocs


# assign new keys for fields to be remapped
def remap_inputdata(inputdata, remapping):
    remapped_data = dict()
    for k, v in inputdata.items():
        if k in remapping.keys():
            remapped_data[remapping[k]] = v
        else:
            remapped_data[k] = v
    return remapped_data

# 'version' field caused problems in indexing. Discard for now.
def fix_version_field(input_item):
    # input_item['version'] = "_" + "-".join(input_item['version'])
    del input_item['version']


# apply remapping field names and fix other problematic fields for bulk indexing
def remap_bulk_batch(items_batch, remappings, fix_version=True):
    remapped = list()
    for item in items_batch:
        new_item = remap_inputdata(item, remappings)
        if fix_version:
            fix_version_field(new_item)
        remapped.append(new_item)
    return remapped


# read filelist, append to ALLAS url. These are the data files on ALLAS.
allas_url = 'https://a3s.fi/'
with open('../dariah-elastic-data/legentic_files.txt', 'r') as txtfile:
    allas_items = [allas_url + item.strip() for item in txtfile.readlines()]

# read dictionary to remap field names. Some caused trouble in indexing.
# see legentic_indexer.py for how the remappings were done.
with open('data/legentic_remappings.json', 'r') as jsonfile:
    remappings = json.load(jsonfile)

# Password for the 'elastic' user generated by Elasticsearch
ELASTIC_PASSWORD = read_elastic_pwd("./secrets.txt")
# Create the client instance
# test env client
client = Elasticsearch("https://ds-es.rahtiapp.fi:443",
                       basic_auth=("elastic", ELASTIC_PASSWORD), request_timeout=60)
index_name = "legentic"

# Get arguments for start and end index

# parser = argparse.ArgumentParser(description='Optional: Input start and end iterations.')
# parser.add_argument('--start', type=int, help='First iter to process.')
# parser.add_argument('--end', type=int, help='First iter to not process.')
# parser.add_argument('--reindex', dest='reindex', action='store_true', help='Do not skip already indexed.')
# parser.add_argument('--no-reindex', dest='reindex', action='store_false', help='Skip indexed. Default.')
# parser.set_defaults(reindex=False)
#
# args = parser.parse_args()

testdata = get_allas_url_ndjson(allas_url=allas_items[0], add_id=True)

import time

def timing_test(dataset, subset_size, repetitions):
    res = list()
    for i in range(0, repetitions):
        inputdata = dataset[:subset_size]
        start_time = time.perf_counter()
        input_remapped = remap_bulk_batch(items_batch=inputdata, remappings=remappings, fix_version=True)
        helpers.bulk(client, input_remapped, index=index_name)
        res.append(time.perf_counter() - start_time)
    return {
        'avg_elapsed': round(sum(res) / repetitions, 1),
        'repetitions': repetitions,
        'bulk_items': subset_size}


testset = list(range(100, 20100, 100))
testout = 'test_results/legentic_bulk_size_test.csv'
fieldnames = ['avg_elapsed', 'repetitions', 'bulk_items']
reps = 100

with open(testout, 'w') as csvf:
    writer = csv.DictWriter(csvf, fieldnames)
    writer.writeheader()

for size in testset:
    print("Testing size: " + str(size) + " repetitions: " + str(reps))
    res = timing_test(testdata, size, repetitions=reps)
    print("  > " + str(res['avg_elapsed']) + "s")
    with open(testout, 'a') as csvf:
        writer = csv.DictWriter(csvf, fieldnames)
        writer.writerow(res)


