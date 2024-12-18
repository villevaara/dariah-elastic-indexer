from smart_open import open
from elasticsearch import Elasticsearch, helpers
import json
from lib.utils import read_elastic_pwd, log_line, read_indexed_log, get_id_from_str
import argparse
from threading import Thread
from threading import Lock
import queue


def get_allas_url_ndjson(allas_url, add_id=True):
    with open(allas_url, encoding='utf-8') as rawfile:
        rawdata = rawfile.read()
        jsondata = json.loads(rawdata)
        docs = jsondata['documents']
        retdocs = list()
        for doc in docs:
            thisdoc = doc['fields']
            if add_id:
                thisdoc['_id'] = get_id_from_str(
                    doc['fields']['url'].replace("https://", "").replace("http://", "").replace("/", "_").replace(":", ""))
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
client = Elasticsearch("https://dariahfi-es.2.rahtiapp.fi:443",
                       basic_auth=("elastic", ELASTIC_PASSWORD), request_timeout=60)
index_name = "legentic"

mapping = {
    "properties": {
        'indexed': {"type": "date", "format": "yyyy-MM-dd HH:mm:ss Z"},
        'published': {"type": "date", "format": "yyyy-MM-dd HH:mm:ss Z"},
        'author.name': {"type": "text"},
        'author.text.hashtag': {"type": "keyword"},
        'author.text.user_mention': {"type": "keyword"},
        'author.text.content': {"type": "keyword"},
        'author.content': {"type": "keyword"},
        'author-uri': {"type": "keyword"},
        'author-community_facebook': {"type": "keyword"},
        'author-id_facebook': {"type": "keyword"},
        'author-following': {"type": "integer"},
        'author-followers': {"type": "integer"},
        'blog_id': {"type": "keyword"},
        'citation.length': {"type": "integer"},
        'citation.content': {"type": "keyword"},
        'description.content': {"type": "text"},
        'facebook_id': {"type": "keyword"},
        'forum_post_id': {"type": "keyword"},
        'google-id': {"type": "keyword"},
        'instagram-author-id': {"type": "keyword"},
        'instagram-ref-author-id': {"type": "keyword"},
        'language': {"type": "keyword"},
        'latency': {"type": "long"},
        'latitude': {"type": "float"},
        'longitude': {"type": "float"},
        'name.content': {"type": "text"},
        'page-title.content': {"type": "text"},
        'quote.content': {"type": "text"},
        'type': {"type": "keyword"},
        'url': {"type": "keyword"},
        'ref-author': {"type": "text"},
        'ref-author-id_facebook': {"type": "keyword"},
        'ref-author-uri': {"type": "keyword"},
        'subject.content': {"type": "keyword"},
        'subject.length': {"type": "integer"},
        'text.address.size': {"type": "integer"},
        'text.content': {"type": "text"},
        'text.email.size': {"type": "integer"},
        'text.hashtag': {"type": "keyword"},
        'text.person.content': {"type": "keyword"},
        'text.person.size': {"type": "integer"},
        'text.phone.canonized': {"type": "keyword"},
        'text.phone.content': {"type": "text"},
        'text.url.content': {"type": "text"},
        'text.url.length': {"type": "integer"},
        'text.user_mention': {"type": "keyword"},
        'thread.title.content': {"type": "text"},
        'twitter_retweet_id': {"type": "keyword"},
        'twitter_tweet_id': {"type": "keyword"},
        'youtube-channel-id': {"type": "keyword"},
        # 'version': {"type": "keyword"},
    }
}

index_settings = {
    'number_of_shards': 10,
    'codec': 'best_compression'
}


logf_path = "logs/legentic_indexed.log"
already_indexed = read_indexed_log(logf_path)

# Get arguments for start and end index
parser = argparse.ArgumentParser(description='Optional: Input start and end iterations.')
parser.add_argument('--start', type=int, help='First iter to process. Optional.')
parser.add_argument('--end', type=int, help='First iter to not process. Optional.')
parser.add_argument('--reindex', dest='reindex', action='store_true', help='Do not skip already indexed.')
parser.add_argument('--no-reindex', dest='reindex', action='store_false', help='Skip indexed. Default.')
parser.add_argument('--threads', type=int, help='Number of threads to use. Default 1.', default=1)
parser.set_defaults(reindex=False)

args = parser.parse_args()

reindex = args.reindex
allas_subset = allas_items[args.start:args.end]
done_i = 0
total_i = len(allas_subset)

lock = Lock()


def process_queue_item(this_q, client, index_name, remappings, lock, already_indexed, reindex):
    while running:
        try:
            this_item = this_q.get()
            if this_item not in already_indexed or reindex:
                inputdata = get_allas_url_ndjson(allas_url=this_item, add_id=True)
                input_remapped = remap_bulk_batch(items_batch=inputdata, remappings=remappings, fix_version=True)
                helpers.bulk(client, input_remapped, index=index_name)
                printline = (this_item + " - done.")
                with lock:
                    log_line(logfile=logf_path, line=this_item)
            else:
                printline = ("!! Skipping " + this_item + " - already indexed.")
            with lock:
                global done_i
                done_i += 1
                left = total_i - done_i
                print(printline + " done: " + str(done_i) + " left: " +str(left))
            this_q.task_done()
        except queue.Empty:
            pass


# create workers and threads
num_threads = args.threads
this_q = queue.Queue(maxsize=0)
running = True

for i in range(num_threads):
    worker = Thread(target=process_queue_item, args=(
        this_q, client, index_name, remappings, lock, already_indexed, reindex))
    worker.daemon = True
    worker.start()

for item in allas_subset:
    this_q.put(item)

this_q.join()
running = False
