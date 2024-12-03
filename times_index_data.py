from glob import glob
from index_times.read_times_xml import get_xml_file_articles
import argparse
from lib.utils import read_elastic_pwd, log_line, read_indexed_log
from elasticsearch import Elasticsearch, helpers
import os
from tqdm import tqdm
import hashlib


def get_item_id(item):
    hashstr = item['article_id']
    hash_object = hashlib.sha256(hashstr.encode('utf-8'))
    hex_dig = hash_object.hexdigest()
    return hex_dig


def assign_ids(articles_data):
    for item in articles_data:
        item['_id'] = get_item_id(item)


def write_bulk(client, index_name, xml_file, logfile, verbose=False):
    items = get_xml_file_articles(xml_file)
    if items is None:
        print("Skipping: " + xml_file.split('/')[-1])
        log_line(logfile=logfile, line=xml_file.split('/')[-1])
        return None
    assign_ids(items)
    helpers.bulk(client, items, index=index_name)
    log_line(logfile=logfile, line=xml_file.split('/')[-1])
    if verbose:
        print("Wrote {} items to {}".format(len(items), index_name))


# Get arguments for start and end index
parser = argparse.ArgumentParser(description='Optional: Input start and end iterations.')
parser.add_argument('--datapath', type=str, help='Path to datafiles.')
parser.add_argument('--reindex', dest='reindex', action='store_true', help='Do not skip already indexed.')
parser.add_argument('--no-reindex', dest='reindex', action='store_false', help='Skip indexed. Default.')
parser.add_argument('--tqdm', dest='tqdm', action='store_true', help='Use tqdm for prgress bar.')
parser.add_argument('--no-tqdm', dest='tqdm', action='store_false', help='Do not use tqdm. Default.')
parser.set_defaults(reindex=False)
parser.set_defaults(tqdm=False)
parser.set_defaults(datapath='../dariah-elastic-data/times/tda/TDAO0001/TDAO0001-C00000/Newspapers/0FFO/')


args = parser.parse_args()

ELASTIC_PASSWORD = read_elastic_pwd("./secrets.txt")
client = Elasticsearch("https://dariahfi-es.2.rahtiapp.fi:443",
                       basic_auth=("elastic", ELASTIC_PASSWORD), request_timeout=60)
index_name = "bl-times"
verbose_logging = True

logfile = "logs/times_indexer.log"
if os.path.exists(logfile):
    processed = read_indexed_log(logfile, convert_to_int=False)
    print("Logfile found at: " + logfile)
    print("Log length: " + str(len(processed)))
else:
    print("No prior log found at: " + logfile)
    processed = list()

# fileroot = '../../dariah-elastic-data/times/tda/TDAO0001/TDAO0001-C00000/Newspapers/0FFO/'
print("Indexing data at: " + args.datapath)
all_files = glob(args.datapath + '/*.xml')
print("Found " + str(len(all_files)) + " .xml files.")

if args.tqdm:
    all_files = tqdm(all_files)

for xml_file in all_files:
    if not args.reindex:
        if xml_file.split('/')[-1] in processed:
            continue
    write_bulk(client, index_name, xml_file, logfile, verbose=False) # log xml filename
    if verbose_logging:
        print("indexed: " + xml_file)
