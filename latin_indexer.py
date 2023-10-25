import pandas as pd
import re
from smart_open import open
from elasticsearch import Elasticsearch, helpers
from glob import glob
from lib.utils import read_elastic_pwd
import math
from tqdm import tqdm
import ndjson
import hashlib
import argparse


def read_id_text(txt_path, doc_id):
    txt_f = txt_path + doc_id.split('.')[0] + ".txt"
    with open(txt_f, 'r') as f:
        text = f.read()
    return text


def read_ndjson(ndjson_file):
    with open(ndjson_file, 'r') as jsonfile:
        data = ndjson.load(jsonfile)
    return data


def filter_tr_data_fields(tr_data):
    for item in tr_data:
        del item['text1_source']
        del item['text2_source']


def get_tr_id(tr_item):
    hashstr = (
            tr_item['text1_id'].split('.')[0] +
            "-" +
            tr_item['text2_id'].split('.')[0] +
            "-" +
            str(tr_item['text1_text_start']) +
            "-" +
            str(tr_item['text1_text_end']) +
            "-" +
            str(tr_item['text2_text_start']) +
            "-" +
            str(tr_item['text2_text_end'])
    )
    hash_object = hashlib.sha256(hashstr.encode('utf-8'))
    hex_dig = hash_object.hexdigest()
    return hex_dig


def assign_tr_id(tr_data):
    for item in tr_data:
        item['_id'] = get_tr_id(item)


parser = argparse.ArgumentParser(description='Optional: Input path.')
parser.add_argument('--input', type=str, help='Input path.')

args = parser.parse_args()

if args.input is None:
    inputpath = '../dariah-elastic-data/latin_textreuse'
else:
    inputpath = args.input

data_df = pd.read_csv(inputpath + '/metadata.csv')
tr_datafiles = glob(inputpath + '/reuses_jsonl/*.jsonl')
txt_path = inputpath + '/raw_txt/'

reasonably_populated_columns = list()
for col in data_df.columns:
    if not data_df[col].isna().sum() >= len(data_df) -10:
        reasonably_populated_columns.append(col)

reasonable_df = data_df[reasonably_populated_columns].copy()
colnames = list(reasonable_df.columns)
repdict = dict()
for colname in colnames:
    repdict[colname] = re.sub(r'([A-Z]{1})', r'_\1', colname).lower().replace('@', '').replace('#', '')

reasonable_df.rename(columns=repdict, inplace=True)
reasonable_df = reasonable_df.where((pd.notnull(reasonable_df)), None)
reasonable_dict = reasonable_df.to_dict('records')


xml_prefix = "https://a3s.fi/latin-tr/"
for i in reasonable_dict:
    i['xml_url'] = xml_prefix + i['doc_id']
    i['txt'] = read_id_text(txt_path, i['doc_id'])
    if math.isnan(i['publication_stmt_date']):
        i['publication_stmt_date'] = None
    else:
        i['publication_stmt_date'] = str(int(i['publication_stmt_date']))
    i['_id'] = i['doc_id'].split('.')[0]


mapping = {
    "properties": {
        'doc_id': {"type": "keyword"},
        'edition_stmt_edition': {"type": "keyword"},
        'edition_stmt_editor': {"type": "keyword"},
        'notes_stmt_note': {"type": "text"},
        'publication_stmt_date': {"type": "date", "format": "yyyy"},
        'publication_stmt_edition': {"type": "keyword"},
        'publication_stmt_pub_place': {"type": "keyword"},
        'publication_stmt_publisher': {"type": "keyword"},
        'series_stmt_idno': {"type": "keyword"},
        'series_stmt_title': {"type": "keyword"},
        'source_desc_p': {"type": "keyword"},
        'title_stmt_author_text': {"type": "text"},
        'title_stmt_author_ref': {"type": "keyword"},
        'title_stmt_author_date': {"type": "text"},
        'title_stmt_title': {"type": "text"},
        'xml_url': {"type": 'keyword'},
        'txt': {"type": 'text'},
    }
}


ELASTIC_PASSWORD = read_elastic_pwd("./secrets.txt")
# Create the client instance
client = Elasticsearch("https://ds-es.rahtiapp.fi:443",
                       basic_auth=("elastic", ELASTIC_PASSWORD), request_timeout=60)

# index_settings = {
#     'number_of_shards': 2
# }
index_name = "latin-textreuse-text"

# deleting an index
# client.indices.delete(index=index_name)

# create the index if it doesn't exist
# client.indices.create(index=index_name, mappings=mapping)

print("Indexing texts.")
helpers.bulk(client, reasonable_dict, index=index_name)
# for item in tqdm(reasonable_dict):
#     client.index(index=index_name, id=(item["doc_id"]), document=item)

del reasonable_dict
del reasonable_df

# Index text reuse data. These take longer.
tr_mapping = {
    "properties": {
        'text1_id': {"type": "keyword"},
        'text1_text_start': {"type": "integer"},
        'text1_text_end': {"type": "integer"},
        'text2_id': {"type": "keyword"},
        'text2_text_start': {"type": "integer"},
        'text2_text_end': {"type": "integer"},
        'align_length': {'type': 'integer'},
        'positives_percent': {'type': 'float'}
    }
}

index_settings = {
    'number_of_shards': 10
}

tr_index_name = "latin-textreuse-reuses"

# deleting an index
# client.indices.delete(index=tr_index_name)

# client.indices.create(index=tr_index_name, mappings=tr_mapping, settings=index_settings)

file_i = 1
for ndjsonfile in tr_datafiles:
    print(ndjsonfile + " " + str(file_i) + "/" + str(len(tr_datafiles)))
    this_data = read_ndjson(ndjsonfile)
    filter_tr_data_fields(this_data)
    assign_tr_id(this_data)
    # Break the list into chunks using list comprehension to allow bulk indexing.
    n = 50000
    this_data_chunks = [this_data[i:i + n] for i in range(0, len(this_data), n)]
    for chunk in tqdm(this_data_chunks):
        helpers.bulk(client, chunk, index=tr_index_name)
    file_i += 1

# for item in tqdm(reasonable_dict):
#     client.index(index=index_name, id=(item["doc_id"]), document=item)

