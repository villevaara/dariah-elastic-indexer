import fnmatch
import math

from lxml import etree
from smart_open import open
import zipfile
import json
import argparse
import re
from lib.utils import read_elastic_pwd, log_line, read_indexed_log
from elasticsearch import Elasticsearch, helpers
import os
from tqdm import tqdm


def strip_ns_prefix(tree):
    #xpath query for selecting all element nodes in namespace
    query = "descendant-or-self::*[namespace-uri()!='']"
    #for each element returned by the above xpath query...
    for element in tree.xpath(query):
        #replace element name with its local name
        element.tag = etree.QName(element).localname
    return tree


def get_block_text(block):
    block_text = list()
    for item in block.findall(".//String"):
        if 'SUBS_TYPE' in item.attrib.keys():
            if item.attrib['SUBS_TYPE'] == 'HypPart1':
                block_text.append(item.attrib['SUBS_CONTENT'])
            else:
                continue
        else:
            block_text.append(item.attrib['CONTENT'])
    return " ".join(block_text)


def get_issue_text_from_strings(files_read):
    page_texts = list()
    for page_xml_s in files_read:
        root = strip_ns_prefix(etree.fromstring(page_xml_s))
        page = root.find(".//Page")
        if page is not None:
            text_blocks = page.findall(".//TextBlock")
            page_text = list()
            for text_block in text_blocks:
                page_text.append(get_block_text(text_block))
            page_text_s = "\n\n".join(page_text)
        else:
            page_text_s = ""
        page_texts.append(page_text_s)
        page_texts_f = [text for text in page_texts if text != '']
    return "\n\n\n".join(page_texts_f)


def get_id_text_from_archive(item_id, archive, sorted_names):
    files = []
    for name in sorted_names:
        if fnmatch.fnmatch(name, '*/' + item_id + '/alto/*.xml'):
            files.append(archive.read(name))
    if len(files) == 0:
        print("No xml files for item_id: {}".format(item_id))
        return ""
    this_text = get_issue_text_from_strings(files)
    return this_text


def prepare_metadata(metadata, add_id=True):
    new_meta = list()
    for item in metadata:
        new_item = dict()
        for k, v in item.items():
            if k in ['authors', 'terms', 'score']:
                continue
            new_key = re.sub(r'([A-Z])', r'_\1', k).lower()
            new_item[new_key] = v
        new_item['binding_id'] = str(new_item['binding_id'])
        new_item['publication_date'] = new_item['date']
        del new_item['date']
        if add_id:
            new_item['_id'] = new_item['binding_id']
        new_meta.append(new_item)
    return new_meta


def write_bulk(client, items, index_name, logfile, verbose=False):
    helpers.bulk(client, items, index=index_name)
    processed_ids = "\n".join([str(item['binding_id']) for item in items])
    log_line(logfile=logfile, line=processed_ids)
    if verbose:
        print("Wrote {} items to {}".format(len(items), index_name))


def get_metadata_subset(metadata, id_prefix):
    subset = [item for item in metadata if item['binding_id'][:len(id_prefix)] == id_prefix]
    return subset


# Get arguments for start and end index
parser = argparse.ArgumentParser(description='Optional: Input start and end iterations.')
parser.add_argument('--zippath', type=str, help='Zip files path.', required=True)
parser.add_argument('--type', type=str, help='"journal" or "newspaper"', required=True)
parser.add_argument('--chunk', type=int, help='Bulk chunk size.', default=100)
parser.add_argument('--prefix', type=str, help='Leading number of id to process.', required=True)
parser.add_argument('--reindex', dest='reindex', action='store_true', help='Do not skip already indexed.')
parser.add_argument('--no-reindex', dest='reindex', action='store_false', help='Skip indexed. Default.')
parser.set_defaults(reindex=False)


args = parser.parse_args()
zip_path = args.zippath
data_subset = args.type

with open('data/nlf_' + data_subset + '_meta.json', 'r') as jsonf:
    metadata = prepare_metadata(json.load(jsonf))

metadata = get_metadata_subset(metadata, args.prefix)

ELASTIC_PASSWORD = read_elastic_pwd("./secrets.txt")
client = Elasticsearch("https://ds-es.rahtiapp.fi:443",
                       basic_auth=("elastic", ELASTIC_PASSWORD), request_timeout=60)
index_name = "nlf-periodicals"

logfile = "logs/nlf_indexer.log"
if os.path.exists(logfile):
    processed = read_indexed_log(logfile, convert_to_int=False)
else:
    processed = list()

bulk_chunk_size = args.chunk
bulk_buffer = list()

this_zip_path = zip_path + "/col-861_" + args.prefix + ".zip"

archive = zipfile.ZipFile(this_zip_path, mode="r")
sorted_names = archive.namelist()
sorted_names.sort()

for item in tqdm(metadata):
    if not args.reindex:
        if item['binding_id'] in processed:
            continue
    this_text = get_id_text_from_archive(item_id=item['binding_id'], archive=archive, sorted_names=sorted_names)
    if this_text == '':
        print("No text for item " + item['binding_id'])
        log_line(logfile=logfile, line=item['binding_id'])
        continue
    item['issue_text'] = this_text
    bulk_buffer.append(item)
    if len(bulk_buffer) == bulk_chunk_size:
        write_bulk(client, bulk_buffer, index_name, logfile, verbose=True)
        bulk_buffer.clear()

if len(bulk_buffer) > 0:
    write_bulk(client, bulk_buffer, index_name, logfile, verbose=True)

archive.close()
print("Done.")

#
# this_text = get_id_text_from_zip("454545", 'temp/realzip.zip')
#
# with open(item_id + '.txt', 'w') as f:
#     f.write(this_text)

# unzip /scratch/project_2006633/nlf-harvester/zip/col-861_7.zip "*/70445/70445/*" -d $HOME/temp/nlf-metsalto
# python nlf_process_periodicals_byzip.py --type "journal" --chunk 100 --zippath "/scratch/project_2006633/nlf-harvester/zip" --prefix "10"

# TODO:
# 1. process one zip at time (add parameter for zip/id prefix)
# 2. Add reindex -parameter
