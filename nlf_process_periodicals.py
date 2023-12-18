import fnmatch
from lxml import etree
from glob import glob
from smart_open import open
import zipfile
import json
import argparse
import re
from lib.utils import read_elastic_pwd, log_line, read_indexed_log, get_id_from_str
from elasticsearch import Elasticsearch, helpers
import os


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


def get_issue_text(issue_path):
    pages = glob(issue_path + "alto/al-*.xml")
    pages.sort()
    page_texts = list()
    for page_path in pages:
        root = etree.parse(page_path).getroot()
        # Print the loaded XML
        page = root.find(".//Page")
        text_blocks = page.findall(".//TextBlock")
        page_text = list()
        for text_block in text_blocks:
            page_text.append(get_block_text(text_block))
        page_text_s = "\n\n".join(page_text)
        page_texts.append(page_text_s)
    page_texts_f = [text for text in page_texts if text != '']
    return "\n\n\n".join(page_texts_f)


def get_issue_text_from_strings(files_read):
    page_texts = list()
    for page_xml_s in files_read:
        root = etree.fromstring(page_xml_s)
        page = root.find(".//Page")
        text_blocks = page.findall(".//TextBlock")
        page_text = list()
        for text_block in text_blocks:
            page_text.append(get_block_text(text_block))
        page_text_s = "\n\n".join(page_text)
        page_texts.append(page_text_s)
    page_texts_f = [text for text in page_texts if text != '']
    return "\n\n\n".join(page_texts_f)


def get_id_text_from_zip(item_id, zipfile_path):
    files = []
    with open(zipfile_path, 'rb') as fin:
        with zipfile.ZipFile(fin) as zipf:
            sorted_names = list(zipf.namelist())
            sorted_names.sort()
            for name in sorted_names:
                if fnmatch.fnmatch(name, '*' + item_id + '/alto*.xml'):
                    files.append(zipf.read(name))
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


def write_bulk(client, items, index_name, logfile):
    helpers.bulk(client, items, index=index_name)
    processed_ids = "\n".join([str(item['binding_id']) for item in items])
    log_line(logfile=logfile, line=processed_ids)


# Get arguments for start and end index
parser = argparse.ArgumentParser(description='Optional: Input start and end iterations.')
parser.add_argument('--zippath', type=str, help='zipfiles path.', required=True)
parser.add_argument('--type', type=str, help='"journal" or "newspaper"', required=True)

args = parser.parse_args()
zip_path = args.zipfile
data_subset = args.type

with open('data/nlf_' + data_subset + '_meta.json', 'r') as jsonf:
    metadata = prepare_metadata(json.load(jsonf))

ELASTIC_PASSWORD = read_elastic_pwd("./secrets.txt")
client = Elasticsearch("https://ds-es.rahtiapp.fi:443",
                       basic_auth=("elastic", ELASTIC_PASSWORD), request_timeout=60)
index_name = "nlf-periodicals"

logfile = "logs/nlf_indexer.log"
if os.path.exists(logfile):
    processed = read_indexed_log(logfile, convert_to_int=True)
else:
    processed = list()

bulk_chunk_size = 1000
bulk_buffer = list()

for item in metadata:
    if item['binding_id'] in processed:
        continue
    if str(item['binding_id'])[0] == '1':
        zip_prefix = str(item['binding_id'])[:2]
    else:
        zip_prefix = str(item['binding_id'])[0]
    this_zip_path = zip_path + "col-861_" + zip_prefix + ".zip"
    this_text = get_id_text_from_zip(item_id=item['binding_id'], zipfile_path=this_zip_path)
    if this_text == '':
        continue
    item['issue_text'] = this_text
    bulk_buffer.append(item)
    if len(bulk_buffer) == bulk_chunk_size:
        write_bulk(client, bulk_buffer, index_name, logfile)
        bulk_buffer.clear()

if len(bulk_buffer) > 0:
    write_bulk(client, bulk_buffer, index_name, logfile)


#
# this_text = get_id_text_from_zip("454545", 'temp/realzip.zip')
#
# with open(item_id + '.txt', 'w') as f:
#     f.write(this_text)

# unzip /scratch/project_2006633/nlf-harvester/zip/col-861_7.zip "*/70445/70445/*" -d $HOME/temp/nlf-metsalto
