import fnmatch
from lxml import etree
from glob import glob
from smart_open import open
import zipfile
import json
import argparse


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


# with open("data/nlf_newspaper_meta.json", 'r') as jsonf:
#     data = json.load(jsonf)

# 1. read metadata json into memory.
# 2. take iteration n as parameter
# 3. bulk index in blocks of 10.000

# read each entry in metadata
# unzip the relevant items into memory
# add to indexing buffer until bulk index size is reached, then index all


# Get arguments for start and end index
parser = argparse.ArgumentParser(description='Optional: Input start and end iterations.')
parser.add_argument('--id', type=str, help='ID to extract.')
parser.add_argument('--zipfile', type=str, help='zipfile path.')
parser.set_defaults(reindex=False)

args = parser.parse_args()

# item_id = '38586'
# this_zip = 'temp/realzip.zip'

item_id = args.id
this_zip = args.zipfile


this_text = get_id_text_from_zip(item_id, this_zip)

with open(item_id + '.txt', 'w') as f:
    f.write(this_text)
