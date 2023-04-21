from glob import glob
from tqdm import tqdm
import pandas as pd
import re
import csv
import json
import sys


def check_entry_validity(this_entry):
    if type(this_entry['publication_year']) == str:
        if not this_entry['publication_year'].isdigit():
            return False
    if len(this_entry['publication_year']) != 4:
        return False
    if not this_entry['pagecount'].isdigit():
        return False
    if this_entry['estc_id'] is None:
        return False
    if this_entry['estc_id'] == "":
        return False
    return True


def get_txt_file_data_entry(txtfile, mdict):
    this_meta_id = txtfile.split('/')[-2]
    # read txt to content
    with open(txtfile, 'r') as f:
        txtcontent = f.read()
        txtcontent = re.sub(r"(?<!\n)\n(?!\n)", " ", txtcontent)
    this_meta = mdict[this_meta_id]
    this_id = txtfile.split('/')[-1].split('.txt')[0]
    if 'note' in this_id:
        this_structure_type = 'note'
    else:
        this_structure_type = 'main_text'
    retdict = dict()
    retdict['part_type'] = this_structure_type
    retdict['file_id'] = this_id
    retdict['content'] = txtcontent
    retdict['eebo_id'] = this_meta_id

    for k, v in this_meta.items():
        retdict[k] = v
    return retdict


def write_ndjson(data_in, out_path, json_suffix):
    json_fname = out_path + "eebo_" + str(json_suffix) + ".ndjson"
    jsonfile = open(json_fname, 'w')
    for row in data_in:
        json.dump(row, jsonfile)
        jsonfile.write('\n')
    jsonfile.close()


datadir = '../dariah-elastic-data/raw/eebo/'
outdir = '../dariah-elastic-data/work/eebo/'

all_txt = glob(datadir + '**/*.txt', recursive=True)

# get metadata
idpairs = pd.read_csv('data/raw/eebo/idpairs_ecco_eebo_estc.csv',
                      dtype={'estc_id': str, 'document_id': str, 'document_id_octavo': str,
                             'collection': str,  'estc_id_student_edition': str})
idpairs = idpairs[['estc_id_student_edition', 'document_id']]
idpairs = idpairs.rename(columns={'estc_id_student_edition': 'id'})

estc_old = pd.read_csv('data/raw/eebo/estc_processed.csv',
                      dtype={
                      'language': str,
                      'title': str,
                      'pagecount': str,
                      'subject_topic': str,
                      'publisher': str,
                      'publication_place': str,
                      'author': str,
                      'publication_year': str,
                      'id': str,
                      'document_type': str
                      }, keep_default_na=False)
estc_old = estc_old[['language', 'title', 'pagecount', 'subject_topic', 'publisher', 'publication_place', 'author', 'publication_year', 'id', 'document_type']]
estc_old = estc_old.loc[estc_old['id'] != 'NA']
estc_old = estc_old.loc[estc_old['id'] != '(CU-RivES']

meta_merged = pd.merge(left=idpairs, right=estc_old, how='left', on='id')
meta_merged.drop_duplicates(subset=['document_id'], keep='first', inplace=True)
meta_merged.replace('NA', "", inplace=True)
meta_merged.fillna("", inplace=True)
meta_merged.rename(columns={'id': 'estc_id'}, inplace=True)
mdict = meta_merged.set_index('document_id').to_dict(orient='index')


# process records
# read files from filöelist, write a ndjson file when set length (100 seems to work for indexing in this case) is reached.
write_len = 100
jsonrecords = []
suffix = 0
for entry in tqdm(all_txt):
    this_entry = get_txt_file_data_entry(entry, mdict)
    if check_entry_validity(this_entry):
        jsonrecords.append(this_entry)
        if len(jsonrecords) == write_len:
            write_ndjson(jsonrecords, outdir, suffix)
            suffix += 1
            jsonrecords = []

write_ndjson(jsonrecords, outdir, suffix)
