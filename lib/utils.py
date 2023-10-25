import csv
import json
import sys
import hashlib


csv.field_size_limit(sys.maxsize)


def get_json_file(ndjson_out, json_suffix):
    json_fname = ndjson_out.split(".ndjson")[0] + "_" + str(json_suffix) + ".ndjson"
    jsonfile = open(json_fname, 'w')
    return jsonfile


def csv_to_ndjson(csv_in, ndjson_out, n_entries):
    with open(csv_in, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        json_suffix = 0
        jsonfile = get_json_file(ndjson_out, json_suffix)
        counter = 0
        for row in reader:
            counter += 1
            json.dump(row, jsonfile)
            jsonfile.write('\n')
            if counter == n_entries:
                counter = 0
                jsonfile.close()
                json_suffix += 1
                jsonfile = get_json_file(ndjson_out, json_suffix)


def read_elastic_pwd(pwd_file):
    with open(pwd_file, 'r') as f:
        pwd = f.read().strip()
    return pwd


def log_line(logfile, line):
    with open(logfile, 'a') as f:
        f.write(line + "\n")


def read_indexed_log(logfile):
    with open(logfile, 'r') as f:
        lines = f.readlines()
    lines = [line.strip() for line in lines]
    return lines


def get_id_from_str(source_str):
    hash_object = hashlib.sha256(source_str.encode('utf-8'))
    hex_dig = hash_object.hexdigest()
    return hex_dig
