from lib.utils import csv_to_ndjson


csv_file = "./data/raw/yleeurheilu20220522-115734.csv"
ndjson_file = "./data/work/yleeurheilu20220522-115734.ndjson"

csv_to_ndjson(csv_file, ndjson_file, 1000)
