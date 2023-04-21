from lib.utils import csv_to_ndjson
from glob import glob
from tqdm import tqdm


datadir = '../dariah-elastic-data/raw/parlamenttisampo/speeches-all.csv/'
outdir = '../dariah-elastic-data/work/parlamenttisampo/'

all_csv = glob(datadir + '*.csv')

for csv_file in tqdm(all_csv):
    ndjson_file = outdir + csv_file.split('/')[-1][:-4] + '.ndjson'
    csv_to_ndjson(csv_file, ndjson_file, 1000)
