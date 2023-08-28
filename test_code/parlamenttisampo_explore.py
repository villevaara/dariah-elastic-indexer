import pandas as pd
from glob import glob

datadir = '../dariah-elastic-data/raw/parlamenttisampo/speeches-all.csv/'

all_csv = glob(datadir + '*.csv')

li = []
for filename in all_csv:
    df = pd.read_csv(filename, index_col=None, header=0)
    li.append(df)

ps = pd.concat(li, axis=0, ignore_index=True)
