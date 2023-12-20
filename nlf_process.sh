#!/bin/bash

for i in 10 11 12 13 14 15 16 17 18 19 2 3 4 5 6 7 8 9
do
  echo "Running journals $i"
  python nlf_process_periodicals_byzip.py --type "journal" --chunk 100 --zippath "/scratch/project_2006633/nlf-harvester/zip" --prefix $i
  echo "Running newspapers $i"
  python nlf_process_periodicals_byzip.py --type "newspaper" --chunk 100 --zippath "/scratch/project_2006633/nlf-harvester/zip" --prefix $i
done
