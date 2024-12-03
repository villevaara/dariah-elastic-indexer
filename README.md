# dariah-elastic-indexer

Indexer for DARIAH-FI Elasticsearch project.

This is built with a Python wrapper for the ES API. Documentation for the wrapper can be found at https://elasticsearch-py.readthedocs.io/en/v8.7.0/

The main components are data wranglers that make the input data ready for indexing (`\*_to_ndjson.py`) and the bulk indexing scripts (`*_index_data.py`). The code has been run with Python 3.10, and the `requirements.txt` has the required `pip` packages listed (`pip install -r requirements.txt`).

## legentic_indexer_puhti.py

Accepts some command line parameters to allow indexing a slice of source files. Eg.:

`python legentic_indexer_puhti.py --start 20000 --end 30000`

If you want to reindex already indexed files, either empty the logfile of successfully indexed ones (`legentic_indexed.log`),
or set the re-index parameter `--reindex`.

## Parameters

Number of shards depends on the overall data size. A helpful formula can be found here: https://nsvarun14.medium.com/capacity-planning-for-elasticsearch-cde3c0693add

## TODO:

Times:
* Times for year 2010 is in new format, not yet covered.
* ~5% of the xml files are broken. Investigate further?
