from elasticsearch import Elasticsearch
from lib.utils import read_elastic_pwd

ELASTIC_PASSWORD = read_elastic_pwd("./secrets.txt")
client = Elasticsearch("https://ds-es.rahtiapp.fi:443",
                       basic_auth=("elastic", ELASTIC_PASSWORD), request_timeout=60)
index_name = "nlf-periodicals"

# drop:
# authors, terms, score

mapping = {
    "properties": {
        'binding_id': {"type": "keyword"},
        'binding_title': {"type": "text"},
        'publication_id': {"type": "keyword"},
        'publication_date': {"type": "date", "format": "yyyy-MM-dd"},  # date in data
        'general_type': {"type": "keyword"},
        'copyright_warnings': {"type": "boolean"},
        'publisher': {"type": "text"},
        'issue': {"type": "keyword"},
        'import_date': {"type": "date", "format": "yyyy-MM-dd"},
        'date_accuracy': {"type": "keyword"},
        'place_of_publication': {"type": "text"},
        'url': {"type": "keyword"},
        'pdf_url': {"type": "keyword"},
        'base_url': {"type": "keyword"},
        'page_count': {"type": "integer"},
        'issue_text': {'type': 'text'}
    }
}

index_settings = {
    'number_of_shards': 10,
    'codec': 'best_compression'
}

# create the index if it doesn't exist
client.indices.create(index=index_name, mappings=mapping, settings=index_settings)
# deleting an index
# client.indices.delete(index=index_name)
