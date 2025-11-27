from elasticsearch import Elasticsearch
from lib.utils import read_elastic_pwd


ELASTIC_PASSWORD = read_elastic_pwd("./secrets.txt")
client = Elasticsearch("https://dariahfi-es.2.rahtiapp.fi:443",
                       basic_auth=("elastic", ELASTIC_PASSWORD), request_timeout=60)
index_name = "bn"

mapping = {
    "properties": {
        "issue_id": {"type": "keyword"},
        "article_type": {"type": "keyword"},
        "article_id": {"type": "keyword"},
        "newspaper_id": {"type": "keyword"},
        "text": {"type": "text"},
        "title": {"type": "text"},
        "paragraphs": {"type": "integer"},
        "newspaper_title": {"type": "keyword"},
        "vol_no": {"type": "text"},
        "issue_no": {"type": "text"},
        "thematic_collection": {"type": "keyword"},
        "octavo_collection": {"type": "keyword"},
        "issue_date_start": {"type": "date", "format": "yyyy-MM-dd"},
        "issue_date_end": {"type": "date", "format": "yyyy-MM-dd"},
        "original_issue_date": {"type": "text"}
    }
}

index_settings = {
    'number_of_shards': 10,
    'codec': 'best_compression'
}

# creating the index
client.indices.create(index=index_name, mappings=mapping, settings=index_settings)

# deleting an index
# client.indices.delete(index=index_name)
