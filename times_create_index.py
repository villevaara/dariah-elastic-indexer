from elasticsearch import Elasticsearch
from lib.utils import read_elastic_pwd

ELASTIC_PASSWORD = read_elastic_pwd("./secrets.txt")
client = Elasticsearch("https://dariahfi-es.2.rahtiapp.fi:443",
                       basic_auth=("elastic", ELASTIC_PASSWORD), request_timeout=60)
index_name = "bl-times"


mapping = {
    "properties": {
        'article_ocr_quality': {"type": "scaled_float", "scaling_factor": 100}, # actually and int, saves space
        'article_id': {"type": "keyword"},
        'article_type': {"type": "keyword"},
        'article_title': {"type": "keyword"},
        'article_title_additional': {"type": "keyword"},
        'section_title': {"type": "keyword"},
        'article_page_count': {"type": "integer"},
        'article_illustration_count': {"type": "integer"},
        'article_authors': {"type": "keyword"}, # Array
        'article_authors_count': {"type": "integer"},
        'article_text': {'type': 'text'},
        'article_text_token_count': {"type": "integer"},
        'article_text_char_count': {"type": "integer"},
        'article_page_ids': {"type": "keyword"}, # Array
        'article_page_numbers': {"type": "integer"}, # Array
        'newspaper_id': {"type": "keyword"},
        'issue_publication_date': {"type": "date", "format": "yyyyMMdd"},
        'issue_page_count': {"type": "integer"},
        # 'issue_number': {"type": "integer"},
        'issue_id': {"type": "keyword"},
        'issue_publication_weekday': {"type": "keyword"}
    }
}

index_settings = {
    'number_of_shards': 30,
    'codec': 'best_compression'
}

# create the index if it doesn't exist
client.indices.create(index=index_name, mappings=mapping, settings=index_settings)
# deleting an index
# client.indices.delete(index=index_name)
