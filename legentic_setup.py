from elasticsearch import Elasticsearch
from lib.utils import read_elastic_pwd


ELASTIC_PASSWORD = read_elastic_pwd("./secrets.txt")
client = Elasticsearch("https://dariahfi-es.2.rahtiapp.fi:443",
                       basic_auth=("elastic", ELASTIC_PASSWORD), request_timeout=60)
index_name = "legentic"

mapping = {
    "properties": {
        'indexed': {"type": "date", "format": "yyyy-MM-dd HH:mm:ss Z"},
        'published': {"type": "date", "format": "yyyy-MM-dd HH:mm:ss Z"},
        'author.name': {"type": "text"},
        'author.text.hashtag': {"type": "keyword"},
        'author.text.user_mention': {"type": "keyword"},
        'author.text.content': {"type": "keyword"},
        'author.content': {"type": "keyword"},
        'author-uri': {"type": "keyword"},
        'author-community_facebook': {"type": "keyword"},
        'author-id_facebook': {"type": "keyword"},
        'author-following': {"type": "integer"},
        'author-followers': {"type": "integer"},
        'blog_id': {"type": "keyword"},
        'citation.length': {"type": "integer"},
        'citation.content': {"type": "keyword"},
        'description.content': {"type": "text"},
        'facebook_id': {"type": "keyword"},
        'forum_post_id': {"type": "keyword"},
        'google-id': {"type": "keyword"},
        'instagram-author-id': {"type": "keyword"},
        'instagram-ref-author-id': {"type": "keyword"},
        'language': {"type": "keyword"},
        'latency': {"type": "long"},
        'latitude': {"type": "float"},
        'longitude': {"type": "float"},
        'name.content': {"type": "text"},
        'page-title.content': {"type": "text"},
        'quote.content': {"type": "text"},
        'type': {"type": "keyword"},
        'url': {"type": "keyword"},
        'ref-author': {"type": "text"},
        'ref-author-id_facebook': {"type": "keyword"},
        'ref-author-uri': {"type": "keyword"},
        'subject.content': {"type": "keyword"},
        'subject.length': {"type": "integer"},
        'text.address.size': {"type": "integer"},
        'text.content': {"type": "text"},
        'text.email.size': {"type": "integer"},
        'text.hashtag': {"type": "keyword"},
        'text.person.content': {"type": "keyword"},
        'text.person.size': {"type": "integer"},
        'text.phone.canonized': {"type": "keyword"},
        'text.phone.content': {"type": "text"},
        'text.url.content': {"type": "text"},
        'text.url.length': {"type": "integer"},
        'text.user_mention': {"type": "keyword"},
        'thread.title.content': {"type": "text"},
        'twitter_retweet_id': {"type": "keyword"},
        'twitter_tweet_id': {"type": "keyword"},
        'youtube-channel-id': {"type": "keyword"},
        # 'version': {"type": "keyword"},
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
