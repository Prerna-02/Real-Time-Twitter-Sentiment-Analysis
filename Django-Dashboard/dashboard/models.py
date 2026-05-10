
from django.conf import settings
from pymongo import MongoClient

_client = MongoClient(settings.MONGO_URI)
collection = _client[settings.MONGO_DB][settings.MONGO_COLLECTION]
