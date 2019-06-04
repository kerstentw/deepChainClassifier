import pymongo


class MongoHandler(object):
    def __init__(self, _db_name, _uri = 'localhost', _port = 27017, _validator = None):
        self.client = pymongo.MongoClient(_uri, _port)
        self.db = self.client[_db_name]
        self.validator = _validator() if _validator else None

    def insertOne(self, _collection, _struct):
        # TODO: Validation goes Here
        return self.db[_collection].insert_one(_struct)

    def insertMany(self, _collection, _array):
        # TODO: Validation goes Here
        return self.db[_collection].insert_many(_array)
