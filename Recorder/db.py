import pymongo


class MongoHandler(object):
    def __init__(self,
                 _db_name,
                 _uri = 'localhost',
                 _port = 27017,
                 _user = None,
                 _auth_source = "admin",
                 _pw = None,
                 _validator = None):

        if _user and _pw and _auth_source:
            print("CONNECTING WITH AUTH")
            uri = "%s:%s" % (_uri, _port)
            self.client = pymongo.MongoClient(uri, _port, username=_user, password=_pw, authSource = _auth_source)
        else:
            print("CONNECTING WITH NO AUTH")
            self.client = pymongo.MongoClient(_uri, _port)

        self.db = self.client[_db_name]
        self.validator = _validator() if _validator else None

    def insertOne(self, _collection, _struct):
        # TODO: Validation goes Here
        return self.db[_collection].insert_one(_struct)

    def insertMany(self, _collection, _array):
        # TODO: Validation goes Here
        print("TRANSARRAY: %s" % _array)
        return self.db[_collection].insert_many(_array)

    def pullFullCollection(self,_collection):
        # TODO: Validation goes Here
        # This is a dangerous function that can put a huuuuuuge Load
        # on a Database... may have to create a buffer.
        return self.db[_collection].find()
