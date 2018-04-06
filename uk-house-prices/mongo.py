from pymongo import MongoClient

MONGO_HOST = 'localhost'
MONGO_PORT = 27017
MONGO_DATABASE = 'property'
MONGO_USERNAME = None
MONGO_PASSWORD = None

mongo = MongoClient(MONGO_HOST, MONGO_PORT)
db = mongo[MONGO_DATABASE]

if MONGO_USERNAME:
    db.authenticate(MONGO_USERNAME, MONGO_PASSWORD)


if __name__ == '__main__':
    print('Connected to database...')
    for collection in db.collection_names():
        print(collection)
