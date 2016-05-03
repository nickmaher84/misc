from pymongo import MongoClient

SITE = 'http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/'
API = 'https://api.postcodes.io/postcodes/{0}'

db = MongoClient()
prices = db.property.prices
