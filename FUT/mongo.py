from pymongo import MongoClient
from settings import MONGODB_HOST, MONGODB_PORT, MONGODB_DATABASE, MONGODB_USERNAME, MONGODB_PASSWORD

mongo = MongoClient(MONGODB_HOST, MONGODB_PORT, connect=True)
db = mongo[MONGODB_DATABASE]
db.authenticate(MONGODB_USERNAME, MONGODB_PASSWORD)

CLUBS = db.fut_clubs
LEAGUES = db.fut_leagues
NATIONS = db.fut_nations
PLAYERS = db.fut_players
POSITIONS = db.fut_positions
FORMATIONS = db.fut_formations
