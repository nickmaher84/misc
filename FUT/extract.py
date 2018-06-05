import requests
import json
from datetime import datetime
from mongo import PLAYERS, CLUBS, NATIONS, LEAGUES, POSITIONS

URL = 'https://www.easports.com/uk/fifa/ultimate-team/api/fut/'


def scrape_players(page=1, rating=None, quality=None, league=None, club=None, nation=None, position=None):
    filters = {'page': page}

    if rating is not None:
        filters['ovr'] = rating

    if quality is not None:
        filters['quality'] = quality

    if league is not None:
        filters['league'] = league

    if club is not None:
        filters['club'] = club

    if nation is not None:
        filters['country'] = nation

    if position is not None:
        filters['position'] = position

    response = requests.get(URL + 'item', params={'jsonParamObject': json.dumps(filters)})
    response.raise_for_status()
    data = response.json()

    print('Processing {count} of {totalResults} results (page {page} of {totalPages})'.format(**data))

    for item in data['items']:
        parse_item(item)

    if data['page'] < data['totalPages']:
        scrape_players(data['page'] + 1, rating, quality, league, club, position)

    # update_prices({'price': {'$exists': False}})


def parse_item(item):
    assert isinstance(item, dict)
    if item['itemType'] == 'player':
        add_player(item)
    else:
        raise TypeError


def add_player(item):
    POSITIONS.update_one(
        {'id': item['position']},
        {'$setOnInsert':
             {'name': item['positionFull']}
         },
        upsert=True)

    nation = item['nation']
    NATIONS.update_one(
        {'id': nation['id']},
        {'$setOnInsert':
             {'name': nation['name']}
         },
        upsert=True)

    league = item['league']
    LEAGUES.update_one(
        {'id': league['id']},
        {'$setOnInsert':
             {'name': league['name']
                 , 'code': league['abbrName']}
         },
        upsert=True)

    club = item['club']
    CLUBS.update_one(
        {'id': club['id']},
        {'$setOnInsert':
             {'name': club['name']
                 , 'league': league['id']}
         },
        upsert=True)

    PLAYERS.update_one(
        {'id': int(item['id'])},
        {'$setOnInsert': {'base_id': item['baseId'],
                          'name': item['name'],
                          'first_name': item.get('firstName'),
                          'last_name': item.get('lastName'),
                          'common_name': item.get('commonName'),
                          'height': item['height'],
                          'weight': item['weight'],
                          'age': item['age'],
                          'date_of_birth': datetime.strptime(item['birthdate'], '%Y-%m-%d'),
                          'goalkeeper': item['isGK'],
                          'position': item['position'],
                          'nation': nation['id'],
                          'league': league['id'],
                          'club': club['id'],
                          'rating': item['rating'],
                          'quality': item['quality'],
                          'special': item['isSpecialType'],
                          'player_type': item['playerType'],
                          'color': item['color'],
                          'traits': item['traits'],
                          'specialities': item['specialities'],
                          'def_work_rate': item['defWorkRate'],
                          'att_work_rate': item['atkWorkRate'],
                          'preferred_foot': item['foot'],
                          'weak_foot': item['weakFoot'],
                          'skill_moves': item['skillMoves'],
                          'attributes': {a['name'][-3:]: a['value'] for a in item['attributes']},
                          'stats.acceleration': item['acceleration'],
                          'stats.aggression': item['aggression'],
                          'stats.agility': item['agility'],
                          'stats.balance': item['balance'],
                          'stats.ballcontrol': item['ballcontrol'],
                          'stats.crossing': item['crossing'],
                          'stats.curve': item['curve'],
                          'stats.dribbling': item['dribbling'],
                          'stats.finishing': item['finishing'],
                          'stats.freekickaccuracy': item['freekickaccuracy'],
                          'stats.gkdiving': item['gkdiving'],
                          'stats.gkhandling': item['gkhandling'],
                          'stats.gkkicking': item['gkkicking'],
                          'stats.gkpositioning': item['gkpositioning'],
                          'stats.gkreflexes': item['gkreflexes'],
                          'stats.headingaccuracy': item['headingaccuracy'],
                          'stats.interceptions': item['interceptions'],
                          'stats.jumping': item['jumping'],
                          'stats.longpassing': item['longpassing'],
                          'stats.longshots': item['longshots'],
                          'stats.marking': item['marking'],
                          'stats.penalties': item['penalties'],
                          'stats.positioning': item['positioning'],
                          'stats.potential': item['potential'],
                          'stats.reactions': item['reactions'],
                          'stats.shortpassing': item['shortpassing'],
                          'stats.shotpower': item['shotpower'],
                          'stats.slidingtackle': item['slidingtackle'],
                          'stats.sprintspeed': item['sprintspeed'],
                          'stats.stamina': item['stamina'],
                          'stats.standingtackle': item['standingtackle'],
                          'stats.strength': item['strength'],
                          'stats.vision': item['vision'],
                          'stats.volleys': item['volleys']}
         },
        upsert=True)

    # print(item['rating'], item['name'])


def update_prices(p=None):
    if p is None:
        p = dict()

    for player in PLAYERS.find(p).sort('price.timestamp').batch_size(128):
        print(player['rating'], player['name'])
        try:
            price_range = get_price_range(player['id'])
            xbox, ps = get_futbin_data(player['id'])
            date = max(xbox.keys()) if xbox else None
            PLAYERS.update_one(
                {'id': player['id']},
                {'$set': {'price.xbox.lower': price_range['xboxone']['minPrice'],
                          'price.xbox.upper': price_range['xboxone']['maxPrice'],
                          'price.xbox.latest': xbox.get(date, None),
                          'price.xbox.history': [{'date': datetime.utcfromtimestamp(k/1000), 'value': v} for k, v in sorted(xbox.items())],
                          'price.ps4.lower': price_range['ps4']['minPrice'],
                          'price.ps4.upper': price_range['ps4']['maxPrice'],
                          'price.ps4.latest': ps.get(date, None),
                          'price.ps4.history': [{'date': datetime.utcfromtimestamp(k/1000), 'value': v} for k, v in sorted(ps.items())],
                          'price.pc.lower': price_range['pc']['minPrice'],
                          'price.pc.upper': price_range['pc']['maxPrice'],
                          'price.timestamp': datetime.now()}
                 }
            )
        except requests.exceptions.ConnectionError:
            print('Connection Failed')


def get_price_range(player_id):
    response = requests.get(URL + 'price-band/{0}'.format(player_id))
    response.raise_for_status()
    data = response.json()
    return data[str(player_id)]['priceLimits']


def get_futbin_data(player_id):
    url = 'https://www.futbin.com/pages/player/graph.php'
    parameters = {
        'type': 'daily_graph',
        'year': 17,
        'player': player_id,
        '_': datetime.utcnow()
    }

    response = requests.get(url, params=parameters)
    response.raise_for_status()
    data = response.json()
    xbox = dict(data['xbox'])
    ps = dict(data['ps'])
    return xbox, ps


if __name__ == '__main__':
    scrape_players()
    # update_prices()
