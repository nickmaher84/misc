from mongo import PLAYERS, FORMATIONS, NATIONS, LEAGUES


def generate_team(quality, nation=None, league=None, types=('standard', 'rare'), price=None, worst=False):
    parameters = {'quality': quality}

    if isinstance(nation, int):
        parameters['nation'] = nation
    elif isinstance(nation, str):
        lookup = NATIONS.find_one({'name': nation})
        parameters['nation'] = lookup['id']

    if isinstance(league, int):
        parameters['league'] = league
    elif isinstance(league, str):
        lookup = LEAGUES.find_one({'code': league})
        parameters['league'] = lookup['id']

    if isinstance(types, str):
        parameters['player_type'] = types
    elif type(types) in (list, set, tuple):
        parameters['player_type'] = {'$in': types}

    if isinstance(price, int):
        parameters['price.ps4.latest'] = {'$lte': price}

    players = [p for p in PLAYERS.find(parameters).sort('price.ps4.latest')]
    print('{0} possible players found.'.format(len(players)))
    players.sort(key=lambda p: p['rating'], reverse=not worst)

    formation_name, rating = None, 9999 if worst else 0
    for formation in FORMATIONS.find().sort('name'):
        print(formation['name'])
        team = pick_team(players, formation)
        print()

        if (team > rating and not worst) or (worst and team and team < rating):
            rating = team
            formation_name = formation['name']

    if formation_name:
        # print('{0: <5}'.format(league), rating, formation_name)
        print('BEST FORMATION:', formation_name)
        formation = FORMATIONS.find_one({'name': formation_name})
        pick_team(players, formation)
    else:
        print('No viables formations found.')
        pass


def pick_team(players, formation):
    positions = list()
    positions.extend(formation['positions'])  # First Team
    positions.extend(formation['positions'])  # Substitutes

    bases = set()

    for player in players:
        if player['position'] in positions and player['base_id'] not in bases:
            index = positions.index(player['position'])
            positions[index] = player
            bases.add(player['base_id'])

    if [p for p in positions if type(p) is str]:
        print('No team possible')
        return False

    else:
        for p in positions:
            print('{0: <3} {1} {2} ({3})'.format(p['position'], p['rating'], p['name'], p.get('price', {}).get('ps4', {}).get('latest', '???')))
        return sum([p['rating'] for p in positions])


if __name__ == '__main__':
    generate_team('gold', league='ITA 1')
