from settings import SITE, API, prices
from requests import get
from csv import DictReader
from uuid import UUID
from datetime import datetime

PROPERTY_TYPES = {'D': 'Detached',
                  'S': 'Semi-Detached',
                  'T': 'Terraced',
                  'F': 'Flat/Maisonette',
                  'O': 'Other'}


def download_file(filename='pp-monthly-update.txt'):
    response = get(SITE+filename)
    print(response.status_code, response.url)
    header = ['uuid', 'price', 'date', 'postcode', 'property_type', 'new_build', 'freehold',
              'PAON', 'SAON', 'street', 'locality', 'town', 'district', 'county', 'standard_price', 'status']
    reader = DictReader(response.text.splitlines(), fieldnames=header)

    for row in reader:
        row['uuid'] = UUID(row['uuid'])
        row['date'] = datetime.strptime(row['date'], '%Y-%m-%d %H:%M')
        row['price'] = int(row['price'])
        row['standard_price'] = row['standard_price'] == 'A'

        row['detail'] = {
            'property_type': PROPERTY_TYPES[row.pop('property_type')],
            'new_build': row.pop('new_build') == 'Y',
            'freehold': row.pop('freehold') == 'F'
        }

        row['location'] = {}
        for field in ['PAON', 'SAON', 'street', 'locality', 'town', 'district', 'county', 'postcode']:
            row['location'][field] = row.pop(field)

        if row.get('status') == 'D':
            prices.delete_one({'uuid': row['uuid']})
        else:
            row.pop('status')
            prices.update_one({'uuid': row['uuid']}, {'$set': row}, upsert=True)
        print('{uuid} {date}: £{price:,.0f} {location[town]}'.format(**row))

        if reader.line_num % 100 == 0:
            print('{0:,.0f} rows loaded'.format(reader.line_num))

    print('{0:,.0f} rows loaded'.format(reader.line_num))


def load_postcodes(q=None, limit=None):
    for price in prices.find(q if q else {}).limit(limit):
        r = get(API.format(price['location']['postcode'])).json()
        if r['status'] == 200:
            for k, v in r['result'].items():
                price['location'][k] = v
        else:
            print(r)
        prices.save(price)


if __name__ == '__main__':
    download_file()
    load_postcodes()
