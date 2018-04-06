""" Read this: https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads """

from requests import get
from csv import DictReader
from uuid import UUID
from datetime import datetime
from mongo import db

endpoint = 'http://prod2.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/'

PROPERTY_TYPES = {'D': 'Detached',
                  'S': 'Semi-Detached',
                  'T': 'Terraced',
                  'F': 'Flat/Maisonette',
                  'O': 'Other'}


def current_month():
    return endpoint + 'pp-monthly-update.txt'


def current_year():
    year = datetime.today().year
    return previous_year(year)


def previous_year(year):
    return endpoint + 'pp-{year}.txt'.format(year=year)


def complete():
    return endpoint + 'pp-complete.txt'


def download_file(url):
    collection = db['house-prices']
    fieldnames = [
        'uuid',
        'price',
        'date',
        'address.postcode',
        'details.property_type',
        'details.new_build',
        'details.freehold',
        'address.PAON',
        'address.SAON',
        'address.street',
        'address.locality',
        'address.town',
        'address.district',
        'address.county',
        'details.standard_price',
        'status',
    ]

    response = get(url, stream=True, allow_redirects=False)
    print(response.status_code, response.url)
    response.raise_for_status()

    reader = DictReader(response.iter_lines(decode_unicode=True), fieldnames=fieldnames)
    for row in reader:
        uuid = UUID(row.pop('uuid'))
        row['date'] = datetime.strptime(row['date'], '%Y-%m-%d %H:%M')
        row['price'] = int(row['price'])

        if row.get('status') == 'D':
            collection.delete_one(
                {'_id': uuid}
            )

        elif row.get('status') == 'U':
            row.pop('status')
            collection.update_one(
                {'_id': uuid},
                {'$set': row}
            )

        else:
            row.pop('status')
            collection.update_one(
                {'_id': uuid},
                {'$setOnInsert': row},
                upsert=True
            )

        if reader.line_num % 10000 == 0:
            print('{0:,.0f} rows loaded'.format(reader.line_num))

    print('{0:,.0f} rows loaded'.format(reader.line_num))


def load_postcodes(query):
    collection = db['house-prices']
    url = 'https://api.postcodes.io/postcodes/{0}'

    postcodes = collection.distinct('address.postcode', query=query)
    print('{0} postcodes found'.format(len(postcodes)))

    for postcode in postcodes:
        response = get(url.format(postcode))
        data = response.json()

        if data['status'] == 200:
            locality = {'locality': data['result']}
        else:
            locality = {'locality': data}

        result = collection.update_many(
            {'address.postcode': postcode},
            {'$set': locality}
        )

        print('{0}: {1} records updated'.format(postcode, result.modified_count))


if __name__ == '__main__':
    f = current_month()
    download_file(f)
