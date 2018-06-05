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

NEW_BUILD = {'Y': 'New build',
             'N': 'Existing property'}

FREEHOLD = {'F': 'Freehold',
            'L': 'Leasehold'}

PRICE_PAID = {'A': 'Standard Price Paid',
              'B': 'Additional Price Paid'}


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
    """ https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads """

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
    """ https://api.postcodes.io/ """

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


def load_epc(address=None, postcode=None, local_authority=None, constituency=None):
    """ https://epc.opendatacommunities.org/docs/api/domestic """
    n = 0

    params = {
        'from-year': 1995,
        'to-year': 2018,
        'size': 5000,
    }
    if address:
        params['address'] = address
    if postcode:
        params['postcode'] = postcode
    if local_authority:
        params['local-authority'] = local_authority
    if constituency:
        params['constituency'] = constituency

    # collection = db['house-prices']
    collection = db['epc-certificates']
    url = 'https://epc.opendatacommunities.org/api/v1/domestic/search'

    # key = 'f7708db9ad243efc7bc8f7f2a191c5d5f04babb2'
    key = 'bmlja21haGVyODRAZ21haWwuY29tOmY3NzA4ZGI5YWQyNDNlZmM3YmM4ZjdmMmExOTFjNWQ1ZjA0YmFiYjI='

    headers = {
        'Accept': 'text/csv',
        'Authorization': 'Basic '+key,
    }

    response = get(url, params=params, headers=headers, stream=True)
    print(response.status_code, response.url)
    response.raise_for_status()

    reader = DictReader(response.iter_lines(decode_unicode=True))
    for row in reader:
        ref = row['building-reference-number']
        collection.update_one(
            {'_id': ref},
            {'$setOnInsert': row},
            upsert=True
        )
        n += 1

    print('{1}: {0} records loaded'.format(n, postcode))


if __name__ == '__main__':
    f = current_month()
    download_file(f)
