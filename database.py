from __future__ import print_function

import argparse
import json
import pprint
import requests
import sys
import urllib
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.parse import urlencode
from time import gmtime, strftime

import boto3

API_KEY= """5_y3m6tfS54pEPTPPW4SmIrRPpl_OCwf2GBVzfS9m272QDN6eg6Rqr07qeHuwrjJW4Qgfj7BSv6jwWAYuMhEbfV6Dh6xBljWjaWDKMJg9kjnfG16x3K8Nprr4yBfYXYx""" 


API_HOST = 'https://api.yelp.com'
SEARCH_PATH = '/v3/businesses/search'
BUSINESS_PATH = '/v3/businesses/'  


# Defaults for our simple example.
DEFAULT_TERM = 'restaurants'
DEFAULT_LOCATION = 'New York, NY'
SEARCH_LIMIT = 50


def request(host, path, api_key, url_params=None):
    """Given your API_KEY, send a GET request to the API.

    Args:
        host (str): The domain host of the API.
        path (str): The path of the API after the domain.
        API_KEY (str): Your API Key.
        url_params (dict): An optional set of query parameters in the request.

    Returns:
        dict: The JSON response from the request.

    Raises:
        HTTPError: An error occurs from the HTTP request.
    """
    url_params = url_params or {}
    url = '{0}{1}'.format(host, quote(path.encode('utf8')))
    headers = {
        'Authorization': 'Bearer %s' % api_key,
    }

    print(u'Querying {0} ...'.format(url))

    response = requests.request('GET', url, headers=headers, params=url_params)

    return response.json()


def search(api_key, term, location):
    """Query the Search API by a search term and location.

    Args:
        term (str): The search term passed to the API.
        location (str): The search location passed to the API.

    Returns:
        dict: The JSON response from the request.
    """

    url_params = {
        'term': term.replace(' ', '+'),
        'location': location.replace(' ', '+'),
        'limit': SEARCH_LIMIT
    }
    return request(API_HOST, SEARCH_PATH, api_key, url_params=url_params)


def get_business(api_key, business_id):
    """Query the Business API by a business ID.

    Args:
        business_id (str): The ID of the business to query.

    Returns:
        dict: The JSON response from the request.
    """
    business_path = BUSINESS_PATH + business_id

    return request(API_HOST, business_path, api_key)


def query_api(term, location):
    """Queries the API by the input values from the user.

    Args:
        term (str): The search term to query.
        location (str): The location of the business to query.
    """
    

    
    temp={}
    session = boto3.Session(
    aws_access_key_id="AKIATIKE3SY6ZENULU5R",
    aws_secret_access_key="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",
    region_name="us-west-2"
    )
    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table('yelp-restaurants')
    now = strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime())

    for i in range(0,25):
        response = search(API_KEY, term, location)

        businesses = response.get('businesses')

        if not businesses:
            print(u'No businesses for {0} in {1} found.'.format(term, location))
            return

        for biz in businesses:
            try:
                print(biz['id'])
                cate=set()
                if biz.get("categories",[]):
                    #print(biz.get("categories",[]))
                    for c in  biz.get("categories",[]):
                        cate.add(c.get("alias","").lower())
                        cate.add(c.get("title","").lower())   
                #print(",".join(cate))         
                if biz['id'] and biz.get("location"):
                    response = table.put_item(
                        Item={
                            'restaurant_id':biz['id'] ,
                            'name':biz.get("name",""),
                            'alias':biz.get("alias",""),
                            'location':biz["location"].get("city",""),
                            'address':",".join(biz["location"].get("display_address","")),
                            'categories':",".join(cate),
                            'zipcode':biz["location"].get("zip_code",""),
                            'rating':str(biz.get("rating","")),
                            'reviews':biz.get("review_count",0),
                            'cost':biz.get("price",""),
                            'phone':biz.get("display_phone",""),
                            'coordinates':str(biz.get("coordinates",{})),
                            'timestamp':now
                            })
            except Exception as e:
                print("An exception occurred")
                print(e)
                break
    #print(u'Result for business "{0}" found:'.format(business_id))
    #pprint.pprint(response, indent=2)
    #print(response.get("categories"))


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('-q', '--term', dest='term', default=DEFAULT_TERM,
                        type=str, help='Search term (default: %(default)s)')
    parser.add_argument('-l', '--location', dest='location',
                        default=DEFAULT_LOCATION, type=str,
                        help='Search location (default: %(default)s)')

    input_values = parser.parse_args()

    try:
        query_api(input_values.term, input_values.location)
    except HTTPError as error:
        sys.exit(
            'Encountered HTTP error {0} on {1}:\n {2}\nAbort program.'.format(
                error.code,
                error.url,
                error.read(),
            )
        )


if __name__ == '__main__':
    main()