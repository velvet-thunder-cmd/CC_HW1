

from __future__ import print_function
import argparse
import json
import requests
import sys
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.parse import urlencode
from time import gmtime, strftime
from elasticsearch import Elasticsearch, RequestsHttpConnection
import boto3
API_KEY= """5_y3m6tfS54pEPTPPW4SmIrRPpl_OCwf2GBVzfS9m272QDN6eg6Rqr07qeHuwrjJW4Qgfj7BSv6jwWAYuMhEbfV6Dh6xBljWjaWDKMJg9kjnfG16x3K8Nprr4yBfYXYx""" 
API_HOST = 'https://api.yelp.com'
SEARCH_PATH = '/v3/businesses/search'
BUSINESS_PATH = '/v3/businesses/'  
# data parameters
DEFAULT_TERM = 'restaurants'
DEFAULT_LOCATION = 'New York, NY'
SEARCH_LIMIT = 50
def request(host, path, api_key, url_params=None):
    url_params = url_params or {}
    url = '{0}{1}'.format(host, quote(path.encode('utf8')))
    headers = {
        'Authorization': 'Bearer %s' % api_key,
    }
    print(u'Querying {0} ...'.format(url))
    response = requests.request('GET', url, headers=headers, params=url_params)
    return response.json()
def search(api_key, term, location):
    url_params = {
        'term': term.replace(' ', '+'),
        'location': location.replace(' ', '+'),
        'limit': SEARCH_LIMIT
    }
    return request(API_HOST, SEARCH_PATH, api_key, url_params=url_params)
def get_business(api_key, business_id):
    business_path = BUSINESS_PATH + business_id
    return request(API_HOST, business_path, api_key)
def query_api(term, location):
    temp={}
    session = boto3.Session(
    aws_access_key_id="AKIATIKE3SY6ZENULU5R",
    aws_secret_access_key="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",
    region_name="us-west-2"
    )
    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table('yelp-restaurants')
    now = strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime())
    host = 'https://search-restaurantsindex-g7rmy6ena7nhgued3azgxnd2ne.us-west-2.es.amazonaws.com' 
    es = Elasticsearch(
        hosts = [{'host': host, 'port': 443}],
        use_ssl = True,
        verify_certs = True,
        connection_class = RequestsHttpConnection
    )
    print(es)
    tempp={}
    for i in range(0,2):
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
                    for c in  biz.get("categories",[]):
                        cate.add(c.get("alias","").lower())
                        cate.add(c.get("title","").lower())   
                if True:
                    if biz['id']:
                        te=[]
                        for ca in cate:
                            te.append({"name":ca})
                        index_data = {
                            'id': biz['id'],
                            'categories': te
                        }
                    url = host+'/restaurants/_doc/'+biz['id']+"?pretty"
                    headers = {'Content-type': 'application/json'}
                    x = requests.post(url, headers=headers,data = json.dumps(index_data))
                    print(x.text)
                
                if True:
                    if biz['id']:
                        for cu in cate:
                            if cu in tempp.keys():
                                tempp[cu]=tempp[cu]+", "+biz['id']
                            else:
                                tempp[cu]=biz['id']
                
                if True:       
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
