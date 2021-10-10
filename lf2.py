import boto3
import json
import logging
from boto3.dynamodb.conditions import Key, Attr
import requests
from requests_aws4auth import AWS4Auth
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
def getSQSMsg():
    SQS = boto3.client("sqs")
    url = https://sqs.us-west-2.amazonaws.com/224019584573/DiningConciergeSQS
    response = SQS.receive_message(
        QueueUrl=url,
        AttributeNames=['SentTimestamp'],
        MessageAttributeNames=['All'],
        VisibilityTimeout=0,
        WaitTimeSeconds=0
    )
    print(url,response)
    try:
        message = response['Messages'][0]
        if message is None:
            logger.debug("Empty message")
            return None
    except KeyError:
        logger.debug("No message in the queue")
        return None
    message = response['Messages'][0]
    return json.loads(message["Body"])
def lambda_handler(event, context):
    """
        Query SQS to get the messages
        Store the relevant info, and pass it to the Elastic Search
    """
    message = getSQSMsg() 
    print("here is the message")
    print(message)
    if message is None:
        logger.debug("No Cuisine or PhoneNum key found in message")
        return
    cuisine = message.get("cuisine","")
    phoneNumber = message.get("phone","")
    email = message.get("email","")
    location=message.get("diningDate","")
    numOfPeople=message.get("diningTime","")
    date=message.get("NoofPeople","")
    time=message.get("Location","")
    phoneNumber = "+1" + phoneNumber
    if not cuisine or not phoneNumber:
        logger.debug("No Cuisine or PhoneNum key found in message")
        return
    print(cuisine,email)
    region = 'us-west-2' 
    service = 'es'
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
    host = 'https://search-restaurantsindex-g7rmy6ena7nhgued3azgxnd2ne.us-west-2.es.amazonaws.com' # The OpenSearch domain endpoint with https://
    index = 'restaurants'
    url = host + '/' + index + '/_search'
    print(url)
    query = {
  "query": {
    "nested": {
      "path": "categories",
      "query": {
        "match": { "categories.name": "korean" }
      }
    }
  },
   "_source": ["id"]
}
    headers = { "Content-Type": "application/json" }
    r = requests.get(url, auth=awsauth, headers=headers, data=json.dumps(query))
    data = json.loads(r.content.decode('utf-8'))
    try:
        esData = data["hits"]["hits"]
    except KeyError:
        logger.debug("Error extracting hits from ES response")
    ids = []
    for restaurant in esData:
        ids.append(restaurant["_source"]["id"])
    print(ids)
    messageToSend = 'Hello! Here are my {cuisine} restaurant suggestions in {location} for {numPeople} people, for {diningDate} at {diningTime}: '.format(
            cuisine=cuisine,
            location=location,
            numPeople=numOfPeople,
            diningDate=date,
            diningTime=time,
        )
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('yelp-restaurants')
    itr = 1
    prevRestaurants=""
    for id in ids:
        if itr == 6:
            break
        response = table.scan(FilterExpression=Attr('restaurant_id').eq(id))
        item = response['Items'][0]
        if response is None:
            continue
        print(response)
        restaurantMsg = '' + str(itr) + '. '
        name = item["name"]
        prevRestaurants+=str(name)+","
        address = item["address"]
        restaurantMsg += name +', located at ' + address +'. '
        messageToSend += restaurantMsg
        itr += 1
    print("test"+str(prevRestaurants))
    if prevRestaurants:
        response = table.put_item(
                Item={
                                'restaurant_id':"prevRestaurants" ,
                                'name':prevRestaurants[:-1],
                })
    messageToSend += "Enjoy your food!!"
    print(messageToSend)
