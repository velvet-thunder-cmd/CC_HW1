import boto3
import json
import logging
import time
client = boto3.client('lex-runtime')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
DEFAULT_TIMEZONE = "America/New_York"
VALID_CUISINES = ['italian', 'chinese', 'indian', 'american', 'mexican', 'spanish', 'greek', 'latin', 'Persian', "Korean", "South Indian"]
PEOPLE_LIMIT = [0, 10] #[min, max]
def lambda_handler(event, context):
    logger.debug('event.bot.name={}'.format(event['bot']['name']))
    return handle_event(event)
def get_slots(intent_request):
    return intent_request['currentIntent']['slots']
def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }
def handle_event(event):
    logger.info(
        'dispatch userId={}, intentName={}'.format(event['userId'], event['currentIntent']['name']))
    intent_type = event['currentIntent']['name']
    if intent_type == 'GreetingIntent':
        return handle_greeting(event)
    elif intent_type == 'DiningSuggestionIntent':
        return handle_dining_suggestion_event(event)
    elif intent_type == 'ThankYouIntent':
        return handle_thank_you_event(event)
    raise Exception("We don't support the intent {}".format(intent_type))
def handle_greeting(event):
    logger.debug("Parsing a greeting event {}".format(event))
    return {
        'dialogAction': {
            "type": "ElicitIntent",
            'message': {
                'contentType': 'PlainText',
                'content': 'Hi there, how can I help?'}
        }
    }
def handle_thank_you_event(event):
    logger.debug("Parsing a thank you event {}".format(event))
    return {
        'dialogAction': {
            "type": "ElicitIntent",
            'message': {
                'contentType': 'PlainText',
                'content': 'You are welcome!'}
        }
    }
def parse_int(n):
    try:
        return int(n)
    except ValueError:
        return float('nan')
def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }
def validate_dining_suggestion(location, cuisine, num_people, date, time):
    if cuisine is not None and cuisine.lower() not in VALID_CUISINES:
        return validation_response(False,
                                       'cuisine',
                                       'Cuisine not available. Please try another.')
    if num_people is not None:
        num_people = int(num_people)
        if num_people > PEOPLE_LIMIT[1] or num_people < PEOPLE_LIMIT[0]:
            return validation_response(False,
                                           'people',
                                           'Due to covid, we are not allowing more than 10 people.')
    return validation_response(True, None, None)
def validation_response(is_valid, violated_slot, message_content):
    if message_content is None:
        return {
            "isValid": is_valid,
            "violatedSlot": violated_slot
        }
    return {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }
def handle_dining_suggestion_event(event):
    logger.info("Parsing a dining event - {}".format(event))
    location = get_slots(event)["Location"]
    cuisine = get_slots(event)["Cuisine"]
    
    date = get_slots(event)["Date"]
    time = get_slots(event)["Time"]
    num_people = get_slots(event)["NumberOfPeople"]
    source = event['invocationSource']
    phone = get_slots(event)["MobileNumber"]
    email = get_slots(event)["Email"]
    sqs= boto3.client('sqs')
    msg = {"cuisine": cuisine, "phone": phone, "email": email}
        #response = queue.send_message(MessageBody=json.dumps(msg))
    response=sqs.send_message( QueueUrl='https://sqs.us-west-2.amazonaws.com/224019584573/DiningConciergeSQS',
    MessageBody=json.dumps(msg)
)
        
    return close(event['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': 'Thank you! You will recieve suggestion shortly'})
def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }
    return response

