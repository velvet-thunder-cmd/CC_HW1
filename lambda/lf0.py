import boto3
import json

client = boto3.client('lex-runtime')


def lambda_handler(event, context):
    
    
    print(event)
    
    UserMessage=event.get('messages')
    lastUserMessage=UserMessage[0].get("unstructured").get("text")
    
    print(lastUserMessage)
    if lastUserMessage is None or len(lastUserMessage) < 1:
        print (lastUserMessage)
        return {
            'statusCode': 200,
            'body': json.dumps(botMessage)
        }
    response = client.post_text(
        botName='DiningAssistant',
        botAlias='testbot',
        userId='lf0',
        inputText=lastUserMessage)
    
    if response['message'] is not None or len(response['message']) > 0:
        botMessage = response['message']
    
    print("Bot message", botMessage)
    
    botResponse =  [{
        'type': 'unstructured',
        'unstructured': {
          'text': botMessage
        }
      }]
    return {
        'statusCode': 200,
        'messages': botResponse
    }
