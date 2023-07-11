import logging
import time
import pika
import json
import os



def on_message_received(ch, method, properties, body):

    # Converting RabbitMQ JSON Payload to Python Dictionary.
    data1 = json.loads(body)

    # Fetching Key and Event name from Dictionary
    keyname = data1['Key']
    event_name1 = data1['EventName']
    print(data1)
    #print(object1.object.size)
    # Checking the Put, Delete and Get condition on the basis of Event name and sending a message.
    try:
        if event_name1 == 's3:ObjectCreated:Put':
            print(f'A Message for  {keyname} file has been received.')
            print("spark transformation started")
            os.system('SparkTransformation.py')


        elif event_name1 == 's3:ObjectRemoved:Delete':
            print(f' Received a Message for {keyname} has been deleted.')

        else:
            print(f'A Message for the Filename: {event_name1} {keyname} has been detected.')

        time.sleep(body.count(b'.'))

        ch.basic_ack(delivery_tag=method.delivery_tag)
        print("Acknowledgement sent and item removed from main queue")

    except Exception:
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print("exception raise Acknowledgement sent and item removed from main queue")


connection_parameters = pika.ConnectionParameters('localhost')

connection = pika.BlockingConnection(connection_parameters)

channel = connection.channel()

channel.basic_consume(queue='ronit', on_message_callback=on_message_received, auto_ack=False)

print("Start Consuming, waiting for the file")

channel.start_consuming()
