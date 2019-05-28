import pika
import pickle
import pymongo

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["processingFile"]
collection = db["stringsOfFile"]

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='asyncProcess')

def callback(ch, method, properties, body):
    row = pickle.loads(body)
    x = collection.insert_one(row[0])
    # print(x)

channel.basic_consume(queue='asyncProcess',
                      auto_ack=True,
                      on_message_callback=callback)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()