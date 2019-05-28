import pika
import pandas as pd
import pickle

def AsyncProcess(file):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='asyncProcess')

    table = pd.read_excel(file)
    for i in range(len(table)):
        row = table.loc[[i]].to_dict(orient='records')
        channel.basic_publish(exchange='',
                              routing_key='asyncProcess',
                              body=pickle.dumps(row))