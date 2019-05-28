import pika
import pickle
import pandas as pd
import numpy as np


def Map(file, field, count, n_parts):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='distribProcess')

    table = pd.read_excel(file)
    parts = np.array_split(table, n_parts)
    for part in parts:
        dict = {'table': part,
                'field': field,
                'count': count}
        channel.basic_publish(exchange='',
                              routing_key='distribProcess',
                              body=pickle.dumps(dict))

def Reduce(n_parts, field, count):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='sortedPartsDistribProcess')

    sorted_parts = []
    for method, properties, body in channel.consume('sortedPartsDistribProcess'):
        channel.basic_ack(method.delivery_tag)
        sorted_parts.append(pickle.loads(body))
        if method.delivery_tag == n_parts:
            break

    channel.cancel()
    channel.close()
    connection.close()
    table = pd.concat(sorted_parts)
    result = table.sort_values(by=[field]).head(int(count))
    return result
