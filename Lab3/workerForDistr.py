import pika
import pickle

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='distribProcess')

def callback(ch, method, properties, body):
    dict = pickle.loads(body)
    table = dict['table']
    field = dict['field']
    count = dict['count']
    result = table.sort_values(by=[field])
    result = result.head(int(count))
    print(result)

    channel.queue_declare(queue='sortedPartsDistribProcess')
    channel.basic_publish(exchange='',
                          routing_key='sortedPartsDistribProcess',
                          body=pickle.dumps(result))


channel.basic_consume(queue='distribProcess',
                      auto_ack=True,
                      on_message_callback=callback)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()