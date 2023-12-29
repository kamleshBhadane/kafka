from config import conf

from confluent_kafka import Producer

producer = Producer(conf)


def send_message_to_kafka(topic, key, message):
    producer.produce(topic=topic, key=key, value=message, callback=callback)


def callback(err, event):
    print('Here')
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(event), str(err)))
    else:
        print(f"Message produced {str(event.offset())}: {str(event.value().decode('utf-8'))}")


def flush_producer():
    producer.flush()
