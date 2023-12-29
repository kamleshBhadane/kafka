from kafka import KafkaProducer
from  json import dumps
from time import sleep
# Kafka broker address

# Kafka topic to which messages will be produced

# Configuration for the Kafka producer


# Create a Kafka producer instance
# producer = KafkaProducer(
#  bootstrap_servers='localhost:9092',
#  linger_ms=0,
#  #security_protocol="PLAINTEXT",
#  api_version=(0,11,5),
#  #value_serializer=lambda x: dumps(x).encode('utf-8'),
#  #key_serializer=lambda x: dumps(x).encode('utf-8'),
#
# )

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         api_version=(0, 11, 5),
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))
# Example message to be sent
# Produce a message to the Kafka topic
print('sending data')
for i in range(2):
    data= {'number':i}
    #print(data)
    producer.send(topic='T_Test_SecData', value=data)
    #sleep(5)   

print('sent all data')
producer.flush()
producer.close()


# Close the producer


