import time
from kafka import KafkaProducer



TOPIC_NAME = "T_Test_Data"
# Choose an appropriate SASL mechanism, for instance:
SASL_MECHANISM = 'SCRAM-SHA-256'

producer = KafkaProducer(
    bootstrap_servers=f"kafka-14205623-kamleshbhadane87.a.aivencloud.com:28170",
    sasl_mechanism = SASL_MECHANISM,
    sasl_plain_username = "avnadmin",
    sasl_plain_password = "AVNS_etTtQ3nHJi3FZ4qvyma",
    security_protocol="SASL_SSL",
    ssl_cafile="ca.pem",
)

for i in range(2):
    message = f"Hello from Python using SASL {i + 1}!"
    producer.send(TOPIC_NAME, message.encode('utf-8')).add_callback()
    print(f"Message sent: {message}")
    time.sleep(1)

producer.close()