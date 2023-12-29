import socket

conf = {'bootstrap.servers': '127.0.0.1:9092',
        # 'security.protocol': 'PLAINTEXT',
        #'api_version': (0,11,5),
        # 'sasl.mechanism': 'PLAIN',
        # 'sasl.username': '<CLUSTER_API_KEY>',
        # 'sasl.password': '<CLUSTER_API_SECRET>',
         'client.id': socket.gethostname()
        }

# conf = {
#     'bootstrap.servers':'kafka-14205623-kamleshbhadane87.a.aivencloud.com:28170',
#     'sasl.mechanism' : 'SCRAM-SHA-256',
#     'sasl.username' : 'avnadmin',
#     'sasl.password' : 'AVNS_etTtQ3nHJi3FZ4qvyma',
#     'security.protocol':'SASL_SSL',
#     'ssl.ca.location':'ca.pem'
#     }