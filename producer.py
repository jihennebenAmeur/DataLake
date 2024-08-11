from kafka import KafkaProducer
import json
import pandas as pd
import time

# Chemin vers le fichier CSV
file_path = '/Data/dataset_sismique.csv'

# Chargement du dataset
data = pd.read_csv(file_path)

# Configuration du producteur Kafka
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Nom du topic Kafka
topic_name = 'topic1'

try:
    # Envoi des messages dans une boucle infinie
    while True:
        for index, row in data.iterrows():
            message = {
                "timestamp": row['date'],
                "secousse": row['secousse'],
                "magnitude": row['magnitude'],
                "tension_entre_plaque": row['tension entre plaque']
            }
            producer.send(topic_name, value=message)
            print(f"Message envoyé: {message}")
            time.sleep(1)
except KeyboardInterrupt:
    print("Arrêt du producteur...")

producer.close()
