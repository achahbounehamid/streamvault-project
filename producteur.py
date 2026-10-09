import os
import time
import random
import uuid
from datetime import datetime, timezone
from dotenv import load_dotenv  # 1. Importer la fonction pour charger le .env
from pymongo import MongoClient
from bson import json_util
from azure.eventhub import EventHubProducerClient, EventData, TransportType

# 2. Charger les variables d'environnement du fichier .env
load_dotenv()

# CONFIGURATION EVENT HUB & MONGODB

CONNECTION_STR = os.getenv("AZURE_EVENTHUB_CONNECTION_STRING")

producer = EventHubProducerClient.from_connection_string(
    conn_str=CONNECTION_STR,
    eventhub_name="commandes-hub",
    transport_type=TransportType.AmqpOverWebsocket
)

mongo_client = MongoClient("mongodb://localhost:27017/")
db = mongo_client["streamvault"]

def get_random_doc(collection_name):
    """Tire un document au hasard directement depuis la BDD Mongo"""
    return next(db[collection_name].aggregate([{"$sample": {"size": 1}}]))

# BOUCLE DE PRODUCTION

print("Démarrage de la production des commandes...")

try:
    while True:
        # Tirage au sort direct depuis MongoDB
        client = get_random_doc("Customers")
        media = get_random_doc("Movies_Livres")

        # Prix fixe calculé de façon déterministe pour un même media
        prix = round(random.Random(str(media["_id"])).uniform(4.99, 29.99), 2)

            # 1. Génération du dictionnaire Python
        commande = {
            "id_commande": str(uuid.uuid4()),
            "id_client": client.get("nom", client["_id"]),
            "id_media": media.get("titre", media["_id"]),
            "datetime": datetime.now(timezone.utc),
            "prix": prix
        }

        # 2. Sérialisation directe en JSON MongoDB (bytes)
        payload_json = json_util.dumps(commande).encode('utf-8')

        # 3. Ajout direct dans EventHub sans clé d'enrobage
        batch = producer.create_batch()
        batch.add(EventData(payload_json))
        producer.send_batch(batch)

        print(
           commande
        )

        time.sleep(3)

except KeyboardInterrupt:
    print("\nArrêt du producteur.")
finally:
    producer.close()
    mongo_client.close()