from kafka import KafkaProducer
import json
import os
import sys

print("Démarrage du script ingestion.py")
print(f"Arguments reçus : {sys.argv}")

try:
    producer = KafkaProducer(
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    print("Producteur Kafka initialisé avec succès")
except Exception as e:
    print(f"Erreur lors de l'initialisation du producteur Kafka : {e}")
    sys.exit(1)

def ingest_video(video_path, topic, period , date):
    print(f"Vérification du fichier : {video_path}")
    if not os.path.exists(video_path):
        print(f"Erreur : {video_path} n'existe pas")
        sys.exit(1)
    data = {
        "video_path": video_path,
        "period": period,
        "camera": topic.split("_")[0],
        "date"  : date ,  
        "timestamp": f"{date} {period.split('_')[0]}",
        "status": "ready_for_processing"
    }
    print(f"Envoi des données à Kafka : {data}")
    try:
        producer.send(topic, value=data)
        producer.flush()
        print(f"Vidéo {video_path} envoyée à {topic}")
    except Exception as e:
        print(f"Erreur lors de l'envoi à Kafka : {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Erreur : Nombre incorrect d'arguments. Usage : ingestion.py <video_path> <topic> <period>")
        sys.exit(1)
    video_path, topic, period , date = sys.argv[1], sys.argv[2], sys.argv[3] , sys.argv[4]
    ingest_video(video_path, topic, period , date)