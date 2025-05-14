from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField, StringSerializer
from pydantic import BaseModel
import json
import threading
import time
import hashlib
from datetime import datetime
import uuid
import os

app = FastAPI()

# Allow CORS for local development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

KAFKA_CONFIG = {
    'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'UFBQII2GABZX2WP6',
    'sasl.password': 'TCcljEy0zUPW0jXHvZ7F2dmnFdYM9gi4qD/f/XStPqwT3m7ejpZkSjM9Mk9V874D',
    'group.id': 'fastapi-backend-group2',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
}

SCHEMA_REGISTRY_CONFIG = {
    'url': 'https://psrc-l6oz3.us-east-2.aws.confluent.cloud',
    'basic.auth.user.info': 'FIV6SF2KVDL3JWQD:h9FyumGZmY4kWI2orSCD1NRHFkHAFsAxfX+DRMlpEZxTUnxOKWWKSZt/wQwoGxYk'
}

TOPICS = {
    "recommendation_results": "recommendation_results"
}

# Global state for topic data
global_topic_data = {key: [] for key in TOPICS}

# Schema Registry client (shared)
schema_registry_client = SchemaRegistryClient(SCHEMA_REGISTRY_CONFIG)

def deduplicate_messages(messages):
    seen = set()
    deduped = []
    for msg in messages:
        # Use a hash of the stringified message as a unique identifier
        msg_hash = hashlib.sha256(str(msg).encode('utf-8')).hexdigest()
        if msg_hash not in seen:
            seen.add(msg_hash)
            deduped.append(msg)
    return deduped

# Background consumer thread for each topic
def kafka_topic_updater(topic_key, topic_name, value_format='avro', poll_interval=1.0, max_messages=50):
    subject = f"{topic_name}-value"
    schema_obj = schema_registry_client.get_latest_version(subject)
    schema_str = schema_obj.schema.schema_str
    avro_deserializer = AvroDeserializer(schema_str=schema_str, schema_registry_client=schema_registry_client)

    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([topic_name])
    messages = []
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                time.sleep(poll_interval)
                continue
            val = msg.value()
            if value_format == 'avro':
                try:
                    context = SerializationContext(topic_name, MessageField.VALUE)
                    val = avro_deserializer(val, context)
                except Exception as e:
                    print(f"Avro deserialization error: {e}")
                    continue
            elif value_format == 'json':
                try:
                    val = json.loads(val.decode('utf-8'))
                except Exception:
                    continue
            elif value_format == 'string':
                val = val.decode('utf-8')
            messages.append(val)
            if len(messages) > max_messages:
                messages = messages[-max_messages:]
            # Deduplicate before updating global state
            global_topic_data[topic_key] = deduplicate_messages(messages)
    finally:
        consumer.close()

# Start background threads for all topics
def start_all_topic_threads():
    for key, topic in TOPICS.items():
        t = threading.Thread(target=kafka_topic_updater, args=(key, topic), daemon=True)
        t.start()

# Start threads on server startup
@app.on_event("startup")
def startup_event():
    start_all_topic_threads()

# Use the correct schema and topic for user recommendation requests
RECO_SCHEMA_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../schemas/user_recommendation_requests.avsc'))
RECO_TOPIC = "user_recommendation_requests"

print(f"Current working directory: {os.getcwd()}")
print(f"Schema absolute path: {RECO_SCHEMA_PATH}")

# Load the schema string
with open(RECO_SCHEMA_PATH) as f:
    reco_schema_str = f.read()

schema_registry_client = SchemaRegistryClient(SCHEMA_REGISTRY_CONFIG)
reco_avro_serializer = AvroSerializer(schema_registry_client, reco_schema_str, lambda obj, ctx: obj)
reco_string_serializer = StringSerializer('utf_8')
reco_producer = Producer(KAFKA_CONFIG)

class RecommendationRequest(BaseModel):
    desired_food_items: str

@app.post("/chat/send")
def send_recommendation_request(req: RecommendationRequest):
    # Generate random user_id and request_id
    user_id = str(uuid.uuid4())
    request_id = str(uuid.uuid4())
    msg_dict = {
        "request_id": request_id,
        "user_id": user_id,
        "desired_food_items": req.desired_food_items
    }
    reco_producer.produce(
        topic=RECO_TOPIC,
        key=reco_string_serializer(user_id, SerializationContext(RECO_TOPIC, MessageField.KEY)),
        value=reco_avro_serializer(msg_dict, SerializationContext(RECO_TOPIC, MessageField.VALUE)),
    )
    reco_producer.flush()
    return {"status": "sent", "user_id": user_id, "request_id": request_id}

@app.get("/chat/messages")
def get_chat_messages():
    return global_topic_data["recommendation_results"]

@app.get("/chat/message/{request_id}")
def get_message_by_request_id(request_id: str):
    messages = global_topic_data["recommendation_results"]
    for msg in messages:
        if msg.get("request_id") == request_id:
            return msg.get("recommended_restaurants_and_explanation")
    raise HTTPException(status_code=404, detail="Message not found for the given request_id") 