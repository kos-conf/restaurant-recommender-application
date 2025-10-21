import os
import csv

from dotenv import load_dotenv
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField, StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


class UserVisit(object):
    """
    User Visit Record

    Args:
        visit_id (str): Visit id
        user_id (str): User id
        restaurant_id (str): Restaurant id
        restaurant_name (str): Name of the restaurant
        food_items_ordered (str): Food items ordered
        visit_date (str): Visit date
    """
    def __init__(self, visit_id, user_id, restaurant_id, restaurant_name, food_items_ordered, visit_date):
        self.visit_id = visit_id
        self.user_id = user_id
        self.restaurant_id = restaurant_id
        self.restaurant_name = restaurant_name
        self.food_items_ordered = food_items_ordered
        self.visit_date = visit_date


def visit_to_dict(visit, ctx):
    """
    Returns a dict representation of a UserVisit instance for serialization.

    Args:
        visit (UserVisit): UserVisit instance.

        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

    Returns:
        dict: Dict populated with user visit attributes to be serialized.
    """
    return dict(
        visit_id=visit.visit_id,
        user_id=visit.user_id,
        restaurant_id=visit.restaurant_id,
        restaurant_name=visit.restaurant_name,
        food_items_ordered=visit.food_items_ordered,
        visit_date=visit.visit_date
    )


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.
        in this case, msg.key() will return the restaurant id, since, that is set
        as the key in the message.
    """
    if err is not None:
        print("Delivery failed for Visit record {}: {}".format(msg.key(), err))
        return
    print('Visit record with Id {} successfully produced to Topic:{} Partition: [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main():
    topic = 'user_restaurant_visits'
    schema = '../schemas/user_visit.avsc'

    # Load variables from .env file into the environment
    load_dotenv()

    cc_config = {
        'bootstrap.servers': os.getenv("BOOTSTRAP_SERVERS"),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': os.getenv("KAFKA_API_KEY"),
        'sasl.password': os.getenv("KAFKA_API_SECRET"),
    }

    sr_config = {
        'url': os.getenv("SR_ENDPOINT_URL"),
        'basic.auth.user.info': f'{os.getenv("SR_API_KEY")}:{os.getenv("SR_API_SECRET")}'
    }

    with open(f"{schema}") as f:
        schema_str = f.read()

    schema_registry_conf = sr_config
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_serializer = AvroSerializer(schema_registry_client,
                                     schema_str,
                                     visit_to_dict)
    string_serializer = StringSerializer('utf_8')

    producer = Producer(cc_config)

    print("Producing user visit records to topic {}. ^C to exit.".format(topic))

    with open('../csv/user_restaurant_visits.csv', 'r') as f:
        next(f)
        reader = csv.reader(f, delimiter=',')
        for column in reader:
            visit = UserVisit(
                visit_id=column[0],
                user_id=column[1],
                restaurant_id=column[2],
                restaurant_name=column[3],
                food_items_ordered=column[4],
                visit_date=column[5]
            )

            producer.produce(topic=topic,
                             key=string_serializer(str(visit.visit_id), SerializationContext(topic=topic, field=MessageField.KEY)),
                             value=avro_serializer(visit, SerializationContext(topic, MessageField.VALUE)),
                             on_delivery=delivery_report)

    producer.poll(10000)
    producer.flush()


if __name__ == '__main__':
    main()
