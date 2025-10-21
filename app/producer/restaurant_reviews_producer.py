import os
import csv

from dotenv import load_dotenv
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField, StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


class RestaurantReview(object):
    """
    Restaurant Review Record

    Args:
        restaurant_id (str): Restaurant id
        restaurant_name (str): Name of the restaurant
        user_id (str): User id
        review_text (str): Review text
        food_items_mentioned_in_review (str): Food item reviewed
        rating (str): Rating of the restaurant
    """
    def __init__(self, restaurant_id, restaurant_name, food_items_mentioned_in_review, review_text, user_id, rating):
        self.restaurant_id = restaurant_id
        self.restaurant_name = restaurant_name
        self.user_id = user_id
        self.review_text = review_text
        self.food_items_mentioned_in_review = food_items_mentioned_in_review
        self.rating = rating


def review_to_dict(review, ctx):
    """
    Returns a dict representation of a RestaurantReview instance for serialization.

    Args:
        review (RestaurantReview): RestaurantReview instance.

        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

    Returns:
        dict: Dict populated with restaurant review attributes to be serialized.
    """
    return dict(
        restaurant_id=review.restaurant_id,
        restaurant_name=review.restaurant_name,
        food_items_mentioned_in_review=review.food_items_mentioned_in_review,
        review_text=review.review_text,
        user_id=review.user_id,
        rating=review.rating
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
        print("Delivery failed for Review record {}: {}".format(msg.key(), err))
        return
    print('Review record with Id {} successfully produced to Topic:{} Partition: [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main():
    topic = 'restaurant_reviews'
    schema = '../schemas/restaurant_review.avsc'

    # Load variables from .env file into the environment
    load_dotenv()
    cc_config = {
        'bootstrap.servers': os.getenv("BOOTSTRAP_SERVERS"),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': os.getenv("KAFKA_API_KEY"),
        'sasl.password': os.getenv("KAFKA_API_SECRET")
    }

    sr_config = {
        'url': os.getenv("SR_ENDPOINT_URL"),
        'basic.auth.user.info': f'{os.getenv("SR_API_KEY")}:f{os.getenv("SR_API_SECRET")}'
    }

    with open(f"{schema}") as f:
        schema_str = f.read()

    schema_registry_conf = sr_config
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_serializer = AvroSerializer(schema_registry_client,
                                     schema_str,
                                     review_to_dict)
    string_serializer = StringSerializer('utf_8')

    producer = Producer(cc_config)

    print("Producing restaurant review records to topic {}. ^C to exit.".format(topic))

    with open('../csv/restaurant_reviews.csv', 'r') as f:
        next(f)
        reader = csv.reader(f, delimiter=',')
        for column in reader:
            review = RestaurantReview(
                restaurant_id=column[0],
                restaurant_name=column[1],
                user_id=column[2],
                review_text=column[3],
                food_items_mentioned_in_review=column[4],
                rating=column[5]
            )

            producer.produce(topic=topic,
                             key=string_serializer(str(review.restaurant_id), SerializationContext(topic=topic, field=MessageField.KEY)),
                             value=avro_serializer(review, SerializationContext(topic, MessageField.VALUE)),
                             on_delivery=delivery_report)

    producer.poll(10000)
    producer.flush()


if __name__ == '__main__':
    main()
