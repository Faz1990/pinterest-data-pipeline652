import requests
from time import sleep
import random
import logging
import sqlalchemy
from sqlalchemy import text
import json
import yaml

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

random.seed(100)

class AWSDBConnector:
    def __init__(self):
        with open('db_creds.yaml', 'r') as file:
            creds = yaml.safe_load(file)
        self.HOST = creds['HOST']
        self.USER = creds['USER']
        self.PASSWORD = creds['PASSWORD']
        self.DATABASE = creds['DATABASE']
        self.PORT = creds['PORT']

    def create_db_connector(self):
        try:
            engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
            return engine
        except sqlalchemy.exc.SQLAlchemyError as e:
            logging.error("Database connection failed: %s", e)
            raise

def send_data_to_kinesis(data, stream_name):
    try:
        invoke_url = "https://fqso1f4f05.execute-api.us-east-1.amazonaws.com/beta"
        url = f"{invoke_url}/streams/{stream_name}/record"
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, headers=headers, data=json.dumps(data))
        response.raise_for_status()
        logging.info("Data sent to Kinesis successfully: %s", response.status_code)
    except requests.RequestException as e:
        logging.error("Failed to send data to Kinesis: %s", e)
        return None

def fetch_and_send_data(connection, query, stream_name, transform_function):
    result = connection.execute(query).fetchone()
    if result:
        data_dict = dict(result._mapping)
        payload = {
            "Data": json.dumps(transform_function(data_dict)),
            "PartitionKey": str(random.randint(0, 1000))
        }
        send_data_to_kinesis(payload, stream_name)
        logging.info("Data sent: %s", data_dict)

def transform_pin_data(pin_dict):
    return {
        "index": pin_dict["index"],
        "unique_id": pin_dict["unique_id"],
        "title": pin_dict["title"],
        "description": pin_dict["description"],
        "poster_name": pin_dict["poster_name"],
        "follower_count": pin_dict["follower_count"],
        "tag_list": pin_dict["tag_list"],
        "is_image_or_video": pin_dict["is_image_or_video"],
        "image_src": pin_dict["image_src"],
        "downloaded": pin_dict["downloaded"],
        "save_location": pin_dict["save_location"],
        "category": pin_dict["category"]
    }

def transform_geo_data(geo_dict):
    return {
        "ind": geo_dict["ind"],
        "timestamp": geo_dict["timestamp"].isoformat(),
        "latitude": geo_dict["latitude"],
        "longitude": geo_dict["longitude"],
        "country": geo_dict["country"]
    }

def transform_user_data(user_dict):
    return {
        "ind": user_dict["ind"],
        "first_name": user_dict["first_name"],
        "last_name": user_dict["last_name"],
        "age": user_dict["age"],
        "date_joined": user_dict["date_joined"].isoformat()
    }

def run_infinite_post_data_loop():
    while True:
        try:
            sleep(random.uniform(0, 2))
            random_row = random.randint(0, 11000)
            engine = new_connector.create_db_connector()

            with engine.connect() as connection:
                # Pinterest data
                pin_query = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
                fetch_and_send_data(connection, pin_query, "streaming-0ec6d756577b-pin", transform_pin_data)

                # Geolocation data
                geo_query = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
                fetch_and_send_data(connection, geo_query, "streaming-0ec6d756577b-geo", transform_geo_data)

                # User data
                user_query = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
                fetch_and_send_data(connection, user_query, "streaming-0ec6d756577b-user", transform_user_data)

        except Exception as e:
            logging.error("An error occurred: %s", e)

if __name__ == "__main__":
    new_connector = AWSDBConnector()
    run_infinite_post_data_loop()
