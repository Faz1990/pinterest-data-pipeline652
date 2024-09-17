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

new_connector = AWSDBConnector()

def run_infinite_post_data_loop():
    while True:
        try:
            sleep(random.uniform(0, 2))  
            random_row = random.randint(0, 11000)
            engine = new_connector.create_db_connector()

            with engine.connect() as connection:
                # Pinterest Data
                pin_query = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
                pin_selected_row = connection.execute(pin_query)
                for row in pin_selected_row:
                    pin_result = dict(row._mapping)
                    pin_payload = json.dumps({
                        "StreamName": "streaming-12b83b649269-pin",  
                        "Data": {
                            "index": pin_result["index"],
                            "unique_id": pin_result["unique_id"],
                            "title": pin_result["title"],
                            "description": pin_result["description"],
                            "poster_name": pin_result["poster_name"],
                            "follower_count": pin_result["follower_count"],
                            "tag_list": pin_result["tag_list"],
                            "is_image_or_video": pin_result["is_image_or_video"],
                            "image_src": pin_result["image_src"],
                            "downloaded": pin_result["downloaded"],
                            "save_location": pin_result["save_location"],
                            "category": pin_result["category"]
                        },
                        "PartitionKey": "pin_partition1"
                    })
                    logging.info(f"Pin Payload: {pin_payload}")

                # Geolocation Data
                geo_query = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
                geo_selected_row = connection.execute(geo_query)
                for row in geo_selected_row:
                    geo_result = dict(row._mapping)
                    geo_payload = json.dumps({
                        "StreamName": "streaming-12b83b649269-geo", 
                        "Data": {
                            "ind": geo_result["ind"],
                            "timestamp": geo_result["timestamp"].isoformat(),
                            "latitude": geo_result["latitude"],
                            "longitude": geo_result["longitude"],
                            "country": geo_result["country"]
                        },
                        "PartitionKey": "geo_partition1"
                    })
                    logging.info(f"Geo Payload: {geo_payload}")

                # User Data
                user_query = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
                user_selected_row = connection.execute(user_query)
                for row in user_selected_row:
                    user_result = dict(row._mapping)
                    user_payload = json.dumps({
                        "StreamName": "streaming-12b83b649269-user", 
                        "Data": {
                            "ind": user_result["ind"],
                            "first_name": user_result["first_name"],
                            "last_name": user_result["last_name"],
                            "age": user_result["age"],
                            "date_joined": user_result["date_joined"].isoformat()
                        },
                        "PartitionKey": "user_partition1"
                    })
                    logging.info(f"User Payload: {user_payload}")

                # Sending data to Kinesis
                headers = {'Content-Type': 'application/json'}
                pin_response = requests.request("PUT", "https://p5k3s07dwl.execute-api.us-east-1.amazonaws.com/beta/streams/streaming-12b83b649269-pin/record", headers=headers, data=pin_payload)
                geo_response = requests.request("PUT", "https://p5k3s07dwl.execute-api.us-east-1.amazonaws.com/beta/streams/streaming-12b83b649269-geo/record", headers=headers, data=geo_payload)
                user_response = requests.request("PUT", "https://p5k3s07dwl.execute-api.us-east-1.amazonaws.com/beta/streams/streaming-12b83b649269-user/record", headers=headers, data=user_payload)

                # Log the responses
                logging.info(f"Pin Response: {pin_response.status_code}")
                logging.info(f"Geo Response: {geo_response.status_code}")
                logging.info(f"User Response: {user_response.status_code}")

        except Exception as e:
            logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    run_infinite_post_data_loop()
