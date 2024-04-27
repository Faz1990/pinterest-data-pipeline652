import requests
from time import sleep
import random
import logging
import sqlalchemy
from sqlalchemy import text
import json

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

random.seed(100)

class AWSDBConnector:
    def __init__(self):
        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306

    def create_db_connector(self):
        try:
            engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
            return engine
        except sqlalchemy.exc.SQLAlchemyError as e:
            logging.error("Database connection failed: %s", e)
            raise

def send_data_to_api(data, topic, payload):
    try:
        invoke_url = "https://fqso1f4f05.execute-api.us-east-1.amazonaws.com/beta"
        url = f"{invoke_url}/topics/{topic}"
        headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
        response = requests.post(url, headers=headers, data=payload)
        response.raise_for_status()
        logging.info("Data sent to API successfully: %s", response.status_code)
    except requests.RequestException as e:
        logging.error("Failed to send data to API: %s", e)
        return None

def run_infinite_post_data_loop():
    while True:
        try:
            sleep(random.randrange(0, 2))
            random_row = random.randint(0, 11000)
            engine = new_connector.create_db_connector()

            with engine.connect() as connection:
                # Fetch and send Pinterest data
                pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
                pin_result = connection.execute(pin_string).fetchone()
                if pin_result:
                    pin_dict = dict(pin_result._mapping)
                    pin_payload = json.dumps({
                        "records": [
                            {
                                "value": {
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
                            }
                        ]
                    })
                    send_data_to_api(pin_dict, "0ec6d756577b.pin", pin_payload)
                    logging.info("Pinterest data sent: %s", pin_dict['unique_id'])

                # Fetch and send Geolocation data
                geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
                geo_result = connection.execute(geo_string).fetchone()
                if geo_result:
                    geo_dict = dict(geo_result._mapping)
                    geo_payload = json.dumps({
                        "records": [
                            {
                                "value": {
                                    "ind": geo_dict["ind"],
                                    "timestamp": geo_dict["timestamp"].isoformat(),
                                    "latitude": geo_dict["latitude"],
                                    "longitude": geo_dict["longitude"],
                                    "country": geo_dict["country"]
                                }
                            }
                        ]
                    })
                    send_data_to_api(geo_dict, "0ec6d756577b.geo", geo_payload)
                    logging.info("Geolocation data sent: %s", geo_dict['ind'])

                # Fetch and send User data
                user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
                user_result = connection.execute(user_string).fetchone()
                if user_result:
                    user_dict = dict(user_result._mapping)
                    user_payload = json.dumps({
                        "records": [
                            {
                                "value": {
                                    "ind": user_dict["ind"],
                                    "first_name": user_dict["first_name"],
                                    "last_name": user_dict["last_name"],
                                    "age": user_dict["age"],
                                    "date_joined": user_dict["date_joined"].isoformat()
                                }
                            }
                        ]
                    })
                    send_data_to_api(user_dict, "0ec6d756577b.user", user_payload)
                    logging.info("User data sent: %s", user_dict['ind'])

        except Exception as e:
            logging.error("An error occurred: %s", e)

if __name__ == "__main__":
    new_connector = AWSDBConnector()
    run_infinite_post_data_loop()