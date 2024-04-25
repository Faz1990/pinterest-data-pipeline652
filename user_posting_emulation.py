import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
from datetime import datetime


random.seed(100)


class AWSDBConnector:

    def __init__(self):

        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError ("Type not serializable")

def send_data_to_api(topic_suffix, data):
    base_url = "https://fqso1f4f05.execute-api.us-east-1.amazonaws.com/beta/topics"
    topic_url = f"{base_url}/{topic_suffix}"
    headers = {'Content-Type': 'application/json'} 
    try:
        response = requests.post(topic_url, data=json.dumps(data, default=json_serial), headers=headers)
        if response.status_code == 200:
            print(f"Successfully sent data to {topic_suffix} topic.")
        else:
            print(f"Failed to send data to {topic_suffix} topic. Status code: {response.status_code}")
    except Exception as e:
        print(f"An error occurred: {e}")



def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            for table_name, topic_suffix in [("pinterest_data", "0ec6d756577b.pin"), ("geolocation_data", "0ec6d756577b.geo"), ("user_data", "0ec6d756577b.user")]:
                query = text(f"SELECT * FROM {table_name} LIMIT {random_row}, 1")
                result = connection.execute(query).fetchone()
                if result:
                    data = result._asdict() 
                    topic = topic_suffix
                    send_data_to_api(topic, data)
                    print(f"Sent data from {table_name} to {topic}")


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    


