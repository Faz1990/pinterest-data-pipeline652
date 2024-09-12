import requests
import json

# Define the API endpoint and headers
url = "https://fqso1f4f05.execute-api.us-east-1.amazonaws.com/beta/streams/streaming-0ec6d756577b-pin/record"
headers = {'Content-Type': 'application/json'}

# Define the payload
payload = {
    "Data": json.dumps({"key": "value"}),  # Example payload
    "PartitionKey": "test"
}

# Send the POST request to the API endpoint
try:
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    response.raise_for_status()  # Raise an exception for HTTP errors
    print("Response status code:", response.status_code)
    print("Response body:", response.json())
except requests.RequestException as e:
    print(f"Error: {e}")
