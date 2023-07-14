from pymongo import MongoClient
import configparser
import requests

config = configparser.ConfigParser()
config.read('config.ini')

# Get values from the webserver section
solr_url = config.get('Settings', 'SOLR_URL')
mongo_connection = config.get('Settings', 'MONGO_CONNECTION')
mongo_db = config.get('Settings', 'MONGO_DB')
mongo_collection = config.get('Settings', 'MONGO_COLLECTION')
mongo_operations = config.get('Settings', 'OPERATIONS_TO_MONITOR')


client = MongoClient(mongo_connection)
db = client[mongo_db]
collection = db[mongo_collection]

update_api = solr_url + '/update/json/docs'

# Create a change stream
change_stream = collection.watch()

# Loop through the change stream and watch for new insert events
for change in change_stream:
    if change['operationType'] == 'insert':
        new_document = change['fullDocument']
        # Post to API
        response = requests.post(update_api, json=new_document)

        # Error handling for the API request would go here
        print(response.status_code)
    else:
        print(change['operationType'] + " does not require any updates")