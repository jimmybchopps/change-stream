from pymongo import MongoClient
import configparser
import logging
import requests

config = configparser.ConfigParser()
config.read('config.ini')

# Get values from the webserver section
solr_url = config.get('Settings', 'SOLR_URL')
mongo_connection = config.get('Settings', 'MONGO_CONNECTION')
mongo_db = config.get('Settings', 'MONGO_DB')
mongo_collection = config.get('Settings', 'MONGO_COLLECTION')
mongo_operations = config.get('Settings', 'OPERATIONS_TO_MONITOR')

# Set up Logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')
file_handler = logging.FileHandler('change-stream.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Set up connection to MongoDB
client = MongoClient(mongo_connection)
db = client[mongo_db]
collection = db[mongo_collection]

# Set up API Calls
update_api = solr_url + '/update/json/docs'

# Create a change stream
change_stream = collection.watch()

# Loop through the change stream and watch for new insert events
for change in change_stream:
    if change['operationType'] == 'insert':
        new_document = change['fullDocument']
        # Post to API and log
        response = requests.post(update_api, json=new_document)
        logger.info('Added document into solr core: {}'.format(new_document))
    else:
        logger.exception('Failed to add document to solr core: {}'.format(
            new_document), exc_info=True)
