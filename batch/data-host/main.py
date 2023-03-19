import etl
from pymongo import MongoClient
import threading
import logging

BASE_URL = "" # API URL : If you want to use this ETL pipeline for other API, you can change this value
MONGO_CONNECTION_URI = "mongodb://deteam:1234@mongodb:27017/?authMechanism=DEFAULT&authSource=healthcare"
MAX_PAGE = 20

logging.basicConfig(filename='logfile.txt', level=logging.INFO, format='%(asctime)s %(message)s')

def log(message):
    logging.info(message)

def transform_and_load(data, page_count):
    log("Transform phase Started(page: " + str(page_count) + ")")
    transformed_data = etl.transform(data)
    log("Transform phase Ended(page: " + str(page_count) + ")")

    log("Load phase Started(page: " + str(page_count) + ")")
    doc_count = etl.load(collection, transformed_data)
    log("Data inserted sucessfully. Total number of documents:" + str(doc_count))
    log("Load phase Ended(page: " + str(page_count) + ")")

try:
    log("ETL pipeline will start. connect DB")
    with MongoClient(MONGO_CONNECTION_URI) as client:
        db = client["healthcare"]
        collection = db["user"]

        etl = etl.Etl()

        next_page_token = ''
        page_count = 1
        pages = []
        target_raw_data_bundles = []
        while page_count <= MAX_PAGE:
            log("Extract phase Started(page: " + str(page_count) + ")")
            # You can retrieve the `nextPageToken` from the result and append `?pageToken=<nextPageToken>` to the URL to retrieve the next page.
            extracted_data, next_page_token = etl.extract(BASE_URL, next_page_token)
            if not extracted_data:
                log("page " + str(page_count) + " is not for extraction. Extract phase will end")
                break

            target_raw_data_bundles.append(extracted_data)
            pages.append(page_count)
            page_count += 1
            log("Extract phase Ended(page: " + str(page_count) + ")")

        threads = []
        for idx, data in enumerate(target_raw_data_bundles):
            thread = threading.Thread(target=transform_and_load, args=(data, pages[idx]))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        client.close()
        log("ETL pipeline finished")
except Exception as e:
    print("Error: ETL batch failed: {}".format(e))
