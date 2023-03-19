import requests
import pymongo
import re

DATA_COLUMNS = ['ID:', 'NAME:', 'AGE:', 'INSTITUTION:', 'ACTIVITY:', 'COMMENT:']

class Etl:
    def extract(self, url, next_page_token):
        next_page_param = {'pageToken': next_page_token} if next_page_token else {}
        response = requests.get(url, params=next_page_param)

        if response.status_code != 200:
            raise Exception("Error: Request failed with status code {}".format(response.status_code))

        data = response.json()
        if 'nextPageToken' not in data:
            # Last token's response is for an index file. So skip parsing
            return [], ''
        else:
            next_page_token = data['nextPageToken']

        download_urls = [item['mediaLink'] for item in data['items']]
        return download_urls, next_page_token

    def transform(self, download_urls):
        responses = [requests.get(url) for url in download_urls]
        user_information_texts = [r.text for r in responses]
        generation_codes = [self.__get_generation_code(url) for url in download_urls]

        user_records = []
        for generation_code, user_information_text in zip(generation_codes, user_information_texts):
            user_information_fields = user_information_text.split()
            data = {}
            for index, col_name in enumerate(DATA_COLUMNS):
                key = col_name[:-1]
                if index == len(DATA_COLUMNS) - 1:
                    data[key] = self.__get_value(user_information_fields, index, True)
                else:
                    # NAME: anonymizing user name
                    if col_name == 'NAME:':
                        data[key] = 'user-' + self.__get_value(user_information_fields, index - 1)
                    else:
                        data[key] = self.__get_value(user_information_fields, index)
            data['GENERATION'] = generation_code
            user_records.append(data)

        return user_records

    def load(self, collection, records):
        for record in records:
            try:
                existing_doc = collection.find_one({'ID': record['ID']})
                if existing_doc:
                    if not any(item['GENERATION_CODE'] == record['GENERATION'] for item in existing_doc['HISTORY']):
                        age = record['AGE']
                        age_numeric = re.search('\d+', age)
                        if age_numeric:
                            age_numeric = age_numeric.group()
                        else:
                            age_numeric = None

                        update_query = {
                            'GENERATION_CODE': record['GENERATION'],
                            'AGE': age_numeric,
                            'INSTITUTION': record['INSTITUTION'],
                            'ACTIVITY': record['ACTIVITY'],
                            'COMMENT': record['COMMENT']
                        }
                        collection.update_one({'ID': record['ID']}, {'$push': {'HISTORY': update_query }})
                else:
                    age = record['AGE']
                    age_numeric = re.search('\d+', age)
                    if age_numeric:
                        age_numeric = age_numeric.group()
                    else:
                        age_numeric = None

                    collection.insert_one({
                        'ID': record['ID'],
                        'NAME': record['NAME'],
                        'HISTORY': [
                            {
                                'GENERATION_CODE': record['GENERATION'],
                                'AGE': age_numeric,
                                'INSTITUTION': record['INSTITUTION'],
                                'ACTIVITY': record['ACTIVITY'],
                                'COMMENT': record['COMMENT']
                            }
                        ]
                    })
            except pymongo.errors.PyMongoError as e:
                raise Exception("Error: Failed to update MongoDB data: {}".format(e))

        return collection.count_documents({})

    def __get_generation_code(self, url):
        target_key = 'generation='
        return url[url.find(target_key)+len(target_key):].split('&')[0]

    def __get_value(self, user_information_fields, start_column_index, last = False):
        start_index = user_information_fields.index(DATA_COLUMNS[start_column_index])
        if last:
            end_index = -1
        else:
            end_index = user_information_fields.index(DATA_COLUMNS[start_column_index + 1])
        result = user_information_fields[start_index + 1 : end_index]
        return " ".join(result)
