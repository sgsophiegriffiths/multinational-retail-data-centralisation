
from database_utils import DatabaseConnector
import pandas as pd 
import tabula
import requests
import boto3
from io import StringIO


class DataExtractor:
    def __init__(self):
        self.connector = DatabaseConnector()
        self.db_engine = self.connector.init_db_engine("db_creds.yaml")
        self.tables = self.connector.list_db_tables(self.db_engine)
        self.s3_client = boto3.client('s3')
        

    """uses engine to extract database table and returns pandas dataframe"""
    def read_rds_table(self):
        # SQL query to select data from a table
        query = f"SELECT * FROM {self.tables[2]}"
        df = pd.read_sql_query(query, self.db_engine)
        return df

    """take link and return pandas df"""
    def retrieve_pdf_data(self, pdf_link):
        # Use tabula to extract tables from the PDF
        tables = tabula.read_pdf(pdf_link, pages='all', multiple_tables=True)

        # Concatenate all tables into a single DataFrame
        df_card_details = pd.concat(tables, ignore_index=True)
        return df_card_details

    """lists number of stores from the API"""
    def list_number_of_stores(self, number_of_stores_endpoint, header_dict):
        base_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/'
        url = base_url + number_of_stores_endpoint
        response = requests.get(url, headers=header_dict)

        if response.status_code == 200:
            return response.json()
        else:
            # Handle error cases
            print(f"Error: {response.status_code}")
            return None
    
    """take the store_endpoint as an argument and extract all the stores from the API saving them in a pandas DataFrame"""
    def retrieve_stores_data(self, number_of_stores, store_endpoint, header_dict):
        base_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/'
        url = base_url + store_endpoint
        # response = requests.get(url, headers=header_dict)

        all_stores_data = []
        
        # self.number_of_stores = self.list_number_of_stores(number_of_stores_endpoint, header_dict)
        number_stores = number_of_stores.get("number_stores")

            # Loop through all store numbers(put in number_stores value instead of typing 451)
        for store_number in range(1, number_stores):
                store_url = url.format(store_number=store_number)
                store_response = requests.get(store_url, headers=header_dict)

                if store_response.status_code == 200:
                    store_data = store_response.json()# ['store_data']
                    all_stores_data.append(store_data)
                else:
                    print(f"Error fetching data for store {store_number}: {store_response.status_code}")

            # Create a DataFrame from the store data
        if all_stores_data:
                df_store_data = pd.DataFrame(all_stores_data)
                return df_store_data
        else:
                print("No store data found.")
                return None
        
    """download and extract csv from s3, return pandas df"""
    def extract_from_s3(self, s3_address):
        # split to get bucket name and object key
        s3_parts = s3_address.replace("s3://", "").split("/")
        bucket_name = s3_parts[0]
        object_key = "/".join(s3_parts[1:])
        
        # Create an S3 client
        s3 = boto3.client('s3')
        
        # Download csv file from S3
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        csv_content = response['Body'].read().decode('utf-8')
        
        # Convert csv to a pandas df
        df_products = pd.read_csv(StringIO(csv_content))
        
        return df_products

        
    """download and extract json from s3, return pandas df"""
    def extract_json_from_s3(self, url):
        # # split to get bucket name and object key
        # s3_parts = s3_address.replace("s3://", "").split("/")
        # bucket_name = s3_parts[0]
        # object_key = "/".join(s3_parts[1:])
        
        # # Create an S3 client
        # s3 = boto3.client('s3')
        
        # # Download json file from S3
        # response = s3.get_object(Bucket=bucket_name, Key=object_key)
        # csv_content = response['Body'].read().decode('utf-8')
        
        # # Convert csv to a pandas df
        # # df_products = pd.read_csv(StringIO(csv_content))

        # #covert json to pandas df
        # df_products = pd.read_json()

        response = requests.get(url)

        with open("date_details.json", "wb") as file:
            file.write(response.content)
            
        df_date = pd.read_json("date_details.json")
        
        return df_date
        







# extractor = DataExtractor()
# pdf_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
# df_card_details = extractor.retrieve_pdf_data(pdf_link)

# Display the DataFrame
# print(df_card_details)


