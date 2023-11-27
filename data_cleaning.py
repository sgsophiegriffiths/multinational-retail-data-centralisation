import pandas as pd 
import re

"""cleans the data, dropping NaN values and replacing some incorrectly types values"""
class DataCleaning():
    def __init__(self):
        pass

    """takes df and calls all other needed methods"""
    def clean_user_data(self,df):
        self.df = df
        self.df = self.clean_type(self.df)
        self.df = self.clean_index(self.df)
        self.df.dropna(inplace = True)
        self.df = self.clean_date_of_birth(self.df)
        self.df = self.clean_join_date(self.df)
        # self.df = self.clean_country(self.df)
        # self.df = self.clean_country_code(self.df)
        # self.df = self.clean_phone_numbers(self.df)
        return self.df

    """Changes types"""
    def clean_type(self, df):
        self.df = df
        self.df.first_name = self.df.first_name.astype('string')
        self.df.last_name = self.df.last_name.astype('string')
        self.df.email_address = self.df.email_address.astype('string')
        self.df.address = self.df.address.astype('string')
        self.df.company = self.df.company.astype('string')
        self.df.dropna(inplace = True)
        return self.df

    """drops incorrect index column"""
    def clean_index(self, df):
        self.df = df  
        self.df = self.df.drop(columns=['index'])
        return self.df
    
    """change DOB to datetime and drop incorrect values"""
    def clean_date_of_birth(self, df):
        self.df = df
        self.df['date_of_birth'] = pd.to_datetime(self.df['date_of_birth'], format='mixed', errors='coerce')
        self.df.dropna(inplace = True)
        return self.df
    
    """change join_date to datetime and drops rows with incorrect values"""
    def clean_join_date(self, df):
        self.df= df
        self.df['join_date'] = pd.to_datetime(self.df['join_date'], format='mixed', errors='coerce')
        self.df.dropna(inplace = True)
        return self.df

    """drops rows where country isnt DE, GB or US"""
    def clean_country(self, df):
        self.df = df
        lst = ['Germany', 'United Kingdom', 'United States']
        self.df = self.df[self.df['country'].isin(lst)]
        return self.df

    """drop rows where country_code isnt DE, GB or US"""
    def clean_country_code(self, df):
        self.df = df
        lst = ['DE', 'GB', 'US']
        self.df = self.df[self.df['country_code'].isin(lst)]
        return self.df

    """check valid phone numbers using regex"""
    def check_phone_number(self, phone_number):
        pattern_uk = r'^(?:(?:\(?(?:0(?:0|11)\)?[\s-]?\(?|\+)44\)?[\s-]?(?:\(?0\)?[\s-]?)?)|(?:\(?0))(?:(?:\d{5}\)?[\s-]?\d{4,5})|(?:\d{4}\)?[\s-]?(?:\d{5}|\d{3}[\s-]?\d{3}))|(?:\d{3}\)?[\s-]?\d{3}[\s-]?\d{3,4})|(?:\d{2}\)?[\s-]?\d{4}[\s-]?\d{4}))(?:[\s-]?(?:x|ext\.?|\#)\d{3,4})?$' #Our regular expression to match
        pattern_usa = r'^(\+1\s?)?(\(\d{3}\)\s?|\d{3}[-.\s]?)\d{3}[-.\s]?\d{4}$'
        pattern_germany = r'^\+49\s?(\(0\)\s?|\d{2}[-.\s]?)\d{3,4}[-.\s]?\d{4,5}$'

        if re.match(pattern_uk, phone_number):
            return 'GB'
        elif re.match(pattern_usa, phone_number):
            return 'US'
        elif re.match(pattern_germany, phone_number):
            return 'DE'
        else:
            return 'Other'

    """call check_phone_number and then drop invalid rows"""       
    def clean_phone_numbers(self, df):
        self.df = df
        self.df['country_to_check'] = self.df['phone_number'].apply(self.check_phone_number)
        # Drop rows where the country is 'Other' and then drop column
        self.df = self.df.dropna(subset=['country_to_check'])
        self.df = self.df[self.df['country_to_check'] != 'Other']
        self.df = self.df.drop(columns=['country_to_check'])
        return self.df
    
    # CARD HERE #
    # CARD HERE #
    # CARD HERE #
    # CARD HERE #

    """drop NaN and call date_payment_confirmed"""
    def clean_card_data(self, df_card_details):
        self.df_card_details = df_card_details
        self.df_card_details.dropna(inplace = True)
        self.df_card_details.card_number = self.df_card_details.card_number.astype('string')
        self.df_card_details['card_number'] = self.df_card_details['card_number'].str.replace('?', '')
        self.df_card_details = self.clean_date_payment_confirmed(self.df_card_details)
        return self.df_card_details

    """change date_payment_confirmed to datetime"""
    def clean_date_payment_confirmed(self, df_card_details):
        self.df_card_details = df_card_details
        self.df_card_details['date_payment_confirmed'] = pd.to_datetime(self.df_card_details['date_payment_confirmed'], format='mixed', errors='coerce')
        self.df_card_details.dropna(inplace = True)
        return self.df_card_details
    
    # STORE HERE #
    # STORE HERE #
    # STORE HERE #
    # STORE HERE #

    """call all other needed methods"""
    def clean_store_data(self, df_store_data):
        self.df_store_data = df_store_data
        self.df_store_data = self.clean_opening_date(self.df_store_data)
        self.df_store_data = self.clean_continent(self.df_store_data)
        self.df_store_data = self.clean_lat(self.df_store_data)
        # self.df_store_data = self.clean_types_stores(self.df_store_data)
        return self.df_store_data 

    """change opening_date to datetime"""
    def clean_opening_date(self, df_store_data):
        self.df_store_data= df_store_data
        self.df_store_data['opening_date'] = pd.to_datetime(self.df_store_data['opening_date'], format='mixed', errors='coerce')
        self.df_store_data.dropna(subset=['opening_date'], inplace=True)
        return self.df_store_data
    
    """replace incorrect continents"""
    def clean_continent(self, df_store_data):
        self.df_store_data = df_store_data
        self.df_store_data = self.df_store_data.replace('eeEurope','Europe')
        self.df_store_data = self.df_store_data.replace('eeAmerica','America')
        return self.df_store_data

    """remove lat column"""
    def clean_lat(self, df_store_data):
        self.df_store_data = df_store_data
        self.df_store_data = self.df_store_data.drop(['lat'], axis=1)
        return self.df_store_data
    
    """change types and drop index"""
    def clean_types_stores(self, df_store_data):
        self.df_store_data = df_store_data
        self.df_store_data.longitude = pd.to_numeric(self.df_store_data.longitude, errors='coerce')
        self.df_store_data.latitude = pd.to_numeric(self.df_store_data.latitude, errors='coerce')
        self.df_store_data.address = self.df_store_data.address.astype('string')
        self.df_store_data.staff_numbers = pd.to_numeric(self.df_store_data.staff_numbers, errors='coerce')
        self.df_store_data.dropna(subset=['staff_numbers'], inplace = True)
        self.df_store_data.locality = self.df_store_data.locality.astype('string')
        self.df_store_data = self.df_store_data.drop(['index'], axis=1)
        self.df_store_data = self.df_store_data.drop(['Unnamed: 0'], axis=1)
        self.df_store_data = self.df_store_data.reset_index(drop=True)
        return self.df_store_data
    

    # PRODUCTS HERE #
    # PRODUCTS HERE #
    # PRODUCTS HERE #

    """convert all product weights to kg"""
    def convert_product_weights(self, df_products):
        self.df_products = df_products
        
        # Function to clean and convert weights
        def convert_weight(weight):
            try:
                # Check if the weight is already in kilograms
                if 'kg' in weight:
                    # Convert to float and remove characters
                    weight = float(re.search(r'\d+\.*\d*', str(weight)).group())
                else:
                    # Handle cases like '8 x 85g', '40 x 100g', etc.
                    if 'x' in weight:
                        parts = weight.split('x')
                        weight = float(parts[0]) * float(''.join(filter(str.isdigit, parts[1]))) / 1000.0
                    else:
                        # Remove excess characters, divide by 1000 and convert to float
                        weight = float(''.join(filter(str.isdigit, str(weight)))) / 1000.0
                
                return weight
            except (ValueError, TypeError, AttributeError):
                # Handle cases where conversion is not possible
                return None
        
        # call convert_weight function 
        df_products['weight'] = df_products['weight'].apply(convert_weight)
        return df_products

    """call all cleaning methods for products"""
    def clean_products_data(self, df_products):
        self.df_products = df_products
        # self.df_products = self.clean_date_added(self.df_products)
        # self.df_products = self.clean_types(self.df_products)
        return df_products

    """convert date_added to datetime"""
    def clean_date_added(self, df_products):
        self.df_products = df_products
        self.df_products['date_added'] = pd.to_datetime(self.df_products['date_added'], format='mixed', errors='coerce')
        self.df_products.dropna(subset=['date_added'], inplace=True)
        self.df_products.dropna(inplace = True)
        return self.df_products

    """convert column types"""
    def clean_types(self, df_products):
        self.df_products = df_products
        self.df_products.product_name = self.df_products.product_name.astype('string')
        self.df_products.category = self.df_products.category.astype('string')
        self.df_products.removed = self.df_products.removed.astype('string')
        self.df_products.uuid = self.df_products.uuid.astype('string')
        self.df_products.EAN = self.df_products.EAN.astype('string')
        self.df_products.product_code = self.df_products.product_code.astype('string')
        self.df_products['product_price'] = pd.to_numeric(self.df_products['product_price'].str.replace('£', ''), errors='coerce').astype('float')
        self.df_products.dropna(inplace = True)
        self.df_products = self.df_products.reset_index(drop=True)
        self.df_products = self.df_products.drop('Unnamed: 0', axis='columns')
        return self.df_products

    # ORDERS HERE #
    # ORDERS HERE #
    # ORDERS HERE #

    """drop not needed columns from df_orders"""
    def clean_orders_data(self, df_orders):
        self.df_orders = df_orders
        self.df_orders = self.df_orders.drop('first_name', axis='columns')
        self.df_orders = self.df_orders.drop('last_name', axis='columns')
        self.df_orders = self.df_orders.drop('1', axis='columns')
        self.df_orders = self.df_orders.drop('index', axis='columns')
        self.df_orders = self.df_orders.drop('level_0', axis='columns')
        return self.df_orders

    # TIME HERE #
    # TIME HERE #
    # TIME HERE #

    """clean df_date by changing column types"""
    def clean_date_data(self, df_date):
        self.df_date = df_date
        # Convert the 'timestamp' column to datetime type
        self.df_date['timestamp'] = pd.to_datetime(self.df_date['timestamp'], format='mixed', errors='coerce')

        # Extract the time part from the 'timestamp' column
        self.df_date['time'] = self.df_date['timestamp'].dt.time

        # Drop the original 'timestamp' column
        self.df_date = self.df_date.drop('timestamp', axis=1)

        self.df_date.month = pd.to_numeric(self.df_date.month, errors='coerce')
        self.df_date.year = pd.to_numeric(self.df_date.year, errors='coerce')
        self.df_date.day = pd.to_numeric(self.df_date.day, errors='coerce')
        self.df_date.date_uuid = self.df_date.date_uuid.astype('string')
        lst = ['Evening', 'Morning', 'Midday', 'Late_Hours']
        self.df_date = self.df_date[self.df_date['time_period'].isin(lst)]
        self.df_date = self.df_date.reset_index(drop=True)

        return self.df_date




