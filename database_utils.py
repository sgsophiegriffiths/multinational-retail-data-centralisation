import yaml
from yaml.loader import SafeLoader
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import pandas as pd
 
"""connects to db"""
class DatabaseConnector:
    """reads db_creds and returns data"""
    def read_db_creds(self, creds_file_path):
        # open file and return
        with open(creds_file_path, 'r') as fp:
            data = yaml.load(fp, Loader=SafeLoader)
            return data
        
    """takes data and returns engine"""
    def init_db_engine(self, creds_file_path):
        creds = self.read_db_creds(creds_file_path)
        db_url = f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        engine = create_engine(db_url)
        return engine

    """returns a list of DB tables found by the engine"""
    def list_db_tables(self, engine): 
        with engine.connect() as connection:
            query = text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            result = connection.execute(query)
            tables = [row[0] for row in result]
            return tables
        
    """uploads df to postgresql database"""
    def upload_to_db(self, df, table_name):
        self.table_name = table_name
        self.df = df

        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = 'my-first-rds-db.cqlhb8lnh8vd.eu-north-1.rds.amazonaws.com'
        USER = 'postgres_un_Soph'
        PASSWORD = 'Psg93671!'
        DATABASE = 'sales_data'
        PORT = 5432
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        # connection = self.engine.connect()

        # df = pd.read_csv('database_post_cleaning_11Nov.csv')
        self.df.to_sql(table_name, engine, if_exists='replace', index=False)
        # try:
            # Connect to the database
            # connection = self.engine.connect()

            # Convert DataFrame to SQL table
        #     dataframe.to_sql(name='dim_users', con=connection, if_exists='replace', index=False)

        #     print(f"Data uploaded successfully to table: {table_name}")

        # except Exception as e:
        #     print(f"Error uploading data to table {table_name}: {str(e)}")

        # finally:
        #     # Close the database connection
        #     connection.close()

# cleaner = DataCleaning()
# db_connector = DatabaseConnector()
# db_connector.upload_to_db(cleaner.df, 'dim_users')
# db_connector.upload_to_db('dim_users')
# # call class and method
# db_connector = DatabaseConnector()
# creds = db_connector.read_db_creds("db_creds.yaml")
# engine = db_connector.init_db_engine("db_creds.yaml")
# tables = db_connector.list_db_tables(engine)


