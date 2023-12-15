from sqlalchemy import create_engine
from sqlalchemy.sql import text
import yaml
from yaml.loader import SafeLoader


class DatabaseConnector:
    """connects to db"""
    def read_db_creds(self, creds_file_path):
        """reads db_creds and returns data"""
        # open file and return
        with open(creds_file_path, 'r') as fp:
            data = yaml.load(fp, Loader=SafeLoader)
            return data
        
    def init_db_engine(self, creds_file_path):
        """takes data and returns engine"""
        creds = self.read_db_creds(creds_file_path)
        db_url = f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        engine = create_engine(db_url)
        return engine

    def list_db_tables(self, engine): 
        """returns a list of DB tables found by the engine"""
        with engine.connect() as connection:
            query = text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            result = connection.execute(query)
            tables = [row[0] for row in result]
            return tables
        
    def upload_to_db(self, df, table_name):
        """uploads df to postgresql database"""
        self.table_name = table_name
        self.df = df

        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = 'host' # replace with your host
        USER = 'username' # replace with your pgadmin database username
        PASSWORD = 'password' # replace with your pgadmin database password 
        DATABASE = 'name' # replace with your database name on pgadmin
        PORT = 5432
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
       
        self.df.to_sql(table_name, engine, if_exists='replace', index=False)
        