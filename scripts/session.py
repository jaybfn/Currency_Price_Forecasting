# This script is using SQLAlchemy, a Python library for working with databases. It has several functions:

"""
            get_engine: This function takes in the details for connecting to a PostgreSQL database 
            (username, password, host, port, and database name),and creates a new database if it 
            doesn't already exist. It then returns an SQLAlchemy engine object.

            get_engine_from_settings: This function imports the settings variable from a separate 
            engine_pass module, and uses it to call the get_engine function. 
            This allows the script to use a separate file for storing the connection details, 
            rather than having them hardcoded in the script.

            get_session: This function calls get_engine_from_settings to get an engine, and then creates 
            and returns a new SQLAlchemy session object, bound to that engine.
            
            The if __name__ == '__main__': block at the end of the script creates a new session and prints it out.

"""

from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.orm import sessionmaker
# import the credential file for connecting to postgresql
from engine_pass import postgresql as settings

def get_engine(user, passwd, host, port, db):

    url = f"postgresql://{user}:{passwd}@{host}:{port}/{db}"

    if not database_exists(url):
        create_database(url)
    engine = create_engine(url, pool_size= 50, echo = False)
    return engine

def get_engine_from_settings():

    keys = ['pguser', 'pgpass', 'host', 'port', 'pgdb']

    if not all (key in keys for key in settings.keys()):
        raise Exception('Bad cofig file')

    return get_engine(settings['pguser'], 
                    settings['pgpass'], 
                    settings['host'], 
                    settings['port'], 
                    settings['pgdb'])

def get_session():

    engine = get_engine_from_settings()
    session = sessionmaker(bind = engine)()
    return session

if __name__ == '__main__':

    session = get_session()
    print(session)
    session.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR, age INTEGER)")
