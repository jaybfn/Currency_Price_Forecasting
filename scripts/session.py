from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.orm import sessionmaker
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


session = get_session()
print(session)
