import configparser
import psycopg2

def config(filename='./dags/files/pipeline.conf', section='postgres-fitbit'):
    parser = configparser.ConfigParser()
    parser.read(filename)
    db ={}

    if parser.has_section(section):
        params = parser.items(section)

        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the config file.'.format(section))

    return db

def create_connection():
    params = config()
    print('Connecting to the PostgreSQL database...')
    conn = psycopg2.connect(**params)

    return conn

def close_connection(filename, cursor):
    print('File updated to table:', filename)
    cursor.close()