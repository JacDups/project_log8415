import os
import paramiko
import random
import pymysql
from typing import Dict, List
from sshtunnel import open_tunnel


def load_private_key(name: str) -> paramiko.RSAKey:
    if not os.path.isfile(f'../{name}.pem'):
        return None
    file = open(f'../{name}.pem', 'r')

    return paramiko.RSAKey.from_private_key(file)


PRIVATE_KEY = load_private_key('mykey')
MASTER_HOSTNAME = '172-31-81-59'
SLAVES_HOSTMANE = ['172.31.87.8', '172.31.83.42', '172.31.81.125']
MASTER_DB_NAME = "masterDB"
SLAVES_DB_NAME = ['slave1DB', 'slave2DB', 'slave3DB']


def execute(query: str, mode: str) -> tuple[List[Dict], int, str]:

    db, name = get_instance(mode)
    if db is None:
        raise Exception('No host found to handle this query.')

    try:
        with open_tunnel(
            (db, 22),
            ssh_username='ubuntu',
            ssh_pkey=PRIVATE_KEY,
            remote_bind_address=(MASTER_HOSTNAME, 3306),
            local_bind_address=('0.0.0.0', 3306)) as tunnel:

            try:
                with pymysql.connect(
                        host='localhost',
                        user='jac',
                        password='password',
                        port=3306,
                        charset='utf8mb4',
                        database=MASTER_DB_NAME,
                        cursorclass=pymysql.cursors.DictCursor) as conn:

                    # Try to execute the query
                    try:
                        with conn.cursor() as cursor:
                            cursor.execute(query)
                            conn.commit()
                            return cursor.fetchall(), cursor.rowcount, name
                    except Exception as e:
                        raise Exception(e)

            except Exception as e:
                raise Exception(
                    'An error occured while connecting to the MySQL server: ' + str(e))

    except Exception as e:
        raise Exception(e)

# Get the correct instance depending on the current mode


def get_instance(mode: str) -> tuple[str, str]:
    if mode == 'direct':
        return MASTER_HOSTNAME, MASTER_DB_NAME

    elif mode == 'random':
        i = random.randint(0, 2)
        return SLAVES_HOSTMANE[i], SLAVES_DB_NAME[i]

    else:
        return None
