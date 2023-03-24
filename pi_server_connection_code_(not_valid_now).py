import psycopg2 as pg
from sshtunnel import SSHTunnelForwarder


def execute_id_search(conn):
    print("Executing query")
    db_cursor = conn.cursor()
    db_cursor.execute("SELECT * FROM testing")
    records = db_cursor.fetchall()
    for record in records:
        print(record)

def connect_to_db():
    
    server = SSHTunnelForwarder(
        ssh_address=('174.112.60.115',8022),
        ssh_username='remote_user',
        ssh_password='remote',
        remote_bind_address=('174.112.60.115',5432))

    server.start()

    conn = pg.connect(
        host='localhost',
        port=server.local_bind_port,
        user='service',
        password='service',
        database='postgres')

if __name__ == '__main__':

    connection = connect_to_db()
    execute_id_search(connection)
    connection.stop()
