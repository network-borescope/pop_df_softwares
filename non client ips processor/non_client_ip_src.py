import sqlite3
from sqlite3 import Error
import datetime
import json
from sys import argv, exit
from os import mkdir

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        create_database(db_file, conn)
        return conn
    except Error as e:
        print(e)

    return conn

def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

def create_database(DATABASE, conn = None):
    sql_non_client_ips = """ CREATE TABLE IF NOT EXISTS non_client_ips (
                                        ip VARCHAR(20),
                                        first_time TIMESTAMP,
                                        last_time TIMESTAMP,
                                        client_id INT,
                                        CONSTRAINT PK_serie_temp PRIMARY KEY (ip)
                                    ); """


    close_at_end = False
    # create a database connection
    if conn is None:
        conn = create_connection(DATABASE)
        close_at_end = True

    # create tables
    if conn is not None:
        # create table
        create_table(conn, sql_non_client_ips)

        if close_at_end: conn.close()
    else:
        print("Error! cannot create the database connection.")

def insert_ip(conn, client_id, ip, time):
    sql = ''' INSERT INTO non_client_ips(ip, first_time, last_time, client_id) VALUES (?, ?, ?, ?)'''
    cur = conn.cursor()
    cur.execute(sql, (ip, time, time, client_id))
    conn.commit()

    return cur.rowcount

   
def update_last_time(conn, ip, time):

    sql = ''' SELECT * FROM non_client_ips WHERE ip = ?'''
    cur = conn.cursor()
    cur.execute(sql, (ip,))

    res = cur.fetchall()
    if len(res) == 0: return 0

    first_time = datetime.datetime.strptime(res[0][1], "%Y-%m-%d %H:%M:%S")
    last_time = datetime.datetime.strptime(res[0][2], "%Y-%m-%d %H:%M:%S")
    
    if (time < first_time):
        first_time = time
        
    elif (time > last_time):
        last_time = time
    
    else:
       return # o dado ja existe e nao deve ser atualizado: retorna None

    sql = ''' UPDATE non_client_ips SET first_time = ?, last_time = ? WHERE ip = ?'''
    cur = conn.cursor()
    cur.execute(sql, (first_time, last_time, ip))
    conn.commit()

    return cur.rowcount

def get_all(conn):
    sql = ''' SELECT * FROM non_client_ips ORDER BY first_time ASC, last_time DESC'''
    cur = conn.cursor()
    cur.execute(sql)

    res = cur.fetchall()

    return res




def build_timestamp(d_data, d_hora, d_min):
    year, month, day = d_data.split("-")
    
    d = datetime.datetime(year=int(year), month=int(month), day=int(day), hour=int(d_hora), minute=int(d_min))

    return d

def main():
    arguments = argv[1:]
    ids_list = ["1"]

    if len(arguments) == 1:
        filename = arguments[0]
    elif len(arguments) > 1:
        filename = arguments[0]
        
        for id in arguments[1:]: ids_list.append(id)
    else: exit(1)

    # data positions
    D_DATA = 0
    #D_D_SEMANA = 1
    D_HORA = 1
    D_MIN = 2
    D_ID_CLIENTE = 3
    D_SIP= 4
    #D_DIST = 6
    #D_TTL = 7
    #D_DPORT = 8
    #D_SERV = 9
    #D_DID = 10
    D_COUNT = 5

    data = []

    fin = open(filename, "r")
    conn = create_connection(r"non_client_ips.db")

    lines = fin.readlines()
    lines.sort() # precisa ordenar o arquivo out

    for line in lines:
        # reinicia as variaveis de memoria
        data = []

        # prepara novo dado a ser processado
        clean_line = line.strip()
        data = clean_line.split(",")

        if len(data) < 5 or data[D_ID_CLIENTE] not in ids_list: continue

        time = build_timestamp(data[D_DATA], data[D_HORA], data[D_MIN])

        if update_last_time(conn, data[D_SIP], time) == 0:
            insert_ip(conn, data[D_ID_CLIENTE], data[D_SIP], time)

    fin.close()


    try:
        mkdir("dynamic_ips")
    except OSError: pass

    with open("dynamic_ips/dynamic_ips.js", "w") as fout:
        json.dump(get_all(conn), fout, indent=1)
    
    conn.close()

if __name__ == "__main__":
    main()