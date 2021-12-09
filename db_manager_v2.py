import sqlite3
from sqlite3 import Error

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
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

def insert_hostname(conn, hostname):
    """
    Create a new hostname into the hostname table
    :param conn:
    :param hostname:
    :return: hostname id
    """
    sql = ''' INSERT INTO hostname(hostname)
              VALUES(?) '''
    cur = conn.cursor()
    cur.execute(sql, (hostname,))
    conn.commit()
    return cur.lastrowid

def insert_ip(conn, ip):
    """
    Create a new task
    :param conn:
    :param ip:
    :return:
    """

    sql = ''' INSERT INTO host_ip(ip)
              VALUES(?) '''
    cur = conn.cursor()
    cur.execute(sql, (ip,))
    conn.commit()

    return cur.lastrowid

def insert_hostname_ip(conn, hostname, ip, date):
    """
    Create a new task
    :param conn:
    :param hostname_ip:
    :return:
    """

    sql = ''' INSERT INTO hostname_host_ip(hostname, ip, validade)
              VALUES(?, ?, ?) '''
    cur = conn.cursor()
    cur.execute(sql, (hostname, ip, date))
    conn.commit()

    return cur.lastrowid

def insert_ip_hostname(conn, ip, hostname, date):
    """
    Create a new task
    :param conn:
    :param ip_hostname:
    :return:
    """

    sql = ''' INSERT INTO host_ip_hostname(ip, hostname, validade)
              VALUES(?, ?, ?) '''
    cur = conn.cursor()
    cur.execute(sql, (ip, hostname, date))
    conn.commit()

    return cur.lastrowid

def create_database(DATABASE):
    sql_create_hostname = """ CREATE TABLE IF NOT EXISTS hostname (
                                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                                        hostname TEXT NOT NULL UNIQUE,
                                        count INT
                                    ); """

    sql_create_host_ip = """CREATE TABLE IF NOT EXISTS host_ip (
                                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    ip TEXT NOT NULL UNIQUE,
                                    count INT
                                );"""

    sql_create_hostname_host_ip = """CREATE TABLE IF NOT EXISTS hostname_host_ip (
                                    hostname TEXT,
                                    ip TEXT,
                                    validade TIMESTAMP,

                                    CONSTRAINT PK_hostname_host_ip PRIMARY KEY (hostname, ip)
                                );"""

    sql_create_host_ip_hostname = """CREATE TABLE IF NOT EXISTS host_ip_hostname (
                                    ip TEXT,
                                    hostname TEXT,
                                    validade TIMESTAMP,

                                    CONSTRAINT PK_host_ip_hostname PRIMARY KEY (ip, hostname)
                                );"""

    # create a database connection
    conn = create_connection(DATABASE)

    # create tables
    if conn is not None:
        # create hostname table
        create_table(conn, sql_create_hostname)

        # create host_ip table
        create_table(conn, sql_create_host_ip)

        # create hostname_host_ip table
        create_table(conn, sql_create_hostname_host_ip)

        # create host_ip_hostname table
        create_table(conn, sql_create_host_ip_hostname)

        conn.close()
    else:
        print("Error! cannot create the database connection.")

def select_id_from_hostname(conn, hostname):
    """
    Query tasks by hostname
    :param conn: the Connection object
    :param hostname: the hostname of the host queried
    :return:
    """

    sql = ''' SELECT id FROM hostname WHERE hostname = ? '''
    cur = conn.cursor()
    cur.execute(sql, (hostname,))
    conn.commit()

    res = cur.fetchall()

    if len(res) == 0: return None

    return res[0][0]

def select_host_ip_id(conn, host_ip):
    """
    Query tasks by host_ip
    :param conn: the Connection object
    :param host_ip: the ip of the host queried
    :return:
    """

    sql = ''' SELECT id FROM host_ip WHERE ip = ? '''
    cur = conn.cursor()
    cur.execute(sql, (host_ip,))
    conn.commit()

    res = cur.fetchall()

    if len(res) == 0: return None

    return res[0][0]

def select_hostname_from_ip(conn, host_ip):
    """
    Query hostname by host_ip
    :param conn: the Connection object
    :param host_ip: the ip of the host queried
    :return:
    """

    sql = ''' SELECT hostname FROM host_ip_hostname WHERE ip = ? '''
    cur = conn.cursor()
    cur.execute(sql, (host_ip,))
    conn.commit()

    return cur.fetchall()

def select_ip_from_hostname(conn, hostname):
    """
    Query host ip by hostname
    :param conn: the Connection object
    :param host_ip: the hostname of the host queried
    :return:
    """

    sql = ''' SELECT ip FROM hostname_host_ip WHERE hostname = ? '''
    cur = conn.cursor()
    cur.execute(sql, (hostname,))
    conn.commit()

    return cur.fetchall()

def select_ip_hostname_from_host_ip_hostname(conn, ip, hostname):
    """
    Query row by ip and hostname
    :param conn: the Connection object
    :param host_ip: the hostname of the host queried
    :return:
    """

    sql = ''' SELECT * FROM host_ip_hostname WHERE ip = ? AND hostname = ? '''
    cur = conn.cursor()
    cur.execute(sql, (ip, hostname))
    conn.commit()

    return cur.fetchall()[0]

def select_hostname_ip_from_hostname_host_ip(conn, hostname, ip):
    """
    Query row by hostname and ip
    :param conn: the Connection object
    :param host_ip: the hostname of the host queried
    :return:
    """

    sql = ''' SELECT * FROM hostname_host_ip WHERE hostname = ? AND  ip = ? '''
    cur = conn.cursor()
    cur.execute(sql, (hostname, ip))
    conn.commit()

    return cur.fetchall()[0]

### get or insert

def get_or_insert_ip(conn, ip):
    id = select_host_ip_id(conn, ip)

    if id is not None: return id
    
    return insert_ip(conn, ip)

def get_or_insert_hostname(conn, hostname):
    id = select_id_from_hostname(conn, hostname)

    if id is not None: return id
    
    return insert_hostname(conn, hostname)

def get_or_insert_ip_hostname(conn , ip, hostname, date):
    id = select_ip_hostname_from_host_ip_hostname(conn, ip, hostname)

    if id is not None: return id
    
    return insert_ip_hostname(conn, ip, hostname, date)

def get_or_insert_hostname_ip(conn , hostname, ip, date):
    id = select_ip_hostname_from_host_ip_hostname(conn, hostname, ip)

    if id is not None: return id
    
    return insert_hostname_ip(conn, hostname, ip, date)

if __name__ == '__main__':
    #DATABASE = r"C:\sqlite\db\coletaPoP-DF.db"
    DATABASE = r"syn_dns_ether.db"
    create_database(DATABASE)
