import os

import jaydebeapi


def connect_to_postgress_db(con=None):
    dsn_database = con.postgress_database
    dsn_hostname = con.postgress_host
    dsn_port = con.postgress_port
    dsn_uid = con.postgress_username
    dsn_pwd = con.postgress_password
    jdbc_driver_name = "org.postgresql.Driver"
    #jdbc_driver_loc = os.path.join(r'D:\Jar\postgresql-9.3-1100-jdbc41.jar')
    jdbc_driver_loc = os.path.join('/DATA/HOME_DIR/srikanth/postgresql-42.4.0.jar')
    connection_string='jdbc:postgresql://'+ dsn_hostname+':'+ dsn_port +'/'+ dsn_database
    url = '{0}:user={1};password={2}'.format(connection_string, dsn_uid, dsn_pwd)
    conn = jaydebeapi.connect(jdbc_driver_name, connection_string, {'user': dsn_uid, 'password': dsn_pwd},jars=jdbc_driver_loc)
    curs = conn.cursor()
    return curs