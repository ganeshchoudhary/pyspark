import pandas as pd
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
from cassandra.query import tuple_factory

def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)


def create_cassandra_cluster_session(con=None):
    # main_logger.info("connecting to cassandra host .....")
    auth_provider = PlainTextAuthProvider(username=con.cassandra_username, password=con.cassandra_password)
    cluster = Cluster([con.cassandra_host], port=con.cassandra_port, auth_provider=auth_provider)  # 25399
    session = cluster.connect(con.cassandra_keyspace)
    session.default_timeout = 20000000  # 6000
    session.default_fetch_size = None
    session.row_factory = pandas_factory
    return session
