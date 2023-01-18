from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster

from main.src.config.CanssendraConfig import pandas_factory

cassandra_host = "localhost"
cassandra_username = "cassandra"
cassandra_password = "cassandra"
cassandra_keyspace = "partner_customer_loan_db"
cassandra_table = "emp"


def create_cassandra_cluster_session():
    # main_logger.info("connecting to cassandra host .....")
    auth_provider = PlainTextAuthProvider(username=cassandra_username, password=cassandra_password)
    cluster = Cluster([cassandra_host], port=9042, auth_provider=None)  # 25399
    csession = cluster.connect(cassandra_keyspace)
    csession.default_timeout = 20000000  # 6000
    csession.default_fetch_size = None
    csession.row_factory = pandas_factory
    return csession

def customDef(*user ,**kwargs):
    print(kwargs.get("name"))
    print(kwargs.get("user"))
    kwargs["test"] ="test"
    print(user)
    print(kwargs)


if __name__ == '__main__':
    details = dict()
    details["name"] = "ganesh"
    print(details)
    user = list()
    user.append("ganesh")
    customDef(*user,**details)


    # session = create_cassandra_cluster_session()
    # clist = session.execute("select * from emp")
    # print(clist._current_rows)
