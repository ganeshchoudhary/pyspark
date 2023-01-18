# This is a sample Python script.
import os

from pyspark.python.pyspark.shell import spark
from pyspark.sql import SparkSession


# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

def print_hi( con=None):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {con.postgress_table}')  # Press Ctrl+F8 to toggle the breakpoint.

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
from cassandra.query import tuple_factory

class Config:
    pass

if __name__ == '__main__':
    IsLiveCustomer_dict = {
        '1.0': 'Client On boarded or Proposal is under process',
        '2.0': 'Disburement Done',
        '3.0': 'Loan Account Closed',
        '6.0': 'Loan Account/Proposal Cancelled'
    }
    print(IsLiveCustomer_dict.items())

# Press the green button in the gutter to run the script.
# if __name__ == '__main__':
#     cassandra_host = '3.108.179.144'  # '172.31.9.179'#'3.108.179.144'
#     cassandra_username = 'srikanth'
#     cassandra_port = 9042
#     cassandra_password = 'Sr!k@tH@9760'
#     cassandra_keyspace = 'partner_customer_loan_db'
#     cassandra_table = 'sonata_customer_actual_transactions_table'  # sonata_customer_actual_transactions_table_2
#     os.environ[
#         'PYSPARK_SUBMIT_ARGS'] = '--packages org.postgresql:postgresql:42.1.1 --packages com.amazonaws:aws-java-sdk-bundle:1.11.875,org.apache.hadoop:hadoop-aws:3.2.0,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 --conf spark.cassandra.connection.host=3.108.179.144 --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions --conf spark.sql.catalog.lcc=com.datastax.spark.connector.datasource.CassandraCatalog --conf spark.cassandra.connection.timeoutMS=30000000 --conf spark.sql.defaultCatalog=lcc --conf spark.cassandra.input.split.sizeInMB=1024 --conf spark.executor.memory=12g --conf spark.executor.pyspark.memory=12g pyspark-shell --conf spark.executor.cores=12 --conf spark.speculation=true --conf spark.speculation.interval=100 --conf spark.speculation.quantile=0.75 --conf spark.speculation.multiplier=1.5'
#
#     os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.postgresql:postgresql:42.1.1 --packages spark-cassandra-connector-driver_2.12;3.1.0 ' \
#                                         '--packages com.amazonaws:aws-java-sdk-bundle:1.11.875,org.apache.hadoop:hadoop-aws:3.2.0,' \
#                                         'com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 ' \
#                                         '--conf spark.cassandra.connection.host=3.108.179.144 ' \
#                                         '--conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions ' \
#                                         '--conf spark.sql.catalog.lcc=com.datastax.spark.connector.datasource.CassandraCatalog ' \
#                                         '--conf spark.cassandra.connection.timeoutMS=30000000 ' \
#                                         '--conf spark.sql.defaultCatalog=lcc --conf spark.cassandra.input.split.sizeInMB=1024 ' \
#                                         '--conf spark.executor.memory=12g --conf spark.executor.pyspark.memory=12g pyspark-shell ' \
#                                         '--conf spark.executor.cores=12 --conf spark.speculation=true ' \
#                                         '--conf spark.speculation.interval=100 --conf spark.speculation.quantile=0.75 -' \
#                                         '-conf spark.speculation.multiplier=1.5'
#
#     DF = spark.read.format("org.apache.spark.sql.cassandra") \
#         .option("spark.cassandra.connection.host", cassandra_host) \
#         .option("spark.cassandra.auth.username", cassandra_username) \
#         .option("spark.cassandra.auth.password", cassandra_password) \
#         .options(table=cassandra_table, keyspace=cassandra_keyspace) \
#         .load()
#     DF.show(10)
#     # data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
#     # spark_session = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
#     # rdd = spark.sparkContext.parallelize(data)
#     # df= rdd.toDF()
#     # df.show()
# # See PyCharm help at https://www.jetbrains.com/help/pycharm/
