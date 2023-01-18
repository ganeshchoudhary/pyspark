from pyspark import SparkConf
from pyspark.sql import SparkSession


def push_dataframe_to_postgress(dataframe, con=None):
    postgress_url = "jdbc:postgresql://" + con.postgress_host + ":" + con.postgress_port + "/" + con.postgress_database
    properties = {"user": con.postgress_username, "password": con.postgress_password, "driver": "org.postgresql.Driver"}
    dataframe.write.jdbc(url=postgress_url, table=con.postgress_table, mode="append", properties=properties)

class Config:
    pass


if __name__ == '__main__':
    con = Config()
    con.postgress_host = '172.19.0.2';
    con.postgress_table = 'customer'
    con.postgress_username = 'test'
    con.postgress_password = 'test'
    con.postgress_port = '5432'
    con.postgress_database = 'test'
    conf = SparkConf()  # create the configuration
    conf.set("spark.jars", "../resources/postgresql-42.4.0.jar")  # set the spark.jars

    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config(conf= conf) \
        .getOrCreate()
    schema = ['id', 'name']
    df =spark.read.csv(path='../../resources/test.csv', header=True, inferSchema=True);
    df.show()
    push_dataframe_to_postgress(df, con)