from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame



def postgress_df_read(spark: SparkSession, con=None) -> DataFrame:
    """

    :param spark:
    :param con:
    :return:
    """
    postgress_url = "jdbc:postgresql://" + con.postgress_host + ":" + con.postgress_port + "/" + con.postgress_database
    postgress_df = spark.read \
        .format("jdbc") \
        .option("url", postgress_url) \
        .option("dbtable", con.postgress_table) \
        .option("user", con.postgress_username) \
        .option("password", con.postgress_password) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    return postgress_df


if __name__ == '__main__':
    print("starting")
    # os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages mysql-connector-java-8.0.22.jar'
    conf = SparkConf()  # create the configuration
    # conf.set("spark.jars", "../resources/postgresql-42.4.0.jar,../resources/mysql-connector-java-8.0.22.jar")  # set the spark.jars
    conf.set("spark.jars.packages",
             "mysql:mysql-connector-java:8.0.22,org.postgresql:postgresql:42.4.0,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0")  # set the spark.jars
    conf.set("spark.sql.catalog.cass200", "org.postgresql.Driver")
    spark = SparkSession \
        .builder.master('spark://3.6.75.0:7077').config(conf=conf).appName(
        "Python Spark SQL basic example").getOrCreate()

    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://{url}:5432/partner_customer_loan_db") \
        .option("dbtable", "sonata_risk_mangement_transaction_table_v6") \
        .option("user", "user") \
        .option("password", "pass") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    df.show(n=10)

    # df = spark.read \
    #     .format("jdbc") \
    #     .option("url", "jdbc:mysql://localhost:3306/kiscore_service") \
    #     .option("dbtable", "score") \
    #     .option("user", "root") \
    #     .option("password", "root") \
    #     .option("driver", "com.mysql.jdbc.Driver") \
    #     .load()
    # df.printSchema()
    # df.show()
    # df.createTempView("test")

    # spark.read.format("org.apache.spark.sql.cassandra") \
    #     .options(table="sonata_customer_actual_transactions_table_v6", keyspace="partner_customer_loan_db") \
    #     .load() \
    #     .show(10, truncate=False)
    # print(spark.catalog.listTables())
