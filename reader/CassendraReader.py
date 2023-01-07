from pyspark.conf import SparkConf

cassandra_host = "172.19.0.3"
cassandra_username = ""
cassandra_password = ""
cassandra_keyspace = "tutorialspoint"
cassandra_table = "emp"


def cassandra_df_read(spark):
    return spark.read.format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", cassandra_host).options(table=cassandra_table,
                                                                           keyspace=cassandra_keyspace) \
        .load()


def cassandra_df_read_with_credentials(spark):
    return spark.read.format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", cassandra_host) \
        .option("spark.cassandra.auth.username", cassandra_username) \
        .option("spark.cassandra.auth.password", cassandra_password) \
        .options(table="api_batch_config", keyspace="partner_customer_loan_db") \
        .load()


def create_spark_session(node):
    os.environ[
        'PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions --conf spark.executor.memory=13g --conf spark.driver.memory=13g --conf spark.executor.pyspark.memory=13g pyspark-shell --conf spark.executor.cores=12 --conf spark.driver.cores=12 --conf spark.speculation=true --conf spark.speculation.interval=100 --conf spark.speculation.quantile=0.75 --conf spark.speculation.multiplier=1.5'

    """
        This Function is used to Create a Spark Session with the specified configuration. T
    :return:Spark Session
    """
    # main_logger.info("Receiving the request to create the spark session")
    # creating a sparkSession as spark
    spark = SparkSession.builder \
        .appName("risk_management_postgress_pipeline") \
        .getOrCreate()

    # main_logger.info("Successfully created the Spark session")
    sc = spark.sparkContext
    # making a spark connection with s3
    # main_logger.info("Attempting to Connect to S3")

    # sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
    # hadoop_conf = sc._jsc.hadoopConfiguration()
    # hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
    # hadoop_conf.set("fs.s3a.access.key", con.aws_access_key)
    # hadoop_conf.set("fs.s3a.secret.key", con.aws_secret_key)
    # hadoop_conf.set("fs.s3a.multipart.size", '104857600')
    # hadoop_conf.set("fs.s3a.blocksize", '332648')
    # hadoop_conf.set("fs.s3a.endpoint", con.aws_endpoint_region)
    # hadoop_conf.set('fs.s3a.committer.staging.conflict-mode', 'replace')
    # hadoop_conf.set('fs.s3a.committer.name', 'partitioned')
    # hadoop_conf.set("fs.s3a.fast.upload.buffer", "bytebuffer")

    # main_logger.info("connected to s3 successfully")

    return spark


import os

from pyspark.sql import SparkSession

if __name__ == '__main__':
    # os.environ['SPARK_HOME'] = "C:\\Users\\SANKAR\\Downloads\\spark-3.1.3-bin-hadoop3.2"
    # os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 ' \
    #                                     '--conf spark.cassandra.connection.timeoutMS=30000000 ' \
    #                                     '--conf spark.cassandra.connection.host=localhost --conf ' \
    #                                     'spark.cassandra.input.split.sizeInMB=1024 --conf spark.executor.memory=1g ' \
    #                                     '--conf spark.executor.pyspark.memory=1g pyspark-shell --conf ' \
    #                                     'spark.executor.cores=1 --conf spark.speculation=true --conf ' \
    #                                     'spark.speculation.interval=100 --conf spark.speculation.quantile=0.75 --conf ' \
    #                                     'spark.speculation.multiplier=1.5'
    # ' below config can be added  ' \
    # cassandra config - https://github.com/datastax/spark-cassandra-connector/blob/master/doc/reference.md
    conf = SparkConf()
    conf.set("spark.jars.packages", 'com.datastax.spark:spark-cassandra-connector_2.12:3.1.0')
    conf.set("spark.executor.memory", '8g')
    conf.set("spark.cassandra.connection.host", 'localhost')
    conf.set("spark.cassandra.connection.port", '9042')

    spark_session = SparkSession.builder.config(conf=conf).master("local[*]") \
        .appName("sank") \
        .getOrCreate()
    # print(conf.getAll())
    spark_session.read.format("org.apache.spark.sql.cassandra") \
        .options(table="sonata_customer_actual_transactions_table_v0", keyspace="partner_customer_loan_db") \
        .load() \
        .show(10, truncate=False)
    # spark_session.read.format("org.apache.spark.sql.cassandra").options(table="sonata_incremental_account_capture_v6", keyspace="partner_customer_loan_db").load().show(10, truncate=False)
    spark_session.sparkContext.addPyFile("/DATA/HOME_DIR/ganeshc/SonataPipeline/ds-data-engineering/ds-data-engg.zip")