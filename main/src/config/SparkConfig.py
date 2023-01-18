from pyspark.sql import SparkSession


def create_spark_session(node, con=None):
    """
        This Function is used to Create a Spark Session with the specified configuration. T
    :return:Spark Session
    """
    # main_logger.info("Receiving the request to create the spark session")
    # creating a sparkSession as spark
    spark = SparkSession.builder.master(node) \
        .appName("risk_management_postgress_pipeline") \
        .config("spark.jars", "/DATA/HOME_DIR/srikanth/postgresql-42.4.0.jar") \
        .getOrCreate()
    # main_logger.info("Successfully created the Spark session")
    sc = spark.sparkContext
    # making a spark connection with s3
    # main_logger.info("Attempting to Connect to S3")

    sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
    hadoop_conf.set("fs.s3a.access.key", con.aws_access_key)
    hadoop_conf.set("fs.s3a.secret.key", con.aws_secret_key)
    hadoop_conf.set("fs.s3a.multipart.size", '104857600')
    hadoop_conf.set("fs.s3a.blocksize", '332648')
    hadoop_conf.set("fs.s3a.endpoint", con.aws_endpoint_region)
    # hadoop_conf.set('fs.s3a.committer.staging.conflict-mode', 'replace')
    # hadoop_conf.set('fs.s3a.committer.name', 'partitioned')
    # hadoop_conf.set("fs.s3a.fast.upload.buffer", "bytebuffer")

    # main_logger.info("connected to s3 successfully")

    return spark


