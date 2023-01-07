import os
import time

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import *


def create_spark_session(sparknode):
    """
        This Function is used to Create a Spark Session with the specified configuration. T
    :return:Spark Session
    """
    os.environ[
        'PYSPARK_SUBMIT_ARGS'] = '--packages org.postgresql:postgresql:42.1.1 --packages com.amazonaws:aws-java-sdk-bundle:1.11.875,org.apache.hadoop:hadoop-aws:3.2.0,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 --conf spark.cassandra.connection.host=172.31.9.179 --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions --conf spark.sql.catalog.lcc=com.datastax.spark.connector.datasource.CassandraCatalog --conf spark.cassandra.connection.timeoutMS=30000000 --conf spark.sql.defaultCatalog=lcc --conf spark.cassandra.input.split.sizeInMB=1024 --conf spark.executor.memory=12g --conf spark.executor.pyspark.memory=12g pyspark-shell --conf spark.executor.cores=12 --conf spark.speculation=true --conf spark.speculation.interval=100 --conf spark.speculation.quantile=0.75 --conf spark.speculation.multiplier=1.5'

    print("Receiving the request to create the spark session")
    # creating a sparkSession as spark
    spark_session = SparkSession.builder \
        .master(sparknode) \
        .appName("sonata_risk_management_data_pipeline") \
        .getOrCreate()
    print("Successfully created the Spark session")
    spark_context = spark_session.sparkContext
    # making a spark connection with s3
    print("Attempting to Connect to S3")
    spark_context.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
    hadoop_conf = spark_context._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
    hadoop_conf.set("fs.s3a.access.key", 'AKIA2QF73PNMLXGUEAE7')
    hadoop_conf.set("fs.s3a.secret.key", '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')
    hadoop_conf.set("fs.s3a.multipart.size", '104857600')
    hadoop_conf.set("fs.s3a.blocksize", '332648')
    hadoop_conf.set("fs.s3a.endpoint", "s3." + 'ap-south-1' + ".amazonaws.com")
    print("connected to s3 successfully")

    return spark_session


def numDisbLoans(dataframe):
    win = Window.partitionBy("customer_id").orderBy(F.col('disbursement_date').desc())
    win2 = Window.partitionBy('customer_id')

    # test1 = dataframe.withColumn("rn",row_number().over(win))

    test1 = dataframe.withColumn('max_disb_date', F.max(to_date('disbursement_date')).over(win2))

    test1 = test1.withColumn('disb_date_diff', datediff(to_date('max_disb_date'), to_date('disbursement_date'))) \
        .withColumn("disb_date_diff_bool", when(F.col("disb_date_diff") > 360, 1).otherwise(0))

    test1 = test1.withColumn('numdisbloansgt360', sum('disb_date_diff_bool').over(win2))

    return test1



def numDisbLoans2(dataframe):
    # win = Window.partitionBy("customer_id").orderBy(F.col('disbursement_date').desc())
    win2 = Window.partitionBy('customer_id')

    test1 = dataframe.withColumn('disbursement_date2', to_date(col("disbursement_date"))).drop(
        'disbursement_date').withColumnRenamed('disbursement_date2', 'disbursement_date') \
        .withColumn("last_disbursement_date", max(col("disbursement_date")).over(win2)) \
        .filter(datediff(col('last_disbursement_date'), col('disbursement_date')) < 360) \
        .withColumn("numdisbloansgt360", count(col('customer_id')).over(win2)).drop('disbursement_date') \
        .dropDuplicates(['customer_id'])

    # test1 = dataframe.withColumn("rn",row_number().over(win))

    # test1 = dataframe.withColumn('max_disb_date', max(to_date('disbursement_date')).over(win2))
    # test1.show()
    # # test1 = test1.withColumn('disb_date_diff', datediff(to_date('max_disb_date'), to_date('disbursement_date')))
    # # test1.show()
    # test1.groupBy(col('customer_id')).count().alias("count")
    # print("etst")
    # # test1.show()
    # test1 = test1.withColumn('disb_date_diff', datediff(to_date('max_disb_date'), to_date('disbursement_date'))) \
    #     .withColumn("disb_date_diff_bool", when(F.col("disb_date_diff") > 360, 1).otherwise(0))
    #
    # test1 = test1.withColumn('numdisbloansgt360', sum('disb_date_diff_bool').over(win2))
    #
    # test1.show(20)

    # test1 = test1.groupBy('customer_id').count()
    # test1.show(20)

    # test1 = test1.withColumn('numdisbloansgt360', sum('disb_date_diff_bool').over(win2))

    return test1


# create table if not exists test_keyspace.sonata_customer_actual_transactions_table_v10
# (
#     customer_id         text,
#     disbursement_date   date,
#     disb_date_diff      text,
#     disb_date_diff_bool boolean,
#     max_disb_date       text,
#     numdisbloansgt360   text,
#     primary key ((customer_id, disbursement_date))
# );

if __name__ == '__main__':
    # 'spark://172.31.29.252:7077'
    spark = create_spark_session("local[*]")
    print(spark.conf)
    t1 = time.time()
    dataframe = spark.read.format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", '3.108.179.144') \
        .option("spark.cassandra.auth.username", 'srikanth') \
        .option("spark.cassandra.auth.password", 'Sr!k@tH@9760') \
        .options(table='sonata_customer_actual_transactions_table_v5', keyspace='partner_customer_loan_db') \
        .load().limit(100).select(['customer_id', 'disbursement_date', 'loan_account_no']).dropDuplicates(
        ['loan_account_no']).na.drop()
    print(dataframe.count())
    dataframe.where()
    t2 = time.time()
    print("Time taken:", t2 - t1)
    dataframe.show()
    print("loan disbursement ")
    t3 = time.time()
    feat = numDisbLoans2(dataframe)
    t4 = time.time()
    print('Time taken- 2:', t4 - t3)
    feat.show(20)
    t5 = time.time()
    # test2 = feat.limit(100)
    #
    # feat.write.format("org.apache.spark.sql.cassandra") \
    #     .option("spark.cassandra.connection.host", 'localhost') \
    #     .option("spark.cassandra.auth.username", 'cassandra') \
    #     .option("spark.cassandra.auth.password", 'cassandra') \
    #     .mode('append') \
    #     .option("confirm.truncate", "true") \
    #     .options(table='sonata_customer_actual_transactions_table_v10', keyspace='test_keyspace') \
    #     .save()
    # t6 = time.time()
    # print('Time taken in writing:', t6 - t5)
