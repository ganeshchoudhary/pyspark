import time

import pyspark.sql.functions as f
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import rank, col, row_number

import sonata_config as con
import standard_file_format as std
from config.CanssendraConfig import pandas_factory
from versioning import apply_version_logic


def create_session():
    # os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.postgresql:postgresql:42.1.1,com.amazonaws:aws-java-sdk-bundle:1.11.375,org.apache.hadoop:hadoop-aws:3.2.0' \
    #                                     ' --conf spark.executor.memory=12g --conf spark.executor.pyspark.memory=12g pyspark-shell --conf spark.executor.cores=12 --conf spark.speculation=true --conf spark.speculation.interval=100 --conf spark.speculation.quantile=0.75 --conf spark.speculation.multiplier=1.5'

    bucket_name = "kaleido-ds2"
    aws_access_key = 'AKIA2QF73PNMAKWTPPQR'  # 'AKIA2QF73PNML34LVKFB' #'AKIA2QF73PNMO63W5R6A'
    aws_secret_key = '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'  # 'dcPmtGG4wazhd81fYDAhXKwvqXjcI0Eg3KGVsPH0' #'c6IO75OzfW9wgJCbtEJ50rx0EVYPyszcWeaZdpkx'
    aws_endpoint_region = "s3." + 'ap-south-1' + ".amazonaws.com"
    conf = SparkConf()  # create the configuration
    conf.set("file", "local")
    conf.set("spark.jars.packages",
             "org.postgresql:postgresql:42.1.1,com.amazonaws:aws-java-sdk-bundle:1.11.375,org.apache.hadoop:hadoop-aws:3.2.0")
    # conf.set("spark.jars", "hadoop-common-3.0.0.jar, aws-java-sdk-1.12.293.jar, hadoop-aws-3.2.0.jar, hadoop-client-3.3.0.jar")
    spark_session = SparkSession.builder.config(conf=conf).appName('test').getOrCreate()
    spark_context = spark_session.sparkContext
    # making a spark connection with s3
    spark_context.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
    hadoop_conf = spark_context._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
    hadoop_conf.set("fs.s3a.access.key", aws_access_key)
    hadoop_conf.set("fs.s3a.secret.key", aws_secret_key)
    hadoop_conf.set("fs.s3a.multipart.size", '104857600')
    hadoop_conf.set("fs.s3a.blocksize", '332648')
    hadoop_conf.set("fs.s3a.endpoint", aws_endpoint_region)
    return spark_session


def first_method(trans, spark):
    trans = trans.na.drop(subset=["DisbursementID", "CustomerInfoId", "InstallmentId"])

    trans.createOrReplaceTempView('transt')
    trans_df = spark.sql("""select *, ROW_NUMBER() OVER (Partition by DisbursementId,CustomerInfoId,InstallmentId ORDER BY ScheduleDate desc)
                                  as filter_duplicates 
                                   from transt
                               """)
    trans_df.createOrReplaceTempView('transt1')
    trans_df = spark.sql("""select *,ROW_NUMBER() OVER (Partition by DisbursementId ORDER BY index asc) AS transaction_row_no 
                                from transt1 where filter_duplicates != 1""")
    trans_df.createOrReplaceTempView('transactions')

    return trans_df


def second_method(trans: DataFrame, spark):
    trans = trans.na.drop(subset=["DisbursementID", "CustomerInfoId", "InstallmentId"])
    window = Window.partitionBy(['DisbursementId', 'CustomerInfoId', 'InstallmentId']).orderBy('ScheduleDate')
    window2 = Window.partitionBy('DisbursementId').orderBy('index')
    trans = trans.withColumn('rank', rank().over(window)).filter(col('rank') == 1).drop('rank').withColumn(
        "transaction_row_no",
        row_number().over(
            window2))
    trans.createOrReplaceTempView('transactions')

    return trans


def first_method_for_final(standardized_df: DataFrame, spark: SparkSession, cassandra_session):
    standardized_df.show(4)
    win_duplicate = Window.partitionBy("customer_id", "loan_account_no",
                                       "instalment_number").orderBy(f.col("instalment_number").desc())

    standardized_df = standardized_df.withColumn("rn",
                                                 f.row_number().over(
                                                     win_duplicate)).where(
        "rn=1").drop("rn")
    final_df_non_nulls = standardized_df.filter(
        (col('customer_id') == 0) & (col('loan_account_no') == 0) & (col('disbursement_date') == None))

    final_df__rejected = standardized_df.filter(
        (col('customer_id') != 0) & (col('loan_account_no') != 0) & (col('disbursement_date') != None))

    standardized_df = apply_version_logic(spark, final_df_non_nulls, cassandra_session)

    print("casting the standardized_df dataframe to cassandra schema level")

    standardized_df = std.standard_cassandra_column_casting(standardized_df, spark, cassandra_session,
                                                            con.cassandra_table)

    standardized_df.createOrReplaceTempView("final_master_table")

    rp = str(standardized_df.count())

    print("started loading the onetime batch load data to cassandra")

    standardized_df.write.format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", con.cassandra_host) \
        .option("spark.cassandra.auth.username", con.cassandra_username) \
        .option("spark.cassandra.auth.password", con.cassandra_password) \
        .mode('append') \
        .option("confirm.truncate", "true") \
        .options(table=con.cassandra_table, keyspace=con.cassandra_keyspace) \
        .save()

    print("completed loading the onetime batch load data with valid disb date to cassandra")

    # rejected records
    print("casting the rejected_df dataframe to cassandra schema level")
    rejected_df = std.standard_cassandra_column_casting(final_df__rejected, spark, cassandra_session,
                                                        con.cassandra_table)

    print("started loading the onetime batch load rejected data to cassandra")

    rejected_df.write.format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", con.cassandra_host) \
        .option("spark.cassandra.auth.username", con.cassandra_username) \
        .option("spark.cassandra.auth.password", con.cassandra_password) \
        .mode('append') \
        .option("confirm.truncate", "true") \
        .options(table=con.cassandra_table_rejected, keyspace=con.cassandra_keyspace) \
        .save()

    rr = str(final_df__rejected.count())

    print("completed loading the onetime batch load rejected data to cassandra")

    print("postgress risk data calculation started")


def create_cassandra_cluster_session():
    auth_provider = PlainTextAuthProvider(username=con.cassandra_username, password=con.cassandra_password)
    cluster = Cluster(["localhost"], port=9042, auth_provider=None)  # 25399
    session = cluster.connect(con.cassandra_keyspace)
    session.default_timeout = 200000000  # 6000
    session.default_fetch_size = None
    session.row_factory = pandas_factory
    return session


if __name__ == '__main__':
    spark = create_session()

    branch = spark.read.options(header='True', inferSchema='True').csv(con.branch_csv)
    branch.createOrReplaceTempView('branch')
    branch.show()
    district = spark.read.options(header='True', inferSchema='True').csv(con.district_csv)
    district.createOrReplaceTempView('district')

    division = spark.read.options(header='True', inferSchema='True').csv(con.division_csv)
    division.createOrReplaceTempView('division')
    division.show()
    hub = spark.read.options(header='True', inferSchema='True').csv(con.hub_csv)
    hub.createOrReplaceTempView('hub')
    hub.show()
    region = spark.read.options(header='True', inferSchema='True').csv(con.region_csv)
    region.printSchema()
    region.createOrReplaceTempView('region')
    # broadcastStates = spark.sparkContext.broadcast(hub)
    # broadcastStates.value.show()
    spark.sql("select /*+ BROADCAST(reg) BROADCAST(b) */ * from  branch b INNER JOIN  region reg on reg.branch_id=b.branch_id ").show()

    spark.catalog.dropTempView("")
    region.show()
    # joined_table = branch.join(broadcast(region), branch["branch_id"] == region["branch_id"], "inner").drop(region["branch_id"])
    joined_table.createOrReplaceTempView("joined_table")
    joined_table.show()
    print(spark.catalog.listTables())
    print(spark.catalog.listDatabases())

    sqlc = SQLContext(sparkSession=spark, sparkContext=spark.sparkContext)
    print(sqlc.tableNames())
    # print(sqlc.dropTempTable("branch"))
    print(sqlc.tableNames())
    print(region.explain())
    print(spark.sql("select * from branch").count())
    print(spark.table("hub").show())
    branch.show(4)

    # print("fetching")
    df = spark.read.csv(
        path='/home/ganesh/Downloads/transaction.csv', header=True,
        inferSchema=True)
    print("fetching")
    df.show(4)
    df.printSchema()
    # df.withColumn("CustomerInfoId", df.CustomerInfoId.cast('int'))
    # df.withColumn("AccountId", df.AccountId.cast('int'))

    t1 = time.time()
    windowSpec = Window.partitionBy("CustomerInfoId").orderBy("CustomerInfoId")

    df2 = df.withColumn("test", rank().over(windowSpec)).where("test == 1")
    # df2.createOrReplaceTempView("test")
    # df2 = spark.sql("select * from test where test=1")
    #
    df2.show(10)
    # df3 = df2
    # df3.show(3)
    # sql= "select * from temp"
    # df.createOrReplaceTempView("temp")
    # df2 = spark.sql(sql)
    # df2.show(10)
    # df2.createOrReplaceTempView("temp2")
    # sql2= "select * from temp2 "
    # df3 = spark.sql(sql2)
    # df3.show(3)
    print("count : " + str(df.count()))
    cassandra_session = create_cassandra_cluster_session()
    # first_method_for_final(df, spark, cassandra_session)
    t2 = time.time()
    print(t2 - t1)
    spark.stop()


    simpleData = (("James", "Sales", 3000), \
                  ("Michael", "Sales", 4600), \
                  ("Robert", "Sales", 4100), \
                  ("Maria", "Finance", 3000), \
                  ("James", "Sales", 3000), \
                  ("Scott", "Finance", 3300), \
                  ("Jen", "Finance", 3900), \
                  ("Jeff", "Marketing", 3000), \
                  ("Kumar", "Marketing", 2000), \
                  ("Saif", "Sales", 4100) \
                  )

    columns = ["employee_name", "department", "salary"]
    df = spark.createDataFrame(data=simpleData, schema=columns)
    df.printSchema()
    df.show(truncate=False)
    from pyspark.sql.functions import rank

    windowSpec = Window.partitionBy("department").orderBy("salary")

    df.withColumn("rank", row_number().over(windowSpec)) \
        .show()