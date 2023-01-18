import time

from pyspark import SparkConf
from pyspark.sql.functions import col
from pyspark.sql.session import SparkSession



if __name__ == '__main__':
    print("starting")
    # os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages mysql-connector-java-8.0.22.jar'
    conf = SparkConf()  # create the configuration
    # conf.set("spark.jars", "../resources/postgresql-42.4.0.jar,../resources/mysql-connector-java-8.0.22.jar")  # set the spark.jars
    conf.set("spark.jars.packages",
             "mysql:mysql-connector-java:8.0.22,org.postgresql:postgresql:42.4.0,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0")  # set the spark.jars
    conf.set("spark.sql.catalog.cass200", "org.postgresql.Driver")
    spark = SparkSession.builder \
        .appName("sonata_risk_management_data_pipeline") \
        .config("spark.jars.packages",
                "mysql:mysql-connector-java:8.0.22,org.postgresql:postgresql:42.4.0,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0") \
        .getOrCreate()
    t1= time.time()
    customers = ["1334",2550400,334]
    print("(SELECT * FROM score WHERE id in {} ) AS t".format(tuple(customers)))
    # df = spark.read \
    #     .format("jdbc") \
    #     .option("url", "jdbc:mysql://localhost:3306/kiscore_service") \
    #     .option("dbtable", "score") \
    #     .option("dbtable", "(SELECT * FROM score WHERE id in {} ) AS t".format(tuple(customers)))\
    #     .option("user", "root") \
    #     .option("password", "root") \
    #     .option("partitionColumn","id") \
    #     .option("lowerBound", 0) \
    #     .option("upperBound", 3)\
    #     .option("numPartitions",3)\
    #     .option("driver", "com.mysql.jdbc.Driver") \
    #     .load()
    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/kiscore_service") \
        .option("dbtable", "score") \
        .option("dbtable", "score") \
        .option("user", "root") \
        .option("password", "root") \
        .option("partitionColumn", "id") \
        .option("lowerBound", 0) \
        .option("upperBound", 3) \
        .option("numPartitions", 3) \
        .option("driver", "com.mysql.jdbc.Driver") \
        .load().filter(col("partner_id") == "sonata").filter(col('customer_id').isin(customers) ).select(col("applied_loan_id").alias("account_id"),
                                                             col("customer_id"),
                                                             col("score_for_the_applied_loan").alias("score")).na.drop()

    # print("test")
    # # df.show()
    # df.filter(col("partner_id") == "sonata").select(col("applied_loan_id").alias("account_id"),
    #                                                          col("customer_id"),
    #                                                          col("score_for_the_applied_loan").alias("score")).na.drop()
    print(df.rdd.getNumPartitions())
    df.printSchema()
    df.show()
    df2= df.filter(col("partner_id") == "sonata")
    df.cache()
    df.count()
    df.unpersist(blocking=True)
    df2.show()
    print(df.count())
    spark.stop()
    t2 = time.time()
    print(t2-t1)
