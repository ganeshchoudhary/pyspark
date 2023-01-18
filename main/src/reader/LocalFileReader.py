from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import col, rank, row_number, dense_rank
from pyspark.sql.window import Window

from pyspark.sql.types import StructType, StructField, StringType
import time


def createSparkSession():
    return SparkSession.builder.appName('test').getOrCreate()


def read_localFile(fileName, session) -> DataFrame:
    dataframe = session.read.csv(path='/home/ganesh/Desktop/Kaleidofin/Files and documentation/DE/' + fileName, header=True, inferSchema=True)
    return dataframe


def write_localFile(filename, dataFrame: DataFrame):
    dataFrame.repartition(1).write.option("header", True).csv(path='../resources/' + filename)
    dataFrame.coalesce(1).write.option("header", True).csv(path='../resources/TREST/' + filename)


if __name__ == '__main__':
    # createDf()
    filename = 'customer_info.csv'
    filename2 = 'customer_account_details.csv'
    t1 = time.time()
    session = createSparkSession()
    df = read_localFile(filename, session)
    # print(df.count())
    df.createTempView("customers")
    df = session.sql("select * from customers")

    t2 = time.time()
    print(t2 -t1)
    df.show(10)
    df.printSchema()
    df=df.withColumn("distId", col('DistrictId').cast("string"))
    wind = Window.partitionBy("distId").orderBy("DistrictId")
    df.filter(df["DistrictId"]>0).select("distId","ApplicantName","DistrictId","CustomerInfoId", rank().over(wind).alias("rank")).show(100)
    df.withColumn("rank", rank().over(wind)).select("rank").distinct().show()
    session.stop()
    # CustomerInfoId = df.rdd.map(lambda x: x["CustomerInfoId"]).collect()
    # from collections import OrderedDict
    # res = list(OrderedDict.fromkeys(CustomerInfoId))
    # print(res[1])
    # print(len(res))
    # print(len(CustomerInfoId))
