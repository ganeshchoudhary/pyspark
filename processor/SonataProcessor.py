import pandas as pd
from pyspark.shell import f
from pyspark.sql import Window
from pyspark.sql.functions import when, col, date_format, datediff
from pyspark.sql.types import LongType, IntegerType

from config.CanssendraConfig import create_cassandra_cluster_session
from config.PostgressConfig import connect_to_postgress_db
from config.SparkConfig import create_spark_session
import config.sonata_config as con
from processor.DataFrameConvertor import convert_schema
from reader.CassendraReader import cassandra_df_read
from reader.PostgresReader import postgress_df_read
from writer.PostgresWriter import push_dataframe_to_postgress


def sonata_incremental_load_to_postgress(partner):
    spark_node = con.spark_node  # "spark://ip-172-31-29-252.ap-south-1.compute.internal:7077"  #"local[*]"
    spark = create_spark_session(spark_node)
    cassandra_session = create_cassandra_cluster_session()

    cassandra_df = spark.read.format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", con.cassandra_host) \
        .option("spark.cassandra.auth.username", con.cassandra_username) \
        .option("spark.cassandra.auth.password", con.cassandra_password) \
        .options(table=con.cassandra_table, keyspace=con.cassandra_keyspace) \
        .load()

    increment_account_df = spark.read.format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", con.cassandra_host) \
        .option("spark.cassandra.auth.username", con.cassandra_username) \
        .option("spark.cassandra.auth.password", con.cassandra_password) \
        .options(table=con.postgress_process_table, keyspace=con.cassandra_keyspace) \
        .load()

    inactive_account_df = spark.read.format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", con.cassandra_host) \
        .option("spark.cassandra.auth.username", con.cassandra_username) \
        .option("spark.cassandra.auth.password", con.cassandra_password) \
        .options(table=con.postgress_inactive_account_table, keyspace=con.cassandra_keyspace) \
        .load()

    postgress_process_df = inactive_account_df.union(increment_account_df).dropDuplicates()

    inc_postgress_df = cassandra_df.join(postgress_process_df,
                                         cassandra_df.loan_account_no == postgress_process_df.loan_account_no,
                                         'inner').select([cassandra_df[each] for each in cassandra_df.columns])

    # customer_id_store = postgress_process_df.select("customer_id").rdd.flatMap(list).collect()
    loan_account_id_store = postgress_process_df.select("loan_account_no").rdd.flatMap(list).collect()

    '''
    col_and_dtypes = std.cassandra_table_column_types(cassandra_session,con.cassandra_table)
    cassandra_session.row_factory = tuple_factory
    #incremental_data_capture = []
    result_set = cassandra_session.execute("select customer_id , loan_account_no from "+con.postgress_process_table)
    incremental_data_capture = result_set._current_rows
    results = [] #cassandra rows
    customer_id_store = []
    loan_account_id_store = []
    for i in range(0, len(incremental_data_capture) , 1):
        customer_id_store.append(incremental_data_capture[i][0])
        loan_account_id_store.append(incremental_data_capture[i][1])
        get_the_latest_instalment_number = """select latest_instalment_number
                                                      from {customer_latest_instalment_table}
                                                      where customer_id = '{cus_id}'
                                                      and loan_account_no = '{loan_id}'
                                                      """.format(customer_latest_instalment_table = con.customer_latest_instalment_tracker,
                                                                 cus_id = incremental_data_capture[i][0],
                                                                 loan_id = incremental_data_capture[i][1]
                                                                )
        rdd = cassandra_session.execute(get_the_latest_instalment_number)
        inst_number = rdd._current_rows
        latest_instalment_number = inst_number[0][0] if len(inst_number)>0 else 0

        latest_instalment_number = int(latest_instalment_number) + 1

        for j in range(1,latest_instalment_number,1):

            standard_cql = std.cql_query_generator(col_and_dtypes) + """from {cassandra_table} 
                                        where customer_id = '{cus_id}'
                                        and loan_account_no = '{loan_id}'
                                        and instalment_number = {instalment_no}
                                        and kiscore_tier = 'none'
                                    """.format(cassandra_table  =  con.cassandra_table,
                                               cus_id           = incremental_data_capture[i][0],
                                               loan_id          = incremental_data_capture[i][1],
                                               instalment_no    = str(j)
                                               )

            re = cassandra_session.execute(standard_cql)
            if str(type(re.one()))=="<class 'NoneType'>":
                #new_cust_ids.append(customer_id_pks[i])
                #new_loan_ids.append(loan_account_no_pks[i])
                pass
            else:
                results.append(re.one())

    schema = std.standard_schema_format(col_and_dtypes)
    inc_postgress_df = spark.createDataFrame(results, schema)
    cassandra_session.row_factory = pandas_factory
    '''
    final_postgress_df = risk_data_calculation(spark, inc_postgress_df, partner)
    current_postgress_df = postgress_df_read(spark)
    casted_final_postgress_df = convert_schema(current_postgress_df, final_postgress_df)

    cursor = connect_to_postgress_db()

    delete_query = """delete from {postgress_table}
                      where loan_account_no in {loan} 
                      """.format(postgress_table=con.postgress_table,
                                 loan=str(loan_account_id_store).replace("[", "(").replace("]", ")").replace("'",
                                                                                                             '').replace(
                                     '"', '')
                                 )

    cursor.execute(delete_query)
    push_dataframe_to_postgress(casted_final_postgress_df)

    cursor.execute("""select distinct customer_id,loan_account_no from {posgress_table}
                    where month_end_month in (select distinct  max(month_end_month) OVER (PARTITION BY loan_account_no) from 
                    {posgress_table})
                    and pos>0
                    and principal_due_cum_sum = disbursed_amount""".format(posgress_table=con.postgress_table))

    result = cursor.fetchall()
    schema = ['customer_id', 'loan_account_no']
    p_df = pd.DataFrame(result, columns=schema)
    incative_account_df = spark.createDataFrame(p_df)
    incative_account_df.write.format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", con.cassandra_host) \
        .option("spark.cassandra.auth.username", con.cassandra_username) \
        .option("spark.cassandra.auth.password", con.cassandra_password) \
        .mode('overwrite') \
        .option("confirm.truncate", "true") \
        .options(table=con.postgress_inactive_account_table, keyspace=con.cassandra_keyspace) \
        .save()
    cassandra_session.execute("truncate table {inc_data_capture}".format(inc_data_capture=con.postgress_process_table))
    spark.catalog.clearCache()
    spark.sparkContext.stop()
    spark.stop()


def risk_data_calculation(spark, dataframe, partner):
    dataframe.createOrReplaceTempView('cassandra_table_raw')
    df_all = spark.sql("""select 
                      '{pd}' partner_id,
                      customer_id,
                      loan_account_no,
                      loan_sanction_id,
                      cast(disbursed_amount as double) disbursed_amount,
                      to_date(disbursement_date,'yyyy-MM-dd') as 
                          disbursement_date,
                      to_date(schedule_date,'yyyy-MM-dd') as 
                          schedule_date,
                      to_date(repayment_date,'yyyy-MM-dd') as 
                          repayment_date,
                      cast(principal_amount as double) principal_amount,
                      cast(principal_collected as double) principal_collected,
                      cast(interest_amount as double) interest_amount,
                      cast(interest_collected as double) interest_collected,
                      to_date(future_date_postgress,'yyyy-MM-dd') as future_date_postgress,
                      (select 
                        to_date(last_day(
                                  greatest(
                                            max(repayment_date),
                                            max(disbursement_date)
                                          )) + 1,'yyyy-MM-dd')
                      from cassandra_table_raw )
                              cut_off_date,
                      case when ((principal_collected + interest_collected) > 0 ) 
                           and ((principal_amount) + (interest_amount) = 0) then 1 else 0 
                      end as m ,
                      case when repayment_frequency='36' then 'Weekly'
                           when repayment_frequency='31' then 'Bi-Monthly'
                           when repayment_frequency='30' then 'Monthly' else 'others' 
                      end as repayment_frequency_description,
                      account_id,
                      account_type,
                      address_id,
                      agriculture_income,
                      animal_value,
                      annual_income,
                      asset_value,
                      bank_branch_name,
                      bank_name,
                      branch_id,
                      cashflow_FurnitureAssets,
                      cashflow_HouseProperty,
                      cashflow_IncrIncome,
                      cashflow_InsurancePremium,
                      cashflow_LiveStocks,
                      cashflow_Medical,
                      cashflow_NetIncrIncome,
                      cashflow_NetTotalIncome,
                      cashflow_Rent,
                      cashflow_RentalIncome,
                      center_id,
                      country,
                      district_name,
                      division_name,
                      funder_name,
                      funder_type,
                      gender,
                      group_code,
                      group_id,
                      home_branch_id,
                      house_ownership,
                      house_type,
                      hub_name,
                      income_details,
                      instalment_amount,
                      interest_rate,
                      loan_category,
                      loan_cycle,
                      loan_purpose,
                      loan_status,
                      loan_sub_purpose,
                      marital_status,
                      member_literacy,
                      no_of_animals,
                      no_of_childrens,
                      no_of_dependents,
                      no_of_members,
                      no_of_members_above_60_yrs,
                      no_of_school_going_children,
                      no_of_unmarried_children_above_18_yrs,
                      occupation,
                      pincode,
                      poverty_status,
                      product_name,
                      product_type,
                      region,
                      repayment_amount,
                      repayment_frequency,
                      schedule_amount,
                      state1,
                      term_period
                      from cassandra_table_raw
                      """.format(pd=partner))
    if partner.lower() == "sonata":
        df_all = df_all.withColumn("principal_amount1",
                                   when(col("m") == 1, col("principal_collected")).otherwise(col("principal_amount"))) \
            .withColumn("interest_amount1",
                        when(col("m") == 1, col("interest_collected")).otherwise(col("interest_amount"))) \
            .drop("principal_amount", "interest_amount") \
            .withColumnRenamed("principal_amount1", "principal_amount") \
            .withColumnRenamed("interest_amount1", "interest_amount")

    matrix_df = df_all.select("customer_id",
                                                "loan_account_no",
                              "loan_sanction_id",
                              "disbursement_date",
                              "disbursed_amount",
                              "partner_id",
                              "repayment_frequency_description",
                              "cut_off_date",
                              'acco',
                              '.'
                              ''
                              ''                        'agriculture_income',
                              'animal_value',
                              'annual_income',
                              'asset_value',
                              'bank_branch_name',
                              'bank_name',
                              'branch_id',
                              'cashflow_FurnitureAssets',
                              'cashflow_HouseProperty',
                              'cashflow_IncrIncome',
                              'cashflow_InsurancePremium',
                              'cashflow_LiveStocks',
                              'cashflow_Medical',
                              'cashflow_NetIncrIncome',
                              'cashflow_NetTotalIncome',
                              'cashflow_Rent',
                              'cashflow_RentalIncome',
                              'center_id',
                              'country',
                              'district_name',
                              'division_name',
                              'funder_name',
                              'funder_type',
                              'gender',
                              'group_code',
                              'group_id',
                              'home_branch_id',
                              'house_ownership',
                              'house_type',
                              'hub_name',
                              'income_details',
                              'instalment_amount',
                              'interest_rate',
                              'loan_category',
                              'loan_cycle',
                              'loan_purpose',
                              'loan_status',
                              'loan_sub_purpose',
                              'marital_status',
                              'member_literacy',
                              'no_of_animals',
                              'no_of_childrens',
                              'no_of_dependents',
                              'no_of_members',
                              'no_of_members_above_60_yrs',
                              'no_of_school_going_children',
                              'no_of_unmarried_children_above_18_yrs',
                              'occupation',
                              'pincode',
                              'poverty_status',
                              'product_name',
                              'product_type',
                              'region',
                              'repayment_amount',
                              'repayment_frequency',
                              'schedule_amount',
                              'state1',
                              'term_period')

    win = Window.partitionBy("customer_id", "loan_account_no", "loan_sanction_id").orderBy(
        f.col('disbursement_date').asc())
    matrix = matrix_df.select("customer_id",
                              "loan_account_no",
                              "loan_sanction_id",
                              "disbursement_date",
                              "disbursed_amount",
                              "partner_id",
                              "repayment_frequency_description",
                              "cut_off_date",
                              'account_id',
                              'account_type',
                              'address_id',
                              'agriculture_income',
                              'animal_value',
                              'annual_income',
                              'asset_value',
                              'bank_branch_name',
                              'bank_name',
                              'branch_id',
                              'cashflow_FurnitureAssets',
                              'cashflow_HouseProperty',
                              'cashflow_IncrIncome',
                              'cashflow_InsurancePremium',
                              'cashflow_LiveStocks',
                              'cashflow_Medical',
                              'cashflow_NetIncrIncome',
                              'cashflow_NetTotalIncome',
                              'cashflow_Rent',
                              'cashflow_RentalIncome',
                              'center_id',
                              'country',
                              'district_name',
                              'division_name',
                              'funder_name',
                              'funder_type',
                              'gender',
                              'group_code',
                              'group_id',
                              'home_branch_id',
                              'house_ownership',
                              'house_type',
                              'hub_name',
                              'income_details',
                              'instalment_amount',
                              'interest_rate',
                              'loan_category',
                              'loan_cycle',
                              'loan_purpose',
                              'loan_status',
                              'loan_sub_purpose',
                              'marital_status',
                              'member_literacy',
                              'no_of_animals',
                              'no_of_childrens',
                              'no_of_dependents',
                              'no_of_members',
                              'no_of_members_above_60_yrs',
                              'no_of_school_going_children',
                              'no_of_unmarried_children_above_18_yrs',
                              'occupation',
                              'pincode',
                              'poverty_status',
                              'product_name',
                              'product_type',
                              'region',
                              'repayment_amount',
                              'repayment_frequency',
                              'schedule_amount',
                              'state1',
                              'term_period') \
        .withColumn('rn', f.row_number().over(win)).where("rn=1") \
        .withColumn("array", f.expr("sequence(to_date(disbursement_date), to_date(cut_off_date), interval 1 month)")) \
        .withColumn("date", f.explode(f.col("array"))) \
        .withColumn("month_end", f.last_day(f.col("date"))) \
        .withColumn("m_e_plusone", f.date_add(f.col("month_end"), 1)) \
        .drop("array", "rn", "date") \
        .withColumn("freq_days", f.when(f.col("repayment_frequency_description") == 'Bi-Monthly', 14)
                    .when(f.col("repayment_frequency_description") == 'Monthly', 30)
                    .otherwise(7))

    main_df = spark.sql("""select 
                      customer_id as customer_id1,
                      loan_account_no as loan_account_no1,
                      cast(schedule_date as date) schedule_date,
                      future_date_postgress repayment_date
                      from cassandra_table_raw
                      """)
    # main_df = main_df.withColumn("repayment_date",main_df.repayment_date_str.cast(DateType())).drop('repayment_date_str')
    # nvl(to_date(repayment_date,'yyyy-MM-dd') , to_date('2025-01-01' ,'yyyy-MM-dd')) repayment_date

    win = Window.partitionBy("customer_id", "loan_account_no", "m_e_plusone").orderBy(f.col('date_diff').asc())
    final_matrix_1 = matrix.join(main_df, (main_df.customer_id1 == matrix.customer_id) & (
                main_df.loan_account_no1 == matrix.loan_account_no) & (matrix.m_e_plusone <= main_df.repayment_date),
                                 "left") \
        .withColumn("date_diff", f.datediff(f.col("repayment_date"), f.col("m_e_plusone"))) \
        .withColumn("rn", f.row_number().over(win)).where("rn=1") \
        .withColumn("month_end_month", f.date_format(f.col("month_end"), "yMM").cast(LongType())) \
        .drop("customer_id1", "loan_account_no1", "date_diff", "rn")

    win_1 = Window.partitionBy("customer_id1", "loan_account_no1", "repayment_month").orderBy(
        f.col('schedule_date1').asc()).rangeBetween(Window.unboundedPreceding, 0)
    win_2 = Window.partitionBy("customer_id1", "loan_account_no1", "repayment_month").orderBy(
        f.col('repayment_date1').desc()).orderBy(f.col('schedule_date1').desc())

    collections_df = df_all.selectExpr("customer_id customer_id1", "loan_account_no loan_account_no1",
                                       "schedule_date schedule_date1", "repayment_date repayment_date1",
                                       "principal_amount",
                                       "interest_amount", "principal_collected", "interest_collected", "m") \
        .withColumn("due_month", date_format(col("schedule_date1"), "yMM").cast(LongType())) \
        .withColumn("repayment_month", date_format(col("repayment_date1"), "yMM").cast(LongType())) \
        .withColumn("Principal_collected_for_month", f.sum('principal_collected').over(win_1)) \
        .withColumn("interest_collected_for_month", f.sum('interest_collected').over(win_1)) \
        .withColumn("m_cum_sum", f.sum('m').over(win_1)).drop("m") \
        .withColumn("ontime_principle1",
                    when(col("repayment_month") == col("due_month"), col("principal_collected")).otherwise(0)) \
        .withColumn("prepayments_principle1",
                    when(col("repayment_month") < col("due_month"), col("principal_collected")).otherwise(0)) \
        .withColumn("overdue_principle1",
                    when(col("repayment_month") > col("due_month"), col("principal_collected")).otherwise(0)) \
        .withColumn("ontime_interest1",
                    when(col("repayment_month") == col("due_month"), col("interest_collected")).otherwise(0)) \
        .withColumn("prepayments_interest1",
                    when(col("repayment_month") < col("due_month"), col("interest_collected")).otherwise(0)) \
        .withColumn("overdue_interest1",
                    when(col("repayment_month") > col("due_month"), col("interest_collected")).otherwise(0)) \
        .withColumn("ontime_principle", f.sum('ontime_principle1').over(win_1)) \
        .withColumn("prepayments_principle", f.sum('prepayments_principle1').over(win_1)) \
        .withColumn("overdue_principle", f.sum('overdue_principle1').over(win_1)) \
        .withColumn("ontime_interest", f.sum('ontime_interest1').over(win_1)) \
        .withColumn("prepayments_interest", f.sum('prepayments_interest1').over(win_1)) \
        .withColumn("overdue_interest", f.sum('overdue_interest1').over(win_1)) \
        .drop("ontime_principle1", "overdue_interest1", "prepayments_interest1", "ontime_interest1",
              "prepayments_principle1", "prepayments_principle1") \
        .selectExpr("customer_id1", "loan_account_no1", "repayment_date1", "schedule_date1", "repayment_month",
                    "Principal_collected_for_month", "interest_collected_for_month", "ontime_principle",
                    "prepayments_principle", "overdue_principle", "ontime_interest", "prepayments_interest",
                    "overdue_interest") \
        .withColumn("rn", f.row_number().over(win_2)).where("rn=1").drop("rn", "schedule_date1")

    final_matrix_2 = final_matrix_1.join(collections_df,
                                         (final_matrix_1.customer_id == collections_df.customer_id1) & (
                                                 final_matrix_1.loan_account_no == collections_df.loan_account_no1) & (
                                                 final_matrix_1.month_end_month == collections_df.repayment_month),
                                         "left") \
        .drop("customer_id1", "loan_account_no1")

    win_1 = Window.partitionBy("customer_id1", "loan_account_no1", "due_month").orderBy(
        f.col('schedule_date1').asc()).rangeBetween(Window.unboundedPreceding, 0)
    win_2 = Window.partitionBy("customer_id1", "loan_account_no1", "due_month").orderBy(f.col('schedule_date1').desc())
    due_df_2 = df_all.selectExpr("customer_id customer_id1", "loan_account_no loan_account_no1",
                                 "schedule_date schedule_date1", "principal_amount", "interest_amount") \
        .withColumn("due_month", date_format(col("schedule_date1"), "yMM").cast(LongType())) \
        .withColumn("schedule_principle_for_month", f.sum('principal_amount').over(win_1)) \
        .withColumn("schedule_interest_for_month", f.sum('interest_amount').over(win_1)) \
        .withColumn("rn", f.row_number().over(win_2)).where("rn=1").drop("rn", "principal_amount", "interest_amount")

    final_matrix_3 = final_matrix_2.join(due_df_2, (final_matrix_2.customer_id == due_df_2.customer_id1) & (
            final_matrix_2.loan_account_no == due_df_2.loan_account_no1) & (
                                                 final_matrix_2.month_end_month == due_df_2.due_month), "left") \
        .drop("customer_id1", "loan_account_no1")

    win_1 = Window.partitionBy("customer_id", "loan_account_no").orderBy(f.col('month_end').asc())
    win = Window.partitionBy("customer_id", "loan_account_no").orderBy(f.col('month_end').asc()).rangeBetween(
        Window.unboundedPreceding, 0)
    final_matrix_4 = final_matrix_3.withColumn("Principal_due_cum_sum1",
                                               f.sum("schedule_principle_for_month").over(win_1)) \
        .withColumn("interest_due_cum_sum1", f.sum("schedule_interest_for_month").over(win_1)) \
        .withColumn("Principal_collected_cum_sum2", f.sum("Principal_collected_for_month").over(win_1)) \
        .withColumn("Principal_collected_cum_sum1", when(col("Principal_collected_cum_sum2").isNull(), 0).otherwise(
        col("Principal_collected_cum_sum2"))).drop("Principal_collected_cum_sum2") \
        .withColumn("interest_collected_cum_sum1", f.sum("interest_collected_for_month").over(win_1)) \
        .withColumn("Principal_due_max", f.max("Principal_due_cum_sum1").over(win)) \
        .withColumn("interest_due_max", f.max("interest_due_cum_sum1").over(win)) \
        .withColumn("Principal_collected_max", f.max("Principal_collected_cum_sum1").over(win)) \
        .withColumn("interest_collected_max", f.max("interest_collected_cum_sum1").over(win)) \
        .withColumn("Principal_due_cum_sum", when(col("Principal_due_cum_sum1").isNull(), col("Principal_due_max")) \
                    .otherwise(col("Principal_due_cum_sum1"))) \
        .withColumn("interest_due_cum_sum", when(col("interest_due_cum_sum1").isNull(), col("interest_due_max")) \
                    .otherwise(col("interest_due_cum_sum1"))) \
        .withColumn("Principal_collected_cum_sum",
                    when(col("Principal_collected_cum_sum1").isNull(), col("Principal_collected_max")) \
                    .otherwise(col("Principal_collected_cum_sum1"))) \
        .withColumn("interest_collected_cum_sum",
                    when(col("interest_collected_cum_sum1").isNull(), col("interest_collected_max")) \
                    .otherwise(col("interest_collected_cum_sum1"))) \
        .drop("Principal_due_cum_sum1", "interest_due_cum_sum1", "Principal_collected_cum_sum1",
              "interest_collected_cum_sum1", "Principal_due_max", "interest_due_max", "Principal_collected_max",
              "interest_collected_max") \
        .withColumn("dpd", datediff(col("month_end"), col("schedule_date")) + 1) \
        .withColumn("final_dpd", when(col("dpd") < 0, 0).otherwise(col("dpd"))).drop("dpd") \
        .withColumn("pos", col("disbursed_amount") - col("Principal_collected_cum_sum")) \
        .withColumn("principal_overdue", col("Principal_due_cum_sum") - col("Principal_collected_cum_sum")) \
        .withColumn("interest_overdue", col("interest_due_cum_sum") - col("interest_collected_cum_sum")) \
        .withColumn("final_dpd_final", when(col("pos") == 0, 0).otherwise(col("final_dpd").cast(IntegerType()))) \
        .withColumn("par_7_inr", when(col("final_dpd_final") >= 7, col("pos")).otherwise(0)) \
        .withColumn("par_30_inr", when(col("final_dpd_final") >= 30, col("pos")).otherwise(0)) \
        .withColumn("par_60_inr", when(col("final_dpd_final") >= 60, col("pos")).otherwise(0)) \
        .withColumn("par_90_inr", when(col("final_dpd_final") >= 90, col("pos")).otherwise(0)) \
        .withColumn("par_0_inr", when(col("final_dpd_final") >= 1, col("pos")).otherwise(0)) \
        .withColumn("dpd_bucket", when(((col("final_dpd_final") >= 0) & (col("final_dpd_final") <= 30)), '0-30') \
                    .when(((col("final_dpd_final") >= 31) & (col("final_dpd_final") <= 60)), "30-60") \
                    .when(((col("final_dpd_final") >= 61) & (col("final_dpd_final") <= 90)), "60-90") \
                    .otherwise('>90'))

    final_matrix_5 = final_matrix_4.withColumn("updated_date", f.expr("current_date()"))

    return final_matrix_5


def sonata_full_load_to_postgress1(spark, df, partner):
    final_postgress_df = risk_data_calculation(spark, df, partner)
    current_postgress_df = postgress_df_read(spark)
    casted_final_postgress_df = convert_schema(current_postgress_df, final_postgress_df)
    push_dataframe_to_postgress(casted_final_postgress_df)
    curs = connect_to_postgress_db()
    curs.execute("""select distinct customer_id,loan_account_no from {posgress_table}
                    where month_end_month in (select distinct  max(month_end_month) OVER (PARTITION BY loan_account_no) from 
                    {posgress_table})
                    and pos>0
                    and principal_due_cum_sum = disbursed_amount""".format(posgress_table=con.postgress_table))

    result = curs.fetchall()
    schema = ['customer_id', 'loan_account_no']
    p_df = pd.DataFrame(result, columns=schema)
    incative_account_df = spark.createDataFrame(p_df)
    incative_account_df.write.format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", con.cassandra_host) \
        .option("spark.cassandra.auth.username", con.cassandra_username) \
        .option("spark.cassandra.auth.password", con.cassandra_password) \
        .mode('overwrite') \
        .option("confirm.truncate", "true") \
        .options(table=con.postgress_inactive_account_table, keyspace=con.cassandra_keyspace) \
        .save()
    # spark.catalog.clearCache()
    # spark.sparkContext.stop()
    # spark.stop()


# calculating risk data for whole table
def sonata_full_load_to_postgress(partner):
    spark_node = con.spark_node  # "spark://ip-172-31-29-252.ap-south-1.compute.internal:7077"  #"local[*]"
    spark = create_spark_session(spark_node)
    # cassandra_session = create_cassandra_cluster_session()
    df = cassandra_df_read(spark)
    final_postgress_df = risk_data_calculation(spark, df, partner)
    current_postgress_df = postgress_df_read(spark)
    casted_final_postgress_df = convert_schema(current_postgress_df, final_postgress_df)
    push_dataframe_to_postgress(casted_final_postgress_df)
    curs = connect_to_postgress_db()
    curs.execute("""select distinct customer_id,loan_account_no from {posgress_table}
                    where month_end_month in (select distinct  max(month_end_month) OVER (PARTITION BY loan_account_no) from 
                    {posgress_table})
                    and pos>0
                    and principal_due_cum_sum = disbursed_amount""".format(posgress_table=con.postgress_table))

    result = curs.fetchall()
    schema = ['customer_id', 'loan_account_no']
    p_df = pd.DataFrame(result, columns=schema)
    incative_account_df = spark.createDataFrame(p_df)
    incative_account_df.write.format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", con.cassandra_host) \
        .option("spark.cassandra.auth.username", con.cassandra_username) \
        .option("spark.cassandra.auth.password", con.cassandra_password) \
        .mode('overwrite') \
        .option("confirm.truncate", "true") \
        .options(table=con.postgress_inactive_account_table, keyspace=con.cassandra_keyspace) \
        .save()
    spark.catalog.clearCache()
    spark.sparkContext.stop()
    spark.stop()


def sonata_incremental_load_to_postgress(partner):
    spark_node = con.spark_node  # "spark://ip-172-31-29-252.ap-south-1.compute.internal:7077"  #"local[*]"
    spark = create_spark_session(spark_node)
    cassandra_session = create_cassandra_cluster_session()

    cassandra_df = spark.read.format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", con.cassandra_host) \
        .option("spark.cassandra.auth.username", con.cassandra_username) \
        .option("spark.cassandra.auth.password", con.cassandra_password) \
        .options(table=con.cassandra_table, keyspace=con.cassandra_keyspace) \
        .load()

    increment_account_df = spark.read.format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", con.cassandra_host) \
        .option("spark.cassandra.auth.username", con.cassandra_username) \
        .option("spark.cassandra.auth.password", con.cassandra_password) \
        .options(table=con.postgress_process_table, keyspace=con.cassandra_keyspace) \
        .load()

    inactive_account_df = spark.read.format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", con.cassandra_host) \
        .option("spark.cassandra.auth.username", con.cassandra_username) \
        .option("spark.cassandra.auth.password", con.cassandra_password) \
        .options(table=con.postgress_inactive_account_table, keyspace=con.cassandra_keyspace) \
        .load()

    postgress_process_df = inactive_account_df.union(increment_account_df).dropDuplicates()

    inc_postgress_df = cassandra_df.join(postgress_process_df,
                                         cassandra_df.loan_account_no == postgress_process_df.loan_account_no,
                                         'inner').select([cassandra_df[each] for each in cassandra_df.columns])

    # customer_id_store = postgress_process_df.select("customer_id").rdd.flatMap(list).collect()
    loan_account_id_store = postgress_process_df.select("loan_account_no").rdd.flatMap(list).collect()

    '''
    col_and_dtypes = std.cassandra_table_column_types(cassandra_session,con.cassandra_table)
    cassandra_session.row_factory = tuple_factory
    #incremental_data_capture = []
    result_set = cassandra_session.execute("select customer_id , loan_account_no from "+con.postgress_process_table)
    incremental_data_capture = result_set._current_rows
    results = [] #cassandra rows
    customer_id_store = []
    loan_account_id_store = []
    for i in range(0, len(incremental_data_capture) , 1):
        customer_id_store.append(incremental_data_capture[i][0])
        loan_account_id_store.append(incremental_data_capture[i][1])
        get_the_latest_instalment_number = """select latest_instalment_number
                                                      from {customer_latest_instalment_table}
                                                      where customer_id = '{cus_id}'
                                                      and loan_account_no = '{loan_id}'
                                                      """.format(customer_latest_instalment_table = con.customer_latest_instalment_tracker,
                                                                 cus_id = incremental_data_capture[i][0],
                                                                 loan_id = incremental_data_capture[i][1]
                                                                )
        rdd = cassandra_session.execute(get_the_latest_instalment_number)
        inst_number = rdd._current_rows
        latest_instalment_number = inst_number[0][0] if len(inst_number)>0 else 0

        latest_instalment_number = int(latest_instalment_number) + 1

        for j in range(1,latest_instalment_number,1):

            standard_cql = std.cql_query_generator(col_and_dtypes) + """from {cassandra_table} 
                                        where customer_id = '{cus_id}'
                                        and loan_account_no = '{loan_id}'
                                        and instalment_number = {instalment_no}
                                        and kiscore_tier = 'none'
                                    """.format(cassandra_table  =  con.cassandra_table,
                                               cus_id           = incremental_data_capture[i][0],
                                               loan_id          = incremental_data_capture[i][1],
                                               instalment_no    = str(j)
                                               )

            re = cassandra_session.execute(standard_cql)
            if str(type(re.one()))=="<class 'NoneType'>":
                #new_cust_ids.append(customer_id_pks[i])
                #new_loan_ids.append(loan_account_no_pks[i])
                pass
            else:
                results.append(re.one())

    schema = std.standard_schema_format(col_and_dtypes)
    inc_postgress_df = spark.createDataFrame(results, schema)
    cassandra_session.row_factory = pandas_factory
    '''
    final_postgress_df = risk_data_calculation(spark, inc_postgress_df, partner)
    current_postgress_df = postgress_df_read(spark)
    casted_final_postgress_df = convert_schema(current_postgress_df, final_postgress_df)

    cursor = connect_to_postgress_db()

    delete_query = """delete from {postgress_table}
                      where loan_account_no in {loan} 
                      """.format(postgress_table=con.postgress_table,
                                 loan=str(loan_account_id_store).replace("[", "(").replace("]", ")").replace("'",
                                                                                                             '').replace(
                                     '"', '')
                                 )

    cursor.execute(delete_query)
    push_dataframe_to_postgress(casted_final_postgress_df)

    cursor.execute("""select distinct customer_id,loan_account_no from {posgress_table}
                    where month_end_month in (select distinct  max(month_end_month) OVER (PARTITION BY loan_account_no) from 
                    {posgress_table})
                    and pos>0
                    and principal_due_cum_sum = disbursed_amount""".format(posgress_table=con.postgress_table))

    result = cursor.fetchall()
    schema = ['customer_id', 'loan_account_no']
    p_df = pd.DataFrame(result, columns=schema)
    incative_account_df = spark.createDataFrame(p_df)
    incative_account_df.write.format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", con.cassandra_host) \
        .option("spark.cassandra.auth.username", con.cassandra_username) \
        .option("spark.cassandra.auth.password", con.cassandra_password) \
        .mode('overwrite') \
        .option("confirm.truncate", "true") \
        .options(table=con.postgress_inactive_account_table, keyspace=con.cassandra_keyspace) \
        .save()
    cassandra_session.execute("truncate table {inc_data_capture}".format(inc_data_capture=con.postgress_process_table))
    spark.catalog.clearCache()
    spark.sparkContext.stop()
    spark.stop()

