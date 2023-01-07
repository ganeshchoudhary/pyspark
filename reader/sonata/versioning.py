import standard_file_format as std
from cassandra.query import tuple_factory

import sonata_config as con

# logic-for-versioning

version_query = """select 

	current.* , 

case 
	when trim(lower(nvl(previous_table.address1,0))) = trim(lower(nvl(current.address1,0)))
		then
		nvl(previous_table.address_version ,1)
	else
		nvl(previous_table.max_address_version ,0)
		+
		dense_rank() OVER(PARTITION BY current.customer_id ORDER BY current.address1)
end
	address_version,
	
	
case 
	when trim(lower(nvl(previous_table.branch_id,0))) = trim(lower(nvl(current.branch_id,0)))
		then
		nvl(previous_table.branch_id_version ,1)
	else
		nvl(previous_table.max_branch_id_version ,0)
		+
		dense_rank() OVER(PARTITION BY current.customer_id ORDER BY current.branch_id)
end
	branch_id_version,


case 
	when trim(lower(nvl(previous_table.center_id,0))) = trim(lower(nvl(current.center_id,0)))
		then
		nvl(previous_table.center_id_version ,1)
	else
		nvl(previous_table.max_center_id_version ,0)
		+
		dense_rank() OVER(PARTITION BY current.customer_id ORDER BY current.center_id)
end
	center_id_version,
	
case 
	when previous_table.DOB = current.dob
		then
		nvl(previous_table.dob_version ,1)
	else
		nvl(previous_table.max_dob_version ,0)
		+
		dense_rank() OVER(PARTITION BY current.customer_id ORDER BY current.dob)
end
	dob_version,
	
case 
	when trim(lower(nvl(previous_table.hub_id,0))) = trim(lower(nvl(current.hub_id,0)))
		then
		nvl(previous_table.hub_id_version ,1)
	else
		nvl(previous_table.max_hub_id_version ,0)
		+
		dense_rank() OVER(PARTITION BY current.customer_id ORDER BY current.hub_id)
end
	hub_id_version,
	
case 
	when trim(lower(nvl(previous_table.loan_sanction_id,0))) = trim(lower(nvl(current.loan_sanction_id,0)))
		then
		nvl(previous_table.loan_version ,1)
	else
		nvl(previous_table.max_loan_version ,0)
		+
		dense_rank() OVER(PARTITION BY current.customer_id ORDER BY current.loan_sanction_id)
end
	loan_version,
	
case 
	when trim(lower(nvl(previous_table.phone_number,0))) = trim(lower(nvl(current.phone_number,0)))
		then
		nvl(previous_table.phone_version ,1)
	else
		nvl(previous_table.max_phone_version ,0)
		+
		dense_rank() OVER(PARTITION BY current.customer_id ORDER BY current.phone_number)
end
	phone_version,
	
case 
	when trim(lower(nvl(previous_table.pincode,0))) = trim(lower(nvl(current.pincode,0)))
		then
		nvl(previous_table.pincode_version ,1)
	else
		nvl(previous_table.max_pincode_version ,0)
		+
		dense_rank() OVER(PARTITION BY current.customer_id ORDER BY current.pincode)
end
	pincode_version,
	
	
case 
	when trim(lower(nvl(previous_table.sfo_id,0))) = trim(lower(nvl(current.sfo_id,0)))
		then
		nvl(previous_table.sfo_id_version ,1)
	else
		nvl(previous_table.max_sfo_id_version ,0)
		+
		dense_rank() OVER(PARTITION BY current.customer_id ORDER BY current.sfo_id)
end
	sfo_id_version,
    
case 
	when trim(lower(nvl(previous_table.nominee_relation,0))) = trim(lower(nvl(current.nominee_relation,0)))
		then
		nvl(previous_table.nominee_relation_version ,1)
	else
		nvl(previous_table.max_nominee_relation_version ,0)
		+
		dense_rank() OVER(PARTITION BY current.customer_id ORDER BY current.nominee_relation)
end
	nominee_relation_version,
    
case 
	when nvl(previous_table.annual_income,0) = nvl(current.annual_income,0)
		then
		nvl(previous_table.annual_income_version,1)
	else
		nvl(previous_table.max_annual_income_version ,0)
		+
		dense_rank() OVER(PARTITION BY current.customer_id ORDER BY current.annual_income)
end
	annual_income_version,


case 
	when trim(lower(nvl(previous_table.marital_status,0))) = trim(lower(nvl(current.marital_status,0)))
		then
		nvl(previous_table.marital_status_version ,1)
	else
		nvl(previous_table.max_marital_status_version ,0)
		+
		dense_rank() OVER(PARTITION BY current.customer_id ORDER BY current.marital_status)
end
	marital_status_version

    
    FROM 
    
    current_table1 current
    left join previous_table 
    on trim(current.customer_id) = trim(previous_table.customer_id)
	and trim(current.loan_account_no) = trim(previous_table.loan_account_no)
"""

# tracker-table-query

version_table = """select 
current_table2.customer_id , 
loan_account_no,
current_table2.address1, 
current_table2.address2, 
address_version,
max(address_version) over (PARTITION BY current_table2.customer_id) max_address_version,

current_table2.branch_id,
branch_id_version,
max(branch_id_version) over (PARTITION BY current_table2.customer_id) max_branch_id_version,

current_table2.center_id,
center_id_version,
max(center_id_version) over (PARTITION BY current_table2.customer_id) max_center_id_version,

current_table2.hub_id,
hub_id_version,
max(hub_id_version) over (PARTITION BY current_table2.customer_id) max_hub_id_version,

current_table2.loan_sanction_id,
loan_version,
max(loan_version) over (PARTITION BY current_table2.customer_id) max_loan_version,

current_table2.phone_number,
phone_version,
max(phone_version) over (PARTITION BY current_table2.customer_id) max_phone_version,

current_table2.pincode,
pincode_version,
max(pincode_version) over (PARTITION BY current_table2.customer_id) max_pincode_version,

current_table2.dob,
dob_version,
max(dob_version) over (PARTITION BY current_table2.customer_id) max_dob_version,

current_table2.sfo_id,
sfo_id_version,
max(sfo_id_version) over (PARTITION BY current_table2.customer_id) max_sfo_id_version,

current_table2.nominee_relation,
nominee_relation_version,
max(nominee_relation_version) over (PARTITION BY current_table2.customer_id) max_nominee_relation_version,

current_table2.annual_income,
annual_income_version,
max(annual_income_version) over (PARTITION BY current_table2.customer_id) max_annual_income_version,

current_table2.marital_status,
marital_status_version,
max(marital_status_version) over (PARTITION BY current_table2.customer_id) max_marital_status_version

from current_table2;"""


def apply_version_logic(spark, standardized_df, cassandra_session):

    standardized_df = standardized_df.drop("address_version", "branch_id_version", "center_id_version",
                                           "dob_version", "hub_id_version", "loan_version", "phone_version",
                                           "pincode_version", "sfo_id_version", "nominee_relation_version",
                                           "annual_income_version", "marital_status_version")

    customer_id_pks = list(set(standardized_df.select("customer_id").rdd.flatMap(list).collect()))

    col_and_dtypes = std.cassandra_table_column_types(cassandra_session, con.cassandra_version_table)
    cassandra_session.row_factory = tuple_factory
    results = []
    for i in range(0, len(customer_id_pks), 1):
        standard_cql = std.cql_query_generator(col_and_dtypes) + """from {cassandra_table} 
                                        where customer_id = {cus_id}
                                    """.format(cassandra_table=con.cassandra_version_table,
                                               cus_id=customer_id_pks[i])
        re = cassandra_session.execute(standard_cql)
        if str(type(re.one())) == "<class 'NoneType'>":
            pass
        else:
            results.append(re.one())

    schema = std.standard_schema_format(col_and_dtypes)
    previous_table_df = spark.createDataFrame(results, schema)
    previous_table_df = std.standard_cassandra_column_casting(previous_table_df, spark, cassandra_session,
                                                              con.cassandra_version_table)

    previous_table_df.createOrReplaceTempView("previous_table")
    standardized_df.createOrReplaceTempView('current_table1')

    standardized_df = spark.sql(version_query)

    standardized_df.createOrReplaceTempView("current_table2")

    version_df = spark.sql(version_table)

    version_df = std.standard_cassandra_column_casting(version_df, spark, cassandra_session,
                                                       con.cassandra_version_table)


    version_df.write.format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", con.cassandra_host) \
        .option("spark.cassandra.auth.username", con.cassandra_username) \
        .option("spark.cassandra.auth.password", con.cassandra_password) \
        .mode('append') \
        .option("confirm.truncate", "true") \
        .options(table=con.cassandra_version_table, keyspace=con.cassandra_keyspace) \
        .save()

    return standardized_df
