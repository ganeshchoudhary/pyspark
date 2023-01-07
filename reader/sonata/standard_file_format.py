import pandas as pd
from pyspark.sql.functions import col, lit
from pyspark.sql.types import *

mapp = {"integer": IntegerType(), "string": StringType(), "double": DoubleType(), "boolean": BooleanType(),
        "date": DateType(), "long": LongType(), "float": FloatType(), "bigint": LongType(), "text": StringType(),
        "int": IntegerType()}


def standard_column_casting(old_df, new_df):
    new_df1 = new_df.columns
    new_df_columns = [each_column.lower() for each_column in new_df1]
    for columns_details in old_df.dtypes:
        column_name, data_type = columns_details
        if column_name.lower() in new_df_columns:
            new_df = new_df.withColumn(column_name, col(column_name).cast(data_type))
        else:
            new_df = new_df.withColumn(column_name, lit(None).cast(data_type))

    final_df = new_df.select(old_df.columns)  # Final dataframe
    return final_df


def cql_query_generator(cassandra_table):
    cql_query = "select "
    cassandra_columns = list(cassandra_table.keys())
    for each in cassandra_columns:
        if cassandra_table[each] == "bigint":
            cql_query = cql_query + "cast(" + each + " as bigint) as " + each + " , "
        elif cassandra_table[each] == "int":
            cql_query = cql_query + "cast(" + each + " as int) as " + each + " , "
        elif cassandra_table[each] == "boolean":
            cql_query = cql_query + "cast(" + each + " as boolean) as " + each + " , "
        elif cassandra_table[each] == "text":
            cql_query = cql_query + "cast(" + each + " as varchar) as " + each + " , "
        elif cassandra_table[each] == "float":
            cql_query = cql_query + "cast(" + each + " as float) as " + each + " , "
        elif cassandra_table[each] == "date":
            cql_query = cql_query + "cast(" + each + " as timestamp) as " + each + " , "
        elif cassandra_table[each] == "double":
            cql_query = cql_query + "cast(" + each + " as double) as " + each + " , "
        else:
            cql_query = cql_query + " " + each + " , "
    return cql_query[::-1].replace(",", "", 1)[::-1]


def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)


def convert_pandas_to_spark(result_set, pandasDF, spark_session, cassandra_table):
    # dictonary of conversion ofcassandra datatype to spark datatype
    datatype_conversion_dict = {
        "Int32Type": IntegerType(),
        "FloatType": FloatType(),
        "DateType": DateType(),
        "SimpleDateType": DateType(),
        "DoubleType": DoubleType(),
        "BooleanType": BooleanType(),
        "LongType": LongType(),
        "VarcharType": StringType(),
        "DecimalType": LongType()
    }
    for each in pandasDF.columns:
        if cassandra_table[each] == "int":
            pandasDF[each] = pd.to_numeric(pandasDF[each], errors='coerce').fillna(0).astype("int")
        if cassandra_table[each] == "bigint":
            pandasDF[each] = pd.to_numeric(pandasDF[each], errors='coerce').fillna(0).astype("long")
    # df['no_of_animals'] = pd.to_numeric(df['account_id'], errors='coerce').fillna(0).astype(date)
    cassandra_col_names = result_set.column_names
    cassandra_col_dtype = [str(each).replace("'>", "").split(".")[2].strip() for each in result_set.column_types]
    casted_cassandra_col_name_and_dtype = {cassandra_col_names[i]: cassandra_col_dtype[i] for i in
                                           range(0, len(cassandra_col_names), 1)}
    myschema = StructType(
        [StructField(each, datatype_conversion_dict[casted_cassandra_col_name_and_dtype[each]], True) for each in
         casted_cassandra_col_name_and_dtype])
    return spark_session.createDataFrame(pandasDF, schema=myschema)


def standard_schema_format(dict_df_columns):
    schema = []
    for e in dict_df_columns:
        schema.append(StructField(e, mapp[dict_df_columns[e]], True))
    return StructType(schema)


def standard_cassandra_column_casting(pyspark_df, spark, cassandra_session, cassandra_table):
    '''
    col_and_dtypes = cassandra_table_column_types(cassandra_session,cassandra_table)
    #emptyRDD = spark.sparkContext.emptyRDD()
    #cassandra_column_types_df = spark.createDataFrame(emptyRDD , standard_schema_format(col_and_dtypes))
    cql = cql_query_generator(col_and_dtypes) + " from " + cassandra_table + " limit 5 "
    result_set = cassandra_session.execute(cql)
    pandasDF = result_set._current_rows
    #ordered_columns = pandasDF.columns.values.tolist()
    #my_list = str(my_list).replace("[","").replace("]","")
    cassandra_column_types_df = convert_pandas_to_spark(result_set,pandasDF,spark,col_and_dtypes)
    #columns = []
    #for details in pyspark_df.dtypes:
     #   columns.append(details[0].lower())
    
    for columns_details in cassandra_column_types_df.dtypes:
        column_name, data_type = columns_details
        try:
            pyspark_df = pyspark_df.withColumn(column_name, col(column_name).cast(data_type))
        except:
            print(column_name)
            pyspark_df = pyspark_df.withColumn(column_name, lit(None).cast(data_type))
    
    final_df = pyspark_df.select(cassandra_column_types_df.columns)  # Final dataframe
    '''
    cassandra_session.row_factory = pandas_factory
    col_and_dtypes = cassandra_table_column_types(cassandra_session, cassandra_table)
    emptyRDD = spark.sparkContext.emptyRDD()
    cassandra_column_types_df = spark.createDataFrame(emptyRDD, standard_schema_format(col_and_dtypes))
    result_set = cassandra_session.execute("select * from " + cassandra_table + " limit 1")
    pandasDF = result_set._current_rows
    ordered_columns = pandasDF.columns.values.tolist()
    cassandra_df = cassandra_column_types_df.select(ordered_columns)

    # pyspark_df1 = pyspark_df.select([ F.lit(None).cast('string').alias(i.name) if isinstance(i.dataType, NullType) else i.name
    #                                  for i in pyspark_df.schema])
    '''
    cassandra_df = spark.read.format("org.apache.spark.sql.cassandra")\
            .option("spark.cassandra.connection.host", con.cassandra_host) \
             .option("spark.cassandra.auth.username", con.cassandra_username) \
             .option("spark.cassandra.auth.password", con.cassandra_password) \
             .options(table=con.cassandra_table, keyspace=con.cassandra_keyspace)\
             .load()
    '''

    return standard_column_casting(cassandra_df, pyspark_df)


def cassandra_table_column_types(cassandra_session, cassandra_table):
    cql = "select column_name , type from system_schema.columns where table_name = '" + cassandra_table + "' allow filtering;"
    rdd = cassandra_session.execute(cql)
    p_df = rdd._current_rows
    col_and_dtypes = pd.Series(p_df.type.values, index=p_df.column_name).to_dict()
    return col_and_dtypes


def filter_columns(col_and_dtypes, select_columns_list):
    final_list = {}
    for each in select_columns_list:
        if each in col_and_dtypes:
            final_list[each] = col_and_dtypes[each]
    return final_list


customer_account_details = {

    "CustInfoID": "integer",
    "HomeBranchID": "integer",
    "BranchID": "integer",
    "AccountID": "integer",
    "LoanProposalID": "double",
    "LoanSanctionID": "double",
    "IsDeleted": "boolean",
    "CreatedBy": "integer",
    "CreatedDate": "date",
    "UpdatedBy": "double",
    "UpdatedDate": "date",
    "LoanProductID": "double",
    "IsLiveCustomer": "double",
    "DisbursmentDate": "date",
    "ProcessDate": "date",
    "AccountCode": "double",
    "MandateDate": "date",
    "MandateFlag": "boolean",
    "MandateValidFrom": "string",
    "MandateValidTo": "string",
    "GLId": "double",
    "GLAcct": "double"
}

customer_address = {

    "CustAddrID": "integer",
    "CustomerInfoId": "integer",
    "Address1": "string",
    "Address2": "string",
    "ZipCode1": "integer",
    "ZipCode2": "integer",
    "VillageName": "string",
    "Location": "integer",
    "PermenentLocation": "double",
    "ResInfoId": "string",
    "ResProofId": "integer",
    "CustresDoc": "string",
    "CreatedDate": "date",
    "UpdatedDate": "date",
    "CreatedBy": "integer",
    "UpdatedBy": "double"
}
customer_cashflow = {

    "CashFlowId": "integer",
    "CustomerInfoId": "integer",
    "BranchId": "integer",
    "CenterId": "string",
    "LoanType": "integer",
    "AccountId": "integer",
    "StaffId": "integer",
    "ProcessedDate": "date",
    "ShopAssets": "double",
    "PlantAssets": "double",
    "FurnitureAssets": "double",
    "VehicleAssets": "double",
    "LiveStocks": "double",
    "HouseProperty": "double",
    "VehicleHouseholdAssets": "double",
    "HouseholdAppliances": "double",
    "TVHouseholdAssets": "double",
    "LivestocksHouseHoldAssets": "double",
    "TotalBusinessAssets": "double",
    "TotalHouseholdAssets": "double",
    "TotalAssets": "double",
    "BankLiabilities": "double",
    "FinanceCompanyLiabilities": "double",
    "LoanFromOthers": "double",
    "RDSavings": "double",
    "SIPSavings": "double",
    "InsurancePremium": "double",
    "TotalLiabilitiesandAssets": "double",
    "RentalIncome": "double",
    "Pension": "double",
    "Salary": "double",
    "DailyEarnings": "double",
    "Agriculture": "double",
    "Other": "integer",
    "TotalHouseHoldIncome": "double",
    "Education": "double",
    "Fooding": "double",
    "Transportation": "double",
    "Medical": "double",
    "Water": "double",
    "Telephone": "double",
    "Liabilities": "double",
    "OtherExpenses": "double",
    "TotalHouseholdExpenses": "double",
    "TotalIncomeandExpenses": "double",
    "NetHouseholdincome": "double",
    "PresentValuestock": "double",
    "FirstQuarterAvgStock": "double",
    "SecondQuarterAvgStock": "double",
    "ThirdQuarterAvgStock": "double",
    "FourthQuarterAvgStock": "double",
    "TotalSales": "double",
    "TotalMonthlyAvgSale": "double",
    "TotalBusinessIncome": "double",
    "Purchage": "double",
    "Rent": "double",
    "Labour": "double",
    "BusinessTransportation": "double",
    "EquipementRent": "double",
    "BusinessTelephone": "double",
    "Electricity": "double",
    "BusinessExpenseOthers": "double",
    "TotalBusinessExpenses": "double",
    "NetBusinessIncome": "double",
    "NetTotalIncome": "double",
    "BankLoanAmount": "double",
    "BankTerm": "integer",
    "BankCloserDate": "date",
    "FinanceCompanyLoanAmt": "double",
    "FinanceCompanyTerm": "integer",
    "FinanceCompanyCloserDate": "date",
    "LoanFrmOthersLoanAmt": "double",
    "LoanFrmOthersTerm": "integer",
    "LoanFrmOthersCloserDate": "date",
    "RdTerm": "integer",
    "SipTerm": "integer",
    "InsurancePremiumTerm": "integer",
    "CreatedBy": "integer",
    "CreatedDate": "date",
    "Updatedby": "string",
    "UpdatedDate": "date",
    "IncrIncome": "string",
    "IncrExpense": "string",
    "NetIncrIncome": "string"
}

customer_info = {

    "CustomerInfoId": "integer",
    "CustomerCode": "string",
    "EmployeeId": "string",
    "StaffId": "integer",
    "StateId": "integer",
    "PermenentAddrStateId": "integer",
    "DivisionId": "string",
    "PermenentAddrDisId": "integer",
    "DistrictId": "integer",
    "HubId": "integer",
    "BranchId": "integer",
    "ApplicantName": "string",
    "FirstName": "string",
    "MiddleName": "string",
    "LastName": "string",
    "ApplicantNameHindi": "string",
    "Date": "date",
    "mobile_no": "string",
    "EmailID": "string",
    "Photo": "string",
    "DropOutDate": "date",
    "DropOutStatus": "string",
    "DropoutStatus_old": "double",
    "KYC": "string",
    "BDAId": "string",
    "ClientStatus": "string",
    "RepeatedClient": "string",
    "IsHandicaped": "string",
    "Langitude": "string",
    "Latitude": "string",
    "ExistingLiability": "string",
    "LiabilityFrom": "string",
    "CreditRating": "string",
    "CreditRatingDate": "date",
    "BiometricReference": "string",
    "Freeze": "string",
    "IsTransfered": "double",
    "IsDeleted": "boolean",
    "LoanType": "integer",
    "password": "string",
    "CustType": "string",
    "IsClient": "double",
    "CreatedDate": "date",
    "UpdatedDate": "date",
    "CreatedBy": "integer",
    "UpdatedBy": "double",
    "LoanIterationNo": "double",
    "BirthPlace": "string",
    "ProcessDate": "date",
    "IsHusDeath": "boolean",
    "NewEmpId": "integer",
    "FinalTransfer": "string",
    "DropOutReason": "string",
    "IsDeath": "boolean",
    "DeathDate": "date",
    "OldBranchId": "string",
    "Oldstaffid": "double"
}

customer_other_details = {

    "CustOtherID": "integer",
    "CustomerInfoId": "integer",
    "Age": "integer",
    "Sex": "string",
    "DOB": "string",
    "MaritalStatus": "string",
    "PovertyStatus": "double",
    "HusbandName": "string",
    "FatherName": "string",
    "MotherName": "string",
    "IdType": "integer",
    "IdNo": "string",
    "Custiddoc": "string",
    "NomineeName": "string",
    "NomineeRelation": "string",
    "NomineeAge": "double",
    "GuardianName": "string",
    "HouseStatus": "integer",
    "NatureOfBusiness": "string",
    "ShopName": "string",
    "ShopAddress": "string",
    "Religion": "double",
    "Caste": "integer",
    "Incomedetails": "double",
    "NoOfMembers": "integer",
    "NoOfDaughters": "double",
    "NoOfSons": "double",
    "DAgebelow15": "double",
    "DAgeabove15": "double",
    "SAgebelow15": "double",
    "SAgeabove15": "double",
    "NoOfOtherMem": "double",
    "TotalArea": "integer",
    "OwnArea": "boolean",
    "ShareArea": "boolean",
    "OwnIrrigate": "double",
    "OwnNonIrrigate": "double",
    "ShareIrrigate": "double",
    "ShareNonIrrigate": "double",
    "NoOfAnimal": "double",
    "AnimalValue": "double",
    "BcName": "double",
    "Detail": "string",
    "AssetValue": "double",
    "Description": "string",
    "IndexScore": "double",
    "Regular": "boolean",
    "Migration": "boolean",
    "MemberLiteracy": "boolean",
    "HusbandLiteracy": "boolean",
    "Response": "double",
    "BankName": "double",
    "AccountHolderName": "string",
    "IFSCCode": "string",
    "AccountNo": "double",
    "AccountType": "string",
    "BankBranchName": "string",
    "ChequePhotograph": "string",
    "GovtScheme": "boolean",
    "EnrollmentNo": "double",
    "IsDeleted": "boolean",
    "CreatedDate": "date",
    "UpdatedDate": "date",
    "CreatedBy": "integer",
    "UpdatedBy": "double",
    "HusbMiddleName": "string",
    "HusbLastName": "string",
    "HusbFirstName": "string",
    "HusbDOB": "string",
    "AgricultureIncome": "double",
    "NonAgricultureIncome": "double",
    "AnnualIncome": "double",
    "DCBRefNumber": "double"
}

loan_disbursement = {

    "DisbursementID": "integer",
    "LoanSanctionID": "integer",
    "CustomerInfoID": "integer",
    "ProductID": "integer",
    "BranchID": "integer",
    "CenterID": "integer",
    "DisbursedAmt": "double",
    "DisbursementDate": "date",
    "FunderID": "double",
    "InsuMainID": "double",
    "TotalInterest": "double",
    "TransMode": "double",
    "TransModeNo": "double",
    "PincipalOS": "double",
    "InterestOS": "double",
    "TotalInstallment": "double",
    "ActualPaidDate": "date",
    "ChequeDate": "date",
    "IsPreSettlement": "boolean",
    "IsWriteOff": "boolean",
    "WriteOffAmt": "double",
    "ExpectPaidDate": "date",
    "Is_Active": "boolean",
    "LoanType": "double",
    "ProcessDate": "date",
    "CreatedBy": "integer",
    "CreatedDate": "date",
    "UpdatedBy": "double",
    "UpdatedDate": "date",
    "Remarks": "string",
    "TransactionType": "string",
    "IsJV": "string",
    "AccountId": "integer",
    "IsAuthenticated": "boolean",
    "CustomerClassification": "double",
    "CustomerClassificationDt": "string",
    "RefDisbursementId": "double",
    "ActualDisburseAmount": "double",
    "RecoveryAmount": "double",
    "RepaymentMode": "double",
    "UMRNCode": "string",
    "ST_Deducted": "boolean",
    "IP_Deducted": "boolean",
    "IRR_OLD": "double",
    "IRR_New": "double",
    "TOTALINTCALCULATED_MORATORIUM": "double",
    "TOTALINTRECVD_MORATORIUM": "double",
    "FunderIddetails": "double"
}

loan_proposal = {

    "ProposalId": "integer",
    "ProposalCode": "string",
    "GroupId": "integer",
    "ProposalDate": "date",
    "ProductID": "integer",
    "PurposeID": "integer",
    "PurposeGrpID": "integer",
    "LoanType": "integer",
    "LoanAmt": "double",
    "PaymentFrequency": "integer",
    "TermPeriod": "integer",
    "IsDeleted": "boolean",
    "CreatedDate": "date",
    "UpdatedDate": "date",
    "CreatedBy": "integer",
    "UpdatedBy": "double",
    "BranchId": "integer",
    "CustomerInfoId": "integer",
    "StaffId": "integer",
    "CenterId": "integer",
    "ProposalStatus": "string",
    "Remark": "string",
    "LoanStatus": "integer",
    "ProcessDate": "date",
    "ProductDetailsId": "integer",
    "AccountId": "integer",
    "AccHolder": "double",
    "Transfer": "string",
    "NewEmpId": "integer",
    "clsid": "string",
    "DisbursementAcc": "double",
    "RecoveryAmt": "double",
    "NoOfLoanRecover": "double",
    "ProductCategory": "double",
    "ProductFreq": "double",
    "BCStatus": "double",
    "LEAD_ID": "string",
    "PredisbVerified": "boolean"
}

loan_sanction = {

    "LoanSanctionId": "integer",
    "ProposalId": "integer",
    "ProductId": "integer",
    "CustomerInfoId": "integer",
    "BranchId": "integer",
    "SanctionDate": "date",
    "SanctionAmt": "double",
    "TermPeriod": "double",
    "GroupId": "integer",
    "CenterID": "integer",
    "PayMode": "double",
    "DisbursementAmount": "double",
    "DisbursementDate": "date",
    "DisburementStatus": "double",
    "FunderId": "string",
    "InsuranceCoId": "string",
    "CreatedDate": "date",
    "UpdatedDate": "date",
    "CreatedBy": "integer",
    "UpdatedBy": "double",
    "AccountId": "integer",
    "Transfer": "double",
    "ProductDetId": "integer",
    "ProcessDate": "date",
    "RecoveryAmt": "double",
    "FirstInstallmentDate": "date",
    "PartDisbursedAmt": "double"
}

transactions = {

    "InstallScheduleId": "integer",
    "Attendance": "string",
    "BranchId": "integer",
    "DisbursementId": "integer",
    "CustomerInfoId": "integer",
    "CenterId": "integer",
    "InstallmentId": "integer",
    "ScheduleDate": "date",
    "PrincipleAmt": "double",
    "InterestAmt": "double",
    "PincipalOS": "double",
    "InterestOS": "double",
    "PrincipleCollected": "double",
    "InterestCollected": "double",
    "IsPreSettlement": "string",
    "PreSettlementAmt": "double",
    "IsDeleted": "string",
    "CreatedBy": "double",
    "CreatedDate": "date",
    "UpdatedBy": "double",
    "UpdatedDate": "date",
    "CollectedDate": "date",
    "PenaltyAmount": "double",
    "PrincipalArrear": "double",
    "InterestArrear": "double",
    "IsWriteOff": "string",
    "AccountId": "integer",
    "PartialCollectedDate": "date",
    "NACHStatus": "double",
    "ModeOfPayment": "double",
    "DPDFlag": "string",
    "DPDDate": "date",
    "DPDPrinciplePaid": "double",
    "DPDInterestPaid": "double",
    "OLDSCHEDULEDATE": "string",
    "index": "integer",
}

# ref

# a = list(customer_account_details.values())
# b = list(customer_address.values())
# c = list(customer_cashflow.values())
# d = list(customer_info.values())
# e = list(customer_other_details.values())
# f = list(loan_disbursement.values())
# g = list(loan_proposal.values())
# h = list(loan_sanction.values())
# i = list(transactions.values())

# final = a+b+c+d+e+f+g+i
# number_of_datatypes = list(set(final))
