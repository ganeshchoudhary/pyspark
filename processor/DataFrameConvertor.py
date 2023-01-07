def convert_schema(old_df, new_df):
    new_df_cols = [colmn.lower() for colmn in new_df.columns]
    for col_details in old_df.dtypes:
        col_name, dtype = col_details
        if col_name.lower() in new_df_cols:
            new_df = new_df.withColumn(col_name, f.col(col_name).cast(dtype))
        else:
            new_df = new_df.withColumn(col_name, f.lit(None).cast(dtype))
    final_df = new_df.select(old_df.columns)
    return final_df