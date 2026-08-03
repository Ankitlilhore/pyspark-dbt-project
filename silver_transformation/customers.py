

from pyspark.sql.functions import *

df = spark.read.table("pyspark_dbt.bronze.customers")

df_cust = df.withColumn("domain", split("email", "@")[1])

df_cust = df_cust.withColumn("phone_number", regexp_replace("phone_number", r"[^0-9]", ""))

df_custom = (df_cust.withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name"))) 
                   .drop("first_name", "last_name"))


from typing import List
from pyspark.sql.functions import col, concat, row_number, current_timestamp
from pyspark.sql import DataFrame
from pyspark.sql.window import Window 

from utiles.custom_utiles import transformationss

dedup_obj = transformationss(spark)

cust_df_transformation = dedup_obj.dedup(df_custom, ['customer_id'], 'last_updated_timestamp')
process_obj = dedup_obj.process_timestamp(cust_df_transformation)


from delta.tables import DeltaTable

if not spark.catalog.tableExists("pyspark_dbt.silver.customers"):
        df_custom.write.format("delta")\
               .mode("append")\
               .saveAsTable("pyspark_dbt.silver.customers")
else:
        key_cols = ['customer_id']
        merge_condition = ' AND '.join([f"tgr.{i} = src.{i}" for i in key_cols])
        delta_obj = DeltaTable.forName(spark, "pyspark_dbt.silver.customers")
        delta_obj.alias("tgr").merge(process_obj.alias("src"), merge_condition)\
                              .whenMatchedUpdateAll()\
                              .whenNotMatchedInsertAll()\
                              .execute()


