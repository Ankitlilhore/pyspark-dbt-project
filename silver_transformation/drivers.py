


from pyspark.sql.functions import *

df_driver = spark.read.table("pyspark_dbt.bronze.customers")
df_driver = df_driver.withColumn("phone_number", regexp_replace("phone_number", r"[^0-9]", ""))

df_driver = (df_driver.withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))
                      .drop("first_name", "last_name"))

from utiles.custom_utiles import transformationss

driver_obj = transformationss()

df_driver = driver_obj.dedup(df_driver, ['driver_id'], 'last_updated_timestamp')
from delta.tables import DeltaTable

if not spark.catalog.tableExists("pyspark_dbt.silver.drivers"):
        df_driver.write.format("delta")\
               .mode("append")\
               .saveAsTable("pyspark_dbt.silver.drivers")
else:

        key_cols = ['driver_id']
        merge_condition = ' AND '.join([f"tgr.{i} = src.{i}" for i in key_cols])
        delta_obj = DeltaTable.forName(spark, "pyspark_dbt.silver.drivers")
        delta_obj.alias("tgr").merge(df_driver.alias("src"), merge_condition)\
                              .whenMatchedUpdateAll()\
                              .whenNotMatchedInsertAll()\
                              .execute()
