


df_veh = spark.read.table("pyspark_dbt.bronze.vehicles")

from pyspark.sql.functions import upper, col

df_veh = df_veh.withColumn("make", upper(col("make")))
from utiles.custom_utiles import transformationss
veh_obj = transformationss()

df_veh = veh_obj.dedup(df_veh, ['vehicle_id'], 'last_updated_timestamp')
df_veh = veh_obj.process_timestamp(df_veh)


from delta.tables import DeltaTable

if not spark.catalog.tableExists("pyspark_dbt.silver.vehicles"):
        df_veh.write.format("delta")\
                    .mode("append")\
                    .saveAsTable("pyspark_dbt.silver.vehicles")

else:
        key_cols = ['vehicle_id']
        veh_obj  = DeltaTable.forName(spark, "pyspark_dbt.silver.vehicles")
        veh_obj.alias("target").merge(df_veh.alias("source"),
                                       ' AND '.join([f"target.{i} = source.{i}" for i in key_cols]))\
                               .whenMatchedUpdateAll()\
                               .whenNotMatchedInsertAll()\
                               .execute()
