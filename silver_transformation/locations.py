

df_loc = spark.read.table("pyspark_dbt.bronze.locations")



loc_obj = transformationss()
loc_df = loc_obj.dedup(df_loc, ['location_id'], 'last_updated_timestamp')
loc_df = loc_obj.process_timestamp(loc_df)

from delta.tables import DeltaTable

if not spark.catalog.tableExists("pyspark_dbt.silver.locations"):
        loc_df.write.format("delta")\
               .mode("append")\
               .saveAsTable("pyspark_dbt.silver.locations")
else:
 
        key_cols = ['location_id']
        merge_condition = ' AND '.join([f"tgr.{i} = src.{i}" for i in key_cols])
        delta_obj = DeltaTable.forName(spark, "pyspark_dbt.silver.locations")
        delta_obj.alias("tgr").merge(loc_df.alias("src"), merge_condition)\
                              .whenMatchedUpdateAll()\
                              .whenNotMatchedInsertAll()\
                              .execute()
        
