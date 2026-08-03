#---------------------------------------------------------------------------------
# Created scripts for dynamically ingest data from all the tables
#---------------------------------------------------------------------------------

entities = ['customers', 'trips', 'locations', 'payments', 'drivers', 'vehicles']

for entity in entities:

    batch_df  = ( spark.read.format("csv")
                    .option("header", True)
                    .option("inferschema", True)
                    .load(f"/Volumes/pyspark_dbt/source/source_data/{entity}/")
                )

    batch_df_schema = batch_df.schema
      
    df = ( spark.readStream.format("csv")
               .option("header", True)
               .schema(batch_df_schema)
               .load(f"/Volumes/pyspark_dbt/source/source_data/{entity}/")
      
      )
    df.writeStream.format("delta")\
              .outputMode("append")\
              .option("checkpointLocation", f"/Volumes/pyspark_dbt/bronze/check_point/{entity}")\
              .trigger(once=True)\
              .toTable(f"pyspark_dbt.bronze.{entity}")


