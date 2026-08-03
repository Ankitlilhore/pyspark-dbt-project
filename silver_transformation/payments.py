

df_payments = spark.read.table("pyspark_dbt.bronze.payments")

from utiles.custom_utiles import transformationss

payments_obj = transformationss()

df_payments = payments_obj.dedup(df_payments, ['payment_id'], 'last_updated_timestamp')
df_payments = payments_obj.process_timestamp(df_payments)




rom pyspark.sql.functions import when, col

df_payments = df_payments.withColumn("online_payment_status", 
                                     when(
                                          (col("payment_method") == 'Card') & (col("payment_status") == 'Success'), "online-success" )
                                      .when(
                                          (col("payment_method") == 'Card') & (col("payment_status") == 'Failed'),
                                            "online-failed")
                                      .when(
                                          (col("payment_method") == 'Card') & (col("payment_status") == 'Pending'),
                                            "online-pending")
                                      .otherwise("offline"))



from delta.tables import DeltaTable

if not spark.catalog.tableExists("pyspark_dbt.silver.payments"):
                    df_payments.write.format("delta")\
                    .mode("append")\
                    .saveAsTable("pyspark_dbt.silver.payments")

else:
          key_cols = ['payment_id']
          payments_obj = DeltaTable.forName(spark, "pyspark_dbt.silver.payments")
          payments_obj.alias("target").merge(df_payments.alias("source"),
                                              ' AND '.join([f"target.{i} = source.{i}" for i in key_cols]))\
                                      .whenMatchedUpdateAll()\
                                      .whenNotMatchedInsertAll()\
                                      .execute()
                                          
                                      
          





