# Databricks notebook source
# MAGIC %md
# MAGIC ### This notebook as getting all tables from mng schema to load from pc-rep to bronze and save as parameter.

# COMMAND ----------

from pyspark.sql.functions import col

# Create the list of dictionaries
df = spark.table("pcil_travel.mng.tables_to_load")

# Filter the DataFrame where 'load_type' equals 'full'
filtered_df = df.filter(col("load_type") == "incremental")

# Get only table name
filtered_df = filtered_df.select('table_name', 'load_by_1', 'load_by_2')

# Show the filtered DataFrame
display(filtered_df)

# Convert the filtered DataFrame back to a list of dictionaries
result = filtered_df.collect()
table_list = [row.asDict() for row in result]

# Show the resulting list
print(table_list)

dbutils.jobs.taskValues.set(
"table_list", table_list
)
