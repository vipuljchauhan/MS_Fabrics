# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "cde0af6b-b31f-4925-bd0e-4d3da1bf9946",
# META       "default_lakehouse_name": "lk_vdp",
# META       "default_lakehouse_workspace_id": "864f8118-bd12-445b-9f6b-8d025474280c",
# META       "known_lakehouses": [
# META         {
# META           "id": "cde0af6b-b31f-4925-bd0e-4d3da1bf9946"
# META         }
# META       ]
# META     },
# META     "warehouse": {}
# META   }
# META }

# MARKDOWN ********************

# ## PySpark Basics

# CELL ********************

#library

from pyspark.sql.functions import col

from pyspark.sql import functions as f

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# create dataframe

emp_data = [
            (1,'Vipul','Mumbai','2026-09-01 10:11:05'),
            (2,'Ashu','Pune','2026-09-01 10:11:06'),
            (3,'Mandar','Thane','2026-09-01 10:11:07'),
            (4,'Kedar','Thane','2026-09-01 10:11:08'),
            (5,'Sachin','Mumbai','2026-09-01 10:11:09'),
            (5,'Sachin','Mumbai','2026-09-01 10:11:10'),
            (5,'Sachin','Borivali','2026-09-01 10:11:11')
            ]

# create dataframe from above data
df_emp = spark.createDataFrame(emp_data,["id","names","city","created_date"])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# display(df_emp)

# df_emp.printSchema()

# display(df_emp.select("names","city"))

# df_filter = df_emp.filter(df_emp.city == "Pune")
# display(df_filter)

# df_filter = df_emp.filter(col("city") == "Pune")
# display(df_filter)

# df_filter = df_emp.filter(f.col("city") == "Pune")
# display(df_filter)

# df_filter = df_emp.filter(f.col("city").isin ("Thane","Pune"))
# display(df_filter)

# df_filter = df_emp.filter(f.col("city").isin ("Thane","Pune"))
# display(df_filter)

# df_filter = df_emp.filter(
#     (f.col("city").isin ("Thane","Pune")) &
#     (f.col("id") == 3)
#     )
# display(df_filter)


# df_filter = df_emp.filter(
#     (~f.col("city").isin ("Thane","Pune"))
#     )
# display(df_filter)

df_filter = df_emp.filter(
    (f.col("city").like ("Pu%"))
    )
display(df_filter)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add new column in existing dataframe

df_emp = df_emp.withColumn("country",f.lit("India"))

df_emp = df_emp.withColumnRenamed("country","countries")

#df_emp = df_emp.orderBy(f.desc("id"),f.asc("names"))

df_emp = df_emp.orderBy(f.desc("city"),f.asc("names"))

display(df_emp)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# dataframe diplay with alias


display(df_emp.select(f.col("id").alias("emp_id")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# find count of dataframe

df_emp.count()

selectedColumnCount = df_emp.select(f.col("id")).distinct().count()

print ("selectedColumnCount: ", selectedColumnCount)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# remove duplicate records

# display(df_emp)

duplicateRemove = df_emp.drop_duplicates()
display(duplicateRemove)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#how to remove partial  using ranking function
from pyspark.sql.window import Window

window_spect = Window.partitionBy("id", "names").orderBy(f.desc("created_date"))

df_rank = duplicateRemove.withColumn("rn", f.row_number().over(window_spect)).filter(f.col("rn") == "1")

display(df_rank)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
