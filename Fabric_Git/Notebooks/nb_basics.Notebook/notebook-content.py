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

display(df_emp)

# df_emp.printSchema()

# display(df_emp.select("names","city"))

df_filter = df_emp.filter(df_emp.city == "Pune")
display(df_filter)

df_filter = df_emp.filter(col("city") == "Pune")
display(df_filter)

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

# df_filter = df_emp.filter(
#     (f.col("city").like ("Pu%"))
#     )
# display(df_filter)

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

# display(df_emp)

# display(df_emp.select(((f.col("id")).alias("emp_id")),"city","names"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# dataframe diplay with alias

display(df_emp.select(f.col("id").alias("emp_id")))

display(df_emp.select(((f.col("id")).alias("emp_id")),"city","names"))

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

# CELL ********************

#Get a count for ALL cities at once

city_counts_df = df_emp.groupby("city").count()
display(city_counts_df)


# Count employees from a single city (e.g., 'Mumbai')
Mumbai_count = df_emp.filter(df_emp["city"] == "Mumbai").count()
print(f"Total employees in Mumbai: {Mumbai_count}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Aggregate functions

salary_data = [
            (1,'Vipul','10000','Admin'),
            (2,'Ashu','20000','IT'),
            (3,'Mandar','30000','HOD'),
            (4,'Kedar','20000','IT'),
            (5,'Sachin','50000','VP'),
            (5,'Ganesh','30000','IT'),
            (5,'Amit','20000','IT')
            ]

# create dataframe from above data
salary_df = spark.createDataFrame(salary_data,["id","names","salary","dept"])

display(salary_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

salary_df.agg(f.count("*").alias("employee_count"),
f.sum("salary").alias("total_salary")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

salary_df.groupBy("dept").agg(f.count("*").alias("employee_count"),
f.sum("salary").alias("total_salary")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Joins

emp  = [
        (1,"Ganesh"),
        (2,"Amit"),
        (3,"Vipul")
        ]

department = [
            (1,"IT"),
            (2,"HR"),
            (4,"Admin")
            ]        

# create dataframe from above data
df_emp = spark.createDataFrame(emp,["id","names"])

df_department = spark.createDataFrame(department,["id","department"])

display(df_emp)
display(df_department)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # inner join
# df_emp.join(df_department,"id","inner").show()

# # left join
# df_emp.join(df_department,"id","left").show()

# # right join
# df_emp.join(df_department,"id","right").show()

# # outer join
# df_emp.join(df_department,"id","outer").show()

# # left anti join
# df_emp.join(df_department,"id","left_anti").show()

# # anti join
# df_emp.join(df_department,"id","anti").show()

# semi join
df_department.join(df_emp,"id","semi").show()

# # cross join
# df_department.join(df_emp,how="cross").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df_emp.join(df_department,"id","inner").show()
# df_emp.join(df_department,"id","left").show()
# df_emp.join(df_department,"id","right").show()
# df_emp.join(df_department,"id","outer").show()
# df_emp.join(df_department,"id","left_anti").show()
# df_department.join(df_emp,"id","anti").show()
# df_department.join(df_emp,"id","semi").show()
df_department.join(df_emp,how="cross").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# joins with multiple columns

#joins
emp = [
    (1,"Ganesh",101),
    (2,"Pratik",101),
    (3,"Vipul",104)
]

department = [
    (1,"IT",101),
    (2,"HR",102),
    (4,"ADMIN",104)
]

df_emp = spark.createDataFrame(emp,["emp_id","name","emp_dept_id"])

df_department = spark.createDataFrame(department,["employee_id","department","emp_department"])

display(df_emp)
display(df_department)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df_emp.join(df_department,(df_emp.emp_id==df_department.employee_id)
# &(df_emp.emp_dept_id==df_department.emp_department),"inner").select()


df_emp.join(df_department, (df_emp.emp_id==df_department.employee_id)&
(df_emp.emp_dept_id==df_department.emp_department),"inner").select("emp_id","name","department").show()

# df_emp.join(df_department, (df_emp.emp_id==df_department.employee_id)&
# (df_emp.emp_dept_id==df_department.emp_department),"inner").select("emp_id","name","department").display()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 
%fs ls Files/emp

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# read all files from emp folder
emp_df = spark.read.format("csv")\
.option("delimiter","|")\
.option("header",True)\
.option("inferSchema",True)\
.load("Files/emp")
display(emp_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Design custom schema
from pyspark.sql.types import StructField, IntegerType, StringType, StructType

custom_schema = StructType(
    [StructField("emp_id", IntegerType(), True),
    StructField("emp_name", StringType(), True),
    StructField("emp_city", StringType(), True)]
)

# read all files from emp folder
emp_df = spark.read.format("csv")\
.option("delimiter","|")\
.option("header",True)\
.option("inferSchema",True)\
.schema(custom_schema)\
.load("Files/emp")
display(emp_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# read files one by one from emp folder

# make a list of all files in one array
all_files = mssparkutils.fs.ls("Files/emp")
# print(all_files)
for f in all_files:
    # print(f.path)
    emp_df = spark.read.format("csv")\
    .option("delimiter","|")\
    .option("header",True)\
    .option("inferSchema",True)\
    .load(f.path)

    emp_df.write.mode("append").saveAsTable("emp_data")
display(emp_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM emp_data

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("emp_data").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

