# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5e0350cc-d1f3-4c3a-a60e-708d36a37aa1",
# META       "default_lakehouse_name": "lk_customer_billing",
# META       "default_lakehouse_workspace_id": "864f8118-bd12-445b-9f6b-8d025474280c",
# META       "known_lakehouses": [
# META         {
# META           "id": "5e0350cc-d1f3-4c3a-a60e-708d36a37aa1"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# **Executing this notebook to check zip file is good and extracting to right folder**

# CELL ********************

%run "./common_functions"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#declaring variables
folder_name = datetime.now().strftime("%Y%m%d")
# print(folder_name)
run_start_time = datetime.now()
# pipeline_name = "test_pipeline_1"
# task_name = "test_task_1"
run_id = mssparkutils.env.getJobId() + f"_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
# file_name = "client_input_multi_empty_file_03102026_014005.zip"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_input_folder = input_folder+file_name

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#calling unzip function with email notification
try:
    process_log("Start",file_name=file_name)
    spark.sql("""SELECT * FROM control.etl_run_log ORDER BY run_end_time DESC LIMIT 1""").show()
    validation_message = unzip_files(final_input_folder, unzip_folder)
    if "Error : " in validation_message:
        raise Exception(validation_message)
    print(f"validation message after execution : {validation_message}")
    time.sleep(15)
    process_log("Success",file_name=file_name)
    spark.sql("""SELECT * FROM control.etl_run_log ORDER BY run_end_time DESC LIMIT 1""").show()
except Exception as e:
    process_log("Failed",file_name=file_name, error_message=f"{validation_message}")
    send_mail_fn(f'{pipeline_name}-{task_name}-{file_name} is failed',f'{validation_message}')
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
