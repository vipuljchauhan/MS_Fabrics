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

# ### This notebook we use for common function in entire project

# CELL ********************

#import libraries
import zipfile
import os
import smtplib
import time
from email.mime.text import MIMEText
from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run "./variable_assignments"   

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# #define functions - unzip_files
# def unzip_files(zip_path, unzip_path):
#     #validation 1: file exists
#     if not os.path.exists(zip_path):
#         raise FileNotFoundError(f"zip file is not found: {zip_path}")
#     else:
#         print(f"file found: {zip_path}")
#     #validation 2: file is not empty
#     # if os.path.getsize(zip_path) == 0:
#     #     raise Exception(f"zip file: {zip_path} is empty")
#     # else:
#     #     print(f"zip file: {zip_path} is not empty")
#     #validation 2: zip file is empty based on list of the files
#     with zipfile.ZipFile(zip_path, 'r') as zip_reference:
#         list_files = zip_reference.namelist()
#         # print(list_files)
#         if len(list_files) == 0:
#             raise Exception(f"zip file: {zip_path} is empty")
#         else:
#             print(f"zip file: {zip_path} is not empty")
#     #validation 3: zip file is not empty but underneeth file is empty
#     with zipfile.ZipFile(zip_path, 'r') as zip_reference:
#         # for file_info in zip_reference.infolist():
#         #     print(file_info.filename, file_info.file_size)
#         empty_files =   [
#                             file_info.filename
#                             for file_info in zip_reference.infolist() if file_info.file_size == 0 
#                         ]
#         print(f"{empty_files} These are the empty files present in: {zip_path} ")
#     #validation 4: is zip file is a bad file
#     with zipfile.ZipFile(zip_path, 'r') as zip_reference:
#         corrupted_file = zip_reference.testzip()
#         if corrupted_file:
#             raise Exception(f"zip file is corrupted : {zip_path}")
#         else:
#             print(f"zip file is not corrupted : {zip_path}")
#     #validation 5: expected number of file inside the zip file
#     with zipfile.ZipFile(zip_path, 'r') as zip_reference:
#         list_files = zip_reference.namelist()
#         total_files = len(list_files)
#         print(total_files)
#         if total_files == 4:
#             print(f"all files are available under zip file: {zip_path}")
#         else:
#             print(f"total files missing: {excepted_files - total_files}")
#     #to make a directory to extract file
#     actual_unzip_path = unzip_path+folder_name
#     os.makedirs(actual_unzip_path, exist_ok=True)
#     with zipfile.ZipFile(zip_path, 'r') as zip_reference:
#         zip_reference.extractall(actual_unzip_path)            
        

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#define functions - unzip_files
def unzip_files(zip_path, unzip_path):
    #validation 1: file exists
    if not os.path.exists(zip_path):
        validation_message = f"Error : zip file is not found: {zip_path}"
        raise FileNotFoundError(f"Error : zip file is not found: {zip_path}")
    else:
        print(f"file found: {zip_path}")
        validation_message = f"file found: {zip_path}"
    #validation 2: file is not empty - this is not working as blank zip file cannot be zero bytes most of the time.
    # if os.path.getsize(zip_path) == 0:
    #     raise Exception(f"zip file: {zip_path} is empty")
    # else:
    #     print(f"zip file: {zip_path} is not empty")
    #validation 2: zip file is empty based on list of the files
    with zipfile.ZipFile(zip_path, 'r') as zip_reference:
        list_files = zip_reference.namelist()
        if len(list_files) == 0:
            validation_message = f"Error : zip file: {zip_path} is empty"
            raise Exception(f"Error : zip file: {zip_path} is empty")
        else:
            print(f"zip file: {zip_path} is not empty")
            validation_message = f"zip file: {zip_path} is not empty"
    #validation 3: zip file is not empty but underneeth file is empty 
        empty_files =   [
                            file_info.filename
                            for file_info in zip_reference.infolist() if file_info.file_size == 0 
                        ]
        print(f"{empty_files} Error : These are the empty files present in: {zip_path} ")
        validation_message = f"{empty_files} Error : These are the empty files present in: {zip_path}"
    #validation 4: is zip file is a bad file
        corrupted_file = zip_reference.testzip()
        if corrupted_file:
            validation_message = f"Error : zip file is corrupted : {zip_path}"
            raise Exception(f"Error : zip file is corrupted : {zip_path}")
        else:
            print(f"zip file is not corrupted : {zip_path}")
            validation_message = f"zip file is not corrupted : {zip_path}"
    #validation 5: expected number of file inside the zip file
        list_files = zip_reference.namelist()
        total_files = len(list_files)
        print(total_files)
        print(excepted_files)
        if int(total_files) == 4:
            print(f"all files are available under zip file: {zip_path}")
            validation_message = f"all files are available under zip file: {zip_path}"
        else:
            print(f"Error : total files missing: {int(excepted_files) - int(total_files)}")
            validation_message = f"Error : total files missing: {int(excepted_files) - int(total_files)}"
    #to make a directory to extract file
        actual_unzip_path = unzip_path+folder_name
        os.makedirs(actual_unzip_path, exist_ok=True)
        zip_reference.extractall(actual_unzip_path)
    return validation_message

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Loggin Function
def process_log(
    run_status,
    file_name = None,
    total_inserted_count = None,
    total_updated_count = None,
    error_message = None,
    email_status = None,
    business_date = None,
    run_end_time = None
):
    global run_id, run_start_time, pipeline_name, task_name 
    log_schema = """run_id string,
                    pipeline_name string,
                    task_name string,
                    file_name string,
                    business_date date,
                    run_start_time timestamp,
                    run_end_time timestamp,
                    run_status string,
                    total_inserted_count long,
                    total_updated_count long,
                    error_message string,
                    email_status string
                 """

    run_end_time = datetime.now()
    business_date = run_end_time.date()
    log_data = [(run_id, 
                pipeline_name, 
                task_name,
                file_name,
                business_date, 
                run_start_time, 
                run_end_time, 
                run_status, 
                total_inserted_count, 
                total_updated_count, 
                error_message, 
                email_status)
                ]             

    log_df = spark.createDataFrame(log_data, schema=log_schema)
    log_df.createOrReplaceTempView("temp_etl_run_log")
    spark.sql("""
        MERGE INTO control.etl_run_log AS t
        USING temp_etl_run_log AS s
        ON t.run_id=s.run_id
        WHEN MATCHED THEN
        UPDATE SET t.run_status = s.run_status
        WHEN NOT MATCHED THEN INSERT *
        """)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#email notification
def send_mail_fn (f_subject,b_message): 
    sender_email = "amit.aigk@gmail.com"
    to_email = "vipuljchauhan@gmail.com"
    host="smtp.gmail.com"
    port=587
    server=smtplib.SMTP(host,port)
    server.starttls()
    server.login(sender_email,"phmo embc isbs gvlw")
    m_text=b_message
    test=MIMEText(m_text,"html","utf-8")
    test["subject"]=f_subject
    test["From"]=sender_email
    test["to"]=to_email
    server.send_message(test)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
