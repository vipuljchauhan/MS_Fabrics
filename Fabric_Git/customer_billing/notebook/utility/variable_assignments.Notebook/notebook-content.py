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

# ### This notebook we created for creating variable from dev.config

# CELL ********************

# import libraries
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# decraring variable

file_location = "/lakehouse/default/Files/config/dev_config.json"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# open json file
with open(file_location) as f:
    config_data = json.load(f) 
# declaring variables  
env = (config_data)["env"]
input_folder = (config_data)["input_folder"]
unzip_folder = (config_data)["unzip_folder"]
excepted_files = config_data["excepted_files"] 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
