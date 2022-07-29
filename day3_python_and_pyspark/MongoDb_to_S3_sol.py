# Databricks notebook source
#import libraries
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
import urllib


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Add the Mongo DB Connector as a Library
# MAGIC ######1-Navigate to the cluster detail page and select the Libraries tab.
# MAGIC ######2-Click the Install New button.
# MAGIC ######3-Select Maven as the Library Source.
# MAGIC ######4-Enter the Mongo DB Connector for Spark package value into the Coordinates field based on your Databricks Runtime version:
# MAGIC ######5-For Databricks Runtime 7.0.0 and above, enter org.mongodb.spark:mongo-spark-connector_2.12:3.0.0.
# MAGIC ######6-For Databricks Runtime 5.5 LTS and 6.x, enter org.mongodb.spark:mongo-spark-connector_2.11:2.3.4.
# MAGIC ######7-Click Install.

# COMMAND ----------

# MAGIC %md
# MAGIC ####Configure Databricks Cluster with MongoDB Connection URI
# MAGIC ######1-Get the MongoDB connection URI. In the MongoDB Atlas UI, click the cluster you created.
# MAGIC 
# MAGIC ######2- Click the Connect button.
# MAGIC ######3- Click Connect Your Application.
# MAGIC ######4- Select python in the Driver dropdown and 6 or later in the version dropdown.
# MAGIC ######   Copy the generated connection string. It should look like mongodb+srv://\<user\>:\<password\>@\<cluster-name\>-wlcof.azure.mongodb.net/test?retryWrites=true
# MAGIC ###### Configure the user, password, and cluster-name values.
# MAGIC ######5- In the cluster detail page for your Databricks cluster, select the Configuration tab.
# MAGIC ######   Click the Edit button.
# MAGIC ######   Under Advanced Options, select the Spark configuration tab and update the Spark Config using the connection string you copied in the ######previous   
# MAGIC ######step:
# MAGIC 
# MAGIC ######spark.mongodb.output.uri \<connection-string\>
# MAGIC ######spark.mongodb.input.uri \<connection-string\>

# COMMAND ----------

#define the fromat which is in our case "com.mongodb.spark.sql.DefaultSource", define the database name and the collection name
users_tbl = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database", "sample_mflix").option("collection", "users").load()


# COMMAND ----------

# show the table
users_tbl.show(truncate = False)

# COMMAND ----------

# take sample of the pyspark df
users_tbl = users_tbl.limit(5)

# COMMAND ----------

# drop the _id column
users_tbl = users_tbl.drop('_id')

# COMMAND ----------

#mount s3 bucket: starting the s3 connection
ACCESS_KEY = "xx"
secret_key = "xx"
AwsBucketName = "tweeq-academy"
MountName = "/mnt/MOUNT-tweeq-academy"

ENCODED_SECRET_KEY = urllib.parse.quote(string=secret_key, safe="")
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AwsBucketName) 

# COMMAND ----------

#connect
dbutils.fs.mount(SOURCE_URL, MountName)

# COMMAND ----------

#wirte to s3
users_tbl.write.save(f'/mnt/MOUNT-tweeq-academy/users_table.csv',format="csv", header = True)

# COMMAND ----------

# validate the writing process 
df = spark.read.format("csv").load('/mnt/MOUNT-tweeq-academy/users_table.csv', header = True)

# COMMAND ----------

df.show()

# COMMAND ----------


