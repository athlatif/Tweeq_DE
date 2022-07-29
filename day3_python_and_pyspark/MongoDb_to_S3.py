#import pyspark & urllib


# ======= Read from MongoDB ========

#define the fromat which is in our case "com.mongodb.spark.sql.DefaultSource", define the database name [sample_mflix] and the collection name [users]
users_tbl = spark.read.format().option().option().load()

# show the table
users_tbl.show(truncate = False)

# take sample of the pyspark df
users_tbl = 

# drop the _id column
users_tbl = 

# ======= Write to S3 ========

# mount S3, change the ACCESS_KEY, secret_key & AwsBucketName.
ACCESS_KEY = "xx"
secret_key = "xx"
AwsBucketName = "xxxx"
MountName = "/mnt/MOUNT-xxx"
ENCODED_SECRET_KEY = urllib.parse.quote(string=secret_key, safe="")
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AwsBucketName) 

dbutils.fs.mount(SOURCE_URL, MountName)

#wirte to s3 using write.save as csv format
users_tbl.write.

# validate the writing process 
df = spark.read.format().load(, header = True)

df.show()



