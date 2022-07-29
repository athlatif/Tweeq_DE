# [1] import pymongo, awswrangler and pandas

# [2] define MongoClient with URI, replace the password 
client = 

# [3] access the database, you can use [ client.sample_airbnb ] too
mydb = 

# [4] access the collection 
mycol = 

# [5] filter reviews with security_deposit > 100
myquery = { "security_deposit":  }

# [6] apply the filter, and select only _id, listing_url, bedrooms
mydoc = mycol.find(myquery, {})

# [7] convert to list
list_cur = list()
  
# [8] Converting to the DataFrame
df = DataFrame()

# [9] define the path of S3 and save it as parquet
path1 = f"s3://xxx/xx.xx"

# [10] write it to s3 using to_parquet



