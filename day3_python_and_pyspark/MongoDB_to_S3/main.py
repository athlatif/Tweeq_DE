# [1] import pymongo & boto3
from pymongo import MongoClient
import boto3 
from pandas import DataFrame
import os
import awswrangler as wr


# [2] define MongoClient with URI, replace the password 
client = MongoClient("mongodb+srv://tweeq:tweeq@cluster0.fnah7gp.mongodb.net/?retryWrites=true&w=majority", tls=True,
                             tlsAllowInvalidCertificates=True)
# [3] access the database, you can use [ client.sample_airbnb ] too
mydb = client["sample_airbnb"]

# [4] access the collection 
mycol = mydb["listingsAndReviews"]

# [5] if you want to do any transformation or filtering you can do so by defining a query, here we are filtering reviews with 3 bedrooms
myquery = { "bedrooms": 3 }

# [6] apply the filter 

mydoc = mycol.find(myquery, {"_id":1,"listing_url":1,"summary":1,"bedrooms":1})

# [6.1] loop accross the collection (opt)
#for x in mydoc:
#print(x)

# [7] convert to dataframe
list_cur = list(mydoc)
  
# Converting to the DataFrame
df = DataFrame(list_cur)
print(df.head())

#edit credintals file in OS
path1 = f"s3://tweeq-academy/listingsAndReviews.parquet"

wr.s3.to_parquet(df,path1)


