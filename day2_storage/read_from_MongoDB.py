# [1] import pymongo 
from pymongo import MongoClient
from pandas import DataFrame


# [2] define MongoClient with URI, replace the password 
client = MongoClient("uri", tls=True,
                             tlsAllowInvalidCertificates=True)
# [3] access the database, you can use [ client.sample_airbnb ] too
mydb = client["sample_airbnb"]

# [4] access the collection 
mycol = mydb["listingsAndReviews"]

# [5] if you want to do any transformation or filtering you can do so by defining a query, here we are filtering reviews with 3 bedrooms
myquery = { "bedrooms": 3 }

# [6] apply the filter, and select only _id, listing_url, bedrooms
mydoc = mycol.find(myquery, {"_id":1,"listing_url":1,"summary":1,"bedrooms":1})

#[7] convert to list
list_cur = list(mydoc)

print(list_cur)