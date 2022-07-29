# [1] import awswrangler and pandas
import awswrangler as wr
import pandas as pd
 
# list of strings
lst = {'keywords' :['Tweeq', 'Academy', 'Data Engineering', '2022']}

# Calling DataFrame constructor on list
df = pd.DataFrame(lst)

print(df.head())

#edit credintals file in OS
path1 = f"s3://tweeqday2/test44.csv"

wr.s3.to_csv(df,path1)


