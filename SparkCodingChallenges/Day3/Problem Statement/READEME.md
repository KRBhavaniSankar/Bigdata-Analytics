## Problem Statement:
Given a dataset containing information about students, perform the following tasks using Spark SQL:

### 1. Create Spark Session

### 2. Load the dataset into a DataFrame.

### 3. Find the unique duplicate records in the given data

#Narrow transformation

distinct_df =df.distinct()
df.exceptAll(distinct_df)


#wide transformation
groupBy.agg(record_count)
filter_record_count > 1



## Expected Output:
Alice,20,Female,A
David,19,Male,C

