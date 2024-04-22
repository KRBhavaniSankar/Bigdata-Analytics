## Problem Statement:
Given a dataset containing information about students, perform the following tasks using Spark SQL:

### 1. Create Spark Session

### 2. Load the dataset into a DataFrame.

### 3. Find the employee with 2nd Highest salary from each department in a given dataset.
rank() or dense_rank()

deptWindow = Window.partitionBy('department').orderBy('salary'.desc())

withColumn('dense_rank',dense_rank().over(deptWindow)).filter('dense_rank'==2)



## Expected Output:
id,name,department,gender,salary
14,Eli,HR,female,1100
13,shanti,HR,female,1100
3,Gajantut,IT,male,1600
9,Charlie,manager,male,1200
11,Alex,manager,male,1200
6,Alice,sales,male,1500