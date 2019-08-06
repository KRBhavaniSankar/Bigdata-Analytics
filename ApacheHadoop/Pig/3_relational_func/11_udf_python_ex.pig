REGISTER '/Users/bhavani.sankar/Desktop/Bhavani/ApachePig/works/3_relational_func/python_udf.py' using jython as myudfs;

users = LOAD '/Users/bhavani.sankar/Desktop/Bhavani/ApachePig/works/3_relational_func/sample_data.csv' USING PigStorage(',') AS (firstname: chararray, lastname:chararray,salary:int);
-- dump users;
-- describe users;
winning_users    = FOREACH users GENERATE myudfs.deal_with_a_string(firstname);

-- dump winning_users;
full_names = FOREACH users GENERATE myudfs.deal_with_two_strings(firstname,lastname);
-- dump full_names;
bonus_sal = FOREACH users GENERATE myudfs.square_a_number(salary) as bonus_salary ;
-- dump bonus_sal;
