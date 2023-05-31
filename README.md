# CDC-SCD-CDF-Table
Your company recently migrated the data analysis platform from MySQL to Databricks. The MySQL is kept for handle transactions only. You are a data engineer and assigned the task for importing the tables from mysql database to Delta Lake and keep them in sync. In additional to that, there is a new requirement to keep the data change history (SCD2).\
(Use mysql-connector-python (not spark) library for MySQLdatabase create table, insert/update/delete records operations) \
Consider following events sequences: 
 ## 1. Create employee table in MySQL and insert 10 rows of data into MySQL table \
 ### Show the databases in MySQL
  ```
  jdbc_url = 'jdbc:mysql://database.ascendingdc.com:3306/de_005'
user = 'yefanli'
password = 'welcome'
jdbc_driver = "com.mysql.jdbc.Driver"
db_name = 'de_005'

table_list = (
    spark.read.format("jdbc")
    .option("driver", jdbc_driver)
    .option("url", jdbc_url)
    .option("dbtable", "information_schema.tables")
    .option("user", user)
    .option("password", password)
    .load()
    .filter(f"table_schema = '{db_name}'")
    .select("table_name")
)

table_list.show()
```
### Show the database in delta lake
```
%sql
SHOW DATABASES
```
### Create the corresponding database in delta lake
```
%sql
CREATE DATABASE IF NOT EXISTS de_005;
```
### Create a new employee table in the de_005 database I just created
```
%sql
DROP TABLE IF EXISTS de_005.employee;

-- create a new employee table in the de_005 database I just created
CREATE TABLE IF NOT EXISTS de_005.employee(
    emp_id INT,
    fname STRING,
    lname STRING,
    salary INT,
    dept_id INT
)

-- set it to ture to capture and process changes to a table's data over time
TBLPROPERTIES (delta.enableChangeDataFeed = true)
--CDF can track what changes in our table which can be used as the input of SCD Type 2 table
```
### Create the Dataframe and Write the DataFrame to MySQL to create the table
```
# Define MySQL connection details
mysql_properties = {
    'jdbc_url':'jdbc:mysql://database.ascendingdc.com:3306/de_005',
    'user':'yefanli',
    'password':'welcome',
    'jdbc_driver':"com.mysql.jdbc.Driver",
    'dbtable':'employee'
}
```
```
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("fname", StringType(), True),
    StructField("lname", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("dept_id", IntegerType(), True),
])
#2. Insert 10 rows into the table.
data = [(1, 'John', 'Doe', 5000, 1),
        (2, 'Jane', 'Smith', 6000, 2),
        (3, 'Bob', 'Johnson', 7000, 1),
        (4, 'Mary', 'Davis', 8000, 3),
        (5, 'Tom', 'Wilson', 9000, 2),
        (6, 'Lisa', 'Davis', 45000, 1),
        (7, 'Mike', 'Taylor', 80000, 3),
        (8, 'Sara', 'Anderson', 75000, 2),
        (9, 'Mark', 'Wilson', 90000, 3),
        (10, 'Emily', 'Moore', 40000, 1)]
```
```
# Create the DataFrame
df = spark.createDataFrame(data, schema=schema)

# Write the DataFrame to MySQL to create the table
df.write.jdbc(url=mysql_properties['jdbc_url'],
            table=mysql_properties['dbtable'],
            mode='overwrite',
            properties=mysql_properties)
```
## 2. Use spark.read.jdbc to read from MySQL table and merge into Databricks table employee_scd1, Use employee_scd1 CDF as input, merge into Databricks table employee_scd2
### Read the table from MySQL
```
employee_df = spark.read\
        .jdbc(url=mysql_properties['jdbc_url'],
            table=mysql_properties['dbtable'],
            properties=mysql_properties)

employee_df.printSchema()
display(employee_df) 
```
```
employee_df.createOrReplaceTempView("employee_mysqlinput")
```
### SCD1
```
%sql

MERGE INTO de_005.employee as target
USING employee_mysqlinput as source
-- primary key is different, how to merge table with different schema (different column name)?
-- target table/table schema/source table
ON target.emp_id = source.emp_id
WHEN MATCHED AND (target.fname <> source.fname OR target.lname <> source.lname OR target.salary <> source.salary OR target.dept_id <> source.dept_id)
  THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE
```
### SCD2
```
%sql

DESCRIBE HISTORY de_005.employee;
```
```
%sql
--Show CDF

SELECT * FROM table_changes('de_005.employee', 1, 1 )
ORDER BY emp_id
```
```
%sql
-- Create another SCD Type 2 table
-- Define the Schema of SCD Type 2 table
CREATE TABLE de_005.employee_scd2(
    emp_id INT,
    fname STRING,
    lname STRING,
    salary INT,
    dept_id INT,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    is_current BOOLEAN
);
```
```
change_df1 = spark.sql("SELECT * FROM table_changes('de_005.employee', 1, 1)")
```
```
change_df1.createOrReplaceTempView('change_df1_view');
```
```
%sql

MERGE INTO de_005.employee_scd2 AS target
USING change_df1_view AS source
ON target.emp_id = source.emp_id
WHEN NOT MATCHED AND source._change_type = 'insert'
THEN
 INSERT (
    emp_id,
    fname,
    lname,
    salary,
    dept_id,
    start_date,
    end_date,
    is_current
  ) VALUES (
    source.emp_id,
    source.fname,
    source.lname,
    source.salary,
    source.dept_id,
    CURRENT_TIMESTAMP,
    NULL,
    TRUE
  );
```
```
%sql
SELECT * FROM de_005.employee_scd2;
```
## 3. Update one row of data in MySQL table, Use spark.read.jdbc to read from MySQL table and merge into Databricks table employee_scd1, Use employee_scd1 CDF as input, merge into Databricks table employee_scd2
```
# --1. Update one row of data in MySQL table

import mysql.connector

# Connect to the MySQL server
mydb = mysql.connector.connect(
  host="database.ascendingdc.com",
  port=3306,
  user="yefanli",
  password="welcome",
  database="de_005"
)

# Create cursor object
mycursor = mydb.cursor()

# -- 1. Update one row of data in MySQL table & Execute query
mycursor.execute("UPDATE de_005.employee SET salary = 12000 WHERE emp_id = 5")

# Commit changes to database
mydb.commit()

# Print number of rows affected
print(mycursor.rowcount, "record(s) affected")
```
```
# Read the updated table from mysql into a DataFrame
update_df = (spark.read
            .jdbc(url=mysql_properties['jdbc_url'],
            table=mysql_properties['dbtable'],
            properties=mysql_properties) 
            )

update_df.printSchema()
display(update_df)
update_df.createOrReplaceTempView("employee_updated")
```
### SCD1
```
%sql

MERGE INTO de_005.employee as target
USING employee_updated as source
ON target.emp_id = source.emp_id
WHEN MATCHED AND (target.fname <> source.fname OR target.lname <> source.lname OR target.salary <> source.salary OR target.dept_id <> source.dept_id)
  THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE
```
```
%sql

DESCRIBE HISTORY de_005.employee;
```
### CDF
```
%sql
--Show CDF

SELECT * FROM table_changes('de_005.employee', 2, 2)
ORDER BY emp_id
```
```
change_df2 = spark.sql("SELECT * FROM table_changes('de_005.employee', 2, 2)")
```
```
change_df2.createOrReplaceTempView('change_df2_view');
```
### SCD2
```
%sql

MERGE INTO de_005.employee_scd2 AS target
USING (
  SELECT * FROM change_df2_view WHERE _change_type = 'update_postimage'
) AS source ON target.emp_id = source.emp_id
WHEN MATCHED AND target.is_current = TRUE
THEN
  UPDATE SET
  target.end_date = CURRENT_TIMESTAMP,
  target.is_current = FALSE;

INSERT INTO de_005.employee_scd2
SELECT emp_id, fname, lname, salary, dept_id, CURRENT_TIMESTAMP, NULL, True from change_df2_view
WHERE _change_type = 'update_postimage';
```
```
%sql

SELECT *
FROM de_005.employee_scd2;
```
## 4. Delete one row of data from MySQL table. Use spark.read.jdbc to read from MySQL table and merge into Databricks table employee_scd1. Use employee_scd1 CDF as input, merge into Databricks table employee_scd2
```
# --2. Delete one row of data from MySQL table

mycursor.execute("DELETE FROM de_005.employee WHERE emp_id = 3")

# Commit changes to database
mydb.commit()

# Print number of rows affected
print(mycursor.rowcount, "record(s) affected")
```
```
# Read the delete table from mysql into a DataFrame
delete_df = (spark.read
            .jdbc(url=mysql_properties['jdbc_url'],
            table=mysql_properties['dbtable'],
            properties=mysql_properties) 
            )

delete_df.printSchema()
display(delete_df)
delete_df.createOrReplaceTempView("employee_delete")
```
### SCD1
```
%sql

MERGE INTO de_005.employee as target
USING employee_delete as source
ON target.emp_id = source.emp_id
WHEN MATCHED AND (target.fname <> source.fname OR target.lname <> source.lname OR target.salary <> source.salary OR target.dept_id <> source.dept_id)
  THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE
```
### CDF
```
%sql
--Show CDF

SELECT * FROM table_changes('de_005.employee', 3, 3)
ORDER BY emp_id
```
```
%sql

DESCRIBE HISTORY de_005.employee;
```
```
change_df3 = spark.sql("SELECT * FROM table_changes('de_005.employee', 3, 3)")
```
```
change_df3.createOrReplaceTempView('change_df3_view');
```
### SCD2
```
%sql

-- Use employee_scd1 CDF as input, merge into Databricks table employee_scd2
MERGE INTO de_005.employee_scd2 AS target
USING (
  SELECT * FROM change_df3_view WHERE _change_type = 'delete'
) AS source
ON target.emp_id = source.emp_id
WHEN MATCHED AND target.is_current = TRUE
THEN
  UPDATE SET
  target.end_date = CURRENT_TIMESTAMP,
  target.is_current = FALSE;
```
```
%sql

SELECT *
FROM de_005.employee_scd2;
```
## 5. Insert one row, update one row and delete one row in MySQL table， Use spark.read.jdbc to read from MySQL table and merge into Databricks table employee_scd1，Use employee_scd1 CDF as input, merge into Databricks table employee_scd2
```
# Insert one row, update one row and delete one row in MySQL table

# Insert one row
mycursor.execute("INSERT INTO de_005.employee (emp_id, fname, lname, salary, dept_id) VALUES (12, 'Jim', 'Wang', 4000, 101)")

# Update one row
mycursor.execute("UPDATE de_005.employee SET salary = 6800 WHERE emp_id = 5")

# Delete one row
mycursor.execute("DELETE FROM de_005.employee WHERE emp_id = 4")

# Commit changes to database
mydb.commit()

# Close cursor and database connection
mycursor.close()
mydb.close()
```
```
# Read the updated table from mysql into a DataFrame
combo_df = (spark.read
           .jdbc(url=mysql_properties['jdbc_url'],
            table=mysql_properties['dbtable'],
            properties=mysql_properties) 
            )

combo_df.printSchema()
display(combo_df)
combo_df.createOrReplaceTempView("employee_combo")
```
### SCD1
```
%sql

-- merge employee_updated into Databricks table employee_scd1

MERGE INTO de_005.employee as target
USING employee_combo as source
ON target.emp_id = source.emp_id
WHEN MATCHED AND (target.fname <> source.fname OR target.lname <> source.lname OR target.salary <> source.salary OR target.dept_id <> source.dept_id)
  THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE
```
```
%sql

DESCRIBE HISTORY de_005.employee;
```
### CDF
```
%sql
--Show CDF

SELECT * FROM table_changes('de_005.employee', 4, 4);
```
```
%sql

MERGE INTO de_005.employee_scd2 AS target
-- primary key in source data must be unique
USING (
  SELECT * FROM change_df4_view WHERE _change_type != 'update_preimage'
 ) AS source
ON target.emp_id = source.emp_id
WHEN MATCHED AND source._change_type = 'delete'
THEN
  UPDATE SET
  target.end_date = CURRENT_TIMESTAMP,
  target.is_current = FALSE
  
WHEN MATCHED AND source._change_type = 'update_postimage' AND target.is_current = TRUE
THEN
  UPDATE SET
  target.end_date = CURRENT_TIMESTAMP,
  target.is_current = FALSE

WHEN NOT MATCHED AND source._change_type = 'insert'
THEN
 INSERT (
    emp_id,
    fname,
    lname,
    salary,
    dept_id,
    start_date,
    end_date,
    is_current
  ) VALUES (
    source.emp_id,
    source.fname,
    source.lname,
    source.salary,
    source.dept_id,
    CURRENT_TIMESTAMP,
    NULL,
    TRUE
  );

INSERT INTO de_005.employee_scd2
SELECT emp_id, fname, lname, salary, dept_id, CURRENT_TIMESTAMP, NULL, True from change_df4_view
WHERE _change_type = 'update_postimage';
```
```
%sql

SELECT * FROM de_005.employee_scd2;
```
