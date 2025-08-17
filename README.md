# Spark SQL Interview Scenarios – Explained

This repository is a comprehensive collection of **36 real-world Spark SQL interview scenarios**. Each scenario is designed to test your technical ability, problem-solving mindset, and readiness for hands-on data engineering interviews. These questions focus on core Spark SQL, PySpark, and common data processing situations you may face in technical rounds.

Explore **scenario-based questions** with clear input/output examples, Markdown tables, and concise explanations—perfect for both interview preparation and practical reference.

## 📚 Table of Contents

| #  | Scenario Description                                                                |
|----|-------------------------------------------------------------------------------------|
| 1  | [Query to get who are getting equal salary](#scenario-1)                             |
| 2  | [Need the dates when the status gets changed like ordered to dispatched](#scenario-2)|
| 3  | [Calculate sensor value differences partitioned by sensor ID](#scenario-3)           |
| 4  | [List unique customer names with number of addresses](#scenario-4)                   |
| 5  | [Data ingestion, column addition, append, and filtering invalid emails](#scenario-5) |
| 6  | [Employee designation (Manager/Employee) based on salary](#scenario-6)               |
| 7  | [Find highest quantity sales per year](#scenario-7)                                  |
| 8  | [Generate all possible team matches](#scenario-8)                                    |
| 9  | [Participant with most rank=1 occurrences](#scenario-9)                              |
| 10 | [Find lowest commission per employee](#scenario-10)                                  |
| 11 | [Grade salary brackets (A/B/C)](#scenario-11)                                        |
| 12 | [Mask email and mobile columns](#scenario-12)                                        |
| 13 | [Departmentwise employee count](#scenario-13)                                        |
| 14 | [Calculate total marks column](#scenario-14)                                         |
| 15 | [List extend/append in PySpark/Scala](#scenario-15)                                  |
| 16 | [Remove duplicate records](#scenario-16)                                             |
| 17 | [Join/merge DataFrames on custom keys](#scenario-17)                                 |
| 18 | [Reverse words in a column](#scenario-18)                                            |
| 19 | [Flatten a complex DataFrame schema](#scenario-19)                                   |
| 20 | [Generate (build) a complex DataFrame schema](#scenario-20)                          |
| 21 | [Calculate roundtrip distance](#scenario-21)                                         |
| 22 | [Cumulative sum within partitions](#scenario-22)                                     |
| 23 | [Customers who bought all listed products](#scenario-23)                             |
| 24 | [Aggregate column values into lists](#scenario-24)                                   |
| 25 | [Handle and load bad/corrupt data into DF](#scenario-25)                             |
| 26 | [Compare two tables and mark differences](#scenario-26)                              |
| 27 | [Calculate yearly salary increments](#scenario-27)                                   |
| 28 | [Retrieve grandparent relationships](#scenario-28)                                   |
| 29 | [Set-based table difference](#scenario-29)                                           |
| 30 | [Second highest salary per department](#scenario-30)                                 |
| 31 | [Explode and split multiple columns](#scenario-31)                                   |
| 32 | [Join food and rating tables, format stars](#scenario-32)                            |
| 33 | [Maximum discount tours per family](#scenario-33)                                    |
| 34 | [Bucket/group ages and count](#scenario-34)                                          |
| 35 | [Create, clean, and join dataframes](#scenario-35)                                   |
| 36 | [Aggregate and count products sold by date](#scenario-36)                            |


## Scenario 1: Query to get who are getting equal salary

**Description:**  
Identify employees who share the same salary with at least one other colleague.

**Input Table:**
```markdown
| workerid | firstname | lastname | salary | joiningdate        | depart |
|----------|-----------|----------|--------|--------------------|--------|
| 001      | Monika    | Arora    | 100000 | 2014-02-20 09:00:00| HR     |
| 002      | Niharika  | Verma    | 300000 | 2014-06-11 09:00:00| Admin  |
| 003      | Vishal    | Singhal  | 300000 | 2014-02-20 09:00:00| HR     |
| 004      | Amitabh   | Singh    | 500000 | 2014-02-20 09:00:00| Admin  |
| 005      | Vivek     | Bhati    | 500000 | 2014-06-11 09:00:00| Admin  |
```

**Expected Output Table:**
```markdown
| workerid | firstname | lastname | salary | joiningdate        | depart |
|----------|-----------|----------|--------|--------------------|--------|
| 002      | Niharika  | Verma    | 300000 | 2014-06-11 09:00:00| Admin  |
| 003      | Vishal    | Singhal  | 300000 | 2014-02-20 09:00:00| HR     |
| 004      | Amitabh   | Singh    | 500000 | 2014-02-20 09:00:00| Admin  |
| 005      | Vivek     | Bhati    | 500000 | 2014-06-11 09:00:00| Admin  |
```
**Explanation:**  
Return rows for any salary that appears more than once in the input table.

***

## Scenario 2: Need the dates when the status gets changed (e.g., ordered to dispatched)

**Description:**  
Extract the dates when the status field changes between states (e.g., from “ordered” to “dispatched”).

**Input Table:**
```markdown
| orderid | statusdate | status     |
|---------|------------|------------|
| 1       | 1-Jan      | Ordered    |
| 1       | 2-Jan      | dispatched |
| 1       | 3-Jan      | dispatched |
| 1       | 4-Jan      | Shipped    |
| 1       | 5-Jan      | Shipped    |
| 1       | 6-Jan      | Delivered  |
| 2       | 1-Jan      | Ordered    |
| 2       | 2-Jan      | dispatched |
| 2       | 3-Jan      | shipped    |
```

**Expected Output Table:**
```markdown
| orderid | statusdate | status     |
|---------|------------|------------|
| 1       | 2-Jan      | dispatched |
| 1       | 3-Jan      | dispatched |
| 2       | 2-Jan      | dispatched |
```
**Explanation:**  
Selects only those records where the status changed to "dispatched".

***

## Scenario 3: Calculate sensor value differences partitioned by sensor ID

**Description:**  
For each sensor, calculate the differences between consecutive value readings.

**Input Table:**
```markdown
| sensorid | timestamp   | values |
|----------|------------|--------|
| 1111     | 2021-01-15 | 10     |
| 1111     | 2021-01-16 | 15     |
| 1111     | 2021-01-17 | 30     |
| 1112     | 2021-01-15 | 10     |
| 1112     | 2021-01-15 | 20     |
| 1112     | 2021-01-15 | 30     |
```

**Expected Output Table:**
```markdown
| sensorid | timestamp   | values |
|----------|------------|--------|
| 1111     | 2021-01-15 | 5      |
| 1111     | 2021-01-16 | 15     |
| 1112     | 2021-01-15 | 10     |
| 1112     | 2021-01-15 | 10     |
```
**Explanation:**  
Shows the difference in readings from the previous value for each sensor by date.

***

## Scenario 4: List unique customer names with number of addresses

**Description:**  
Aggregate all addresses for each unique customer as a list.

**Input Table:**
```markdown
| custid | custname     | address |
|--------|--------------|---------|
| 1      | Mark Ray     | AB      |
| 2      | Peter Smith  | CD      |
| 1      | Mark Ray     | EF      |
| 2      | Peter Smith  | GH      |
| 2      | Peter Smith  | CD      |
| 3      | Kate         | IJ      |
```

**Expected Output Table:**
```markdown
| custid | custname     | address    |
|--------|--------------|------------|
| 1      | Mark Ray     | [EF, AB]   |
| 2      | Peter Smith  | [CD, GH]   |
| 3      | Kate         | [IJ]       |
```
**Explanation:**  
Expected output aggregates each customer’s unique addresses into a list.

***

## Scenario 5: Data ingestion, column addition, append, and filtering invalid emails

**Description:**  
Append data, add a constant salary, and filter for valid emails.

**Input Table:**
```markdown
df1                         df2
| id | name | age | email         | id | name| age| email         | salary |
|----|------|-----|---------------|----|-----|----|---------------|--------|
|  1 | abc  |  31 | abc@gmail.com | 11 | jkl | 22 | abc@gmail.com | 1000   |
|  2 | def  |  23 | defyahoo.com  | 12 | vbn | 33 | vbn@yahoo.com | 3000   |
|  3 | xyz  |  26 | xyz@gmail.com | 13 | wer | 27 | wer           | 2000   |
|  4 | qwe  |  34 | qwegmail.com  | 14 | zxc | 30 | zxc.com       | 2000   |
|  5 | iop  |  24 | iop@gmail.com | 15 | lkj | 29 |lkj@outlook.com| 2000   |
```

**Expected Output Table:**
```markdown
| id | name | age | email          | salary |
|----|------|-----|----------------|--------|
|  1 | abc  | 31  | abc@gmail.com  | 1000   |
|  3 | xyz  | 26  | xyz@gmail.com  | 1000   |
|  5 | iop  | 24  | iop@gmail.com  | 1000   |
| 11 | jkl  | 22  | abc@gmail.com  | 1000   |
| 12 | vbn  | 33  | vbn@yahoo.com  | 3000   |
| 15 | lkj  | 29  | lkj@outlook.com| 2000   |
```
**Explanation:**  
After appending, only rows with valid email addresses (containing “@”) remain.

***
Here are scenarios 6–10, each with a description, input/output as Markdown tables, and an explanation:

***

## Scenario 6: Employee designation (Manager/Employee) based on salary

**Description:**  
Assign the designation “Manager” for employees with a salary greater than 10,000; otherwise, set as “Employee”.

**Input Table:**
```markdown
| empid | name | salary |
|-------|------|--------|
| 1     | a    | 10000  |
| 2     | b    | 5000   |
| 3     | c    | 15000  |
| 4     | d    | 25000  |
| 5     | e    | 50000  |
| 6     | f    | 7000   |
```

**Expected Output Table:**
```markdown
| empid | name | salary | Designation |
|-------|------|--------|-------------|
| 1     | a    | 10000  | Employee    |
| 2     | b    | 5000   | Employee    |
| 3     | c    | 15000  | Manager     |
| 4     | d    | 25000  | Manager     |
| 5     | e    | 50000  | Manager     |
| 6     | f    | 7000   | Employee    |
```
**Explanation:**  
Simple CASE logic on the salary value creates the new designation column.

***

## Scenario 7: Find highest quantity sales per year

**Description:**  
Get sales records with highest quantity for each year.

**Input Table:**
```markdown
| sale_id | product_id | year | quantity | price |
|---------|------------|------|----------|-------|
|   1     |   100      | 2010 |   25     | 5000  |
|   2     |   100      | 2011 |   16     | 5000  |
|   3     |   100      | 2012 |    8     | 5000  |
|   4     |   200      | 2010 |   10     | 9000  |
|   5     |   200      | 2011 |   15     | 9000  |
|   6     |   200      | 2012 |   20     | 7000  |
|   7     |   300      | 2010 |   20     | 7000  |
|   8     |   300      | 2011 |   18     | 7000  |
|   9     |   300      | 2012 |   20     | 7000  |
```

**Expected Output Table:**
```markdown
| sale_id | product_id | year | quantity | price |
|---------|------------|------|----------|-------|
|   6     |   200      | 2012 |   20     | 7000  |
|   9     |   300      | 2012 |   20     | 7000  |
|   1     |   100      | 2010 |   25     | 5000  |
|   8     |   300      | 2011 |   18     | 7000  |
```
**Explanation:**  
Results show all sales that tie for highest quantity, per year, using window functions.

***

## Scenario 8: Generate all possible team matches

**Description:**  
Produce all possible match pairings given a list of teams.

**Input Table:**
```markdown
| teams    |
|----------|
| India    |
| Pakistan |
| SriLanka |
```

**Expected Output Table:**
```markdown
| matches                |
|------------------------|
| India Vs Pakistan      |
| India Vs SriLanka      |
| Pakistan Vs SriLanka   |
```
**Explanation:**  
Creates all unique team pairs (without repetitions or reversed duplicates).

***

## Scenario 9: Participant with most rank=1 occurrences

**Description:**  
Return the participant’s name with the highest count of "1" in their rank array.

**Input Table:**
```markdown
| name | rank             |
|------|------------------|
| a    | [1, 1, 1, 3]     |
| b    | [1, 2, 3, 4]     |
| c    | [1, 1, 1, 1, 4]  |
| d    | [3]              |
```

**Expected Output Table:**
```markdown
| name |
|------|
| c    |
```
**Explanation:**  
“c” has the greatest number of 1s in their rank array.

***

## Scenario 10: Find lowest commission per employee

**Description:**  
For each empid, find the record with the smallest commission.

**Input Table:**
```markdown
| empid | commissionamt | monthlastdate |
|-------|--------------|---------------|
| 1     | 300          | 31-Jan-2021   |
| 1     | 400          | 28-Feb-2021   |
| 1     | 200          | 31-Mar-2021   |
| 2     | 1000         | 31-Oct-2021   |
| 2     | 900          | 31-Dec-2021   |
```

**Expected Output Table:**
```markdown
| empid | commissionamt | monthlastdate |
|-------|--------------|---------------|
| 1     | 200          | 31-Mar-2021   |
| 2     | 1000         | 31-Oct-2021   |
```
**Explanation:**  
Shows only the record for each empid with their minimum commission amount.

***
Here are scenarios 11–20, each fully formatted for your README:
***

## Scenario 11: Grade salary brackets (A/B/C)

**Description:**  
Assign salary grades: A (>10,000), B (5,000–10,000), C (<5,000).

**Input Table:**
```markdown
| emp_id | emp_name        | salary |
|--------|-----------------|--------|
|   1    | Jhon            | 4000   |
|   2    | Tim David       | 12000  |
|   3    | Json Bhrendroff | 7000   |
|   4    | Jordon          | 8000   |
|   5    | Green           | 14000  |
|   6    | Brewis          | 6000   |
```

**Expected Output Table:**
```markdown
| emp_id | emp_name        | salary | grade |
|--------|-----------------|--------|-------|
|   1    | Jhon            | 4000   | C     |
|   2    | Tim David       | 12000  | A     |
|   3    | Json Bhrendroff | 7000   | B     |
|   4    | Jordon          | 8000   | B     |
|   5    | Green           | 14000  | A     |
|   6    | Brewis          | 6000   | B     |
```
**Explanation:**  
Uses CASE or WHEN logic to map salary to grade.

***

## Scenario 12: Mask email and mobile columns

**Description:**  
Replace portions of email and mobile numbers with asterisks.

**Input Table:**
```markdown
| email                  | mobile    |
|------------------------|-----------|
| Renuka1992@gmail.com   | 9856765434|
| anbu.arasu@gmail.com   | 9844567788|
```

**Expected Output Table:**
```markdown
| email                  | mobile    |
|------------------------|-----------|
| R**********92@gma...   | 98*****434|
| a**********su@gma...   | 98*****788|
```
**Explanation:**  
Masks part of the info for privacy/anonymization (e.g., for reports).

***

## Scenario 13: Department-wise employee count

**Description:**  
Count the number of employees in each department.

**Input Table:**
```markdown
| emp_id | emp_name | dept       |
|--------|----------|------------|
|   1    | Jhon     | Development|
|   2    | Tim      | Development|
|   3    | David    | Testing    |
|   4    | Sam      | Testing    |
|   5    | Green    | Testing    |
|   6    | Miller   | Production |
|   7    | Brevis   | Production |
|   8    | Warner   | Production |
|   9    | Salt     | Production |
```

**Expected Output Table:**
```markdown
| dept       | total |
|------------|-------|
| Development| 2     |
| Testing    | 3     |
| Production | 4     |
```
**Explanation:**  
GROUP BY department, count records.

***

## Scenario 14: Calculate total marks column

**Description:**  
Add a "total" column as the sum of marks across all subjects.

**Input Table:**
```markdown
| rollno | name   | telugu | english | maths | science | social |
|--------|--------|--------|---------|-------|---------|--------|
|203040  | rajesh |  10    |  20     |  30   | 40      | 50     |
```

**Expected Output Table:**
```markdown
| rollno | name   | telugu | english | maths | science | social | total |
|--------|--------|--------|---------|-------|---------|--------|-------|
|203040  | rajesh |  10    |  20     |  30   |  40     |  50    | 150   |
```
**Explanation:**  
Add all the subject marks for a new “total” field.

***

## Scenario 15: List extend and append in Python and Scala

**Description:**  
Show how to add elements to a list in Python and Scala.

_No Input/Output table for this scenario, since it's focused on code snippet usage:_

**Explanation:**
- `.append(x)` adds single element; `.extend([x, y])` adds multiple elements to a list (Python).
- `++` or `.:+` can be used in Scala. 

Provide code usage instead of data tables.

***

## Scenario 16: Remove duplicate records

**Description:**  
Drop duplicate rows from the input.

**Input Table:**
```markdown
| id | name | dept       | salary |
|----|------|------------|--------|
| 1  | Jhon | Testing    | 5000   |
| 2  | Tim  | Development| 6000   |
| 3  | Jhon | Development| 5000   |
| 4  | Sky  | Production | 8000   |
```

**Expected Output Table:**
```markdown
| id | name | dept       | salary |
|----|------|------------|--------|
| 1  | Jhon | Testing    | 5000   |
| 2  | Tim  | Development| 6000   |
| 4  | Sky  | Production | 8000   |
```
**Explanation:**  
Keep only the first occurrence for each duplicate combination.

***

## Scenario 17: Join/merge DataFrames on custom keys

**Description:**  
Merge two DataFrames by matching multiple columns.

**Input Table:**
```markdown
df1:
| emp_id | name  | age | state  | country |
|--------|-------|-----|--------|---------|
|   1    | Tim   | 24  | Kerala | India   |
|   2    | Asman | 26  | Kerala | India   |

df2:
| emp_id | name  | age | address  |
|--------|-------|-----|----------|
|   1    | Tim   | 24  | Comcity  |
|   2    | Asman | 26  | bimcity  |
```

**Expected Output Table:**
```markdown
| emp_id | name  | age | state  | country | address  |
|--------|-------|-----|--------|---------|----------|
|   1    | Tim   | 24  | Kerala | India   | Comcity  |
|   2    | Asman | 26  | Kerala | India   | bimcity  |
```
**Explanation:**  
Join on emp_id, name, and age; merge columns from both.

***

## Scenario 18: Reverse words in a column

**Description:**  
Reverse each string in a column.

**Input Table:**
```markdown
| word                |
|---------------------|
| The Social Dilemma  |
```

**Expected Output Table:**
```markdown
| reverse word        |
|---------------------|
| ehT laicoS ammeliD  |
```
**Explanation:**  
Reverse string logic for each row.

***

## Scenario 19: Flatten a complex DataFrame schema

**Description:**  
Convert a deeply nested or complex schema into a flat schema.

**Input Table/Schema:**  
(Example showing fields, subfields, arrays, structs.)

```
|-- code: long
|-- commentCount: long
|-- likeDislike: struct
|    |-- dislikes: long
|    |-- likes: long
|    |-- userAction: long
| ... (more nested/array fields)
```

**Expected Output Table (flattened):**  
```
|-- code: long
|-- commentCount: long
|-- dislikes: long
|-- likes: long
|-- userAction: long
... (all atomic fields as top-level columns)
```
**Explanation:**  
Each atomic value, even if nested, becomes a simple column.

***

## Scenario 20: Generate (build) a complex DataFrame schema

**Description:**  
Write code or schema to generate a complex (nested, array, struct) dataframe structure.

**Explanation:**  
- Code creates or describes required nested structures.
- Example: Create a `likeDislike` struct inside a row; an array of `multiMedia` elements, etc.

_No input/output table—focus is on code and Spark schema creation._

***
Here are scenarios 21–30, ready to copy into your README:

***

## Scenario 21: Calculate roundtrip distance

**Description:**  
For each route (from, to), calculate the roundtrip distance by summing the "out" and "back" rows.

**Input Table:**
```markdown
| from | to  | dist |
|------|-----|------|
| SEA  | SF  | 300  |
| CHI  | SEA | 2000 |
| SF   | SEA | 300  |
| SEA  | CHI | 2000 |
| SEA  | LND | 500  |
| LND  | SEA | 500  |
| LND  | CHI | 1000 |
| CHI  | NDL | 180  |
```

**Expected Output Table:**
```markdown
| from | to  | roundtrip_dist |
|------|-----|---------------|
| SEA  | SF  | 600           |
| CHI  | SEA | 4000          |
| LND  | SEA | 1000          |
```
**Explanation:**  
Adds both directions; e.g., SEA → SF + SF → SEA.

***

## Scenario 22: Cumulative sum within partitions

**Description:**  
Cumulative (running) sum of price for each pid, ordered by date.

**Input Table:**
```markdown
| pid | date   | price |
|-----|--------|-------|
|  1  | 26-May | 100   |
|  1  | 27-May | 200   |
|  1  | 28-May | 300   |
|  2  | 29-May | 400   |
|  3  | 30-May | 500   |
|  3  | 31-May | 600   |
```

**Expected Output Table:**
```markdown
| pid | date   | price | new_price |
|-----|--------|-------|-----------|
|  1  | 26-May | 100   |   100     |
|  1  | 27-May | 200   |   300     |
|  1  | 28-May | 300   |   600     |
|  2  | 29-May | 400   |   400     |
|  3  | 30-May | 500   |   500     |
|  3  | 31-May | 600   |   1100    |
```
**Explanation:**  
For each group (pid), new_price is the cumulative sum.

***

## Scenario 23: Customers who bought all listed products

**Description:**  
Identify customers who bought every key product.

**Input Table:**
```markdown
customer:
| customer_id | product_key |
|-------------|-------------|
| 1           | 5           |
| 2           | 6           |
| 3           | 5           |
| 3           | 6           |
| 1           | 6           |

products:
| product_key |
|-------------|
| 5           |
| 6           |
```

**Expected Output Table:**
```markdown
| customer_id |
|-------------|
| 1           |
| 3           |
```
**Explanation:**  
Customers 1 and 3 purchased *both* products 5 and 6.

***

## Scenario 24: Aggregate column values into lists

**Description:**  
Group and collect user journeys as ordered lists of pages.

**Input Table:**
```markdown
| userid | page        |
|--------|-------------|
| 1      | home        |
| 1      | products    |
| 1      | checkout    |
| 1      | confirmation|
| 2      | home        |
| 2      | products    |
| 2      | cart        |
| 2      | checkout    |
| 2      | confirmation|
| 2      | home        |
| 2      | products    |
```

**Expected Output Table:**
```markdown
| userid | pages                                               |
|--------|-----------------------------------------------------|
| 1      | [home, products, checkout, confirmation]            |
| 2      | [home, products, cart, checkout, confirmation, home, products] |
```
**Explanation:**  
Aggregates user navigation paths.

***

## Scenario 25: Handle and load bad/corrupt data into DataFrame

**Description:**  
Filter out "bad" records while reading CSVs and load only valid rows.

**Input Table:**
```
emp_no,emp_name,dep
101,Murugan,HealthCare
Invalid Entry,Description: Bad Record Entry
102,Kannan,Finance
103,Mani,IT
Connection lost,Description: Poor Connection
104,Pavan,HR
Bad Record,Description:Corrupt Record
```

**Expected Output Table:**
```markdown
| emp_no | emp_name | dep        |
|--------|----------|------------|
| 101    | Murugan  | HealthCare |
| 102    | Kannan   | Finance    |
| 103    | Mani     | IT         |
| 104    | Pavan    | HR         |
```
**Explanation:**  
Skip or drop malformed/bad lines on load using appropriate Spark options.

***

## Scenario 26: Compare two tables and mark differences

**Description:**  
Combine two tables and flag new/mismatching records.

**Input Table:**
```
source:
| id | name |
|----|------|
| 1  | A    |
| 2  | B    |
| 3  | C    |
| 4  | D    |

target:
| id1 | name1 |
|-----|-------|
| 1   | A     |
| 2   | B     |
| 4   | X     |
| 5   | F     |
```

**Expected Output Table:**
```markdown
| id | comment         |
|----|-----------------|
| 3  | new in source   |
| 4  | mismatch        |
| 5  | new in target   |
```
**Explanation:**  
- "new in source": present in source, not in target
- "new in target": present in target, not in source
- "mismatch": id same, name different

***

## Scenario 27: Calculate yearly salary increments

**Description:**  
Show, for each employee and year, how much their salary increased from previous year.

**Input Table:**
```markdown
| empid | salary | year |
|-------|--------|------|
| 1     | 60000  | 2018 |
| 1     | 70000  | 2019 |
| 1     | 80000  | 2020 |
| 2     | 60000  | 2018 |
| 2     | 65000  | 2019 |
| 2     | 65000  | 2020 |
| 3     | 60000  | 2018 |
| 3     | 65000  | 2019 |
```

**Expected Output Table:**
```markdown
| empid | salary | year | incresalary |
|-------|--------|------|-------------|
| 1     | 60000  | 2018 |    0        |
| 1     | 70000  | 2019 | 10000       |
| 1     | 80000  | 2020 | 10000       |
| 2     | 60000  | 2018 |    0        |
| 2     | 65000  | 2019 |  5000       |
| 2     | 65000  | 2020 |    0        |
| 3     | 60000  | 2018 |    0        |
| 3     | 65000  | 2019 |  5000       |
```
**Explanation:**  
Shows salary difference from previous year (first year is zero).

***

## Scenario 28: Retrieve grandparent relationships

**Description:**  
Given parent-child relationships, determine each child's grandparent.

**Input Table:**
```markdown
| child | parent |
|-------|--------|
| A     | AA     |
| B     | BB     |
| C     | CC     |
| AA    | AAA    |
| BB    | BBB    |
| CC    | CCC    |
```

**Expected Output Table:**
```markdown
| child | parent | grandparent |
|-------|--------|------------|
| A     | AA     | AAA        |
| C     | CC     | CCC        |
| B     | BB     | BBB        |
```
**Explanation:**  
Do a self-join: for each child's parent, look up that parent's parent.

***

## Scenario 29: Set-based table difference

**Description:**  
Show numbers in one table not in another (set difference).

**Input Table:**
```markdown
first:
| col |
|-----|
| 1   |
| 2   |
| 3   |

second:
| col1 |
|------|
| 1    |
| 2    |
| 3    |
| 4    |
| 5    |
```

**Expected Output Table:**
```markdown
| col |
|-----|
| 4   |
| 5   |
```
**Explanation:**  
Second table rows not in first.

***

## Scenario 30: Second highest salary per department

**Description:**  
Find the employee with the second highest salary in each department.

**Input Table:**
```markdown
emp:
| emp_id | name | dept_id | salary  |
|--------|------|---------|---------|
| 1      | A    | A       | 1000000 |
| 2      | B    | A       | 2500000 |
| 3      | C    | G       | 500000  |
| 4      | D    | G       | 800000  |
| 5      | E    | W       | 9000000 |
| 6      | F    | W       | 2000000 |

dept:
| dept_id1 | dept_name |
|----------|-----------|
| A        | AZURE     |
| G        | GCP       |
| W        | AWS       |
```

**Expected Output Table:**
```markdown
| emp_id | name | dept_name | salary  |
|--------|------|-----------|---------|
| 1      | A    | AZURE     | 1000000 |
| 3      | C    | GCP       | 500000  |
| 6      | F    | AWS       | 2000000 |
```
**Explanation:**  
Dense rank salary within each department and select rank=2.

***
Here are scenarios 31–36, completing the set for your README:

***

## Scenario 31: Explode and split multiple columns

**Description:**  
Split and flatten values from multiple columns into a single column using explode.

**Input Table:**
```markdown
| col1 | col2  | col3     | col4        |
|------|-------|----------|-------------|
| m1   | m1,m2 | m1,m2,m3 | m1,m2,m3,m4 |
```

**Expected Output Table:**
```markdown
| col         |
|-------------|
| m1          |
| m1,m2       |
| m1,m2,m3    |
| m1,m2,m3,m4 |
```
**Explanation:**  
Concatenate specified columns, split by delimiter, and explode so each part is a separate row.

***

## Scenario 32: Join food and rating tables, format stars

**Description:**  
Join food and ratings tables; add a "stars" string column reflecting the rating.

**Input Table:**
```markdown
foodtab:
| food_id | food_item        |
|---------|------------------|
| 1       | Veg Biryani      |
| 2       | Veg Fried Rice   |
| 3       | Kaju Fried Rice  |
| 4       | Chicken Biryani  |
| 5       | Chicken Dum Biryani|
| 6       | Prawns Biryani   |
| 7       | Fish Birayani    |

ratingtab:
| food_id | rating |
|---------|--------|
| 1       | 5      |
| 2       | 3      |
| 3       | 4      |
| 4       | 4      |
| 5       | 5      |
| 6       | 4      |
| 7       | 4      |
```

**Expected Output Table:**
```markdown
| food_id | food_item         | rating | stars   |
|---------|-------------------|--------|---------|
| 1       | Veg Biryani       |   5    | *****   |
| 2       | Veg Fried Rice    |   3    | ***     |
| 3       | Kaju Fried Rice   |   4    | ****    |
| 4       | Chicken Biryani   |   4    | ****    |
| 5       | Chicken Dum Biryani|  5    | *****   |
| 6       | Prawns Biryani    |   4    | ****    |
| 7       | Fish Birayani     |   4    | ****    |
```
**Explanation:**  
Joins on food_id; use the rating to set the number of asterisks.

***

## Scenario 33: Maximum discount tours per family

**Description:**  
Find the family that can choose the maximum number of discount tours (based on family size and country discount rules).

**Input Table:**
```markdown
family:
| id        | name          | family_size |
|-----------|---------------|-------------|
| ...       | ...           | ...         |

country:
| id        | name          | min_size | max_size |
|-----------|---------------|----------|----------|
| ...       | ...           | ...      | ...      |
```
(**Actual rows omitted for brevity—copy from your source list**)

**Expected Output Table:**
```markdown
| name           | number_of_countries |
|----------------|--------------------|
| Emily Johnson  | 4                  |
```
**Explanation:**  
Count, for each family, the countries whose min–max size range includes the family's size.

***

## Scenario 34: Bucket/group ages and count

**Description:**  
Categorize customers into age groups and count members of each group.

**Input Table:**
```markdown
| customer_id | name   | age | gender |
|-------------|--------|-----|--------|
| 1           | Alice  | 25  | F      |
| 2           | Bob    | 40  | M      |
| 3           | Raj    | 46  | M      |
| 4           | Sekar  | 66  | M      |
| 5           | Jhon   | 47  | M      |
| 6           | Timoty | 28  | M      |
| 7           | Brad   | 90  | M      |
| 8           | Rita   | 34  | F      |
```

**Expected Output Table:**
```markdown
| age_group | count |
|-----------|-------|
| 19–35     | 3     |
| 36–50     | 3     |
| 51+       | 2     |
```
**Explanation:**  
Group by age buckets and count the number of records in each.

***

## Scenario 35: Create, clean, and join dataframes

**Description:**  
Data engineering steps: create DataFrames, count/filter nulls, join, fill missing, and filter on age (usually shown with code).

_No input/output table; focus on sequential code steps:_

**Explanation:**  
- Create a DataFrame
- Count and remove nulls, store dropped in new DataFrame
- Join with a second DataFrame
- Fill missing age values with the mean
- Filter for students aged 18 and above

***

## Scenario 36: Aggregate and count products sold by date

**Description:**  
For each date, gather all products sold and count them.

**Input Table:**
```markdown
| sell_date  | product     |
|------------|------------|
|2020-05-30  | Headphone  |
|2020-06-01  | Pencil     |
|2020-06-02  | Mask       |
|2020-05-30  | Basketball |
|2020-06-01  | Book       |
|2020-06-02  | Mask       |
|2020-05-30  | T-Shirt    |
```

**Expected Output Table:**
```markdown
| sell_date  | products                | null_sell |
|------------|-------------------------|-----------|
|2020-05-30  | [T-Shirt, Basketball...]|    3      |
|2020-06-01  | [Pencil, Book]          |    2      |
|2020-06-02  | [Mask]                  |    1      |
```
**Explanation:**  
Aggregates products sold each date into a list and counts the unique items per date.

***
***

## 🤝 Contributing

Contributions, improvements, and scenario suggestions are **welcome**!  
If you have additional Spark SQL interview scenarios—real-world or tricky—feel free to open an issue or a pull request. Let’s help the community prepare for interviews and practical data tasks together.

***

## 📄 License

This repository is released under the [MIT License](LICENSE).
Courtesy: https://github.com/mohankrishna02/interview-scenerios-spark-sql

***

## 🙏 Acknowledgments

- Inspired by real-world Spark, PySpark, and SQL interview challenges faced by many data engineers.
- Thanks to the open-source community for continuous learning and sharing.

***

**Happy Coding and Good Luck with Your Interviews! 🚀**

***

