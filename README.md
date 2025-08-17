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

Type **"Next"** for scenarios 6–10, or specify a range!

[1] https://github.com/mohankrishna02/interview-scenerios-spark-sql?tab=readme-ov-file#scenerio-1
