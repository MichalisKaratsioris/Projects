# Data Engineering Trial Exam

## Getting Started

- **Fork** this repository under your own account
- Clone your forked repository to your computer
- Commit your progress frequently and with descriptive commit messages
- All your answers and solutions should go in this repository
- Take care of style guide
- Take care of the naming of classes, fields, variables, files, etc.

## Keep in mind

- You can use any resource online, but **please work individually**

- **Don't just copy-paste** your answers and solutions,
  you need to understand every line of your code
- **Don't push your work** to GitHub until your mentor announces
  that the time is up
- At the end of the exam push your solution to **GitHub**

## Overview

Help GFA with deciding of GFA applicants fate. You will be provided with CSV file that contains dataset about applicants, with the information about their preferred learning language and cognitive test score. Using Airflow your job is to extract the dataset **every week**, transform the dataset and load the data into SQL database and MongoDB. MongoDB will be used for generating and retrieving useful information about students and classes.

You will be provided with [docker-compose file](https://github.com/green-fox-academy/data-eng-trial-exam/blob/master/docker-compose.yaml) which consists of Airflow and MSSQL containers (don't forget to create connection in Airflow for MSSQL and/or MongoDB). Your MongDB database should be running on Atlas cloud, but you can create this database locally, it is up to your decision.

## Extract applicant data, transform and load

Create an Airflow DAG which extract this [dataset](https://github.com/green-fox-academy/data-eng-trial-exam/blob/master/dataset.csv) or use this url https://raw.githubusercontent.com/ejmyyy/gfa-applicants-data/main/dataset.csv (so you don't need to download the file). Your job is to transform this dataset so our backoffice employees will be able to work with the data. (Note that some of the pandas functions are immutable, you have to assign its value to your dataframe to actually change the dataframe).
- Remove duplicated rows
- Fill NaN values with "Uknown"
- Filter out these rows, where applicants' cognitive score is below 50
- Create new column "Class-name" and apply this logic: 
  - If applicants preferred language is Java -> value of the column will be "Panthera", else -> "Celadon".

### SQL

Now after transforming this dataset lets load the dataset to database, where we will be storing all information about applicants. Create a connection to your MSSQL (or any other SQL) database. Create the following tables and store data from dataset into it.
- **Table classes**
  - id
  - name
- **Table applicants**
  - id
  - name and surname
  - age
  - address
  - preferred language
  - cognitive score
  - class_id
  
The relationship between the tables is: One class can have more applicants, but one applicant belongs only to one class.
Amount of DAG tasks is up you. 

(Export the db from Azure Data Studio as csv.)

### MongoDB

Create a NoSQL Database MongoDB. Create a collection named Applicants and create documents with following info:
- name and surname
- age
- cognitive score
- class name

With following collection, GFA employees will be able to make reports.

### SQL Aggregation

Create queries for the below tasks to provide some valuable information:
- Average, minimum and maximum cognitive score by programming languages
- What are the names of applicants between the age of 35 and 65 whose cognitive score is above the overall average?
- Select the top 3 applicants based on cognitive score by programming language
- Select the oldest applicant's name and age by class name and programming language

(Create a separate DAG with the 4 aggregation tasks.)

### Bonus

As a bonus task, create few queries using Mongo:
- Select all applicants, whose cognitive score is more then 80. Return only name and cognitive score.
- Who is the oldest applicant?
- Select applicant that have 'a' in their name and sort the result by name in ascending order.
- What is the average cognitive score of applicants that belongs to Celadon class?

(Create a DAG and use pymongo. Export the db from MongoDB Compass as json.)
