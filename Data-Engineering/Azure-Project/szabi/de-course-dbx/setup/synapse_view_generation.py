# Databricks notebook source
# MAGIC %md
# MAGIC #### Installing and importing the necessary components:

# COMMAND ----------

# MAGIC %sh
# MAGIC curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
# MAGIC curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
# MAGIC sudo apt-get update
# MAGIC sudo ACCEPT_EULA=Y apt-get -q -y install msodbcsql17
# MAGIC sudo apt-get install python3-pip -y
# MAGIC pip3 install --upgrade pyodbc

# COMMAND ----------

import pyodbc
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC #### Connecting to SQL endpoint and creating a database:

# COMMAND ----------

driver = '{ODBC Driver 17 for SQL Server}'
server = 'synapse-project-phase-ondemand.sql.azuresynapse.net'
username = 'sqladminuser'
password = 'gr00nsynapsE'
database = 'project_db'
d = pyodbc.connect(f'''
                    Driver={driver};
                    Server={server};
                    PORT=1433;
                    Database=master;
                    UID={username};
                    Pwd={password};
                    Encrypt=yes;
                    TrustServerCertificate=no;
                    Connection Timeout=30;
                    '''
                    )
params = ()
d.autocommit = True
cursor = d.cursor()
query = f'''
            IF NOT EXISTS(SELECT * 
                          FROM sys.databases
                          WHERE name = 'project_db')
            BEGIN
                CREATE DATABASE {database}
            END
         '''
cursor.execute(query, params)
d.close()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Connecting to SQL database and creating a credential and the bronze, silver, gold data sources and schemas:

# COMMAND ----------

driver = '{ODBC Driver 17 for SQL Server}'
server = 'synapse-project-phase-ondemand.sql.azuresynapse.net'
username = 'sqladminuser'
password = 'gr00nsynapsE'
database = 'project_db'
d = pyodbc.connect(f'''
                    Driver={driver};
                    Server={server};
                    PORT=1433;
                    Database={database};
                    UID={username};
                    Pwd={password};
                    Encrypt=yes;
                    TrustServerCertificate=no;
                    Connection Timeout=30;
                    '''
                    )
params = ()
d.autocommit = True
cursor = d.cursor()
query = f'''
            IF NOT EXISTS(SELECT * 
                          FROM sys.database_credentials
                          WHERE name = 'ManagedIdentityCredential')
            BEGIN
                CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'MasterKeyll.'
                CREATE DATABASE SCOPED CREDENTIAL ManagedIdentityCredential WITH IDENTITY = 'Managed Identity'
            END
         '''
cursor.execute(query, params)
d.close()

# COMMAND ----------

driver = '{ODBC Driver 17 for SQL Server}'
server = 'synapse-project-phase-ondemand.sql.azuresynapse.net'
username = 'sqladminuser'
password = 'gr00nsynapsE'
database = 'project_db'
d = pyodbc.connect(f'''
                    Driver={driver};
                    Server={server};
                    PORT=1433;
                    Database={database};
                    UID={username};
                    Pwd={password};
                    Encrypt=yes;
                    TrustServerCertificate=no;
                    Connection Timeout=30;
                    '''
                    )
params = ()
d.autocommit = True
cursor = d.cursor()
query = f'''
            IF NOT EXISTS(SELECT * 
                          FROM sys.external_data_sources
                          WHERE name = 'bronze')
            BEGIN
                CREATE EXTERNAL DATA SOURCE bronze
            WITH (
                  LOCATION = 'https://dl0projectphase.dfs.core.windows.net/bronze',
                  CREDENTIAL = ManagedIdentityCredential
                 )
            END
            
            IF NOT EXISTS (SELECT *
                           FROM sys.schemas
                           WHERE name = 'bronze')
            BEGIN
                EXEC('CREATE SCHEMA bronze')
            END
         '''
cursor.execute(query, params)
d.close()

# LOCATION = 'https://dl0projectphase.dfs.core.windows.net/bronze' <--- container - bronze - on the right side: "..."(Context menu) - "Container properties"

# COMMAND ----------

driver = '{ODBC Driver 17 for SQL Server}'
server = 'synapse-project-phase-ondemand.sql.azuresynapse.net'
username = 'sqladminuser'
password = 'gr00nsynapsE'
database = 'project_db'
d = pyodbc.connect(f'''
                    Driver={driver};
                    Server={server};
                    PORT=1433;
                    Database={database};
                    UID={username};
                    Pwd={password};
                    Encrypt=yes;
                    TrustServerCertificate=no;
                    Connection Timeout=30;
                    '''
                    )
params = ()
d.autocommit = True
cursor = d.cursor()
query = f'''
            IF NOT EXISTS(SELECT * 
                          FROM sys.external_data_sources
                          WHERE name = 'silver')
            BEGIN
                CREATE EXTERNAL DATA SOURCE silver
            WITH (
                  LOCATION = 'https://dl0projectphase.dfs.core.windows.net/silver',
                  CREDENTIAL = ManagedIdentityCredential
                 )
            END
            
            IF NOT EXISTS (SELECT *
                           FROM sys.schemas
                           WHERE name = 'silver')
            BEGIN
                EXEC('CREATE SCHEMA silver')
            END
         '''
cursor.execute(query, params)
d.close()

# LOCATION = 'https://dl0projectphase.dfs.core.windows.net/silver' <--- container - silver - on the right side: "..."(Context menu) - "Container properties"

# COMMAND ----------

driver = '{ODBC Driver 17 for SQL Server}'
server = 'synapse-project-phase-ondemand.sql.azuresynapse.net'
username = 'sqladminuser'
password = 'gr00nsynapsE'
database = 'project_db'
d = pyodbc.connect(f'''
                    Driver={driver};
                    Server={server};
                    PORT=1433;
                    Database={database};
                    UID={username};
                    Pwd={password};
                    Encrypt=yes;
                    TrustServerCertificate=no;
                    Connection Timeout=30;
                    '''
                    )
params = ()
d.autocommit = True
cursor = d.cursor()
query = f'''
            IF NOT EXISTS(SELECT * 
                          FROM sys.external_data_sources
                          WHERE name = 'gold')
            BEGIN
                CREATE EXTERNAL DATA SOURCE gold
            WITH (
                  LOCATION = 'https://dl0projectphase.dfs.core.windows.net/gold',
                  CREDENTIAL = ManagedIdentityCredential
                 )
            END
            
            IF NOT EXISTS (SELECT *
                           FROM sys.schemas
                           WHERE name = 'gold')
            BEGIN
                EXEC('CREATE SCHEMA gold')
            END
         '''
cursor.execute(query, params)
d.close()

# LOCATION = 'https://dl0projectphase.dfs.core.windows.net/gold' <--- container - gold - on the right side: "..."(Context menu) - "Container properties"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating a view for all of bronze tables:

# COMMAND ----------

driver = '{ODBC Driver 17 for SQL Server}'
server = 'synapse-project-phase-ondemand.sql.azuresynapse.net'
username = 'sqladminuser'
password = 'gr00nsynapsE'
database = 'project_db'
d = pyodbc.connect(f'''
                    Driver={driver};
                    Server={server};
                    PORT=1433;
                    Database={database};
                    UID={username};
                    Pwd={password};
                    Encrypt=yes;
                    TrustServerCertificate=no;
                    Connection Timeout=30;
                    '''
                    )
params = ()
d.autocommit = True
cursor = d.cursor()

query = f'''
            CREATE OR ALTER VIEW bronze.b_github
            AS
            SELECT *
            FROM
                OPENROWSET( BULK 'b_github', DATA_SOURCE= 'bronze', FORMAT='DELTA') AS ROWS
         '''
cursor.execute(query, params)

query = f'''
            CREATE OR ALTER VIEW bronze.b_company_detail
            AS
            SELECT *
            FROM
                OPENROWSET( BULK 'b_company_detail', DATA_SOURCE= 'bronze', FORMAT='DELTA') AS ROWS
         '''
cursor.execute(query, params)

query = f'''
            CREATE OR ALTER VIEW bronze.b_stackoverflow_post_questions
            AS
            SELECT *
            FROM
                OPENROWSET( BULK 'b_stackoverflow_post_questions', DATA_SOURCE= 'bronze', FORMAT='DELTA') AS ROWS
         '''
cursor.execute(query, params)

query = f'''
            CREATE OR ALTER VIEW bronze.b_stackoverflow_post_answers
            AS
            SELECT *
            FROM
                OPENROWSET( BULK 'b_stackoverflow_post_answers', DATA_SOURCE= 'bronze', FORMAT='DELTA') AS ROWS
         '''
cursor.execute(query, params)
d.close()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating a view for all of  tables:

# COMMAND ----------

driver = '{ODBC Driver 17 for SQL Server}'
server = 'synapse-project-phase-ondemand.sql.azuresynapse.net'
username = 'sqladminuser'
password = 'gr00nsynapsE'
database = 'project_db'
d = pyodbc.connect(f'''
                    Driver={driver};
                    Server={server};
                    PORT=1433;
                    Database={database};
                    UID={username};
                    Pwd={password};
                    Encrypt=yes;
                    TrustServerCertificate=no;
                    Connection Timeout=30;'''
                    )
params = ()
d.autocommit = True
cursor = d.cursor()

query = f'''
            CREATE OR ALTER VIEW silver.s_github
            AS
            SELECT *
            FROM
                OPENROWSET( BULK 's_github', DATA_SOURCE= 'silver', FORMAT='DELTA') AS ROWS
         '''
cursor.execute(query, params)

query = f'''
            CREATE OR ALTER VIEW silver.s_company_detail
            AS
            SELECT *
            FROM
                OPENROWSET( BULK 's_company_detail', DATA_SOURCE= 'silver', FORMAT='DELTA') AS ROWS
         '''
cursor.execute(query, params)

query = f'''
            CREATE OR ALTER VIEW silver.s_stackoverflow_post_questions
            AS
            SELECT *
            FROM
                OPENROWSET( BULK 's_stackoverflow_post_questions', DATA_SOURCE= 'silver', FORMAT='DELTA') AS ROWS
         '''
cursor.execute(query, params)

query = f'''
            CREATE OR ALTER VIEW silver.s_stackoverflow_post_answers
            AS
            SELECT *
            FROM
                OPENROWSET( BULK 's_stackoverflow_post_answers', DATA_SOURCE= 'silver', FORMAT='DELTA') AS ROWS
         '''
cursor.execute(query, params)
d.close()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating a view for all of gold tables:

# COMMAND ----------

driver = '{ODBC Driver 17 for SQL Server}'
server = 'synapse-project-phase-ondemand.sql.azuresynapse.net'
username = 'sqladminuser'
password = 'gr00nsynapsE'
database = 'project_db'
d = pyodbc.connect(f'''
                    Driver={driver};
                    Server={server};
                    PORT=1433;
                    Database={database};
                    UID={username};
                    Pwd={password};
                    Encrypt=yes;
                    TrustServerCertificate=no;
                    Connection Timeout=30;'''
                    )
params = ()
d.autocommit = True
cursor = d.cursor()

query = f'''
            CREATE OR ALTER VIEW gold.g_github_daily
            AS
            SELECT *
            FROM
                OPENROWSET( BULK 'g_github_daily', DATA_SOURCE= 'gold', FORMAT='DELTA') AS ROWS
         '''
cursor.execute(query, params)

query = f'''
            CREATE OR ALTER VIEW gold.g_github_monthly
            AS
            SELECT *
            FROM
                OPENROWSET( BULK 'g_github_monthly', DATA_SOURCE= 'gold', FORMAT='DELTA') AS ROWS
         '''
cursor.execute(query, params)

query = f'''
            CREATE OR ALTER VIEW gold.g_github_quarterly
            AS
            SELECT *
            FROM
                OPENROWSET( BULK 'g_github_quarterly', DATA_SOURCE= 'gold', FORMAT='DELTA') AS ROWS
         '''
cursor.execute(query, params)

query = f'''
            CREATE OR ALTER VIEW gold.g_stackoverflow_post_questions_daily
            AS
            SELECT *
            FROM
                OPENROWSET( BULK 'g_stackoverflow_post_questions_daily', DATA_SOURCE= 'gold', FORMAT='DELTA') AS ROWS
         '''
cursor.execute(query, params)

query = f'''
            CREATE OR ALTER VIEW gold.g_stackoverflow_post_questions_monthly
            AS
            SELECT *
            FROM
                OPENROWSET( BULK 'g_stackoverflow_post_questions_monthly', DATA_SOURCE= 'gold', FORMAT='DELTA') AS ROWS
         '''
cursor.execute(query, params)

query = f'''
            CREATE OR ALTER VIEW gold.g_stackoverflow_post_questions_quarterly
            AS
            SELECT *
            FROM
                OPENROWSET( BULK 'g_stackoverflow_post_questions_quarterly', DATA_SOURCE= 'gold', FORMAT='DELTA') AS ROWS
         '''
cursor.execute(query, params)
d.close()

# COMMAND ----------

dbutils.notebook.exit("This notebook successfully run and finished!")

# COMMAND ----------


