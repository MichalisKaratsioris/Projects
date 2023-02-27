0.  General Steps
                Github repo 
                => GCP account/project 
                => dbt account/project 
                => Update Github repo 
                => clone Github repo to pc


I.  Create a GCP (Google Cloud Platform):

                    1.  Create an account: https://cloud.google.com/free-
                    2.  Creare a new project. Make sure to define an ID that is connected to the project, as the project ID
                        will be used later on as the name for the database in the cloud to connect with.
                    3.  Create a new Big Query dataset (location somewhere in the US).
                    4.  Set up for your user, the following permissions:
                        * Hamburger icon in the left top corner => IAM & Admin => IAM => Add
                            - BigQuery Data Editor
                            - BigQuery User
                            - BigQuery Job User
                    5.  Create authentication credentials
                        * Hamburger icon in the left top corner => IAM & Admin => Service Accounts
                            -   Create Service Account
                            -   After finishing creating the Service Account, create a Key for the authentication. 
                                * Keys tab => Add key => Create new key => JSON
                            -   Save the .json file somewhere in the pc.
                    6.  Enable APIs
                        * Hamburger icon in the left top corner => APIs and Services => Enable APIs

------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------

IIa.    dbt Cloud project development:



IIb.    dbt Local project development:
    
                    Python
                        1.  Install Python 3.8:                             https://www.python.org/downloads/release/python-3810/
                            * Make sure that click the "ADD TO PATH" 
                            option in the installation
                        2.  Install the python virtualenv package run:      python -m pip install virtualenv
                        3.  Create a Git repo and clone is in your pc:      git clone git@github.com:<your-github-user>/<your-dbt-repo>.git
                        4.  Go to the cloned repository
                        5.  Create a virtual environment run:               python -m venv .venv
                        6.  Activate the virtual environment run:           .\.venv\Scripts\activate.ps1


                    Google Cloud CLI
                        1.  Install the gcloud CLI:                         https://cloud.google.com/sdk/docs/install#windows
                        2.  Setup gcloud CLI:                               gcloud config set project <your-gcp-project-name>
                                                                            gcloud auth application-default login --scopes=https://www.googleapis.com/auth/bigquery
                                                                            gcloud auth application-default login --scopes=\https://www.googleapis.com/auth/drive.readonly
                                                                            gcloud auth application-default login --scopes=\https://www.googleapis.com/auth/iam.test


                    dbt
                        1.  Install dbt and bigquery:                       pip install dbt-core
                                                                            pip install dbt-bigquery
                        2.  Create an empty .profiles.yml file:             /Users/<your-username>/.dbt/profiles.yml
                        3.  Open a terminal in the repo file:               dbt init <dbt-project-name>
                        4.  Open the folder containing the dbt project      dbt debug
                            with VS Code and in the integrated terminal.    dbt run

------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------

III.    Extra
                    Airbyte
                        1.  Run the following commands in your terminal:    git clone https://github.com/airbytehq/airbyte.git
                                                                            cd airbyte
                                                                            docker-compose up
                        2.  Connect GCP-Bigquery with Google Sheet
                            -   
                            -   
                            -   
                    

