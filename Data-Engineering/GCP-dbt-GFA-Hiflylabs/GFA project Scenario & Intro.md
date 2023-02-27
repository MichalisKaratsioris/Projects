SLIDE 2:
    [SCENARIO] 
    A coding bootcamp called "Black Flying Fox" wants to open new courses based on current technology trends. One of the courses will be 
    for future Data Engineers and some of the course-related questions they asked were:
        (1) Which was the most used technology in 2022 for ingesting data?
        (2) Which transformation technology was most popular in 2022?
        (3) Which orchestration technology was used most in 2022?

    Our business goal is to get a better understanding of current technology trends, so in this project the main focus will be,
    on the emerging data startups, especially companies related to the modern data stack. We want to understand which are the 
    hot projects and see how the monitored projects perform compared to others. The goal is to provide a complete, automated and 
    parametrized solution.

    The main objective is to better understand the development of the tech market. To help with this, there are many data sources 
    available. We selected to work with the freely available public datasets provided by BigQuery about the websites: GitHub and 
    Stackoverflow.

SLIDE 4:
    [ARCHITECTURE]
    We created an ELT pipeline. Using Airbyte and BigQuery for the Extract and Load phases, and dbt for the Tranform phase:
        EXTRACTION & LOAD STAGE: 
            We created a common data repository for public datasets - Github Archives and Stackoverflow - by loading them into a central database, 
            in our case BigQuery in Google Cloud Platform. Then by using Airbyte, we ingested the data from Google Sheet into the same central
            database.
        TRANSFORMATION:
            By using dbt, we were able to transform the data in our database, passing through three different layers: Staging, Intermediate and
            Market.
    The last part of the project was to try and make sense from all the data we collected, and be able to answer simple questions by the aid 
    of a visualization tool such as Power BI.

SLIDE 5:
    [STACKOVERFLOW] 
    Stack Overflow is a question and answer website for professional and enthusiast programmers. It features questions and answers on 
    a wide range of topics in computer programming. It is the largest online community for programmers to learn, share their knowledge 
    and advance their careers. The BigQuery dataset includes an archive of all the Stack Overflow content, including posts, votes, 
    tags and badges. For the Stack Overflow data source, the organizing principle for separating the projects will be the tags in the 
    questions.

SLIDE 6:
    [GITHUB]
    GitHub is a code hosting platform for version control and collaboration. It lets people work together on projects from anywhere. 
    The BigQuery dataset comprises the largest released source of GitHub activities. It contains a full snapshot of the content of 
    repositories including PRs, commits, forks and other activities that can be carried out on the site. For the GitHub data source, 
    the organizing principle for separating the projects will be the organization name and possibly an exact repository account within 
    the organization.

SLIDE 7:
    [GOOGLE-SHEET]
    "Black Flying Fox" provided us with a Google Sheet that contains all the different technologies they are interested in getting 
    insights. The Google Sheet data can be updated at any time from the employees of "Black Flying Fox", so this need to be taken into account.

    To make this data meaningful for end users, a data visualization tool will be used in the final phase of the project. We want 
    to make the data prepared, accessible and meaningful through dashboards, charts, KPIs and other metrics.

    DATASETS:
        - Github Archives
        - Stackoverflow
    Configuration:
        - Google Sheet
    
    The target is to create a common data repository for these datasets, load them into a central database, apply the necessary transformations and create useful insights from the available data, that is presentable with Data Visualizations.



SLIDE 8
[SUMMARY]   PROCESS & TECHNOLOGIES
            - Ingestion and creation of database (Extraction & Load)
            - dbt (Transformation)
                - Staging layer
                - Intermediate layer
                - Market layer
                - dbt jobs / Airflow for automation
            - Power BI for visualization
