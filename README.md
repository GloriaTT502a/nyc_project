Overview
========

Welcome to NYC project! 

This project uses 
    1 [Astro](https://www.astronomer.io/dg/signup-airflow/?utm_term=astro%20airflow&utm_campaign=brand-ft-global&utm_source=adwords&utm_medium=ppc&hsa_acc=4274135664&hsa_cam=21865965766&hsa_grp=169329542829&hsa_ad=743940119888&hsa_src=g&hsa_tgt=kwd-1777215821248&hsa_kw=astro%20airflow&hsa_mt=p&hsa_net=adwords&hsa_ver=3&gad_source=1&gad_campaignid=21865965766&gbraid=0AAAAADP7Y9h6CsvefFMH8xTG9Q-_USBQ8&gclid=Cj0KCQjwoZbBBhDCARIsAOqMEZUoRqNGZytzbavrQZdXT9hYyecnTyi5p1hJg3NOWR7pZm2bxNfPb_YaAoMIEALw_wcB) 
    2 [Cosmos](https://www.astronomer.io/cosmos/) 
    3 [DBT](https://www.getdbt.com/product/what-is-dbt) 
    4 [Apache Airflow](https://airflow.apache.org/)
    5 BigQuery 
    6 Google Bucket 
    7 [Soda](https://www.soda.io/) 
    8 Power BI
to analyze the taxi in new york city. 



Project Contents
================


Workflow 
================
![workflow diagram](https://github.com/GloriaTT502a/nyc_project/blob/img/img/workflow.png)



Deploy Your Project Locally
===========================

1. Start Airflow on your local machine by running 'astro dev start'.

This command will spin up 4 Docker containers on your machine, each for a different Airflow component:

- Postgres: Airflow's Metadata Database
- Webserver: The Airflow component responsible for rendering the Airflow UI
- Scheduler: The Airflow component responsible for monitoring and triggering tasks
- Triggerer: The Airflow component responsible for triggering deferred tasks

2. Verify that all 4 Docker containers were created by running 'docker ps'.

Note: Running 'astro dev start' will start your project with the Airflow Webserver exposed at port 8080 and Postgres exposed at port 5432. If you already have either of those ports allocated, you can either [stop your existing Docker containers or change the port](https://www.astronomer.io/docs/astro/cli/troubleshoot-locally#ports-are-not-available-for-my-local-airflow-webserver).

3. Access the Airflow UI for your local Airflow project. To do so, go to http://localhost:8080/ and log in with 'admin' for both your Username and Password.

You should also be able to access your Postgres Database at 'localhost:5432/postgres'.

Deploy Your Project to Astronomer
=================================

If you have an Astronomer account, pushing code to a Deployment on Astronomer is simple. For deploying instructions, refer to Astronomer documentation: https://www.astronomer.io/docs/astro/deploy-code/

Contact
=======

The Astronomer CLI is maintained with love by the Astronomer team. To report a bug or suggest a change, reach out to our support.
