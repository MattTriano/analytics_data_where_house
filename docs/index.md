# Welcome to Analytics-Data-Where-House Docs!

This platform automates curating a local data warehouse of interesting, up-to-date public data sets. It enables users (well, mainly one user, me) to easily add data sets to the warehouse, build analyses that explore and answer questions with current data, and discover existing assets to accelerate exploring new questions.

At present, it uses docker to provision and run:

* a PostgreSQL + PostGIS database as the data warehouse,
* a pgAdmin4 database administration interface,
* Airflow components to orchestrate tasks (note: uses a LocalExecutor),
* dbt to manage data transformation and cleaning tasks, serve and facilitate search of the data dictionary and catalog, 
* great_expectations for anomaly detection and data monitoring, and
* custom python code that makes it easy to implement an ELT pipeline for [any other table hosted by Socrata](http://www.opendatanetwork.com/)


## ToC

1. [**System setup**](setup/getting_started.md)
2. [**User's Guide**](user_guide/index.md)
3. [**Data Sources**](data_sources/socrata.md)

