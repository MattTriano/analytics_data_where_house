# Welcome to Analytics-Data-Where-House Docs!

## ToC

1. [**System setup**](setup/getting_started.md)
2. [**User's Guide**](user_guide/index.md)
3. [**Data Sources**](data_sources/socrata.md)

## Platform Overview

This platform automates curating a local data warehouse of interesting, up-to-date public data sets. It enables users (well, mainly one user, me) to easily add data sets to the warehouse, build analyses that explore and answer questions with current data, and discover existing assets to accelerate exploring new questions.

At present, it uses docker to provision and run:

* a PostgreSQL + PostGIS database as the data warehouse,
* a **pgAdmin4** database administration interface,

    ![Sample Exploration of a DWH table](/assets/imgs/pgAdmin4/Geospatial_query_and_data_in_pgAdmin4.png)

* **Airflow** components to orchestrate tasks (note: uses a LocalExecutor),

    ![Airflow DagBag for Cook County tag](/assets/imgs/systems/Airflow_Cook_County_Tagged_DagBag.png)

* **dbt** to:
    * manage data transformation + cleaning tasks,
    * serve data documentation and data lineage graphs, and
    * facilitate search of the data dictionary and data catalog 

    ![dbt Data Documentation Interface](/assets/imgs/systems/dbt_data_docs_interface_showing_parcel_sales.png)
    ![dbt Data Lineage Graph](/assets/imgs/systems/dbt_lineage_graph_of_parcel_sales.png)

* great_expectations for anomaly detection and data monitoring, and
* custom python code that makes it easy to implement an ELT pipeline for [any other table hosted by Socrata](http://www.opendatanetwork.com/)

    ![load_data_tg TaskGroup High Level](/assets/imgs/Socrata_ELT_DAG/High_level_load_data_tg.PNG)

    ![data-loading TaskGroups in load_data_tg TaskGroup](/assets/imgs/Socrata_ELT_DAG/Full_view_data_loaders_in_load_data_tg.PNG)

    ![load_data_tg TaskGroup High Level](/assets/imgs/Socrata_ELT_DAG/High_level_load_data_tg.PNG)
