# Welcome to Analytics-Data-Where-House Docs!

## Table of Contents

1. [**System setup**](setup/getting_started.md)
2. [**User's Guide**](user_guide/index.md)
3. [**Developer's Guide**](dev_guide/index.md)
4. [**Data Sources**](data_sources/socrata.md)

## Platform Overview

This platform automates curating a local data warehouse of interesting, up-to-date public data sets. It enables users (well, mainly one user, me) to easily add data sets to the warehouse, build analyses that explore and answer questions with current data, and discover existing assets to accelerate exploring new questions.

At present, it uses docker to provision and run:

* a PostgreSQL + PostGIS database as the data warehouse,
* **Apache Superset** for:
    * Interactive Data Visualization and EDA
    * Dashboarding and Reporting

    ![Geospatial Data Analysis](/assets/imgs/superset/deckgl_polygon_chart_demo.png)
    ![Time Series Analysis](/assets/imgs/superset/median_sale_price_by_property_class.png)    
    ![Dashboarding](/assets/imgs/superset/dashboard_demo.png)

* a **pgAdmin4** database administration interface,

    ![Sample Exploration of a DWH table](/assets/imgs/pgAdmin4/Geospatial_query_and_data_in_pgAdmin4.png)

* **Airflow** components to orchestrate execution of tasks,

    ![Airflow DagBag for Cook County tag](/assets/imgs/Socrata_ELT_DAG/Running_DAGs.png)

* **dbt** to:
    * manage sequential data transformation + cleaning tasks,
    * serve data documentation and data lineage graphs, and
    * facilitate search of the data dictionary and data catalog 
    
    ![dbt Data Lineage Graph](/assets/imgs/systems/dbt_lineage_graph_of_parcel_sales.png)
    ![All Data Tables' Lineage Graphs](/assets/imgs/dbt/lineage_graph_of_all_nodes.png)
    ![One Data Set's Lineage Graph](/assets/imgs/systems/dbt_data_docs_interface_showing_parcel_sales.png)    

* great_expectations for anomaly detection and data monitoring, and

    ![great_expectations Data Docs after checkpoint run](/assets/imgs/workflows/expectations/data_docs_after_a_successful_checkpoint_run.png)

* custom python code that makes it easy to implement an ELT pipeline for [any other table hosted by Socrata](http://www.opendatanetwork.com/)

    ![data-loading TaskGroups in load_data_tg TaskGroup](/assets/imgs/Socrata_ELT_DAG/Full_view_data_loaders_in_load_data_tg.PNG)

    ![load_data_tg TaskGroup High Level](/assets/imgs/Socrata_ELT_DAG/load_data_task_group_w_checkpoints.png)    

    ![automate as much pipeline development as possible](/assets/imgs/Socrata_ELT_DAG/generate_and_run_dbt_models.png)
