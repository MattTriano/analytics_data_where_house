# Analytics Data Where House

**Analytics Data Where House** (or **ADWH**) is a data engineering and analytics engineering project that implements an open-source modern data platform and analyzes public data related to the housing market in Cook County, Illinois.

# Features

This platform automates curating a local data warehouse of interesting, up-to-date public data sets. It enables users (well, mainly one user, me) to easily add data sets to the warehouse, build analyses that explore and answer questions with current data, and discover existing assets to accelerate exploring new questions.

At present, it uses docker to provision and run:
* a PostgreSQL + PostGIS database as the data warehouse,
* a pgAdmin4 database administration interface,
* Airflow components to orchestrate tasks (note: uses a LocalExecutor),
* dbt to manage data transformation and cleaning tasks, serve and facilitate search of the data dictionary and catalog, 
* great_expectations to ensure data meets  and
* custom python code that makes it easy to implement an ELT pipeline for [any other table hosted by Socrata](http://www.opendatanetwork.com/).

<p align="center" width="100%">
 <img src="docs/assets/imgs/superset/Chicago_Single-parcel_sales_dashboard.png" width="80%" alt="Local data is fresh so we will note that and end"/>
</p>

## Motivation

Creating value from data takes a lot of careful work. Failure to detect, understand, and address [defects in a data set](https://github.com/Quartz/bad-data-guide) can mislead you into making bad decisions, and even if you handle every issue, your work with a data set will get stale over time. If this work is done manually in notebooks or by hand, refreshing your analysis might involve redoing a large amount of work.

This system is designed to capture as much of the value created while working with a new data set.
  Creating value from data is Data only creates value when it enables you to make better decisions or ask better questions, and it can take an overwhelming amount of work before you can start creating value from a new raw data set. To create value from a data set:
* the data set must be relevant to questions of interest
* you must understand the data set's features/columns that are relevant to your questions,
* 

Data analysis can create value, The data set's features (or columns) must be understood Insights must be timely, 

Chicago and Cook County publish thousands of interesting data sets to their Socrata data portals, , and there are many pitfalls that can negate 
 Converting public data into actionable,  

I like to research before I buy anything, especially if it's a big-ticket item. I've been considering buying a house for a while, but the methods I use for answering questions like "what phone should I buy?" or "how can I make my apartment less drafty in winter" haven't been adequate to answer questions I have about real estate. Fortunately, the real estate market I've grown fond of has the richest public data culture in the US (that I, a data scientist focused on Chicago-related issues, am aware of). This market's Assessor's Office regularly [publishes data](https://datacatalog.cookcountyil.gov/browse?tags=cook%20county%20assessor) I can mine for answers to some of my biggest questions.

# Documentation

You can see documentation for this platform at [https://docs.analytics-data-where-house.dev/](https://docs.analytics-data-where-house.dev/). This project is still under active development and documentation will continue to evolve with the system.

## System Requirements

To use this system, Docker [Engine](https://docs.docker.com/engine/install/) and [Compose (v2.0.0 or higher)](https://docs.docker.com/compose/install/linux/#install-using-the-repository) are the only hard requirements. 

Having python and GNU make on your host system will provide a lot of quality of life improvements (mainly a streamlined setup process and useful makefile recipes), but they're not strictly necessary.


## Usage

After the [system is set up](https://docs.analytics-data-where-house.dev/setup/getting_started/), you can start up your system, unpause DAGs in the Airflow UI, and start adding Socrata data sets to the warehouse.


### Serving dbt Data Documentation and Discovery UI 

To generate and serve documentation for the data transformations executed by dbt, run the command below, and after the doc server has started up, go to [http://localhost:18080](http://localhost:18080) to explore the documentation UI.

The documentation will be mainly based on the sources, column names, and descriptions recorded in the `.yml` file in the `.../dbt/models/...` directories with table-or-view-producing dbt scripts.

```bash
user@host:.../your_local_repo$ make serve_dbt_docs

```
<p align="center" width="100%">
  <img src="docs/assets/imgs/dbt/dbt_doc_sample_page_w_lineage_graph.PNG" width="90%" alt="dbt documentation page with table lineage graph"/>
</p>

