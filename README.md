# Analytics Data Where House

**Analytics Data Where House** (or **ADWH**) is a data engineering and analytics engineering project that implements an open-source modern data platform and analyzes public data related to the housing market in Cook County, Illinois.

## Motivation

Creating value from data takes a lot of careful work. Failure to detect, understand, and address [defects in a data set](https://github.com/Quartz/bad-data-guide) can mislead you into making bad decisions. Interactive notebooks make it easy for a data scientist to explore raw data, discover defects, and efficiently prototype code to clean and prepare data. However, the notebook's strength in the initial prototyping and exploration stage becomes a weakness if the work needs to be reproduced or updated, as a data scientist, engineer, or analyst would have to manually update or run the notebook and manually interpret any data quality or validation checks performed in the notebook. Manually updating and rerunning notebooks consumes time that a scientist or engineer could spend answering new questions and makes it possible that bugs will be introduced or upstream data issues will violate the assumptions built into analysis code, eating into (or potentially destroying) any value created.

This system automates as much of that work as possible so that you can focus on answering new questions and build on the foundation provided by prior work. 

Additionally, [Chicago](https://data.cityofchicago.org/) and [Cook County](https://datacatalog.cookcountyil.gov/) publish thousands of interesting data sets to their Socrata data portals, including a wealth of data sets focused on property sales and characteristics in Cook County, and this project includes pipelines for a number of those data sets. As a curious scientist, engineer, and craftsman, I love learning to use new tools to efficiently and accurately answer questions and build solutions to problems, like "Where should I buy a house? I need more space for my tools!"

# Features

This platform automates curating a local data warehouse of interesting, up-to-date public data sets. It enables users (well, mainly one user, me) to easily add data sets to the warehouse, build analyses that explore and answer questions with current data, and discover existing assets to accelerate exploring new questions.

At present, it uses docker to provision and run:
* Apache Superset for dashboarding and exploratory data analysis,
* a PostgreSQL + PostGIS database as the data warehouse,
* a pgAdmin4 database administration interface,
* Airflow components to orchestrate tasks (note: uses a LocalExecutor),
* dbt to manage data transformation and cleaning tasks, serve and facilitate search of the data dictionary and catalog, 
* great_expectations to ensure data meets expectations, and
* custom python code that makes it easy to implement an ELT pipeline for [any other table hosted by Socrata](http://www.opendatanetwork.com/).

<p align="center" width="100%">
 <img src="docs/assets/imgs/superset/Chicago_Single-parcel_sales_dashboard.png" width="90%" alt="Local data is fresh so we will note that and end"/>
</p>

# Documentation

You can see documentation for this platform at [https://docs.analytics-data-where-house.dev/](https://docs.analytics-data-where-house.dev/). This project is still under active development and documentation will continue to evolve with the system.

## System Requirements

To use this system, Docker [Engine](https://docs.docker.com/engine/install/) and [Compose (v2.0.0 or higher)](https://docs.docker.com/compose/install/linux/#install-using-the-repository) are the only hard requirements. 

Having python and GNU make on your host system will provide a lot of quality-of-life improvements (mainly a streamlined setup process and useful makefile recipes), but they're not strictly necessary.

## Setup

To get the system up and running:

0. Clone the repo and `cd` into your local clone.
1. Set your credentials for system components:

    ```bash
    make make_credentials
    ```

    Enter credentials at the prompts, or just press enter to use the default values (shown in square brackets).
2. Build ADWH docker images and initialize ADWH databases:

    ```bash
    make initialize_system
    ```

    Building the images will take a few minutes.
3. Start up the system:

    ```bash
    docker compose up
    ```

    This will produce a lot of output and slow down after 20-40 seconds.

4. Open up another terminal window, `cd` back into your local repo clone, and setup schemas in your data warehouse:

    ```bash
    make create_warehouse_infra
    ```

5. Set up a connection between superset and your data warehouse as shown [here](https://docs.analytics-data-where-house.dev/setup/superset_setup/).

A more complete walkthrough of setup instructions is available [here](https://docs.analytics-data-where-house.dev/setup/getting_started/), but the steps above are sufficient to start collecting and then visualizing and exploring data in this system.

## Usage

After the system is set up, refer to the [User's Guide](https://docs.analytics-data-where-house.dev/user_guide/) for instructions on starting up the system (essentially run `docker compose up`), [running](https://docs.analytics-data-where-house.dev/user_guide/running_a_pipeline/) existing data collection and ELT pipelines, and [creating charts and dashboards](https://docs.analytics-data-where-house.dev/user_guide/visualization/) from data in your warehouse.

## Development and Extension

To extend the system or build out new Socrata data sets, see the [Developer's Guide](https://docs.analytics-data-where-house.dev/dev_guide/).

