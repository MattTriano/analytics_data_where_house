# pgAdmin4

The pgAdmin4 UI makes it very easy to explore your data, inspect database internals, and make manual changes while developing features, but before you can make use of this excellent interface, you have to set a connection to a database. This platform uses two separate databases: one as a backend for Airflow, and the other as the data warehouse database.

![Airflow Metadata db contents](/assets/imgs/pgAdmin4/Airflow_metadata_db_example_table.png)
![Sample Exploration of a DWH table](/assets/imgs/pgAdmin4/Geospatial_query_and_data_in_pgAdmin4.png)

## Setting up database connections in pgAdmin4

Before you can explore databases in either database server in pgAdmin4's interface, you have to set up connections to those database servers.


![pgAdmin4 Add New Server](/assets/imgs/pgAdmin4/Landing_page_view.png)

The values needed to set up these connections will mainly come from a dot-env file (`.env` or `.dwh.env`) or from service names in the `docker-compose.yml` file.

### Airflow Metadata Database
To create a new connection, start by clicking the "Add New Server" button (you might have to click the "Servers" line in the lefthand tray first).


#### Register Server: General tab

* **Name:** `airflow_metadata_db`
    * This can be whatever you want, it's just the label that pgAdmin4 will use for the connection.

Don't worry about the **Server group** field, the default is fine.

![Register airflow_metadata_db server general](/assets/imgs/pgAdmin4/Setting_up_pgAdmin4_connection_to_airflow_metadata_pg1.png)

#### Register Server: Connection tab

* **Host name/address:** `airflow_db`
    * This is defined [here](https://github.com/MattTriano/analytics_data_where_house/blob/c75869ba6fae5c033e6601b9203fd178148f2777/docker-compose.yml#L34) in the `docker-compose.yml` file
* **Port:** 5432 
    * This is the database's port number inside the container, as defined to the right of the colon [here](https://github.com/MattTriano/analytics_data_where_house/blob/c75869ba6fae5c033e6601b9203fd178148f2777/docker-compose.yml#L44).
* **Username:** the `POSTGRES_USER` value in your `.env` file.
* **Password:** the `POSTGRES_PASSWORD` value in your `.env` file.

The defaults for **Maintenance database**, **Role**, **Service**, and the two toggles (of `postgres`, blank, blank, off, and off, respectively) are all fine.

![Register airflow_metadata_db server connection](/assets/imgs/pgAdmin4/Setting_up_pgAdmin4_connection_to_airflow_metadata_pg2.png){ width=80% }

Then press **Save** to finalize the connection.

### Data Warehouse Database

Repeat the process to connect to the data warehouse database.

#### Register Server: General tab

* **Name:** `data_warehouse_db`
    * As before, this can be whatever you want

![pgAdmin4 airflow_metadata_db connection](/assets/imgs/pgAdmin4/Setting_up_pgAdmin4_connection_to_data_warehouse_db_pg1.png)

#### Register Server: Connection tab

* **Host name/address:** `dwh_db`
    * This is defined [here](https://github.com/MattTriano/analytics_data_where_house/blob/c75869ba6fae5c033e6601b9203fd178148f2777/docker-compose.yml#L61) in the `docker-compose.yml` file
* **Port:** 5432 
    * This is the database's port number inside the container, as defined to the right of the colon [here](https://github.com/MattTriano/analytics_data_where_house/blob/c75869ba6fae5c033e6601b9203fd178148f2777/docker-compose.yml#L71)
* **Username:** the `DWH_POSTGRES_USER` value in your `.env` file
    * This will match the `POSTGRES_USER` value in your `.dwh.env` file.
* **Password:** the `DWH_POSTGRES_PASSWORD` value in your `.env` file
    * This will match the `POSTGRES_PASSWORD` value in your `.dwh.env` file.

The defaults for **Maintenance database**, **Role**, **Service**, and the two toggles (of `postgres`, blank, blank, off, and off, respectively) remain all fine.

![pgAdmin4 airflow_metadata_db connection](/assets/imgs/pgAdmin4/Setting_up_pgAdmin4_connection_to_data_warehouse_db_pg2.png)

Press **Save** to finalize the connection.

Now pgAdmin4 is configured! Click around, explore the dropdowns, right-click things and see what you see. This is an excellent way to learn about the internals of postgres!