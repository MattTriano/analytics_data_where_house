# Setting up Superset

Log in to superset at [http://localhost:8088](http://localhost:8088) using the `ADMIN_USERNAME` and `ADMIN_PASSWORD` from your `.env.superset` file.

## Connecting to the Data Warehouse Database
Before you can develop charts and dashboards in Apache Superset, you have to create a connection to your data warehouse database.

Click the :material-plus-thick: (in the upper right corner) and then select the **Connect database** option from the **:material-database: Data** menu. 

![Connecting a database](/assets/imgs/superset/connecting_a_db_step_0_of_3.png)

Select the PostgreSQL option and then enter the following credentials:

* **HOST:** enter **dwh_db** (must match the name of the DB service in the `docker-compose.yml`)
* **PORT:** 5432
* **DATABASE NAME:** enter the value of the **DWH_POSTGRES_DB** env var from your `.env` file
* **USERNAME:** enter the value of the **DWH_POSTGRES_USER** env var from your `.env` file
* **PASSWORD:** enter the value of the **DWH_POSTGRES_PASSWORD** env var from your `.env` file
* **DISPLAY NAME:** Anything descriptive (recommended: **dwh_db**)

![Select the PostgreSQL database driver](/assets/imgs/superset/connecting_a_db_step_1_of_3.png){ style="width:49%" }
![Enter your credentials for the data warehouse database](/assets/imgs/superset/connecting_a_db_step_2_of_3.png){ style="width:49%" }

If credentials and configs were entered correctly (and the database container is running), superset will successfully connect to the database and you can save the connection by clicking **FINISH**.

<figure markdown>
  ![Select the PostgreSQL database driver](/assets/imgs/superset/connecting_a_db_step_3_of_3.png)
</figure>