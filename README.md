
## Usage

### Starting up the system

#### Set up credentials
After cloning this repo and `cd`ing into your local, run this command

```bash
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

to create, then add the lines below to that file and replace the fake parameter values with real ones.

```
POSTGRES_USER=replace_me_with_airflow_metadata_db_username
POSTGRES_PASSWORD=replace_me_with_airflow_metadata_db_password
POSTGRES_DB=replace_me_with_airflow_metadata_db_name
PGADMIN_DEFAULT_EMAIL=yours@email.com
PGADMIN_DEFAULT_PASSWORD=replace_me_pgAdmin_pw
DBT_USER=replace_me_dbt_username
DBT_PASSWORD=replace_me_dbt_pw

# fill in the actual values 
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@<name_of_db_service_in_compose-yml>/{POSTGRES_DB}
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@<name_of_db_service_in_compose-yml>/{POSTGRES_DB}
_AIRFLOW_WWW_USER_USERNAME=replace_me_with_a_username_for_Airflow_WebUI
_AIRFLOW_WWW_USER_PASSWORD=replace_me_with_password_for_Airflow_WebUI
AIRFLOW__CORE__FERNET_KEY=replace_me_with_a_frenet_key_you_generate_with_the_snippet_below

DWH_POSTGRES_USER=replace_me_with_data_warehouse_username
DWH_POSTGRES_PASSWORD=replace_me_with_data_warehouse_password
DWH_POSTGRES_DB=replace_me_with_data_warehouse_db_name
```

And we'll also want to create a `.env` file for a separate database to actually use as our data warehouse, which we'll name `.dwh.env` and give it the env-vars:

```bash
POSTGRES_USER=DWH_POSTGRES_USER (from the .env file above)
POSTGRES_PASSWORD=DWH_POSTGRES_USER (from the .env file above)
POSTGRES_DB=DWH_POSTGRES_DB (from the .env file above)
```

And make a file named `profiles.yml`

```yml
cc_where_house:
  target: dev
  outputs:
    dev:
      type: postgres
      host: dwh_db  # or whatever name you gave the warehouse-database-service in the docker-compose file
      user: user: "{{ env_var('DWH_POSTGRES_USER') }}"
      password: "{{ env_var('DWH_POSTGRES_PASSWORD') }}"
      port: 5432 # or whatever port your database is listening to
      dbname: "{{ env_var('DWH_POSTGRES_DB') }}"
      schema: data_raw
      threads: 4
```

Note: if `dbt debug` indicates your `profiles.yml` file was found and is valid but failed to connect to the database, you'll have to at least cycle `docker-compose down` and `docker-compose up` if you change the `profiles.yml` file (and you might also have to `docker-compose up --build`).

##### Generating a Frenet Key to use as env var AIRFLOW__CORE__FERNET_KEY
```python
from cryptography.fernet import Fernet

fernet_key = Fernet.generate_key()
print(fernet_key.decode()) # your fernet_key
```


#### Spinning up the system

Build the docker images used by your docker-compose application and then run the `airflow-init` service to run an initial database migration that creates tables Airflow needs to run, and to create the system's first Airflow user account (which will use the `_AIRFLOW_WWW_USER_USERNAME` and `_AIRFLOW_WWW_USER_PASSWORD` values you set in the `.env` file as the username and user-password; if you didn't set those, the username and password will both be "airflow"). 

```bash
user@host:.../your_local_repo$ docker-compose build
user@host:.../your_local_repo$ docker-compose up airflow-init
```

With images built and `airflow-init`ialized, you can start up your docker-compose app via

```bash
user@host:.../your_local_repo$ docker-compose up
```

And to shut it down, at a terminal in the repo directory (press `ctrl+c` if you want to use the terminal serving the app)

```bash
user@host:.../your_local_repo$ docker-compose down
```

Any time a package is added to a requirements.txt file or any Dockerfile changes at all, images will have to be rebuilt before you'll be able to see the effect. Do this via 

```bash
user@host:.../your_local_repo$ docker-compose build
```

Or if you want to build and start it up in one command

```bash
user@host:.../your_local_repo$ docker-compose up --build
```

If something goes wrong during initialization or something's wrong with the build and deleting the database isn't an issue (i.e. if you haven't put anything into prod yet and you've only been experimenting), run this command to tear down the docker-compose app (which will delete named volumns, which includes both the database and the admin database).

```bash
user@host:.../your_local_repo$ docker-compose down -v
```

#### Accessing the Airflow Web UI

When the airflow system has been initialized and is running, the web ui can be accessed at http://localhost:8080/, and you'll log in with username ${_AIRFLOW_WWW_USER_USERNAME} and password ${_AIRFLOW_WWW_USER_PASSWORD}.

### Setting up Airflow Connections to Data Sources

There are several method you can use to set up connections to data sources that the Airflow executor can use to extract, load, or transform data.

* Environment variables:
  * The naming convention is AIRFLOW_CONN_{CONN_ID}, (all uppercase; single underscores surrounding CONN). 
  * Example: The env-var name of AIRFLOW_CONN_MY_PROD_DB will have the connection_id `my_prod_db`.
  * The value for this env-var will have the form '<conn-type>://<login>:<password>@<host>:<port>/<schema>?param1=val1&param2=val2&...'

You can also create a connection through the admin interface of the web UI by logging in, going to **Admin > Connections** then click the **Plus** sign to add a new connection.
* Connection Id (conn_id): your choice; you'll use this when referencing this connection in DAGs,
* Connection Type: Postgres,
* Host: be the name of the `docker-compose.yml` service for your data warehouse database,
* Schema: this will be the name of the database as set in `.dwh.env` as `POSTGRES_DB`
    * Note: this "schema" field has nothing to do with postgres schemas; the word "schema" was foolishly overloaded with several different database-related meanings),
* Login: the `POSTGRES_USER` env-var set in `.dwh.env`,
* Password: the `POSTGRES_PASSWORD` env-var set in `.dwh.env`,
* Port: the internal port number that the data warehouse database is using (as defined in `docker-compose.yml`, most likly 5432)



[Further information on connections](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html).

### Developing DAGs

DAGs put or developed in the `/<repo>/airflow/dags/` directory will quickly be available through the web UI and can be manually triggered or run there.

At present, a local mount is created at `/<repo>/data_raw` (host-side) to `/opt/airflow/data_raw` (container-side).



#### Get a shell in the dbt-service container

Check `docker ps` for the name of the container for the dbt service (it should contain the name you gave that service in the `docker-compose.yml` file near the end of the name). Then use the command below to get an interactive shell inside that container where you can execute `dbt` commands.

```bash
user@host:.../your_local_repo$ docker exec -it <project_name>_dbt_proj_1 /bin/bash
```

#### Initialize your dbt project (if you don't already have an existing dbt project)

To initialize the dbt project (assuming you don't already have one, like this repo does), enter the command below at an interactive terminal inside the dbt container

```bash
root@bbcffc30e656:/usr/app# dbt init
```

and respond to the prompts.

### Specifying and installing dbt packages

Create a file named `packages.yml` in your dbt project directory and specify any packages you want to use in your project in the format shown below

```yml
packages:
  - package: dbt-labs/dbt_utils
    version: 0.9.2
```

Then, after specifying packages and versions to use, run this command to install packages.

```bash
root@bbcffc30e656:/usr/app# dbt deps
01:33:04  Running with dbt=1.3.0
01:33:05  Installing dbt-labs/dbt_utils
01:33:05    Installed from version 0.9.2
01:33:05    Up to date!
```

 









Notes:
* In the docker-compose.yml file from Airflow's docker quick start guide, the system uses the CeleryExecutor rather than the LocalExecutor. If you regularly need to run so many concurrent tasks that all allocated CPU cores are in use and waiting tasks must be queued until hardware is free, then the CeleryExecutor is a necessary complexity (as it allows you to split up execution over multiple servers). But if your workflows don't simultaneously consume all CPU cores, then the LocalExecutor is probably adequate.
  * If you switch to using the LocalExecutor, you can also remove the Redis bits from the docker-compose, as the Redis service is just the task queue.
  * You can also remove the `airflow-worker` and `flower` services, which are also only used for managing `celery`.


Notes- organization:
Airflow dynamically adds the `/plugins`, `/dags`, and `/config` folders to `PYTHONPATH` (per the documentation), and non-DAG code should be kept out of `/dags`, but `/plugins` isn't checked constantly for changes like `/dags` is, so you'll have to change some configs in `airflow.cfg` (ctrl+f for plugin and change the ones that seem like they should be changed; I think I changed `[core] lazy_load_plugins` to `False` and `[webserver] reload_on_plugin_change` to `True`)













