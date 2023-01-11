## System Setup

Preprequisites:
To use this system, Docker is the only absolutely necessary prerequisite.

Having `GNU make` and core python on your host system will enable you to use included `makefile` recipes and scripts to streamline setup and common operations, but you could get by without them (although you'll have to figure more out).

### Setting up credentials
After cloning this project, `cd` into your local repo, run this `make` command and enter appropriate responses to the prompts. You may want to have a password generator open.

```bash
make make_credentials
```

The program will validate the values you enter, assemble them into some compound values (mostly connection strings), and output these configs into the dot-env files (`.env` and `.dwh.env`) in the top-level repo directory. Review these files and make any changes before you initialize the system (i.e., when these username and password pairs are used to create roles in databases or Airflow starts using the Fernet key to encrypt passwords in connection strings).

### Initializing the system

On the first startup of the system (and after setting your credentials), run the commands below to
1. build the platform's docker images, and initialize the airflow metadata database,
2. start up the system in detached mode (so that you don't have to open another terminal), and
3. create the `metadata` and `data_raw` schemas and the `metadata.table_metadata` table in your data warehouse database.

```bash
user@host:.../your_local_repo$ make initialize_system
user@host:.../your_local_repo$ make quiet_startup
user@host:.../your_local_repo$ make create_warehouse_infra
```

These commands only need to be run on first startup (although you will need to run `make build_images` to rebuild images if you make any changes to any of the `Dockerfile`s or add/remove packages from a `requirements.txt` file).

### Starting up the system

Run this command (which is just an alias for `docker-compose up`) to startup the platform

```bash
user@host:.../your_local_repo$ make startup
```

After systems have started up, you can access:

#### The pgAdmin4 database administration UI
Access the pgAdmin4 database administration UI at [http://localhost:5678](http://localhost:5678)
* Log in using the `PGADMIN_DEFAULT_EMAIL` and `PGADMIN_DEFAULT_PASSWORD` credentials from your `.env` file.

![pgAdmin4 Web UI Login](/assets/imgs/systems/pgAdmin_web_interface_login_view.png)
![pgAdmin4 UI landing page](/assets/imgs/systems/pgAdmin_langing_page_view.png)

#### The Airflow web UI 
Access the Airflow webserver user interface at [http://localhost:8080](http://localhost:8080)

* Log in using the `_AIRFLOW_WWW_USER_USERNAME` and `_AIRFLOW_WWW_USER_PASSWORD` credentials from your `.env` file.

![Airflow Webserver UI Login](/assets/imgs/systems/Airflow_webserver_UI_login_view.png)

#### The dbt Data Documentation and Discovery UI 
Run the command below to generate and serve documentation for the data transformations executed by dbt. After the doc server has started up, go to [http://localhost:18080](http://localhost:18080) to explore the documentation UI.

```bash
user@host:.../your_local_repo$ make serve_dbt_docs
```

![dbt Data Documentation Interface](/assets/imgs/systems/dbt_data_docs_interface_showing_parcel_sales.png)

![dbt Data Lineage Graph](/assets/imgs/systems/dbt_lineage_graph_of_parcel_sales.png)