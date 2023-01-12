After systems have started up, you can access:

#### The pgAdmin4 database administration UI
Access the pgAdmin4 database administration UI at [http://localhost:5678](http://localhost:5678)
* Log in using the `PGADMIN_DEFAULT_EMAIL` and `PGADMIN_DEFAULT_PASSWORD` credentials from your `.env` file.

![pgAdmin4 Web UI Login](/assets/imgs/systems/pgAdmin_web_interface_login_view.png)

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