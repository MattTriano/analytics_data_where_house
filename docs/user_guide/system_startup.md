# ADWH System Startup

In a terminal, navigate to the project's root directory (which contains the `docker-compose.yml` file and dot-env files) and start up the system.

```bash
docker compose up
```

System services will begin spinning up and you will see a significant amount of console output. All services should be up and running after 20 to 40 seconds and the output will slow to a crawl.

Now you can access Airflow and Superset (and any other services with a web UI) in a browser.

Access Airflow at [http://localhost:8080](http://localhost:8080)
* Username: _AIRFLOW_WWW_USER_USERNAME value (from your `.env` file)
* Password: _AIRFLOW_WWW_USER_PASSWORD value (from your `.env` file)

Access Superset at [http://localhost:8088](http://localhost:8088)
* Username: ADMIN_USERNAME value (from your `.env.superset` file)
* Password: ADMIN_PASSWORD value (from your `.env.superset` file)