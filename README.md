
## Usage

### Starting up the system

#### Set up credentials
After cloning this repo and `cd`ing into your local, create a file named `.env` and replace the fake parameter values with real ones.

```
POSTGRES_USER=replaceMe_pg_db_admin_username
POSTGRES_PASSWORD=replaceMe_pg_pw
POSTGRES_DB=replaceMe_pg_db_name
PGADMIN_DEFAULT_EMAIL=yours@email.com
PGADMIN_DEFAULT_PASSWORD=replaceMe_pgAdmin_pw
DBT_USER=replaceMe_dbt_username
DBT_PASSWORD=replaceMe_dbt_pw
```

And make a file named `profiles.yml`

```yml
re_dbt_pg_raw:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: user: "{{ env_var('POSTGRES_USER') }}"
      password: "{{ env_var('POSTGRES_PASSWORD') }}"
      port: 5432 # or whatever port your database is listening to
      dbname: "{{ env_var('POSTGRES_DB') }}"
      schema: data_raw
      threads: 4
```

#### Spinning up the system

```bash
user@host:.../your_local_repo$ docker-compose up --build
```

### Get a shell in the dbt-service container

Check `docker ps` for the name of the container for the dbt service (it should contain the name you gave that service in the `docker-compose.yml` file near the end of the name). Then use the command below to get an interactive shell inside that container where you can execute `dbt` commands.

```bash
user@host:.../your_local_repo$ docker exec -it <project_name>_dbt_pg_1 /bin/bash
```

### Initialize your dbt project (if you don't already have an existing dbt project)

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

 









COPY profiles.yml /root/.dbt/profiles.yml

























