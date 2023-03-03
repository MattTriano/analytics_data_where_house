## System Setup

Preprequisites:
To use this system, Docker is the only absolutely necessary prerequisite.

Having `GNU make` and core python on your host system will enable you to use included `makefile` recipes and scripts to streamline setup and common operations, but you could get by without them (although you'll have to figure more out).

### Setting up credentials
After cloning this project, `cd` into your local repo, run this `make` command and enter appropriate responses to the prompts. You may want to have a password generator open.

```bash
user@host:.../your_local_repo$ make make_credentials
```

The program will validate the values you enter, assemble them into some compound values (mostly connection strings), and output these configs into the dot-env files (`.env` and `.env.dwh`) in the top-level repo directory. Review these files and make any changes before you initialize the system (i.e., when these username and password pairs are used to create roles in databases or Airflow starts using the Fernet key to encrypt passwords in connection strings).

### Initializing the system

On the first startup of the system (and after setting your credentials), run the commands below to
1. build the platform's docker images, and initialize the airflow metadata database,
2. start up the system in detached mode (so that you don't have to open another terminal), and
3. create the `metadata` and `data_raw` schemas and the `metadata.table_metadata` table in your data warehouse database.

```bash
user@host:.../your_local_repo$ make initialize_system
user@host:.../your_local_repo$ make create_warehouse_infra
```

These commands only need to be run on first startup (although you will need to run `make build_images` to rebuild images if you make any changes to any of the `Dockerfile`s or add/remove packages from a `requirements.txt` file).

### Starting up the system

Run this command

```bash
user@host:.../your_local_repo$ docker compose up
```

After a brief time, system services will be up and running, and you can either [set up pgAdmin4](/setup/pgAdmin4)