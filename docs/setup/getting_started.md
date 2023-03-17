# System Setup

Preprequisites:
To use this system, Docker is the only absolutely necessary prerequisite.

Having `GNU make` and core python on your host system will enable you to use included `makefile` recipes and scripts to streamline setup and common operations, but you could get by without them (although you'll have to figure more out).

## Setting up credentials
After cloning this project, `cd` into your local repo, run this `make` command and enter appropriate responses to the prompts. You may want to have a password generator open.

```bash
make make_credentials
```

The program will validate the values you enter, assemble them into some compound values (mostly connection strings), and output these configs into the dot-env files (`.env`, `.env.dwh`, and `.env.superset`) in the top-level repo directory. Review these files and make any changes before you initialize the system (i.e., when these username and password pairs are used to create roles in databases or Airflow starts using the Fernet key to encrypt passwords in connection strings).

### [Optional] Mapbox API Token for Geospatial Superset Maps

To include a basemap underneath geospatial visualizations in Superset, you'll need to:

1. [Create a free Mapbox account](https://account.mapbox.com/auth/signup),
2. Create a new API access token on your [Mapbox account page](https://account.mapbox.com/), and
3. Open your `.env.superset` dot-env file and paste that API access token in for the `MAPBOX_API_KEY` environment variable so that it looks like the example below.

    `MAPBOX_API_KEY=pk.<the rest of the API token you created>`

You can still make geospatial Superset charts without an API key, but your geospatial charts won't have a basemap (like the left example).

![Without Mapbox API key](/assets/imgs/superset/deckgl_polygon_chart_demo_no_basemap.png){ style="width:46.25%" }
![With Mapbox API key](/assets/imgs/superset/deckgl_polygon_chart_demo.png){ style="width:51%" }

## Initializing the system

On the first startup of the system (and after setting your credentials), run the commands below to
1. build the platform's docker images, and initialize the airflow metadata database,
2. start up the system, and after console outputs stabilize (i.e., when services are running)
3. create the `metadata` and `data_raw` schemas and the `metadata.table_metadata` table in your data warehouse database.

```bash
make initialize_system
docker compose up
```

and in another terminal (after `cd`ing back into the project dir)

```bash
make create_warehouse_infra
```

Now the core of the system is up and running, and you can proceed to the next section to set up database connections that will enable you to explore the data and databases through the pgAdmin4 and Superset interfaces.
