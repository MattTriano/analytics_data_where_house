# Setup Overview

This platform was designed to be easy to setup while still being secure enough that the code can be open source without compromising keys and credentials.

To use the system, you will need to have [docker](https://docs.docker.com/get-docker/) installed on your host machine, and setup will be much easier if your system also has python (for running the script to set up credentials) and **GNU make** (for running recipes that set up infrastructure).

Assuming these are installed on your machine, you should be able to set up the system in around 10 minutes and about as many commands by following the instrctions on these pages. you can set up the system by following the 

1. [Set up your credentials and build the images](/setup/getting_started),
2. [Configure database connections for pgAdmin4](/setup/pgAdmin4), and
3. [Configure database connections for Superset](/setup/superset_setup)