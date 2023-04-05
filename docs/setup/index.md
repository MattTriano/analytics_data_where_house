# Setup Overview

This platform is easy to setup while still being secure enough to open source the code without exposing keys or credentials.

## System Requirements

* [Docker](https://docs.docker.com/get-docker/) with `Compose` v2.0 or higher.
* a basic system install of python (for running the script to set up credentials).
* Optional (but recommended): **GNU make** (for running recipes that set up infrastructure).

Note: keep an eye on system storage. This platform automates collection and does not automatically purge old data files.

## Setup Steps

Work through these steps to set up all ADWH credentials and services. It should take around 10 minutes.

1. [Set up your credentials and build the images](/setup/getting_started)
2. [Configure database connections for Superset](/setup/superset_setup)
3. [Configure database connections for pgAdmin4](/setup/pgAdmin4)