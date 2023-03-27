from urllib.request import urlretrieve

from diagrams import Cluster, Diagram, Edge
from diagrams.custom import Custom
from diagrams.generic.storage import Storage
from diagrams.programming.language import Python
from diagrams.onprem.analytics import Superset, Dbt
from diagrams.onprem.container import Docker
from diagrams.onprem.database import Postgresql
from diagrams.onprem.network import Internet
from diagrams.onprem.workflow import Airflow


graph_attr = {
    "fontsize": "45",
    "bgcolor": "white"
    # "bgcolor": "transparent"
}

with Diagram(name="ADWH", show=False, filename="adwh_services", graph_attr=graph_attr):
    data_warehouse = Postgresql("data warehouse")
    superset = Superset("Dashboarding and EDA")
    socrata_data = Internet("public Socrata data")
    python_connectors = Python("Socrata Connector")


    with Cluster("Orchestration", direction="LR"):
        airflow_db = Postgresql("airflow_db")
        airflow_scheduler = Airflow("scheduler/executor")
        airflow_webserver = Airflow("webserver\nport 8080")

        airflow_webserver - airflow_scheduler - Edge(label="airflow_metadata") - airflow_db

    airflow_scheduler - Edge(label="dwh") - data_warehouse

    # great_expectations_url = "https://raw.githubusercontent.com/great-expectations/great_expectations/0c9e4502e7dfd4984a21ca4c8da0d0fcfff37aed/docs/docusaurus/static/img/gx-mark.png"
    great_expectations_icon = "great_expectations.png"
    # urlretrieve(great_expectations_url, great_expectations_icon)

    pgadmin4_url = "https://raw.githubusercontent.com/pgadmin-org/pgadmin4/4bcf0637f936639890f62d9f00de383e07b4d6be/web/pgadmin/static/img/logo-256.png"
    pgadmin4_icon = "pgadmin4_icon.png"
    urlretrieve(pgadmin4_url, pgadmin4_icon)

    great_expectations = Custom("great_expectations", great_expectations_icon)
    pgadmin4 = Custom("pgAdmin4", pgadmin4_icon)

    data_warehouse >> Dbt("transform") >> data_warehouse
    data_warehouse - superset
    data_warehouse - pgadmin4

    airflow_scheduler >> python_connectors >> socrata_data \
    >> python_connectors >> great_expectations >> airflow_scheduler
    


