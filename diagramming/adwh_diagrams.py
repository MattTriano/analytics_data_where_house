from pathlib import Path
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
    "beautify": "True",
    "fontsize": "45",
    "bgcolor": "white",
    # "layout": "nop"
    # "bgcolor": "transparent"
}

with Diagram(name="Analytics Data Warehouse", show=False, filename="adwh_services", graph_attr=graph_attr, direction="LR", outformat="png"):
    with Cluster("Containerized Services"):
        docker = Docker()
        data_warehouse = Postgresql("Data Warehouse")
        superset = Superset("Dashboarding and EDA")
        socrata_data = Internet("Public Socrata Data")
        python_connectors = Python("Socrata Connector")
        # file_storage = Storage("Storage Volume")
        airflow = Airflow("Orchestration")
        dbt = Dbt("Data Cleaning,\n Transformation, and\nFeature Engineering")


    # with Cluster("Orchestration", direction="TB"):
    #     airflow_db = Postgresql("airflow_db", pos="-1,-1!")
    #     airflow_scheduler = Airflow("scheduler/executor", pos="-1,0!")
    #     airflow_webserver = Airflow("webserver\nport 8080", pos="-1,1!")

    #     airflow_webserver - airflow_scheduler - Edge(label="airflow_metadata") - airflow_db

    # airflow_scheduler - Edge(label="dwh") - data_warehouse

        great_expectations_url = "https://raw.githubusercontent.com/great-expectations/great_expectations/0c9e4502e7dfd4984a21ca4c8da0d0fcfff37aed/docs/docusaurus/static/img/gx-mark.png"
        great_expectations_icon_file = Path("great_expectations.png").resolve()
        if not great_expectations_icon_file.is_file():
            urlretrieve(great_expectations_url, great_expectations_icon_file)

        pgadmin4_url = "https://raw.githubusercontent.com/pgadmin-org/pgadmin4/4bcf0637f936639890f62d9f00de383e07b4d6be/web/pgadmin/static/img/logo-256.png"
        pgadmin4_icon_file = Path("pgadmin4.png").resolve()
        if not pgadmin4_icon_file.is_file():
            urlretrieve(pgadmin4_url, pgadmin4_icon_file)

        postgis_url = "https://postgis.net/docs/manual-dev/images/PostGIS_logo.png"
        postgis_icon_file = Path("postgis.png").resolve()
        if not postgis_icon_file.is_file():
            urlretrieve(postgis_url, postgis_icon_file)

        great_expectations = Custom("Data Validation", str(great_expectations_icon_file))
        pgadmin4 = Custom("DB Administration", str(pgadmin4_icon_file))
    
    
        
        data_warehouse >> Edge(forward=True, reverse=True) >> dbt
        data_warehouse >> Edge(forward=True, reverse=True) >> superset
        data_warehouse >> Edge(forward=True, reverse=True) >> pgadmin4
        socrata_data << python_connectors << airflow
        socrata_data >> python_connectors >> great_expectations >> data_warehouse
    


