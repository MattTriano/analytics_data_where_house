from cc_utils.socrata import SocrataTable

CHICAGO_HOMICIDES_AND_SHOOTING_VICTIMIZATIONS = SocrataTable(
    table_id="gumc-mgzr",
    table_name="chicago_homicide_and_shooting_victimizations",
    schedule="0 7,19 * * *",
)

CHICAGO_SHOTSPOTTER_ALERTS = SocrataTable(
    table_id="3h7q-7mdb", table_name="chicago_shotspotter_alerts", schedule="0 7,19 * * *"
)
