# Standard Data Update Pipeline

At a high level, ADWH raw data update pipelines follow a basic pattern. If the data source has new data, collect that data and update the corresponding table(s) in the local warehouse.  

``` mermaid
flowchart TB
    start([Start Data Update DAG])
    start --> check_source
    subgraph Freshness Check
        direction TB
        check_source[Check Souce]
        check_source --> is_update_needed{Is Warehouse\nData Stale?}
    end
    is_update_needed -- No --> record_results
    is_update_needed -- Yes --> download_data
    subgraph Update Raw Data in Warehouse
        direction TB
        download_data[Collect New Data] --> validate_temp{Data meets\n Expectations}
        validate_temp -- Yes --> insert_records_not_in_persistant[Insert New and\nUpdated Records to\nPersistant Table]

    end
    validate_temp -- No --> record_results
    insert_records_not_in_persistant --> record_results
    record_results[Record Metadata] --> end_dag([End Data Update DAG])
```
