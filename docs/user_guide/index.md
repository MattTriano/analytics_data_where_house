# Using the system

The workflow for producing usable tables follows the pattern

1. Set up an ingestion pipeline
    1.1. Extract data to a local file,
    1.2. Load that data into a "temp" table,
    1.3. Select distinct records that aren't already in the warehouse and add them to the warehouse,
    1.4. Define a suite of expectations to evaluate future data updates
2. Implement a dbt script to transformations 

For tables hosted by Socrata, this system reduces steps 1.1 through 1.3 to a [3 minute operation](/user_guide/adding_a_socrata_pipeline)