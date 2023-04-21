# Great Expectations Workflow

## Starting up the Jupyter Server

Run the `make` command 

```$ make serve_great_expectations_jupyterlab```

to start up the jupyter lab server where you can create and edit expectations and checkpoints. You will see output like what is shown below. 

```bash
$ make serve_great_expectations_jupyterlab 
docker compose exec airflow-scheduler /bin/bash -c \
        "mkdir -p /opt/airflow/.jupyter/share/jupyter/runtime &&\
        cd /opt/airflow/great_expectations/ &&\
        jupyter lab --ip 0.0.0.0 --port 18888"
[I 2023-04-20 00:53:29.039 ServerApp] Package jupyterlab took 0.0000s to import
...
[I 2023-04-20 00:53:29.550 ServerApp] Use Control-C to stop this server and shut down all kernels (twice to skip confirmation).
[W 2023-04-20 00:53:29.557 ServerApp] No web browser found: Error('could not locate runnable browser').
[C 2023-04-20 00:53:29.557 ServerApp] 
    
    To access the server, open this file in a browser:
        file:///opt/airflow/.jupyter/share/jupyter/runtime/jpserver-437-open.html
    Or copy and paste one of these URLs:
        http://33972abe32d4:18888/lab?token=be72207c3a182bed5c026af4c3014765250e98e0f8994d9c
        http://127.0.0.1:18888/lab?token=be72207c3a182bed5c026af4c3014765250e98e0f8994d9c
```

Copy the URL starting with `http://127.0.0.1:18888/...` and paste it into a browser.

!!! note

    To copy the URL, highlight it, right click it, and select **Copy**. Don't press ctrl+c as that will shut down the jupyter server.

This will bring you to a jupyterlab interface. Open a terminal (optional: enter `bash` if you want tab-completion and command history), 

## Useful Great Expectations Commands

```bash
default@container_id:/opt/airflow/great_expectations$ great_expectations suite list
```

### Editing a suite

Enter this command to generate a notebook that will help edit expectations. Use the `-nj` flag to prevent `great_expectations` from starting up another jupyter server (you can just access the newly created notebook in the `/expectations` directory).

```bash
default@cfade96635e4:/opt/airflow/great_expectations$ great_expectations suite edit data_raw.temp_chicago_homicide_and_shooting_victimizations.warning -nj
```

In the suite editor notebook, each expectation in the suite will have its own cell that you can run, inspect, and edit. At the end of the notebook, expectations in the `validator` object will be written to file.

#### Removing an expectation

While reviewing expectations, you may find an expectation you want to just remove. You can delete such expectations via the `validator` object's `.remove_expectation()` method, but you have to pass in the expectation that is to be removed from the suite. You can access the expectation by accessing the `.expectation_config` attr of the `ExpectationValidationResult`.


```python
expectation_validation_result = validator.expect_column_values_to_match_regex(
    column='victimization_fbi_cd',
    mostly=1.0,
    regex='-?\d+',
    meta={
        'profiler_details': {
            'evaluated_regexes': {
                '(?:25[0-5]|2[0-4]\\d|[01]\\d{2}|\\d{1,2})(?:.(?:25[0-5]|2[0-4]\\d|[01]\\d{2}|\\d{1,2})){3}': 0.0,
                '-?\\d+': 1.0, '-?\\d+(?:\\.\\d*)?': 1.0, '<\\/?(?:p|a|b|img)(?: \\/)?>': 0.0,
                '[A-Za-z0-9\\.,;:!?()\\"\'%\\-]+': 1.0,
                '\\b[0-9a-fA-F]{8}\\b-[0-9a-fA-F]{4}-[0-5][0-9a-fA-F]{3}-[089ab][0-9a-fA-F]{3}-\\b[0-9a-fA-F]{12}\\b ': 0.0,
                '\\d+': 1.0,
                '\\s+$': 0.0,
                '^\\s+': 0.0,
                'https?:\\/\\/(?:www\\.)?[-a-zA-Z0-9@:%._\\+~#=]{2,255}\\.[a-z]{2,6}\\b(?:[-a-zA-Z0-9@:%_\\+.~#()?&//=]*)': 0.0
            },
            'success_ratio': 1.0
        }
    }
)
```

To confirm that you've removed that expectation, check the length of the expectation suite (via the command below) before and after removing the expectation.

```python
print(f"Number of expectations pre-removal: {len(validator.expectation_suite.expectations)}")
validator.remove_expectation(expectation_validation_result.expectation_config)
print(f"Number of expectations after-removal: {len(validator.expectation_suite.expectations)}")
```