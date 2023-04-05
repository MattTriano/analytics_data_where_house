# Running a Pipeline

To run a data ELT pipeline:

1. Unpause the DAG for that pipeline.

    Note: Unpausing a scheduled DAG for the first time will trigger a run. To manually trigger a run, click the :material-play: icon in the **Actions** column or near the upper righthand corner in subsequent DAG views.

    ![Unpause a DAG](/assets/imgs/airflow/01_initial_dag_view__unpausing_a_DAG.png)

2. Click that DAG's name to enter its grid view.

    ![Grid View](/assets/imgs/airflow/02_DAG_initial_run_after_unpause_grid_view.png)

3. Click **Graph** to enter the DAG's graph view.

    ![High level DAG graph view](/assets/imgs/airflow/03_DAG_initial_run_graph_view_high_level.png)

4. If all tasks end successfully, the **Status** indicator will switch from **running** (light green) to **success** (dark green).

    ![a DAG after a successful run](/assets/imgs/airflow/04_DAG_run_graph_view_after_all_tasks_finish_successfully.png)


Now that you have some data in your warehouse, you can explore that data in [Superset](visualization/index.md).


## Troubleshooting

Occassionally a task will fail. To investigate

1. Click on the failed DAG run (will be red in the bar chart on the left) and click the **Graph** button in that DAG run's details.

    ![Select a failed DAG run and enter its graph view](/assets/imgs/airflow/F01_DAG_failed_run_grid_view.png)

2. Identify and click the task that failed (it will have a red outline and the tooltip will show **Status: failed**).

    ![Click the failed task](/assets/imgs/airflow/F02_DAG_failed_run_graph_view_failed_task.png)

3. Click the **Log** button in the failed task instance's detail pop-up.

    ![Enter the Log view](/assets/imgs/airflow/F03_DAG_failed_run_graph_view_log_button.png)

4. Review the output in the logs to see the error output. You'll probably have to debug the issue or raise the issue upstream before the DAG will run successfully.
    
    ![Unpause a DAG](/assets/imgs/airflow/F04_DAG_failed_run_logs_view_showing_error.png) 

## Resources

See the [Airflow UI documentation](https://airflow.apache.org/docs/apache-airflow/stable/ui.html) for more information about available views and interfaces.