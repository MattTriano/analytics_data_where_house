# Making Visualizations

Create (via the Superset SQL Lab) or select a data set and select a chart type to visualize that data set.

![Building a Data Set via a Query](/assets/imgs/superset/SQL_lab_data_set_query_dev.png)

![Select a chart type](/assets/imgs/superset/superset_chart_type_selection.png)

Select dimensions to group data by and metrics to visualize for those groupings.

Metrics can be basic aggregations like `MIN`, `MAX`, `COUNT`, `COUNT_DISTINCT`, `AVG`, or `SUM`, or you can define custom aggregations from one or more column.

![Setting the metric to visualize](/assets/imgs/superset/chart_dev_setting_the_metric.png)

Filter data to a meaningful subset.

![Filter values in viz](/assets/imgs/superset/chart_dev_filter_data_being_visualized.png)

Format titles, labels, tooltips, timespan slicing, etc.

![Annotate axes and name the table](/assets/imgs/superset/chart_dev_annotate_axes.png)

And when the chart is finished, save the visualization (via the save button in the upper right corner). Now the chart is ready to to add to a dashboard.

![Time Series Analysis](/assets/imgs/superset/median_quarterly_sale_price_by_Chicago_neighborhood.png)

## Resources

Explore [this tutorial](https://superset.apache.org/docs/creating-charts-dashboards/exploring-data) for more on visualizing data in Superset. 
**Preset** also provides [walkthroughs](https://docs.preset.io/docs/creating-a-chart) for many other chart types available in Superset. 