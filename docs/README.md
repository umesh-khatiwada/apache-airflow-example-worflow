# Documentation — table of contents

Read in order if you're new to Airflow. Every chapter from 05 onwards has a DAG in
[`../dags`](../dags) that you can run in the UI while you read.

## Part 1 — Foundations
1. [What is Apache Airflow?](01-what-is-airflow.md)
2. [Architecture: how the pieces fit together](02-architecture.md)
3. [Installation & project setup](03-installation-and-setup.md)
4. [Core concepts in one page](04-core-concepts.md)

## Part 2 — Writing DAGs
5. [Your first DAG](05-first-dag.md) · `01_hello_airflow`
6. [The TaskFlow API](06-taskflow-api.md) · `02_taskflow_api`
7. [Operators & dependencies](07-operators-and-dependencies.md) · `03_operators_and_dependencies`
8. [XCom: passing data between tasks](08-xcom.md) · `04_xcom`
9. [Branching & trigger rules](09-branching-and-trigger-rules.md) · `05_branching_and_trigger_rules`
10. [Dynamic task mapping](10-dynamic-task-mapping.md) · `06_dynamic_task_mapping`
11. [Task groups](11-task-groups.md) · `07_task_groups`

## Part 3 — Waiting, events & configuration
12. [Sensors & deferrable operators](12-sensors-and-deferrable.md) · `08_sensors_and_deferrable`
13. [Assets & data-aware scheduling](13-assets-data-aware-scheduling.md) · `09_assets_*`
14. [Params, Variables & Jinja templating](14-params-variables-templating.md) · `10_params_variables_templating`
15. [Connections & hooks](15-connections-and-hooks.md) · `11_connections_and_hooks`

## Part 4 — Reliability & extending Airflow
16. [Error handling, retries & alerts](16-error-handling-retries-alerts.md) · `12_error_handling_and_alerts`
17. [Custom operators, hooks, sensors & plugins](17-custom-operators-hooks-plugins.md) · `13_custom_operators_hooks_sensors`
18. [Scheduling & timetables](18-scheduling-and-timetables.md) · `14_schedule_*`
19. [Pools, priority & concurrency](19-pools-priority-concurrency.md) · `15_pools_priority_concurrency`
20. [Setup & teardown tasks](20-setup-teardown.md) · `16_setup_teardown`
21. [Dynamic DAG generation](21-dynamic-dag-generation.md) · `17_generated_ingest_*`

## Part 5 — Advanced features
22. [Human-in-the-loop](22-human-in-the-loop.md) · `18_human_in_the_loop`
23. [Task loops (new in 3.4)](23-task-loops.md) · `19_task_loop_*`
24. [Capstone: production-style sales ETL](24-capstone-sales-etl.md) · `20_capstone_sales_etl`, `20_capstone_sales_report`

## Part 6 — Working with Airflow day to day
25. [A tour of the UI](25-ui-tour.md)
26. [CLI & REST API](26-cli-and-rest-api.md)
27. [Testing DAGs](27-testing-dags.md)
28. [Best practices](28-best-practices.md)
29. [Executors & production deployment](29-executors-and-production.md)
30. [Troubleshooting & FAQ](30-troubleshooting-faq.md)
31. [Airflow 2 vs Airflow 3](31-airflow-2-vs-3.md)
32. [Glossary](32-glossary.md)
33. [Exercises & mini-projects](33-exercises.md)
34. [Deploy to DigitalOcean with SkyPilot](34-deploy-digitalocean-skypilot.md)

← [Back to the project README](../README.md)
