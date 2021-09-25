# OLAP vs OLTP
- OLAP (Online analytical Processing): used for business analysis.
    - Ex: finanical analysis, sale analysis, RECOMMENDATION engine
- OLTP: used for data processing
    - Ex: adding book to the shopping card

# Data warehouse:
- Provide a consolidated view of data from a variety of exisiting systems.
- Collecting the data from multiple systems to use for fututre analysis or in decision amking processes.

### BigQuery
**Def**: a cloud hosted analytics data warehouse 
**Notes**:
    - No indices
    - Does full table scans for every query => 1000s machines scan the table at the same time => FAST


# Flow:
PostgreSQL -> Google Cloud Storage -> BigQuery <br>
The flow is automated by Airflow
# PostgreSQL
### Database dump
**Def**: A database dump contains record of the table structure and/or the data from a database. It is usually in formed of SQL statements.
**Usecase**: it helps to backup or duplicate a database

In Postgre, we use the SQL Dump which generates a text file with SQL commands that when fed back to the server will recreate the database in the same state as it was at the time of dump.

# Airflow:
### Components:
- Web Server: Node A
- Scheduler: Node A
- Metadata Database: Node B
- Executor: Node C
Multinode architectures, different components lie in different nodes.
### States
**DAGS**
- Running
- Success
- Failed


# Bigquery
## Datasets
**Def**: a top-level container which is used to organized and control access to tables and views. It is required to create **at least one** dataset before **loading data into BigQuery**.

## Views
**Def**: A view is a *virtual* table. Querying the view is similar to querying the table. View is read-only.
**Usecase**: It stores the results from a particular query. 


# Research:
- Airflow Operator(s)
- Table partitioning Bigquery
- Views


