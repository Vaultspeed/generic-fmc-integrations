# Prerequisites
1. In VaultSpeed, set up a Data Vault with FMC_TYPE = Generic and ETL Generation type = Snowflake dbt and make sure you have all the FMC flows created
2. Python (v3.8+) - https://www.python.org/downloads/
3. Install Python dependencies
```bash
pip install -U prefect
pip install -U Jinja2
# If connecting to Snowflake (otherwise install your connector)
pip install -U snowflake-connector-python
```
4. Start your local Prefect server

<br/>

# Usage
*Note: Currently just Snowflake execution is supported, but it can be easily extended by providing your own run function*

1. Pull your generic FMC `.json` file from the FMC. 
2. (Optional) Edit the `sql_executor()` function if you need to customize your SQL execution or need a different connector
3. (Optional) Start your prefect server for local testing, otherwise use your CI/CD pipelines
```bash
prefect server start
```
4. Run the `prefect_fmc.py` script
```bash
python prefect_fmc.py
```
5. Observe and monitor results in Prefect UI