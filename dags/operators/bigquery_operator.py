from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery

class BigQueryOperator(BaseOperator):
    def __init__(self, data, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data = data

    def execute(self, context):
        hook = BigQueryHook(bigquery_conn_id='bigquery_conn')
        client = hook.get_client()

        schema = [
            bigquery.SchemaField("column1", "STRING"),
            bigquery.SchemaField("column2", "INTEGER"),
        ]
        project_id = 'spotify-dwh'
        dataset_id = 'test'
        table_id = 'test_table'

        table = bigquery.Table(f"{project_id}.{dataset_id}.{table_id}", schema=schema)

        try:
            client.create_table(table, exists_ok=True)
            print("Table created or already exists.")
        except Exception as e:
            print(f"Error creating table: {str(e)}")
       
        result = client.insert_rows_json(f'{project_id}.{dataset_id}.{table_id}', self.data)

        if result:
            self.log.info('Data ingestion successful')
        else:
            self.log.error('Data ingestion failed')
