from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator


default_args = {
    'owner': 'Anthony Criollo'
    'start_date': days_ago(7)
}

dag_arg = {
    'dag_id': '2_simple_storage_bigquery',
    'schedule_interval': '@daily',
    'catchup': False,
    'default_arg': default_args
}


with DAG(**dag_args) as dag:
    #
    cargar_datos = GCSToBigQueryOperator(
        task_id='cargar__datos',
        bucket='original-bucked-987',
        source_objects=['*'],
        source_format='CSV',
        skip_leading_rows=1,
        field_delimiter=';',
        destination_project_datset_table='regal-oasis-291423.working_dataset.retail_years',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_APPEND',
        bigquery_conn_id='google_cloud_default',
        google_cloud_storage_conn_id='google_cloud_default'
    )

    query = (
        '''
        SELECT `year`, `area`, ROUND(AVG(`total_inc`), 4) AS avg_income
        FROM `regal-oasis-291423.working_dataset.retail_years`
        GROUP BY `year`, `area`
        ORDER BY `area` ASC
        '''
    )

    tabla_resumen = BigQueryExecuteQueryOperator(
        task_id='tabla_resumen',
        sql=query,
        destination_dataset_table='regal-oasis-291423.working_dataset.retail_years_resume',
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        location='us-east1',
        bigquery_conn_id='google_cloud_default'
    )


# DEPENDENCIAS
cargar_datos >> tabla_resumen