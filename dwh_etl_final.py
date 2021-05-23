from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

USERNAME = 'sperfilyev'

ODS_SOURCES = (
    {'source_name': 'billing', 'date_field': 'created_at'}, 
    {'source_name': 'issue',   'date_field': 'start_time'}, 
    {'source_name': 'payment', 'date_field': 'pay_date'}, 
    {'source_name': 'traffic', 'date_field': 'traffic_time'},
)

DDS_HUBS = (
    {'hub_name': 'user',            'etl_view': 'user_mdm'},
    {'hub_name': 'user',            'etl_view': 'user_payment'},
    {'hub_name': 'account',         'etl_view': 'account'},
    {'hub_name': 'billing_period',  'etl_view': 'billing_period'},
    {'hub_name': 'paysys',          'etl_view': 'paysys'},
    {'hub_name': 'service',         'etl_view': 'service_issue'},
    {'hub_name': 'service',         'etl_view': 'service_billing'},
    {'hub_name': 'tariff',          'etl_view': 'tariff'},
    {'hub_name': 'device',          'etl_view': 'device'},
)

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2013, 1, 1, 0, 0, 0)
}

dag = DAG(
    dag_id=USERNAME + '_dwh_etl_final',
    default_args=default_args,
    default_view='graph',
    description='DWH ETL final project by ' + USERNAME,
    schedule_interval="@yearly",
    max_active_runs=1,
    params={'schemaName': USERNAME},
)

## ОПИШЕМ ВСЕ ОПЕРАЦИИ ЗАГРУЗКИ ДАННЫХ

ods_reload = [
    PostgresOperator(
        task_id="ods_{0}_reload".format(ods_source), 
        dag=dag,
        params={'odsSource': ods_source['source_name'], 'dateField': ods_source['date_field']},
        sql="""
DELETE FROM {{ params.schemaName }}.ods_t_{{ params.odsSource }} CASCADE
WHERE extract(YEAR FROM {{ params.dateField }}) = {{ execution_date.year }};

DELETE FROM {{ params.schemaName }}.ods_t_{{ params.odsSource }}_hashed CASCADE
WHERE extract(YEAR FROM effective_from) = {{ execution_date.year }};

INSERT INTO {{ params.schemaName}}.ods_t_{{ params.odsSource }}
    (SELECT * 
     FROM {{ params.schemaName }}.stg_v_{{ params.odsSource }} 
     WHERE extract(YEAR FROM {{ params.dateField }}) = {{ execution_date.year }});

INSERT INTO {{ params.schemaName}}.ods_t_{{ params.odsSource }}_hashed
    (SELECT v.*,
            '{{ execution_date }}'::DATE AS load_dts
     FROM {{ params.schemaName }}.ods_v_{{ params.odsSource }}_etl AS v
     WHERE extract(YEAR FROM v.effective_from) = {{ execution_date.year }});"""
    ) for ods_source in ODS_SOURCES
]

dds_hubs_fill = [
    PostgresOperator(
        task_id="hub_{0}_fill".format(hub_name),
        dag=dag,
        sql="""
INSERT INTO {{{{ params.schemaName }}}}.dds_t_hub_{hub_name} 
SELECT * FROM {{ params.schemaName }}.dds_v_hub_{etl_view}_etl;
""".format(hub_name=dds_hub['hub_name'], etl_view=dds_hub['etl_view'])
    ) for dds_hub in DDS_HUBS
]

## ОПРЕДЕЛИМ СТРУКТУРУ DAG'А

etl_start = DummyOperator(task_id="etl_start", dag=dag)
all_ods_reloaded = DummyOperator(task_id="all_ods_reloaded", dag=dag)
all_hubs_loaded = DummyOperator(task_id="all_hubs_loaded", dag=dag)
all_links_loaded = DummyOperator(task_id="all_links_loaded", dag=dag)
all_sats_loaded = DummyOperator(task_id="all_sats_loaded", dag=dag)

etl_start >> ods_reload >> all_ods_reloaded >> dds_hubs_fill >> all_hubs_loaded >> all_links_loaded >> all_sats_loaded
