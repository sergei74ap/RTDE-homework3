from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

USERNAME = 'sperfilyev'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2014, 1, 1, 0, 0, 0)     # данные начинаются с 2013 года
}

dag = DAG(
    dag_id=USERNAME + '_dwh_etl',
    default_args=default_args,
    default_view='graph',
    description='DWH ETL tasks by ' + USERNAME,
    schedule_interval="@yearly",
    max_active_runs=1,
    params={'schemaName': USERNAME},
)

## ОПИШЕМ ВСЕ ОПЕРАЦИИ ЗАГРУЗКИ ДАННЫХ

## Загружаем данные из STG в ODS по годам, эмулируем ежегодную подгрузку порциями "за прошедший период"
ods_load = PostgresOperator(
    task_id="ods_load", 
    dag=dag,
    sql="""
truncate {{ params.schemaName }}.ods_t_payment cascade;
insert into {{ params.schemaName}}.ods_t_payment
    (select stg.*,
            '{{ execution_date }}'::date as load_dts,
            'PAYMENT_DATALAKE'::text as rec_source
     from {{ params.schemaName }}.stg_t_payment as stg
     where extract(year from stg.pay_date) = {{ execution_date.year }})-1;
"""    
)

dds_hub_user = PostgresOperator(
    task_id="dds_hub_user",
    dag=dag,
    sql="""
insert into {{ params.schemaName }}.dds_t_hub_user 
(select * from {{ params.schemaName }}.dds_v_hub_user_etl);
"""
)

dds_hub_account = PostgresOperator(
    task_id="dds_hub_account",
    dag=dag,
    sql="""
insert into {{ params.schemaName }}.dds_t_hub_account 
(select * from {{ params.schemaName }}.dds_v_hub_account_etl);
"""
)

dds_hub_billing_period = PostgresOperator(
    task_id="dds_hub_billing_period",
    dag=dag,
    sql="""
insert into {{ params.schemaName }}.dds_t_hub_billing_period 
(select * from {{ params.schemaName }}.dds_v_hub_billing_period_etl);
"""
)

dds_lnk_payment = PostgresOperator(
    task_id="dds_lnk_payment",
    dag=dag,
    sql="""
insert into {{ params.schemaName }}.dds_t_lnk_payment 
(select * from {{ params.schemaName }}.dds_v_lnk_payment_etl);
"""
)

# TODO: LOAD SATELLITES
dds_sat_user = DummyOperator(task_id="dds_sat_user", dag=dag)
dds_sat_payment = DummyOperator(task_id="dds_sat_payment", dag=dag)


## ОПРЕДЕЛИМ СТРУКТУРУ DAG'А

all_hubs_loaded = DummyOperator(task_id="all_hubs_loaded", dag=dag)
all_links_loaded = DummyOperator(task_id="all_links_loaded", dag=dag)
all_sats_loaded = DummyOperator(task_id="all_sats_loaded", dag=dag)

ods_load >> [dds_hub_user, dds_hub_account, dds_hub_billing_period]

dds_hub_user >> all_hubs_loaded
dds_hub_account >> all_hubs_loaded
dds_hub_billing_period >> all_hubs_loaded

all_hubs_loaded >> dds_lnk_payment
dds_lnk_payment >> all_links_loaded

all_links_loaded >> [dds_sat_user, dds_sat_payment]
dds_sat_user >> all_sats_loaded
dds_sat_payment >> all_sats_loaded
