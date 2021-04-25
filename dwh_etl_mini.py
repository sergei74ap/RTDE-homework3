from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

USERNAME = 'sperfilyev'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2013, 1, 1, 0, 0, 0)
}

dag = DAG(
    dag_id=USERNAME + '_dwh_etl_mini',
    default_args=default_args,
    default_view='graph',
    description='DWH ETL minimal tasks by ' + USERNAME,
    schedule_interval="@yearly",
    max_active_runs=1,
    params={'schemaName': USERNAME},
)

## ОПИШЕМ ВСЕ ОПЕРАЦИИ ЗАГРУЗКИ ДАННЫХ

ods_reload = PostgresOperator(
    task_id="ods_reload", 
    dag=dag,
    sql="""
delete from {{ params.schemaName }}.ods_t_payment cascade
where extract(year from pay_date) = {{ execution_date.year }};

delete from {{ params.schemaName }}.ods_t_payment_hashed cascade
where extract(year from effective_from) = {{ execution_date.year }};

insert into {{ params.schemaName}}.ods_t_payment
    (select * 
     from {{ params.schemaName }}.stg_t_payment 
     where extract(year from pay_date) = {{ execution_date.year }});

insert into {{ params.schemaName}}.ods_t_payment_hashed
    (select v.*,
            '{{ execution_date }}'::date as load_dts
     from {{ params.schemaName }}.ods_v_payment_etl as v
     where extract(year from v.effective_from) = {{ execution_date.year }});

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

dds_sat_user = PostgresOperator(
    task_id="dds_sat_user",
    dag=dag,
    sql="""
delete from {{ params.schemaName }}.dds_t_sat_user 
where extract(year from load_dts) = {{ execution_date.year }};

insert into {{ params.schemaName }}.dds_t_sat_user
with source_data as (
    select user_pk,
           user_hashdiff,
           phone,
           effective_from,
           load_dts,
           rec_source
    from {{ params.schemaName }}.ods_t_payment_hashed
    where extract(year from load_dts) = {{{ execution_date.year }}
),
     update_records as (
         select s.*
         from {{ params.schemaName }}.dds_t_sat_user as s
                  join source_data as src
                       on s.user_pk = src.user_pk and s.load_dts <= (select max(load_dts) from source_data)
     ),
     latest_records as (
         select *
         from (
                  select user_pk,
                         user_hashdiff,
                         load_dts,
                         rank() over (partition by user_pk order by load_dts desc) as row_rank
                  from update_records
              ) as ranked_recs
         where row_rank = 1),
     records_to_insert as (
         select distinct a.*
         from source_data as a
                  left join latest_records
                            on latest_records.user_hashdiff = a.user_hashdiff and
                               latest_records.user_pk = a.user_pk
         where latest_records.user_hashdiff is null
     )
select * from records_to_insert;
"""
)

## ОПРЕДЕЛИМ СТРУКТУРУ DAG'А

ods_reload >> dds_hub_user
dds_hub_user >> dds_sat_user
