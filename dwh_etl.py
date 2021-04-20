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
    dag_id=USERNAME + '_dwh_etl',
    default_args=default_args,
    description='DWH ETL tasks by sperfilyev',
    schedule_interval="@yearly",
    max_active_runs=1,
    params={'schemaName': USERNAME},
)

hub_user_drop_view = PostgresOperator(
    task_id="hub_user_drop_view",
    dag=dag,
    sql="drop view if exists {{ params.schemaName }}.dds_v_hub_user_etl;"
)

hub_user_create_view = PostgresOperator(
    task_id="hub_user_create_view",
    dag=dag,
    sql="""
"""
)

hub_user_insert = PostgresOperator(
    task_id="hub_user_insert",
    dag=dag,
    sql="insert into {{ params.schemaName }}.dds_t_hub_user (select * from {{ params.schemaName }}.dds_v_hub_user_etl);"
)

hub_user_drop_view >> hub_user_create_view >> hub_user_insert


"""
create view {{ params.schemaName }}.dds_v_hub_user_etl as (
with users_numbered as (
    select user_pk,
           user_key,
           load_dts,
           rec_source,
           row_number() over (partition by user_pk order by load_dts asc) as row_num
    from {{ params.schemaName }}.ods_v_payment where extract(year from pay_date) = {{ execution_date.year }}),
     users_rank_1 as (
         select user_pk, user_key, load_dts, rec_source
         from users_numbered
         where row_num = 1),
     records_to_insert as (
         select a.*
         from users_rank_1 as a
                  left join {{ params.schemaName }}.dds_t_hub_user as h
                            on a.user_pk = h.user_pk
         where h.user_pk is null
     )
select *
from records_to_insert
    );
"""
