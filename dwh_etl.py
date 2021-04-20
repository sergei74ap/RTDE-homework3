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
    description='DWH ETL tasks by ' + USERNAME,
    schedule_interval="@yearly",
    max_active_runs=1,
    params={'schemaName': USERNAME},
)

hub_user_insert = PostgresOperator(
    task_id="hub_user_insert",
    dag=dag,
    sql="""
drop view if exists {{ params.schemaName }}.dds_v_hub_user_etl;

create view {{ params.schemaName }}.dds_v_hub_user_etl as (
with users_numbered as (
    select user_pk,
           user_key,
           {{ execution_date }}::date as load_dts,
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

insert into {{ params.schemaName }}.dds_t_hub_user (select * from {{ params.schemaName }}.dds_v_hub_user_etl);
"""
)

hub_account_insert = PostgresOperator(
    task_id="hub_account_insert",
    dag=dag,
    sql="""
drop view if exists {{ params.schemaName }}.dds_v_hub_account_etl;

create view {{ params.schemaName }}.dds_v_hub_account_etl as (
with accounts_numbered as (
    select account_pk,
           account_key,
           {{ execution_date }}::date as load_dts,
           rec_source,
           row_number() over (partition by account_pk order by load_dts asc) as row_num
    from {{ params.schemaName }}.ods_v_payment where extract(year from pay_date) = {{ execution_date.year }}),
     accounts_rank_1 as (
         select account_pk, account_key, load_dts, rec_source
         from accounts_numbered
         where row_num = 1),
     records_to_insert as (
         select a.*
         from accounts_rank_1 as a
                  left join {{ params.schemaName }}.dds_t_hub_account as h
                            on a.account_pk = h.account_pk
         where h.account_pk is null
     )
select *
from records_to_insert
    );

insert into {{ params.schemaName }}.dds_t_hub_account (select * from {{ params.schemaName }}.dds_v_hub_account_etl);
"""
)

hub_billing_period_insert = PostgresOperator(
    task_id="hub_billing_period_insert",
    dag=dag,
    sql="""
drop view if exists {{ params.schemaName }}.dds_v_hub_billing_period_etl;

create view {{ params.schemaName }}.dds_v_hub_billing_period_etl as (
with billing_periods_numbered as (
    select billing_period_pk,
           billing_period_key,
           {{ execution_date }}::date as load_dts,
           rec_source,
           row_number() over (partition by billing_period_pk order by load_dts asc) as row_num
    from {{ params.schemaName }}.ods_v_payment where extract(year from pay_date) = {{ execution_date.year }}),
     billing_periods_rank_1 as (
         select billing_period_pk, billing_period_key, load_dts, rec_source
         from billing_periods_numbered
         where row_num = 1),
     records_to_insert as (
         select a.*
         from billing_periods_rank_1 as a
                  left join {{ params.schemaName }}.dds_t_hub_billing_period as h
                            on a.billing_period_pk = h.billing_period_pk
         where h.billing_period_pk is null
     )
select *
from records_to_insert
    );

insert into {{ params.schemaName }}.dds_t_hub_billing_period (select * from {{ params.schemaName }}.dds_v_hub_billing_period_etl);
"""
)

all_hubs_loaded = DummyOperator(task_id="all_hubs_loaded", dag=dag)

hub_user_insert >> all_hubs_loaded
hub_account_insert >> all_hubs_loaded
hub_billing_period_insert >> all_hubs_loaded
