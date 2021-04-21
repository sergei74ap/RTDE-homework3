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
    default_view='graph',
    description='DWH ETL tasks by ' + USERNAME,
    schedule_interval="@yearly",
    max_active_runs=1,
    params={'schemaName': USERNAME},
)


## ОПИШЕМ ВСЕ ОПЕРАЦИИ ЗАГРУЗКИ ДАННЫХ

# TODO: LOAD ODS FROM STG, PARTITION BY YEAR (EMULATING SEQUENTIAL IMPORTS INTO DWH)
ods_load = DummyOperator(task_id="ods_load", dag=dag)

dds_hub_user = PostgresOperator(
    task_id="dds_hub_user",
    dag=dag,
    sql="""
drop view if exists {{ params.schemaName }}.dds_v_hub_user_etl;

create view {{ params.schemaName }}.dds_v_hub_user_etl as (
with users_numbered as (
    select user_pk,
           user_key,
           '{{ execution_date }}'::date as load_dts,
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

grant all privileges on {{ params.schemaName }}.dds_v_hub_user_etl to {{ params.schemaName }};
insert into {{ params.schemaName }}.dds_t_hub_user (select * from {{ params.schemaName }}.dds_v_hub_user_etl);
"""
)

dds_hub_account = PostgresOperator(
    task_id="dds_hub_account",
    dag=dag,
    sql="""
drop view if exists {{ params.schemaName }}.dds_v_hub_account_etl;

create view {{ params.schemaName }}.dds_v_hub_account_etl as (
with accounts_numbered as (
    select account_pk,
           account_key,
           '{{ execution_date }}'::date as load_dts,
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

grant all privileges on {{ params.schemaName }}.dds_v_hub_account_etl to {{ params.schemaName }};
insert into {{ params.schemaName }}.dds_t_hub_account (select * from {{ params.schemaName }}.dds_v_hub_account_etl);
"""
)

dds_hub_billing_period = PostgresOperator(
    task_id="dds_hub_billing_period",
    dag=dag,
    sql="""
drop view if exists {{ params.schemaName }}.dds_v_hub_billing_period_etl;

create view {{ params.schemaName }}.dds_v_hub_billing_period_etl as (
with billing_periods_numbered as (
    select billing_period_pk,
           billing_period_key,
           '{{ execution_date }}'::date as load_dts,
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

grant all privileges on {{ params.schemaName }}.dds_v_hub_billing_period_etl to {{ params.schemaName }};
insert into {{ params.schemaName }}.dds_t_hub_billing_period (select * from {{ params.schemaName }}.dds_v_hub_billing_period_etl);
"""
)

dds_lnk_payment = PostgresOperator(
    task_id="dds_lnk_payment",
    dag=dag,
    sql="""
drop view if exists {{ params.schemaName }}.dds_v_lnk_payment_etl;

create view {{ params.schemaName }}.dds_v_lnk_payment_etl as (
select distinct p.pay_pk,
                p.user_pk,
                p.account_pk,
                p.billing_period_pk,
                p.effective_from,
                '{{ execution_date }}'::date as load_dts,
                p.rec_source
from {{ params.schemaName }}.ods_v_payment as p
         left join {{ params.schemaName }}.dds_t_lnk_payment as l
                   on p.pay_pk = l.pay_pk
where l.pay_pk is null and extract(year from p.pay_date) = {{ execution_date.year }}
);
    
grant all privileges on {{ params.schemaName }}.dds_v_lnk_payment_etl to {{ params.schemaName }};
insert into {{ params.schemaName }}.dds_t_lnk_payment (select * from {{ params.schemaName }}.dds_v_lnk_payment_etl);
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
