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

## Загружаем данные из STG в ODS по годам, эмулируем ежегодную подгрузку порциями "за прошедший период"
ods_load = PostgresOperator(
    task_id="ods_load", 
    dag=dag,
    sql="""
truncate {{ params.schemaName }}.ods_t_payment cascade;
insert into {{ params.schemaName}}.ods_t_payment
    (select * from {{ params.schemaName }}.stg_t_payment as stg
        where extract(year from stg.pay_date) = {{ execution_date.year }});

drop view if exists {{ params.schemaName }}.ods_v_payment_etl cascade;
create view {{ params.schemaName }}.ods_v_payment_etl as
(
with derived_columns as (
    select *,
           user_id::text            as user_key,
           account::text            as account_key,
           billing_period::text     as billing_period_key,
           'PAYMENT_DATALAKE'::text as rec_source
    from {{ params.schemaName }}.ods_t_payment
),
     hashed_columns as (
         select *,
                cast(md5(nullif(upper(trim(cast(user_id as varchar))), '')) as text)        as user_pk,
                cast(md5(nullif(upper(trim(cast(account as varchar))), '')) as text)        as account_pk,
                cast(md5(nullif(upper(trim(cast(billing_period as varchar))), '')) as text) as billing_period_pk,
                cast(md5(nullif(concat_ws('||',
                                          coalesce(nullif(upper(trim(cast(user_id as varchar))), ''), '^^'),
                                          coalesce(nullif(upper(trim(cast(account as varchar))), ''), '^^'),
                                          coalesce(nullif(upper(trim(cast(billing_period as varchar))), ''), '^^')
                                    ), '^^||^^||^^')) as text)                              as pay_pk,
                cast(md5(nullif(upper(trim(cast(phone as varchar))), '')) as text)          as user_hashdiff,
                cast(md5(nullif(concat_ws('||',
                                          coalesce(nullif(upper(trim(cast(pay_doc_num as varchar))), ''), '^^'),
                                          coalesce(nullif(upper(trim(cast(pay_doc_type as varchar))), ''), '^^'),
                                          coalesce(nullif(upper(trim(cast(pay_sum as varchar))), ''), '^^')
                                    ), '^^||^^||^^')) as text)                              as pay_hashdiff

         from derived_columns
     )
select user_key,
       account_key,
       billing_period_key,
       user_pk,
       account_pk,
       billing_period_pk,
       pay_pk,
       user_hashdiff,
       pay_hashdiff,
       pay_doc_type,
       pay_doc_num,
       pay_sum,
       pay_date,
       phone,
       rec_source,
       '{{ execution_date }}'::date as load_dts,
       pay_date as effective_from
from hashed_columns
    );

grant all privileges on {{ params.schemaName }}.ods_v_payment_etl to {{ params.schemaName }};
"""    
)

dds_hub_user = PostgresOperator(
    task_id="dds_hub_user",
    dag=dag,
    sql="""
drop view if exists {{ params.schemaName }}.dds_v_hub_user_etl;

create view {{ params.schemaName }}.dds_v_hub_user_etl as (
with users_numbered as (
    select user_pk,
           user_key,
           load_dts,
           rec_source,
           row_number() over (partition by user_pk order by load_dts asc) as row_num
    from {{ params.schemaName }}.ods_v_payment_etl),
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
           load_dts,
           rec_source,
           row_number() over (partition by account_pk order by load_dts asc) as row_num
    from {{ params.schemaName }}.ods_v_payment_etl),
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
           load_dts,
           rec_source,
           row_number() over (partition by billing_period_pk order by load_dts asc) as row_num
    from {{ params.schemaName }}.ods_v_payment_etl),
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
                p.load_dts,
                p.rec_source
from {{ params.schemaName }}.ods_v_payment_etl as p
         left join {{ params.schemaName }}.dds_t_lnk_payment as l
                   on p.pay_pk = l.pay_pk
where l.pay_pk is null
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
