from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

USERNAME = 'sperfilyev'
DM_DIMENSIONS = ('billing_year', 'legal_type', 'district', 'registration_year')
DM_DIMS_TEXT = ','.join(dm_dimensions)

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2013, 1, 1, 0, 0, 0)
}

dag = DAG(
    dag_id=USERNAME + '_dwh_datamart_etl',
    default_args=default_args,
    default_view='graph',
    description='DWH DM ETL tasks by ' + USERNAME,
    schedule_interval="@yearly",
    max_active_runs=1,
    params={'schemaName': USERNAME, 'dimensionsText': DM_DIMS_TEXT},
)

## ОПИШЕМ ВСЕ ОПЕРАЦИИ ЗАГРУЗКИ ДАННЫХ

tmp_tbl_collect = PostgresOperator(
    task_id="tmp_tbl_collect", 
    dag=dag,
    sql="""
DROP TABLE IF EXISTS {{ params.schemaName }}.payment_report_tmp_oneyear;
CREATE TABLE {{ params.schemaName }}.payment_report_tmp_oneyear AS (
  WITH raw_data AS (
      SELECT legal_type,
             district,
             EXTRACT(YEAR FROM registered_at) as registration_year,
             is_vip,
             EXTRACT(YEAR FROM to_date(billing_period_key, 'YYYY-MM')) AS billing_year,
             billing_period_key,
             pay_sum AS billing_sum
      FROM {{ params.schemaName }}.dds_t_lnk_payment lp
      JOIN {{ params.schemaName }}.dds_t_hub_billing_period hbp ON lp.billing_period_pk=hbp.billing_period_pk
      JOIN {{ params.schemaName }}.dds_t_hub_user hu ON lp.user_pk=hu.user_pk
      JOIN {{ params.schemaName }}.dds_t_sat_payment sp ON lp.pay_pk=sp.pay_pk
      LEFT JOIN mdm."user" mdmu ON hu.user_key=mdmu.id::TEXT),
  oneyear_data AS (
      SELECT * FROM raw_data
      WHERE billing_year={{ execution_date.year }}
  )
SELECT {{ params.dimensionsText }},
       is_vip, sum(billing_sum)
FROM oneyear_data
GROUP BY {{ params.dimensionsText }}, is_vip
ORDER BY {{ params.dimensionsText }}, is_vip
);
"""
)

dimensions_fill = [
    PostgresOperator(
        task_id="dim_" + dim_name + "_fill",
        dag=dag,
        sql='INSERT INTO {{ params.schemaName }}.payment_report_dim_' + dim_name + '(' + dim_name + '_key)' +\
            ' SELECT DISTINCT ' + dim_name + ' AS ' + dim_name + '_key' +\
            ' FROM {{ params.schemaName }}.payment_report_tmp_oneyear' +\
            ' LEFT JOIN {{ params.schemaName }}.payment_report_dim_' + dim_name +\
            ' ON ' + dim_name + '_key=' + dim_name + ' WHERE ' + dim_name + '_key is NULL;'
    ) for dim_name in DM_DIMENSIONS
]

facts_fill = PostgresOperator(
    task_id="facts_fill",
    dag=dag,
    sql="""
INSERT INTO {{ params.schemaName }}.payment_report_fct
SELECT biy.id, lt.id, d.id, ry.id, tmp.is_vip, tmp.sum
FROM {{ params.schemaName }}.payment_report_tmp_oneyear tmp
JOIN {{ params.schemaName }}.payment_report_dim_billing_year biy ON tmp.billing_year=biy.billing_year_key
JOIN {{ params.schemaName }}.payment_report_dim_legal_type lt ON tmp.legal_type=lt.legal_type_key
JOIN {{ params.schemaName }}.payment_report_dim_district d ON tmp.district=d.district_key
JOIN {{ params.schemaName }}.payment_report_dim_registration_year ry ON tmp.registration_year=ry.registration_year_key;
"""
)

tmp_tbl_drop = PostgresOperator(
    task_id="tmp_tbl_drop",
    dag=dag,
    sql="DROP TABLE {{ params.schemaName }}.payment_report_tmp_oneyear;"
)

## ОПРЕДЕЛИМ СТРУКТУРУ DAG'А

tmp_tbl_collect >> dimensions_fill >> facts_fill >> tmp_tbl_drop
