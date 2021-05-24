from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime

USERNAME = 'sperfilyev'
DM_DIMENSIONS = ('billing_year', 'legal_type', 'district', 'billing_mode', 'registration_year')
DDS_SOURCES = ('payment', 'billing', 'issue', 'traffc')

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2013, 1, 1, 0, 0, 0)
}

dag = DAG(
    dag_id=USERNAME + '_dwh_datamart_final',
    default_args=default_args,
    default_view='graph',
    description='DWH DM final project by ' + USERNAME,
    schedule_interval="@yearly",
    max_active_runs=1,
    params={'schemaName': USERNAME, 'dimensionsText': ', '.join(DM_DIMENSIONS)},
)

# ===============================================================================
# ОПИШЕМ ВСЕ ОПЕРАЦИИ ЗАГРУЗКИ ДАННЫХ

# Собрать временные денормализованные таблицы
tmp_tbl_collect = []

# --- Платежи
tmp_tbl_collect.append(PostgresOperator(
    task_id="tmp_tbl_collect_payment", 
    dag=dag,
    sql="""
DROP TABLE IF EXISTS {{ params.schemaName }}.dm_report_payment_tmp;
CREATE TABLE {{ params.schemaName }}.dm_report_payment_tmp AS (
  WITH raw_data AS (
      SELECT legal_type,
             district,
             billing_mode,
             EXTRACT(YEAR FROM su.effective_from) as registration_year,
             is_vip,
             pay_sum,
             billing_period_key,
             EXTRACT(YEAR FROM to_date(billing_period_key, 'YYYY-MM')) AS report_year
      FROM {{ params.schemaName }}.dds_t_lnk_payment l
      JOIN {{ params.schemaName }}.dds_t_hub_billing_period hbp ON l.billing_period_pk=hbp.billing_period_pk
      JOIN {{ params.schemaName }}.dds_t_hub_user hu ON l.user_pk=hu.user_pk
      JOIN {{ params.schemaName }}.dds_t_sat_payment s ON l.payment_pk=s.payment_pk
      LEFT JOIN {{ params.schemaName }}.dds_t_sat_user_mdm su ON hu.user_pk=su.user_pk),
  oneyear_data AS (
      SELECT * FROM raw_data
      WHERE report_year={{ execution_date.year }}
  )
SELECT {{ params.dimensionsText }}, is_vip, 
       sum(pay_sum) as payment_sum
FROM oneyear_data
GROUP BY {{ params.dimensionsText }}, is_vip
ORDER BY {{ params.dimensionsText }}, is_vip
);""")

# --- Начисления
tmp_tbl_collect.append(PostgresOperator(
    task_id="tmp_tbl_collect_billing", 
    dag=dag,
    sql="""
DROP TABLE IF EXISTS {{ params.schemaName }}.dm_report_billing_tmp;
CREATE TABLE {{ params.schemaName }}.dm_report_billing_tmp AS (
  WITH raw_data AS (
      SELECT legal_type,
             district,
             billing_mode,
             EXTRACT(YEAR FROM su.effective_from) as registration_year,
             is_vip,
             billing_sum,
             billing_period_key,
             EXTRACT(YEAR FROM to_date(billing_period_key, 'YYYY-MM')) AS report_year
      FROM {{ params.schemaName }}.dds_t_lnk_billing l
      JOIN {{ params.schemaName }}.dds_t_hub_billing_period hbp ON l.billing_period_pk=hbp.billing_period_pk
      JOIN {{ params.schemaName }}.dds_t_hub_user hu ON l.user_pk=hu.user_pk
      JOIN {{ params.schemaName }}.dds_t_sat_billing s ON l.billing_pk=s.billing_pk
      LEFT JOIN {{ params.schemaName }}.dds_t_sat_user_mdm su ON hu.user_pk=su.user_pk),
  oneyear_data AS (
      SELECT * FROM raw_data
      WHERE report_year={{ execution_date.year }}
  )
SELECT {{ params.dimensionsText }}, is_vip, 
       sum(billing_sum) as billing_sum
FROM oneyear_data
GROUP BY {{ params.dimensionsText }}, is_vip
ORDER BY {{ params.dimensionsText }}, is_vip
);""")

# --- Обращения
tmp_tbl_collect.append(PostgresOperator(
    task_id="tmp_tbl_collect_issue", 
    dag=dag,
    sql="""
DROP TABLE IF EXISTS sperfilyev.dm_report_issue_tmp;
CREATE TABLE sperfilyev.dm_report_issue_tmp AS (
  WITH raw_data AS (
      SELECT legal_type,
             district,
             billing_mode,
             EXTRACT(YEAR FROM su.effective_from) as registration_year,
             is_vip,
             l.issue_pk AS issue_pk,
             EXTRACT(YEAR FROM l.effective_from) AS report_year
      FROM sperfilyev.dds_t_lnk_issue l
      JOIN sperfilyev.dds_t_hub_user hu ON l.user_pk=hu.user_pk
      JOIN sperfilyev.dds_t_sat_issue s ON l.issue_pk=s.issue_pk
      LEFT JOIN sperfilyev.dds_t_sat_user_mdm su ON hu.user_pk=su.user_pk),
  oneyear_data AS (
      SELECT * FROM raw_data
      WHERE report_year={{ execution_date.year }}
  )
SELECT {{ params.dimensionsText }}, is_vip, 
       count(issue_pk) as issue_cnt
FROM oneyear_data
GROUP BY {{ params.dimensionsText }}, is_vip
ORDER BY {{ params.dimensionsText }}, is_vip
);""")

# --- Трафик
tmp_tbl_collect.append(PostgresOperator(
    task_id="tmp_tbl_collect_traffic", 
    dag=dag,
    sql="""
DROP TABLE IF EXISTS sperfilyev.dm_report_traffic_tmp;
CREATE TABLE sperfilyev.dm_report_traffic_tmp AS (
  WITH raw_data AS (
      SELECT legal_type,
             district,
             billing_mode,
             EXTRACT(YEAR FROM su.effective_from) AS registration_year,
             is_vip,
             bytes_sent, bytes_received,
             EXTRACT(YEAR FROM l.effective_from) AS report_year
      FROM sperfilyev.dds_t_lnk_traffic l
      JOIN sperfilyev.dds_t_hub_user hu ON l.user_pk=hu.user_pk
      JOIN sperfilyev.dds_t_sat_traffic s ON l.traffic_pk=s.traffic_pk
      LEFT JOIN sperfilyev.dds_t_sat_user_mdm su ON hu.user_pk=su.user_pk),
  oneyear_data AS (
      SELECT * FROM raw_data
      WHERE report_year={{ execution_date.year }}
  )
SELECT {{ params.dimensionsText }}, is_vip,
       sum(cast(bytes_sent AS BIGINT) + cast(bytes_received AS BIGINT)) AS traffic_amount
FROM oneyear_data
GROUP BY {{ params.dimensionsText }}, is_vip
ORDER BY {{ params.dimensionsText }}, is_vip
);""")

# --------------------------------------------------------
# Наполнить таблицы измерений данными из временных таблиц
dimensions_fill = [
    PostgresOperator(
        task_id="dim_{0}_fill".format(dim_name),
        dag=dag,
        sql="""
INSERT INTO {{{{ params.schemaName }}}}.dm_report_dim_{0} ({0}_key)
SELECT DISTINCT {0} AS {0}_key
FROM {{{{ params.schemaName }}}}.dm_report_payment_oneyear
LEFT JOIN {{{{ params.schemaName }}}}.dm_report_dim_{0} ON {0}_key={0}
WHERE {0}_key is NULL;""".format(dim_name)
    ) for dim_name in DM_DIMENSIONS
]

# -------------------------------------------------------------
# Наполнить данными таблицу фактов, собрать из временных таблиц
facts_fill = PostgresOperator(
    task_id="facts_fill",
    dag=dag,
    sql="""
INSERT INTO {{ params.schemaName }}.dm_report_fct
SELECT y.id, lt.id, d.id, bm.id, ry.id, pay.is_vip,
       pay.payment_sum, bill.billing_sum, issue_cnt, traffic_amount
FROM {{ params.schemaName }}.dm_report_payment_oneyear pay
JOIN {{ params.schemaName }}.dm_report_dim_report_year y ON pay.report_year=y.report_year_key
JOIN {{ params.schemaName }}.dm_report_dim_legal_type lt ON pay.legal_type=lt.legal_type_key
JOIN {{ params.schemaName }}.dm_report_dim_district d ON pay.district=d.district_key
JOIN {{ params.schemaName }}.dm_report_dim_billing_mode bm ON pay.billing_mode=bm.billing_mode_key
JOIN {{ params.schemaName }}.dm_report_dim_registration_year ry ON pay.registration_year=ry.registration_year_key
LEFT JOIN {{ params.schemaName }}.dm_report_billing_oneyear bill
    ON bill.report_year=y.report_year_key
           AND bill.legal_type=lt.legal_type_key
           AND bill.district=d.district_key
           AND bill.billing_mode=bm.billing_mode_key
           AND bill.registration_year=ry.registration_year_key
           AND bill.is_vip=pay.is_vip
LEFT JOIN {{ params.schemaName }}.dm_report_issue_oneyear issue
    ON issue.report_year=y.report_year_key
           AND issue.legal_type=lt.legal_type_key
           AND issue.district=d.district_key
           AND issue.billing_mode=bm.billing_mode_key
           AND issue.registration_year=ry.registration_year_key
           AND issue.is_vip=pay.is_vip
LEFT JOIN {{ params.schemaName }}.dm_report_traffic_oneyear trf
    ON trf.report_year=y.report_year_key
           AND trf.legal_type=lt.legal_type_key
           AND trf.district=d.district_key
           AND trf.billing_mode=bm.billing_mode_key
           AND trf.registration_year=ry.registration_year_key
           AND trf.is_vip=pay.is_vip;

UPDATE {{ params.schemaName }}.dm_report_fct SET issue_cnt=0 WHERE issue_cnt IS NULL;
UPDATE {{ params.schemaName }}.dm_report_fct SET traffic_amount=0 WHERE traffic_amount IS NULL;
"""
)

# ---------------------------------
# Удалить временные таблицы
drop_sql = '\n'.join(["DROP TABLE {{{{ params.schemaName }}}}.dm_report_{0}_oneyear;".format(dds_source) for dds_source in DDS_SOURCES])
tmp_tbl_drop = PostgresOperator(
    task_id="tmp_tbl_drop",
    dag=dag,
    sql=drop_sql
)

## ОПРЕДЕЛИМ СТРУКТУРУ DAG'А
DummyOperator(task_id="datamart_start", dag=dag) >> tmp_tbl_collect >> \
DummyOperator(task_id="tmp_tbls_done", dag=dag) >> dimensions_fill >> facts_fill >> tmp_tbl_drop
