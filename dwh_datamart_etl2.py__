from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime

USERNAME = 'sperfilyev'
DM_DIMENSIONS = ('billing_year', 'legal_type', 'district', 'registration_year')

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2013, 1, 1, 0, 0, 0)
}

dag = DAG(
    dag_id=USERNAME + '_dwh_datamart_etl2',
    default_args=default_args,
    default_view='graph',
    description='DWH DM ETL tasks by ' + USERNAME,
    schedule_interval="@yearly",
    max_active_runs=1,
    params={'schemaName': USERNAME},
)

## ОПИШЕМ ВСЕ ОПЕРАЦИИ ЗАГРУЗКИ ДАННЫХ

tmp_tbl_collect = PostgresOperator(
    task_id="tmp_tbl_collect", 
    params={'dimensionsText': ', '.join(DM_DIMENSIONS)},
    dag=dag,
    sql="""
DROP TABLE IF EXISTS {{ params.schemaName }}.payment_report_tmp_oneyear;

CREATE TABLE {{ params.schemaName }}.payment_report_tmp_oneyear AS (
  WITH raw_data AS (
      SELECT legal_type,
             district,
             EXTRACT(YEAR FROM su.effective_from) as registration_year,
             is_vip,
             EXTRACT(YEAR FROM to_date(billing_period_key, 'YYYY-MM')) AS billing_year,
             billing_period_key,
             pay_sum
      FROM {{ params.schemaName }}.dds_t_lnk_payment lp
      JOIN {{ params.schemaName }}.dds_t_hub_billing_period hbp ON lp.billing_period_pk=hbp.billing_period_pk
      JOIN {{ params.schemaName }}.dds_t_hub_user hu ON lp.user_pk=hu.user_pk
      JOIN {{ params.schemaName }}.dds_t_sat_payment sp ON lp.pay_pk=sp.pay_pk
      LEFT JOIN {{ params.schemaName }}.dds_t_sat_user_mdm su ON hu.user_pk=su.user_pk),
  oneyear_data AS (
      SELECT * FROM raw_data
      WHERE billing_year={{ execution_date.year }}
  )
SELECT {{ params.dimensionsText }}, is_vip, sum(pay_sum)
FROM oneyear_data
GROUP BY {{ params.dimensionsText }}, is_vip
ORDER BY {{ params.dimensionsText }}, is_vip
);

GRANT ALL PRIVILEGES ON {{ params.schemaName }}.payment_report_tmp_oneyear TO {{ params.schemaName }};
"""
)

dimensions_fill = [
    PostgresOperator(
        task_id="dim_{0}_fill".format(dim_name),
        dag=dag,
        sql="""
INSERT INTO {{{{ params.schemaName }}}}.payment_report_dim_{0} ({0}_key)
SELECT DISTINCT {0} AS {0}_key
FROM {{{{ params.schemaName }}}}.payment_report_tmp_oneyear
LEFT JOIN {{{{ params.schemaName }}}}.payment_report_dim_{0} ON {0}_key={0}
WHERE {0}_key is NULL;""".format(dim_name)
    ) for dim_name in DM_DIMENSIONS
]

all_joins = '\n'.join(
    ["JOIN {{{{ params.schemaName }}}}.payment_report_dim_{dim_name} dim{dim_indx} ON tmp.{dim_name}=dim{dim_indx}.{dim_name}_key".\
     format(dim_name=dim_name, dim_indx=dim_indx) for dim_indx, dim_name in enumerate(DM_DIMENSIONS)]
)
all_ids = ', '.join(
    ['dim{0}.id'.format(dim_indx) for dim_indx, _ in enumerate(DM_DIMENSIONS)]
)   
facts_fill = PostgresOperator(
    task_id="facts_fill",
    dag=dag,
    sql="""
INSERT INTO {{{{ params.schemaName }}}}.payment_report_fct
SELECT {all_ids}, tmp.is_vip, tmp.sum
FROM {{{{ params.schemaName }}}}.payment_report_tmp_oneyear tmp
{all_joins};""".format(all_ids=all_ids, all_joins=all_joins)
)

tmp_tbl_drop = PostgresOperator(
    task_id="tmp_tbl_drop",
    dag=dag,
    sql="DROP TABLE {{ params.schemaName }}.payment_report_tmp_oneyear;"
)

## ОПРЕДЕЛИМ СТРУКТУРУ DAG'А

tmp_tbl_collect >> dimensions_fill >> facts_fill >> tmp_tbl_drop
