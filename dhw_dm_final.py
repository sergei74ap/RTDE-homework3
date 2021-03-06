from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

USERNAME = 'sperfilyev'

# =============================================================
# Подготовим метаданные для кодогенерации SQL

# Какие измерения будут в витрине? 
DM_DIMENSIONS = ('report_year', 'legal_type', 'district', 'billing_mode', 'registration_year')

# Какие агрегаты будут помещены в таблицу фактов витрины?
# Для каждого агрегата определим: из какого источника в DV брать данные? 
# (указываем наименование линка, данные берём из его сателлита)
# Хранятся ли факты в линках в разрезе расчётных периодов? По каким полям считать агрегат? По какой формуле? 
DM_AGGREGATION = {
    'payment': {'from_billing': True,  'fields': "pay_sum",                    'formula': "sum(pay_sum) AS payment_sum"},
    'billing': {'from_billing': True,  'fields': "billing_sum",                'formula': "sum(billing_sum) AS billing_sum"},
    'issue':   {'from_billing': False, 'fields': "issue_pk",                   'formula': "count(*) AS issue_cnt"},
    'traffic': {'from_billing': False, 'fields': "bytes_sent, bytes_received", 'formula': "sum(cast(bytes_sent AS BIGINT) + cast(bytes_received AS BIGINT)) AS traffic_amount"},
}

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2012, 1, 1, 0, 0, 0)
}

dag = DAG(
    dag_id=USERNAME + '_dwh_datamart_final',
    default_args=default_args,
    default_view='graph',
    description='DWH DM final project by ' + USERNAME,
    schedule_interval="@yearly",
    max_active_runs=1,
    params={'schemaName': USERNAME, 'dimensionsText': ', '.join(DM_DIMENSIONS) + ', is_vip'},
)

# ===============================================================================
# ОПИШЕМ ВСЕ ОПЕРАЦИИ ЗАГРУЗКИ ДАННЫХ

# Сгенерируем из метаданных SQL-запрос для сборки временной денормализованной таблицы
def build_tmp_sql(dds_link, our_fields, our_formula, with_billing_period=True):

    our_fields = ', '.join(['s.' + fld.strip() for fld in our_fields.split(',')])  
    if with_billing_period:
        report_date = "to_date(billing_period_key, 'YYYY-MM')"
        our_fields = our_fields + ", billing_period_key"
        join_hbp = "JOIN {{ params.schemaName }}.dds_t_hub_billing_period hbp ON l.billing_period_pk=hbp.billing_period_pk"
    else:
        report_date = "l.effective_from"
        join_hbp = ""

    return """
DROP TABLE IF EXISTS {{{{ params.schemaName }}}}.dm_report_{dds_link}_oneyear;
CREATE TABLE {{{{ params.schemaName }}}}.dm_report_{dds_link}_oneyear AS (
  WITH raw_data AS (
      SELECT legal_type,
             district,
             billing_mode,
             EXTRACT(YEAR FROM su.effective_from) as registration_year,
             is_vip,
             {our_fields},
             EXTRACT(YEAR FROM {report_date}) AS report_year
      FROM {{{{ params.schemaName }}}}.dds_t_lnk_{dds_link} l
      {join_hbp}
      JOIN {{{{ params.schemaName }}}}.dds_t_hub_user hu ON l.user_pk=hu.user_pk
      JOIN {{{{ params.schemaName }}}}.dds_t_sat_{dds_link} s ON l.{dds_link}_pk=s.{dds_link}_pk
      LEFT JOIN {{{{ params.schemaName }}}}.dds_t_sat_user_mdm su ON hu.user_pk=su.user_pk),
  oneyear_data AS (
      SELECT * FROM raw_data
      WHERE report_year={{{{ execution_date.year }}}}
  )
SELECT {{{{ params.dimensionsText }}}},
       {our_formula}
FROM oneyear_data
GROUP BY {{{{ params.dimensionsText }}}}
ORDER BY {{{{ params.dimensionsText }}}}
);

GRANT ALL PRIVILEGES ON {{{{ params.schemaName }}}}.dm_report_{dds_link}_oneyear TO {{{{ params.schemaName }}}};
""".format(dds_link=dds_link, our_fields=our_fields, our_formula=our_formula, \
             report_date=report_date, join_hbp=join_hbp)

# Собрать временные денормализованные таблицы по всем источникам
tmp_tbl_collect = [
    PostgresOperator(
        task_id="tmp_tbl_collect_{0}".format(dds_source), 
        dag=dag,
        sql=build_tmp_sql(
            dds_link=dds_source, 
            our_fields=DM_AGGREGATION[dds_source]['fields'], 
            our_formula=DM_AGGREGATION[dds_source]['formula'],
            with_billing_period=DM_AGGREGATION[dds_source]['from_billing'],
        )
    ) for dds_source in DM_AGGREGATION.keys()
]

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

# -------------------------------------------------------------------------
# Сгенерируем из метаданных SQL-запрос для заливки данных в таблицу фактов
dim_ids = ", ".join(
    ["dim{0}.id AS {1}_id".format(dim_indx, dim_name) \
    for dim_indx, dim_name in enumerate(DM_DIMENSIONS)]
)
fct_dim_refs = ", ".join(
    ["{0}_id".format(dim_name) for dim_name in DM_DIMENSIONS]
)

dim_tbls = "\nCROSS JOIN ".join(
    ["{{{{ params.schemaName }}}}.dm_report_dim_{1} dim{0}".format(dim_indx, dim_name) \
    for dim_indx, dim_name in enumerate(DM_DIMENSIONS)]
)

aggr_flds = [
    DM_AGGREGATION[aggr_src]['formula'].upper().split(" AS ")[-1].strip() \
    for aggr_src in DM_AGGREGATION.keys()
]

tmp_tbls = "\n".join([
    " ".join([
        "LEFT JOIN {{{{ params.schemaName }}}}.dm_report_{0}_oneyear {0}\n ON".format(aggr_src),
        "\n\t AND ".join([
            "{aggr_src}.{dim_name} = dim{dim_indx}.{dim_name}_key".format(
                dim_indx=dim_indx, dim_name=dim_name, aggr_src=aggr_src,
            ) for dim_indx, dim_name in enumerate(DM_DIMENSIONS)
        ]) + "\n\t AND {0}.is_vip = vip.is_vip".format(aggr_src)
    ]) for aggr_src in DM_AGGREGATION.keys()
])

replace_nulls = "\n".join([
    "UPDATE {{{{ params.schemaName }}}}.dm_report_fct SET {0}=0 WHERE {0} IS NULL;".format(aggr_var) \
    for aggr_var in aggr_flds
])

# Наполнить данными таблицу фактов, собрать из всех временных таблиц
facts_fill = PostgresOperator(
    task_id="facts_fill",
    dag=dag,
    sql="""
INSERT INTO {{{{ params.schemaName }}}}.dm_report_fct (
{fct_dim_refs}, is_vip,
{aggr_flds}
)
SELECT 
{dim_ids}, vip.is_vip,
{aggr_flds}
FROM {dim_tbls}
CROSS JOIN (SELECT DISTINCT is_vip FROM {{{{ params.schemaName }}}}.dds_t_sat_user_mdm) vip
{tmp_tbls}
WHERE report_year_key={{{{ execution_date.year }}}};

{replace_nulls}
""".format(
    fct_dim_refs=fct_dim_refs, dim_ids=dim_ids, aggr_flds=",\n".join(aggr_flds),
    dim_tbls=dim_tbls, tmp_tbls=tmp_tbls, replace_nulls=replace_nulls)
)

# ---------------------------------
# Удалить временные таблицы
drop_sql = '\n'.join(["DROP TABLE {{{{ params.schemaName }}}}.dm_report_{0}_oneyear;".format(dds_source) \
for dds_source in DM_AGGREGATION.keys()])

tmp_tbl_drop = PostgresOperator(
    task_id="tmp_tbl_drop",
    dag=dag,
    sql=drop_sql
)

## ОПРЕДЕЛИМ СТРУКТУРУ DAG'А
DummyOperator(task_id="datamart_start", dag=dag) >> tmp_tbl_collect >> \
DummyOperator(task_id="tmp_tbls_done", dag=dag) >> dimensions_fill >> \
DummyOperator(task_id="dims_done", dag=dag) >> facts_fill >> tmp_tbl_drop
