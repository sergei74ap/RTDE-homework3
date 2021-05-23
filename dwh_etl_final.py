from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

USERNAME = 'sperfilyev'

# =============================================================
# Подготовим метаданные для кодогенерации SQL

ODS_SOURCES = (
    {'source_name': 'billing', 'date_field': 'created_at'}, 
    {'source_name': 'issue',   'date_field': 'start_time'}, 
    {'source_name': 'payment', 'date_field': 'pay_date'}, 
    {'source_name': 'traffic', 'date_field': 'traffic_time'},
)

MDM_SOURCES = (
    'user',
)

DDS_HUBS = (
    {'hub_name': 'user',            'etl_views': ['user_mdm', 'user_payment']},
    {'hub_name': 'account',         'etl_views': ['account']},
    {'hub_name': 'billing_period',  'etl_views': ['billing_period']},
    {'hub_name': 'paysys',          'etl_views': ['paysys']},
    {'hub_name': 'service',         'etl_views': ['service_issue', 'service_billing']},
    {'hub_name': 'tariff',          'etl_views': ['tariff']},
    {'hub_name': 'device',          'etl_views': ['device']},
)

DDS_LINKS = (
    'billing', 
    'issue', 
    'payment', 
    'traffic',
);

DDS_SATS = (
    {'sat_name': 'user',    'sat_source': 'payment', 'sat_context': ['phone']},
    {'sat_name': 'payment', 'sat_source': 'payment', 'sat_context': ['pay_doc_num', 'pay_sum']},
    {'sat_name': 'issue',   'sat_source': 'issue',   'sat_context': ['title', 'description', 'end_time']},
    {'sat_name': 'billing', 'sat_source': 'billing', 'sat_context': ['billing_sum']},
    {'sat_name': 'traffic', 'sat_source': 'traffic', 'sat_context': ['bytes_sent', 'bytes_received']},
    {'sat_name': 'device',  'sat_source': 'traffic', 'sat_context': ['device_ip_addr']}, 
)

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2013, 1, 1, 0, 0, 0)
}

dag = DAG(
    dag_id=USERNAME + '_dwh_etl_final',
    default_args=default_args,
    default_view='graph',
    description='DWH ETL final project by ' + USERNAME,
    schedule_interval="@yearly",
    max_active_runs=1,
    params={'schemaName': USERNAME},
)

# =============================================================
# ОПИШЕМ ВСЕ ОПЕРАЦИИ ЗАГРУЗКИ ДАННЫХ

# Перезагружаем данные из MDM в ODS 
mdm_reload = [
    PostgresOperator(
        task_id="mdm_{0}_reload".format(mdm_source), 
        params={'mdmSource': mdm_source},
        dag=dag,
        sql="""
TRUNCATE {{ params.schemaName }}.ods_t_{{ params.mdmSource }};
TRUNCATE {{ params.schemaName }}.ods_t_{{ params.mdmSource }}_hashed;

INSERT INTO {{ params.schemaName }}.ods_t_{{ params.mdmSource }} SELECT * FROM mdm."{{ params.mdmSource }}";

INSERT INTO {{ params.schemaName }}.ods_t_{{ params.mdmSource }}_hashed
    (SELECT v.*, '{{ execution_date }}'::DATE AS load_dts
     FROM {{ params.schemaName }}.ods_v_{{ params.mdmSource }}_etl AS v);"""
    ) for mdm_source in MDM_SOURCES
]

# ------------------------------------------------------------
# Перезагружаем данные из STG в ODS за один год
ods_reload = [
    PostgresOperator(
        task_id="ods_{0}_reload".format(ods_source['source_name']), 
        params={'odsSource': ods_source['source_name'], 'dateField': ods_source['date_field']},
        dag=dag,
        sql="""
DELETE FROM {{ params.schemaName }}.ods_t_{{ params.odsSource }} CASCADE
WHERE extract(YEAR FROM {{ params.dateField }}) = {{ execution_date.year }};

DELETE FROM {{ params.schemaName }}.ods_t_{{ params.odsSource }}_hashed CASCADE
WHERE extract(YEAR FROM effective_from) = {{ execution_date.year }};

INSERT INTO {{ params.schemaName}}.ods_t_{{ params.odsSource }}
    (SELECT * 
     FROM {{ params.schemaName }}.stg_v_{{ params.odsSource }} 
     WHERE extract(YEAR FROM {{ params.dateField }}) = {{ execution_date.year }});

INSERT INTO {{ params.schemaName}}.ods_t_{{ params.odsSource }}_hashed
    (SELECT v.*,
            '{{ execution_date }}'::DATE AS load_dts
     FROM {{ params.schemaName }}.ods_v_{{ params.odsSource }}_etl AS v
     WHERE extract(YEAR FROM v.effective_from) = {{ execution_date.year }});"""
    ) for ods_source in ODS_SOURCES
]

# ------------------------------------------------------------
# Заполняем хабы
dds_hubs_fill = []
for dds_hub in DDS_HUBS:
    one_hub_fill = [
        PostgresOperator(
            task_id="hub_{0}_fill".format(etl_view),
            dag=dag,
            sql="""
INSERT INTO {{{{ params.schemaName }}}}.dds_t_hub_{hub_name} 
SELECT * FROM {{{{ params.schemaName }}}}.dds_v_hub_{etl_view}_etl;
""".format(hub_name=dds_hub['hub_name'], etl_view=etl_view)
        ) for etl_view in dds_hub['etl_views']
    ]
    for i in range(len(one_hub_fill) - 1): 
        one_hub_fill[i] >> one_hub_fill[i + 1]
    dds_hubs_fill.append(one_hub_fill)

# ------------------------------------------------------------
# Заполняем линки
dds_links_fill = [
    PostgresOperator(
        task_id="lnk_{0}_fill".format(dds_link),
        dag=dag,
        sql="""
INSERT INTO {{{{ params.schemaName }}}}.dds_t_lnk_{0} 
SELECT * FROM {{{{ params.schemaName }}}}.dds_v_lnk_{0}_etl;
""".format(dds_link)
    ) for dds_link in DDS_LINKS
]

# ------------------------------------------------------------
## Заполняем сателлиты
def build_sat_sql(sat_name, sat_source, sat_context):
    return """
delete from {{{{ params.schemaName }}}}.dds_t_sat_{sat_name} 
where extract(year from load_dts) = {{{{ execution_date.year }}}};

insert into {{{{ params.schemaName }}}}.dds_t_sat_{sat_name}
with source_data as (
    select {sat_name}_pk, 
           {sat_name}_hashdiff,
           {sat_context_str},
           effective_from,
           load_dts,
           rec_source
    from {{{{ params.schemaName }}}}.ods_t_{sat_source}_hashed
    where extract(year from load_dts) = {{{{ execution_date.year }}}}
),
     update_records as (
         select s.*
         from {{{{ params.schemaName }}}}.dds_t_sat_{sat_name} as s
                  join source_data as src
                       on s.{sat_name}_pk = src.{sat_name}_pk and s.load_dts <= (select max(load_dts) from source_data)
     ),
     latest_records as (
         select *
         from (
                  select {sat_name}_pk, 
                         {sat_name}_hashdiff,
                         load_dts,
                         rank() over (partition by {sat_name}_pk order by load_dts desc) as row_rank
                  from update_records
              ) as ranked_recs
         where row_rank = 1
     ),
     records_to_insert as (
         select distinct a.*
         from source_data as a
                  left join latest_records
                            on latest_records.{sat_name}_hashdiff = a.{sat_name}_hashdiff and
                               latest_records.{sat_name}_pk = a.{sat_name}_pk
         where latest_records.{sat_name}_hashdiff is null
     )
select * from records_to_insert;
""".format(sat_name=sat_name, sat_source=sat_source, sat_context_str=', '.join(sat_context))

dds_sats_fill = [
    PostgresOperator(
        task_id="sat_{0}_fill".format(dds_sat['sat_name']),
        dag=dag,
        sql=build_sat_sql(
            sat_name=dds_sat['sat_name'], 
            sat_source=dds_sat['sat_source'],
            sat_context=dds_sat['sat_context'],
        )
    ) for dds_sat in DDS_SATS
]
dds_sats_fill.append(PostgresOperator(
        task_id="sat_user_mdm_fill",
        dag=dag,
        sql="INSERT INTO {{ params.schemaName }}.dds_t_sat_user_mdm SELECT * FROM {{ params.schemaName }}.dds_v_sat_user_mdm_etl;"
))

# =============================================================
## ОПРЕДЕЛИМ СТРУКТУРУ DAG'А

# Точки сборки
etl_start = DummyOperator(task_id="etl_start", dag=dag)
all_ods_reloaded = DummyOperator(task_id="all_ods_reloaded", dag=dag)
all_hubs_loaded = DummyOperator(task_id="all_hubs_loaded", dag=dag)
all_links_loaded = DummyOperator(task_id="all_links_loaded", dag=dag)
all_sats_loaded = DummyOperator(task_id="all_sats_loaded", dag=dag)

# Последовательность выполнения задач
etl_start >> mdm_reload >> all_ods_reloaded 
etl_start >> ods_reload >> all_ods_reloaded 

for one_hub in dds_hubs_fill:
    all_ods_reloaded >> one_hub[0]
    one_hub[-1] >> all_hubs_loaded

all_hubs_loaded >> dds_links_fill >> all_links_loaded >> dds_sats_fill >> all_sats_loaded
