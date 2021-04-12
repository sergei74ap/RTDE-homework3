show databases;
use sperfilyev;
show tables;

set hive.execution.engine=mr; 
-- use traditional MapReduce engine instead of Tez
-- set hive.execution.engine=tez; 

select version();
-- get version of Hive
-- 3.1.2 on hadoop.rt.dadadata.ru


-- ===================================================================
-- STG layer

CREATE EXTERNAL TABLE stg_billing(user_id int, billing_period string, service string, tariff string, `sum` string, created_at string) STORED AS PARQUET LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-sperfilyev/data_lake/stg/billing';

CREATE EXTERNAL TABLE stg_issue(user_id string, start_time string, end_time string, title string, description string, service string) STORED AS PARQUET LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-sperfilyev/data_lake/stg/issue';

CREATE EXTERNAL TABLE stg_payment(user_id int, pay_doc_type string, pay_doc_num int, account string, phone string, billing_period string, pay_date string, `sum` string) STORED AS PARQUET LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-sperfilyev/data_lake/stg/payment';

CREATE EXTERNAL TABLE stg_traffic(user_id int, `timestamp` bigint, device_id string, device_ip_addr string, bytes_sent int, bytes_received int) STORED AS PARQUET LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-sperfilyev/data_lake/stg/traffic';


-- ПРОВЕРКИ:
show create table sperfilyev.stg_billing;
show create table sperfilyev.stg_issue;
show create table sperfilyev.stg_payment;
show create table sperfilyev.stg_traffic;

select distinct user_id from sperfilyev.stg_billing;
select distinct user_id from sperfilyev.stg_issue;
select distinct user_id from sperfilyev.stg_payment;
select distinct user_id from sperfilyev.stg_traffic;
-- 123 уникальных пользователя во всех таблицах

select count(distinct device_id) from stg_traffic;
-- 9 уникальных устройств, генерирующих трафик
-- d001, d002, d003, d004, d005, d006, d007, d008, d009

select min(`timestamp`), max(`timestamp`) from stg_traffic;
-- 1357150951268, 1615144951268
select min(from_unixtime(floor(`timestamp`/1000))), max(from_unixtime(floor(`timestamp`/1000))) from stg_traffic;
-- 2013-01-02 18:22:31, 2021-03-07 19:22:31

select min(billing_period), max(billing_period) from stg_billing;
-- 2013-01, 2021-03

select to_date(start_time), to_date(end_time) from stg_issue limit 20;
select * from stg_issue where year(start_time) between 2012 and 2014 limit 20;


-- ===================================================================
-- ODS layer

----- ISSUE -----
CREATE EXTERNAL TABLE sperfilyev.ods_issue(user_id int, start_time timestamp, end_time timestamp, title string, description string, service string) PARTITIONED BY (year string) STORED AS PARQUET LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-sperfilyev/data_lake/ods/issue';

INSERT OVERWRITE TABLE sperfilyev.ods_issue PARTITION (year=2021) SELECT * FROM sperfilyev.stg_issue WHERE year(start_time)=2021;
INSERT OVERWRITE TABLE sperfilyev.ods_issue PARTITION (year=2020) SELECT * FROM sperfilyev.stg_issue WHERE year(start_time)=2020;
INSERT OVERWRITE TABLE sperfilyev.ods_issue PARTITION (year=2019) SELECT * FROM sperfilyev.stg_issue WHERE year(start_time)=2019;
INSERT OVERWRITE TABLE sperfilyev.ods_issue PARTITION (year=2018) SELECT * FROM sperfilyev.stg_issue WHERE year(start_time)=2018;
INSERT OVERWRITE TABLE sperfilyev.ods_issue PARTITION (year=2017) SELECT * FROM sperfilyev.stg_issue WHERE year(start_time)=2017;
INSERT OVERWRITE TABLE sperfilyev.ods_issue PARTITION (year=2016) SELECT * FROM sperfilyev.stg_issue WHERE year(start_time)=2016;
INSERT OVERWRITE TABLE sperfilyev.ods_issue PARTITION (year=2015) SELECT * FROM sperfilyev.stg_issue WHERE year(start_time)=2015;
INSERT OVERWRITE TABLE sperfilyev.ods_issue PARTITION (year=2014) SELECT * FROM sperfilyev.stg_issue WHERE year(start_time)=2014;
INSERT OVERWRITE TABLE sperfilyev.ods_issue PARTITION (year=2013) SELECT * FROM sperfilyev.stg_issue WHERE year(start_time)=2013;

-- ПРОВЕРКИ:
show create table ods_issue;
show partitions ods_issue;
describe ods_issue;
describe extended ods_issue;

select min(start_time), max(start_time) from ods_issue;

select year(start_time), count(*) from stg_issue group by year(start_time);
select year(start_time), count(*) from ods_issue group by year(start_time);
select year, count(*) from ods_issue group by year;
-- 2013		1226
-- 2014		1229
-- 2015		1209
-- 2016		1211
-- 2017		1184
-- 2018		1232
-- 2019		1252		
-- 2020		1231
-- 2021		226
select year(start_time), count(*) from ods_issue group by year(start_time) having year(start_time) > 2015;
select year, count(*) from ods_issue group by year having year > 2015;

select min(start_time), max(start_time) from ods_issue;
-- 2013-01-01 22:22:31, 2021-03-06 22:22:31

--ALTER TABLE sperfilyev.ods_issue DROP PARTITION (year=2012);

 
----- BILLING -----
CREATE EXTERNAL TABLE sperfilyev.ods_billing(user_id int, billing_period string, service string, tariff string, `sum` decimal(10,2), created_at date) PARTITIONED BY (year string)STORED AS PARQUET LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-sperfilyev/data_lake/ods/billing';

INSERT OVERWRITE TABLE sperfilyev.ods_billing PARTITION (year=2021) SELECT * FROM sperfilyev.stg_billing WHERE year(created_at)=2021;
INSERT OVERWRITE TABLE sperfilyev.ods_billing PARTITION (year=2020) SELECT * FROM sperfilyev.stg_billing WHERE year(created_at)=2020;
INSERT OVERWRITE TABLE sperfilyev.ods_billing PARTITION (year=2019) SELECT * FROM sperfilyev.stg_billing WHERE year(created_at)=2019;
INSERT OVERWRITE TABLE sperfilyev.ods_billing PARTITION (year=2018) SELECT * FROM sperfilyev.stg_billing WHERE year(created_at)=2018;
INSERT OVERWRITE TABLE sperfilyev.ods_billing PARTITION (year=2017) SELECT * FROM sperfilyev.stg_billing WHERE year(created_at)=2017;
INSERT OVERWRITE TABLE sperfilyev.ods_billing PARTITION (year=2016) SELECT * FROM sperfilyev.stg_billing WHERE year(created_at)=2016;
INSERT OVERWRITE TABLE sperfilyev.ods_billing PARTITION (year=2015) SELECT * FROM sperfilyev.stg_billing WHERE year(created_at)=2015;
INSERT OVERWRITE TABLE sperfilyev.ods_billing PARTITION (year=2014) SELECT * FROM sperfilyev.stg_billing WHERE year(created_at)=2014;
INSERT OVERWRITE TABLE sperfilyev.ods_billing PARTITION (year=2013) SELECT * FROM sperfilyev.stg_billing WHERE year(created_at)=2013;

-- ПРОВЕРКИ:
select year(created_at), count(*) from ods_billing group by year(created_at);
select year, count(*) from ods_billing group by year;
-- 2013		1218
-- 2014		1221
-- 2015		1222
-- 2016		1214
-- 2017		1217
-- 2018		1256
-- 2019		1233		
-- 2020		1180
-- 2021		239

select min(created_at), max(created_at) from ods_billing;
-- 2013-01-01, 2021-03-18


----- PAYMENT -----
CREATE EXTERNAL TABLE sperfilyev.ods_payment(user_id int, pay_doc_type string, pay_doc_num int, account string, phone string, billing_period string, pay_date date, `sum` decimal(10,2)) PARTITIONED BY (year string) STORED AS PARQUET LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-sperfilyev/data_lake/ods/payment';

INSERT OVERWRITE TABLE sperfilyev.ods_payment PARTITION (year=2021) SELECT * FROM sperfilyev.stg_payment WHERE year(pay_date)=2021;
INSERT OVERWRITE TABLE sperfilyev.ods_payment PARTITION (year=2020) SELECT * FROM sperfilyev.stg_payment WHERE year(pay_date)=2020;
INSERT OVERWRITE TABLE sperfilyev.ods_payment PARTITION (year=2019) SELECT * FROM sperfilyev.stg_payment WHERE year(pay_date)=2019;
INSERT OVERWRITE TABLE sperfilyev.ods_payment PARTITION (year=2018) SELECT * FROM sperfilyev.stg_payment WHERE year(pay_date)=2018;
INSERT OVERWRITE TABLE sperfilyev.ods_payment PARTITION (year=2017) SELECT * FROM sperfilyev.stg_payment WHERE year(pay_date)=2017;
INSERT OVERWRITE TABLE sperfilyev.ods_payment PARTITION (year=2016) SELECT * FROM sperfilyev.stg_payment WHERE year(pay_date)=2016;
INSERT OVERWRITE TABLE sperfilyev.ods_payment PARTITION (year=2015) SELECT * FROM sperfilyev.stg_payment WHERE year(pay_date)=2015;
INSERT OVERWRITE TABLE sperfilyev.ods_payment PARTITION (year=2014) SELECT * FROM sperfilyev.stg_payment WHERE year(pay_date)=2014;
INSERT OVERWRITE TABLE sperfilyev.ods_payment PARTITION (year=2013) SELECT * FROM sperfilyev.stg_payment WHERE year(pay_date)=2013;

-- ПРОВЕРКИ:
select year(pay_date), count(*) from ods_payment group by year(pay_date);
select year, count(*) from ods_payment group by year;
-- 2013		1232
-- 2014		1249
-- 2015		1210
-- 2016		1237
-- 2017		1208
-- 2018		1229
-- 2019		1182		
-- 2020		1223
-- 2021		230

-- проверяем, что в системе-источнике только числовые значения в поле pay_doc_num
select distinct (cast(pay_doc_num as bigint) is null) from stg_payment;


----- TRAFFIC -----
CREATE EXTERNAL TABLE sperfilyev.ods_traffic(user_id int, `timestamp` timestamp, device_id string, device_ip_addr string, bytes_sent int, bytes_received int) PARTITIONED BY (year string) STORED AS PARQUET LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-sperfilyev/data_lake/ods/traffic';

INSERT OVERWRITE TABLE sperfilyev.ods_traffic PARTITION (year=2021) SELECT user_id, from_unixtime(floor(`timestamp`/1000)), device_id, device_ip_addr, bytes_sent, bytes_received FROM sperfilyev.stg_traffic WHERE year(from_unixtime(floor(`timestamp`/1000)))=2021;
INSERT OVERWRITE TABLE sperfilyev.ods_traffic PARTITION (year=2020) SELECT user_id, from_unixtime(floor(`timestamp`/1000)), device_id, device_ip_addr, bytes_sent, bytes_received FROM sperfilyev.stg_traffic WHERE year(from_unixtime(floor(`timestamp`/1000)))=2020;
INSERT OVERWRITE TABLE sperfilyev.ods_traffic PARTITION (year=2019) SELECT user_id, from_unixtime(floor(`timestamp`/1000)), device_id, device_ip_addr, bytes_sent, bytes_received FROM sperfilyev.stg_traffic WHERE year(from_unixtime(floor(`timestamp`/1000)))=2019;
INSERT OVERWRITE TABLE sperfilyev.ods_traffic PARTITION (year=2018) SELECT user_id, from_unixtime(floor(`timestamp`/1000)), device_id, device_ip_addr, bytes_sent, bytes_received FROM sperfilyev.stg_traffic WHERE year(from_unixtime(floor(`timestamp`/1000)))=2018;
INSERT OVERWRITE TABLE sperfilyev.ods_traffic PARTITION (year=2017) SELECT user_id, from_unixtime(floor(`timestamp`/1000)), device_id, device_ip_addr, bytes_sent, bytes_received FROM sperfilyev.stg_traffic WHERE year(from_unixtime(floor(`timestamp`/1000)))=2017;
INSERT OVERWRITE TABLE sperfilyev.ods_traffic PARTITION (year=2016) SELECT user_id, from_unixtime(floor(`timestamp`/1000)), device_id, device_ip_addr, bytes_sent, bytes_received FROM sperfilyev.stg_traffic WHERE year(from_unixtime(floor(`timestamp`/1000)))=2016;
INSERT OVERWRITE TABLE sperfilyev.ods_traffic PARTITION (year=2015) SELECT user_id, from_unixtime(floor(`timestamp`/1000)), device_id, device_ip_addr, bytes_sent, bytes_received FROM sperfilyev.stg_traffic WHERE year(from_unixtime(floor(`timestamp`/1000)))=2015;
INSERT OVERWRITE TABLE sperfilyev.ods_traffic PARTITION (year=2014) SELECT user_id, from_unixtime(floor(`timestamp`/1000)), device_id, device_ip_addr, bytes_sent, bytes_received FROM sperfilyev.stg_traffic WHERE year(from_unixtime(floor(`timestamp`/1000)))=2014;
INSERT OVERWRITE TABLE sperfilyev.ods_traffic PARTITION (year=2013) SELECT user_id, from_unixtime(floor(`timestamp`/1000)), device_id, device_ip_addr, bytes_sent, bytes_received FROM sperfilyev.stg_traffic WHERE year(from_unixtime(floor(`timestamp`/1000)))=2013;

-- ПРОВЕРКИ:
select year(`timestamp`), count(*) from ods_traffic group by year(`timestamp`);
select year, count(*) from ods_traffic group by year;
-- 2013		1133
-- 2014		1273
-- 2015		1152
-- 2016		1259
-- 2017		1227
-- 2018		1266
-- 2019		1239		
-- 2020		1233
-- 2021		218


-- ===================================================================
-- DM layer

CREATE EXTERNAL TABLE sperfilyev.dm_traffic(user_id int, bts_received_min int, bts_received_max int, bts_received_avg int, updated_at timestamp, updated_by string) PARTITIONED BY (year string) STORED AS PARQUET LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-sperfilyev/data_lake/dm/traffic';

INSERT OVERWRITE TABLE sperfilyev.dm_traffic PARTITION (year=2021) SELECT user_id, min(bytes_received), max(bytes_received), avg(bytes_received), current_timestamp, current_user() FROM sperfilyev.ods_traffic WHERE year=2021 GROUP BY user_id;
INSERT OVERWRITE TABLE sperfilyev.dm_traffic PARTITION (year=2020) SELECT user_id, min(bytes_received), max(bytes_received), avg(bytes_received), current_timestamp, current_user() FROM sperfilyev.ods_traffic WHERE year=2020 GROUP BY user_id;
INSERT OVERWRITE TABLE sperfilyev.dm_traffic PARTITION (year=2019) SELECT user_id, min(bytes_received), max(bytes_received), avg(bytes_received), current_timestamp, current_user() FROM sperfilyev.ods_traffic WHERE year=2019 GROUP BY user_id;
INSERT OVERWRITE TABLE sperfilyev.dm_traffic PARTITION (year=2018) SELECT user_id, min(bytes_received), max(bytes_received), avg(bytes_received), current_timestamp, current_user() FROM sperfilyev.ods_traffic WHERE year=2018 GROUP BY user_id;
INSERT OVERWRITE TABLE sperfilyev.dm_traffic PARTITION (year=2017) SELECT user_id, min(bytes_received), max(bytes_received), avg(bytes_received), current_timestamp, current_user() FROM sperfilyev.ods_traffic WHERE year=2017 GROUP BY user_id;
INSERT OVERWRITE TABLE sperfilyev.dm_traffic PARTITION (year=2016) SELECT user_id, min(bytes_received), max(bytes_received), avg(bytes_received), current_timestamp, current_user() FROM sperfilyev.ods_traffic WHERE year=2016 GROUP BY user_id;
INSERT OVERWRITE TABLE sperfilyev.dm_traffic PARTITION (year=2015) SELECT user_id, min(bytes_received), max(bytes_received), avg(bytes_received), current_timestamp, current_user() FROM sperfilyev.ods_traffic WHERE year=2015 GROUP BY user_id;
INSERT OVERWRITE TABLE sperfilyev.dm_traffic PARTITION (year=2014) SELECT user_id, min(bytes_received), max(bytes_received), avg(bytes_received), current_timestamp, current_user() FROM sperfilyev.ods_traffic WHERE year=2014 GROUP BY user_id;
INSERT OVERWRITE TABLE sperfilyev.dm_traffic PARTITION (year=2013) SELECT user_id, min(bytes_received), max(bytes_received), avg(bytes_received), current_timestamp, current_user() FROM sperfilyev.ods_traffic WHERE year=2013 GROUP BY user_id;


-- ПРОВЕРКИ:
show create table dm_traffic;
show partitions dm_traffic;

select count(*) from dm_traffic;
-- 1087

select distinct user_id from dm_traffic;
select count(distinct user_id) from dm_traffic;
-- 123

select distinct year from dm_traffic;
-- 2013
-- 2014
-- 2015
-- 2016
-- 2017
-- 2018
-- 2019		
-- 2020
-- 2021

select * from dm_traffic order by bts_received_avg desc limit 100;

select * from dm_traffic where user_id in (10170, 10360, 10820, 10590, 10010, 10440) order by user_id, year desc;

-- По исходным данным проверить случаи, когда min = max = avg. Скорее всего, в данном году для данного юзера есть только 1 запись. Верно?
select user_id, count(*) from ods_traffic where user_id in (10170, 10360, 10820, 10590, 10010, 10440) and year=2021 group by user_id;

-- Найти Топ-20 качков в 2020 году
select * from dm_traffic where year=2020 order by bts_received_avg desc limit 20;

-- Найти 10 наименее неактивных юзеров в 2015 году
select * from dm_traffic where year=2015 order by bts_received_avg limit 10;
