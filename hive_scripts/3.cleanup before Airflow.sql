
-- Очистим таблицы в ODS
ALTER TABLE sperfilyev.ods_issue DROP PARTITION (year>2012);
ALTER TABLE sperfilyev.ods_billing DROP PARTITION (year>2012);
ALTER TABLE sperfilyev.ods_payment DROP PARTITION (year>2012);
ALTER TABLE sperfilyev.ods_traffic DROP PARTITION (year>2012);

show partitions ods_issue;
show partitions ods_billing;
show partitions ods_payment;
show partitions ods_traffic;

-- Очистим DM
ALTER TABLE sperfilyev.dm_traffic DROP PARTITION (year>2012);
show partitions dm_traffic;

