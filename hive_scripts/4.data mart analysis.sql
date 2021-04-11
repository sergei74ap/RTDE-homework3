-- выбрать аналитику по наиболее качавшим пользователям в 2020 году и подцепить их жалобы в этом же году по определённой теме (услуге)
select t.*, i.* from dm_traffic t left join ods_issue i on (t.user_id=i.user_id) where t.year=2020 and i.year=2020 and i.service='Setup Environment' order by t.bts_received_avg desc limit 20;

-- выбрать топ наиболее активных пользователей 2020 года 
select * from dm_traffic where year=2020 order by bts_received_avg desc limit 20;

-- выбрать всех юзеров, которые в 2019 году качали больше среднего по всей клиентской базе 
select collect_set(user_id) from dm_traffic where year=2019 and bts_received_avg > (select avg(bts_received_avg) from dm_traffic where year=2019);
-- [10020,10030,10060,10070,10110,10120,10150,10170,10200,10240,10280,10300,10310,10320,10330,10340,10360,10390,
-- 10400,10420,10440,10460,10480,10510,10520,10530,10540,10550,10580,10590,10600,10620,10650,10660,10670,10690,
-- 10710,10740,10760,10770,10780,10790,10800,10820,10840,10850,10880,10890,10910,10920,10930,10950,10970,10980,
-- 10990,11000,11020,11030,11050,11060,11080,11100,11210,11220,11230]

-- выбрать пользователя с наименьшими закачками трафика за 2017 год
select user_id, bts_received_max, year from dm_traffic where year=2017 order by bts_received_max limit 1;

-- выбрать платежи этого пользователя
select * from ods_payment where year=2017 and user_id = (select user_id from dm_traffic where year=2017 order by bts_received_max limit 1) order by `sum` desc;

-- выбрать максимальные уровни среднегодовой заказчки и вывести по годам 
select year, max(bts_received_avg) from dm_traffic group by year;
