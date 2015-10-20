DROP TABLE stock;
create table stock(date string , open float , high float , low float , close float, volume string , adjclose float ) row format delimited fields terminated by ',' lines terminated by '\n'
location 'hdfs:///data'
tblproperties ("skip.header.line.count"="1");
DROP TABLE stock1;
create table stock1(filename string ,date string, keydate string, adjclose float );
INSERT INTO TABLE stock1 SELECT regexp_replace(INPUT__FILE__NAME,'.*\//.*\/',''),date,substr(Date,0,7), adjclose FROM stock;
DROP TABLE stock;
DROP TABLE trading_days;
create table trading_days(file string , keydate string, first_day string, last_day  string);
INSERT INTO TABLE trading_days SELECT filename,keydate,min(date),max(date) FROM stock1 group by filename,keydate;
DROP TABLE find_xi;
create table find_xi(file string , adjclose_min float, adj_close_max float, xi float);
INSERT INTO TABLE find_xi SELECT t.file,s1.adjclose,s2.adjclose,(s2.adjclose-s1.adjclose)/s1.adjclose from trading_days as t
join stock1 as s1 on(s1.filename=t.file and s1.keydate=t.keydate)
join stock1 as s2 on(s2.filename=t.file and s2.keydate=t.keydate)
where s1.date=t.first_day and s2.date = t.last_day;
DROP TABLE stock1;
DROP TABLE trading_days;
DROP TABLE standard;
create table standard(file string, volatility float);
INSERT INTO TABLE standard select file, stddev_samp(xi) as volatility from find_xi group by file;
DROP TABLE find_xi;
DROP TABLE final;
create table final as select file,volatility, rank() over (order by volatility) as Min_stocks, rank() over (order by volatility DESC) as Max_stocks from standard where volatility > 0.0;







