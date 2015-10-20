A= LOAD 'hdfs:///pigdata/*.csv' using PigStorage(',','-tagFile') as (P:chararray,Q:chararray,R,S,T,U,V,W:float);
B = FOREACH A GENERATE $0,$1,$7;
C = FOREACH B GENERATE $0 as (file:chararray),FLATTEN(STRSPLIT($1,'-',3)) as (year:chararray,month:chararray,day:chararray),$2 as (adjclose:float);
D = GROUP C by (file,year,month); 
E = FOREACH D { 
EA = ORDER C by day; 
EB = LIMIT EA 1; 
GENERATE FLATTEN(group) as (file:chararray,year:chararray,month:chararray),FLATTEN(EB.day) as (day:chararray),FLATTEN(EB.adjclose) as(adjclose:float);
};
F = FOREACH D { 
FA = ORDER C by day DESC; 
FB = LIMIT FA 1; 
GENERATE FLATTEN(group) as (file:chararray,year:chararray,month:chararray),FLATTEN(FB.day) as (day:chararray),FLATTEN(FB.adjclose) as(adjclose:float);
};
G = JOIN E by (file,year,month),F by (file,year,month);
H = FOREACH G GENERATE E::file as file,(F::adjclose-E::adjclose)/E::adjclose as Xi;
I = GROUP H by file;
mean = FOREACH I { 
sum = SUM(H.Xi);
count = COUNT(H.Xi);
GENERATE FLATTEN(group) as (file:chararray) ,FLATTEN(H.Xi) as Xi, sum as sum, count as count, sum/count as avg; 
};
tmp = FOREACH mean { 
dif = (Xi - avg) * (Xi - avg); 
GENERATE file as (file:chararray), (Xi-avg)*(Xi-avg) as (dif:float), count;
}; 
grp = GROUP tmp by file;
standard_tmp = foreach grp generate flatten(group) as (file:chararray),flatten(tmp.dif), SUM(tmp.dif) as sqr_sum, flatten(tmp.count) as count; 
standard = foreach standard_tmp generate file, SQRT(sqr_sum / (count-1)) as volatility;
inter_tmp = GROUP standard by file;
inter1 = FOREACH inter_tmp {  
inter2 = LIMIT standard 1; 
GENERATE FLATTEN(group) as (file:chararray),FLATTEN(inter2.volatility) as (volatility:double);
};

inter1_tmp = FILTER inter1 by volatility!=0;

inter_final1 = ORDER inter1_tmp by volatility;
inter_final11 = LIMIT inter_final1 10;

inter_final2 = ORDER inter1_tmp by volatility DESC;
inter_final22 = LIMIT inter_final2 10;

final = UNION inter_final11,inter_final22;
STORE final into 'hdfs:///pigdata/hw3_out'; 
