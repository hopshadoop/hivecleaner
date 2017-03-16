insert into CDS values(62);
-- insert into CDS values(63);
insert into SERDES values(6482, NULL, 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe');
-- insert into SERDES values(6483, NULL, 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe');
insert into SDS values (6482, 62, 'org.apache.hadoop.mapred.TextInputFormat', 0, 0, 'hdfs://10.0.2.15:8020/user/glassfish/benchmark/2/customer', 'customer', -1, 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat', 335489, 335489, 6482);
insert into SDS values (6483, 62, 'org.apache.hadoop.mapred.TextInputFormat', 0, 0, 'hdfs://10.0.2.15:8020/user/glassfish/benchmark/2/customer', 'customer', -1, 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat', 335489, 335489, 6482);
delete from SDS where SD_ID=6482;
delete from SDS where SD_ID=6483;
-- insert into CDS values (62);