insert into SKEWED_STRING_LIST values(1);
insert into SKEWED_VALUES values(1,1,1);
insert into SKEWED_VALUES values(2,1,1);
insert into SKEWED_COL_VALUE_LOC_MAP values (1,1,"test");
insert into SKEWED_COL_VALUE_LOC_MAP values (2,1,"test1");
delete from SKEWED_VALUES where `SD_ID_OID`=1;
delete from SKEWED_COL_VALUE_LOC_MAP where `SD_ID` =1;
delete from SKEWED_COL_VALUE_LOC_MAP where `SD_ID` =2;
delete from SKEWED_VALUES where `SD_ID_OID`=2;