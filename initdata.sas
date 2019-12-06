/*
For each watch list, it will load 3 CSV files, FULL loading, incremental loadingand SCD2 change loading.
The testing process will check if all data have been loaded and correct data is loaded.

1.ALL data are loaded:
	ROWAmount is the benchmark data set with expected row amount for each dim table
	Columns:
	 	entity: 	the entity name
		tableName:	the dim table name
	 	FullCNT: 	the expected ALL row amount in the full loading
	 	InsCNT: 	the expected of row amount in the incremental loading
 	 	SCDCNT:		the expected row amount in the 3rd loading, SCD
	 	SCDYCNT:	the expected CURRENT row amount in the 3nd loading 	 
	compare row amount of each loading and compare with benchmark value for each dim table.

2.Data is loaded correctly:
	DataExist is the dataset with expected data, it is used to verify the expected data is loaded during SCD2 change
	Columns:
		tableName: 	the dim/bridge table
		wherestm:	the where condition to check the data
		expected:	if the data should exist, for SCD2 table, it is 1, for non_SCD2 table, it is 0
	Check each dim table: 1)the updated data are set to not current; 2)the new data are set to current
	Be noticed the relationship bridge table is not SCD table, the table is wiped out prior to loading.
	If all validations pass, the result table should be empty.

*/


/*WCHK  watch list dataset */

data WCHK_RowAmount;	
 	infile datalines truncover dlm=', ';
 	input entityName :$50. tableName :$50. FullCNT :8.  InsCNT :8. SCDCNT:8. SCDYCNT :8.;
 	datalines ;
ADDRESS,rts_wl_address,12410,12430,12431,1
BIRTHDATE,rts_wl_birth_date,5182,5183,5185,1
BIRTHPLACE,rts_wl_birth_place,3148,3150,3152,0
COUNTRY,rts_wl_country,6498,6500,6502,0
ENTITY,rts_wl_entity,6506,6508,6510,1
ENTITYNAME,rts_wl_entity_name,23201,23278,23281,2
ID,rts_wl_identifier,3698,3698,3702,0
PROGRAM,rts_wl_program,24275,24322,24323,2
RELATIONSHIP,rts_wl_relationship_bridge,24551,24577,24576,0
 ;
run;

%global entity_watch_list_key entity_watch_list_number;


data WCHk_DataExist;* for SCD 2 change ;
	infile datalines truncover dlm='|';
	input tableName :$50. wherestm :$100. expected best.;
	datalines;
rts_wl_entity_name|last_name='AL ZAWAHIRI' and current_ind='N'|1
rts_wl_entity|terrorist_flg='Y' and pep_flg='Y' and current_ind='N'|1
rts_wl_entity|pep_flg='Y' and terrorist_flg='N' and current_ind='Y'|1
rts_wl_birth_date|date_of_birth='19Jun1951'd and current_ind='N'|1
rts_wl_birth_date|date_of_birth='01Jan2000'd and current_ind='Y'|1
rts_wl_birth_place|place_of_birth='LONDON, UNITED KINGDOM' and current_ind='Y'|1
rts_wl_identifier|identifier_number='777777' and current_ind='Y'|1
rts_wl_address|country_name='YEMEN' and current_ind='N'|1
rts_wl_country|entity_country_name='CHINA' and current_ind='Y'|1
rts_wl_entity|gender_cd='U' and current_ind='Y'|1
rts_wl_entity|gender_cd='M' and current_ind='N'|1
rts_wl_relationship_bridge|related_watch_list_number='7633'|1
rts_wl_relationship_bridge|related_watch_list_number='1009050'|0
	;
run;

/*DOW JONES watch list dataset*/

data DWJN_RowAmount;	
 	infile datalines truncover dlm=', ';
 	input entityName :$50. tableName :$50. FullCNT :8.  InsCNT :8. SCDCNT:8. SCDYCNT :8.;
 	datalines ;
ADDRESS,rts_wl_address,3304,3305,3306,2
BIRTHDATE,rts_wl_birth_date,7558,7559,7560,2
BIRTHPLACE,rts_wl_birth_place,2776,2776,2777,0
COUNTRY,rts_wl_country,22951,22952,22953,0
ENTITY,rts_wl_entity,8437,8438,8439,2
ENTITYNAME,rts_wl_entity_name,27923,27947,27949,5
ID,rts_wl_identifier,392,392,394,0
PROGRAM,rts_wl_program,1270,1271,1272,2
RELATIONSHIP,rts_wl_relationship_bridge,73403,73403,73404,0
 ;
run;

data DWJN_DataExist;* for SCD 2 change ;
	infile datalines truncover dlm='|';
	input tableName :$50. wherestm :$100. expected best.;
	datalines;
rts_wl_entity|terrorist_flg='N' and pep_flg='N' and current_ind='N'|1
rts_wl_entity|pep_flg='Y' and terrorist_flg='N' and current_ind='Y'|1
rts_wl_birth_date|date_of_birth='18Mar1984'd and current_ind='N'|1
rts_wl_birth_date|date_of_birth='31Jan2001'd and current_ind='Y'|1
rts_wl_birth_place|place_of_birth='BEIJING' and current_ind='Y'|1
rts_wl_address|country_name='CHINA' and current_ind='Y'|1
rts_wl_address|country_name='UKRAINE' and current_ind='N'|1
rts_wl_relationship_bridge|related_watch_list_number='11090021'|1
rts_wl_relationship_bridge|related_watch_list_number='1009050'|0
	;
run;


*******************************Test Data Done!!!*************************************************************;

/*
data DWJN_RowAmount;	
 	infile datalines truncover dlm=', ';
 	input entityName :$50. tableName :$50. FullCNT :8.  InsCNT :8. SCDCNT:8. SCDYCNT :8.;
 	datalines ;
ADDRESS,rts_wl_address,3230,3231,3232,1
BIRTHDATE,rts_wl_birth_date,7112,7113,7114,1
BIRTHPLACE,rts_wl_birth_place,1219,1219,1220,0
COUNTRY,rts_wl_country,20060,20061,20062,0
ENTITY,rts_wl_entity,7350,7351,7352,1
ENTITYNAME,rts_wl_entity_name,25916,25942,25945,3
ID,rts_wl_identifier,953,953,955,0
PROGRAM,rts_wl_program,932,933,933,0
RELATIONSHIP,rts_wl_relationship_bridge,72379,72380,72380,0
TRANSPORT,rts_wl_transport,0,0,0,0
 	;
run;

%let entity_watch_list_key = 7351;
%let entity_watch_list_number = '11090015';
data DWJN_DataExist;* for SCD 2 change ;
	infile datalines truncover dlm='|';
	input tableName :$50. wherestm :$100. expected best.;
	datalines;
rts_wl_entity|terrorist_flg='N' and pep_flg='N' and current_ind='N'|1
rts_wl_entity|pep_flg='Y' and terrorist_flg='N' and current_ind='Y'|1
rts_wl_birth_date|datepart(date_of_birth)='18Mar1984'd and current_ind='N'|1
rts_wl_birth_date|datepart(date_of_birth)='31Jan2001'd and current_ind='Y'|1
rts_wl_birth_place|place_of_birth='beijing' and current_ind='Y'|1
rts_wl_address|country_name='CHINA' and current_ind='N'|1
rts_wl_address|country_name='UKRAINE' and current_ind='Y'|1
rts_wl_relationship_bridge|related_watch_list_number='11090021'|1
rts_wl_relationship_bridge|related_watch_list_number='1009050'|0
	;
run;



check dupliate record in tables
select entity_watch_list_key,date_of_birth,year_of_birth, count(*)
from rts_watch_list.rts_wl_birth_date
group by entity_watch_list_key, date_of_birth,year_of_birth
having count(*) >1

*/


