%include '/opt/sas/spre/home/SASFoundation/sasautos/fcf_run_autoexec.sas';
%include '/opt/sas/spre/home/SASFoundation/sasautos/rts_run_autoexec.sas';

%rts_run_autoexec;

libname db_rtswl postgres  server="&SYSTCPIPHOSTNAME" port=5432   database="SharedServices" schema="rts_watch_list"  user=fsduser pass=Mercury7;
libname db_rtsrb postgres  server="&SYSTCPIPHOSTNAME" port=5432   database="SharedServices" schema="rts_rule_builder"  user=fsduser pass=Mercury7;


OPTIONS NOTES MPRINT MPRINTNEST SYMBOLGEN MLOGIC PAGESIZE=MAX LINESIZE=MAX;

%global ErrorAmount;
%global CheckAmount;
%global FullLoad;
%let ErrorAmount =0;
%let CheckAmount =0;
%let FullLoad =Y;

%macro fcf_execute_macro(macroname) ;
   %let macroname=&macroname;
   %let thename=source.sasmacr.&macroname..macro;
   %let workname=work.sasmacr.&macroname..macro;
   %if %sysfunc(cexist(&theName))  or
       %sysfunc(cexist(&workName)) %then
      %&macroname;
%mend fcf_execute_macro;



/*check if the row amount loaded into database is correct*/
%macro CheckRowAmount(list_name=,tableName=,ExpectedAmount=,EntityName=,LoadCSV=);

	%let CurrentCondition = where entity_watch_list_key in (select entity_watch_list_key from db_rtswl.rts_wl_entity where watch_list_name="&list_name.");
	%let CheckAmount = %eval(&CheckAmount + 1);
	proc sql noprint;
 		select count(*) into: RowCNT from db_rtswl.&tableName &CurrentCondition;
 	quit;
	%let SucessMsg = &list_name &EntityName row amount PASSED: expected: &ExpectedAmount ,actual: %trim(&RowCNT);
	%let FailMsg = &list_name. &EntityName row amount ERROR: mismatch row count, expected: &ExpectedAmount ,actual: %trim(&RowCNT);
	
	%if %trim(&FullLoad) eq %trim("N") %then %do ;
		%let CurrentCondition = and current_ind='N';
		%let SucessMsg = &list_name. &EntityName Current PASSED: expected: &ExpectedAmount ,actual: %trim(&RowCNT);
		%let FailMsg = &list_name. &EntityName Current ERROR: mismatch  row count, expected: &ExpectedAmount ,actual: %trim(&RowCNT);
	%end;
	
	*%put rowcnt= &RowCNT;
 	%let ExpectedAmount =%trim(&ExpectedAmount);
 	%if %trim(&RowCNT) eq %trim(&ExpectedAmount) %then %do;
 		%put &SucessMsg;
 	%end;
 	%else %do; 
		%let ErrorAmount = %eval(&ErrorAmount + 1);
 		%put &FailMsg;
 	%end;
 %mend;
 
/*check if data exist in the database: 1 - should exist, 0 - should not exist*/
%macro CheckDataExist(list_name=WCHK,tablename=,wherestm=,expected=);
	%let CheckAmount = %eval(&CheckAmount + 1);
	proc sql noprint;
		select count(*) into: AMT from db_rtswl.&tableName where &wherestm and entity_watch_list_key=&entity_watch_list_key and entity_watch_list_key in (select entity_watch_list_key from db_rtswl.rts_wl_entity where watch_list_name="&list_name.");
	quit; 

	%let msg = &list_name. &tableName data exist ERROR, condition = &wherestm,expected = &expected;
	%if %trim(&amt) eq %trim(&expected) %then %do;
		%let msg = &list_name. &tableName data exist Passed, condition = &wherestm,expected = &expected;
	%end;
	%else %do;
		%let ErrorAmount = %eval(&ErrorAmount + 1);
	%end;
	%put &msg;
%mend;

%macro LoadWatchList(list_name=WCHK,file_name=test.csv,full_load=Y);	
	%if &list_name eq WCHK %then %rts_wchk_process(wchk_file=&file_name, textsize=10000,    full_load=&full_load,	stage_lib=STG_WTCH,  	core_lib=db_rtswl 	); 	
	%if &list_name eq DWJN %then %rts_dwjn_process(dwjn_file=&file_name, allow_invalid=N,   full_load=&full_load,	stage_lib=STG_WTCH,  	core_lib=db_rtswl 	); 		
	%if &list_name eq ACUR %then %rts_dwjn_process(dwjn_file=&file_name, allow_invalid=N,   full_load=&full_load,	stage_lib=STG_WTCH,  	core_lib=db_rtswl 	); 					
	
%mend;


%macro VerifyLoadWatchList(list_name=WCHK,Full_file_name=test.csv,Ins_file_name=,SCD_file_name=,RowAmountDS=WCHK_RowAmount,DataExitDS=SCD2,LoadCSV=);
	
	/*full loading and check row amount*/		
	%if &LoadCSV eq Y %then 	%LoadWatchList(list_name=&list_name.,file_name=&Full_file_name,full_load=&FullLoad.);
	
	%let FullLoad = N;
	data aaa;
	 	set &RowAmountDS;	
		command = '%nrstr(%CheckRowAmount(list_name=&list_name.,tableName=' ||strip(tableName) ||',ExpectedAmount='|| FullCNT ||',EntityName=' ||strip(entityName) || '));';
		call execute(command);
		call execute("run;");	
	run;

	/*incrementally loading and check row amount*/	
	%if &LoadCSV eq Y %then %LoadWatchList(list_name=&list_name.,file_name=&Ins_file_name,full_load=&FullLoad.);
	data bbb;
	 	set &RowAmountDS;	
	 	command = '%nrstr(%CheckRowAmount(list_name=&list_name.,tableName=' ||strip(tableName) ||',ExpectedAmount='|| InsCNT ||',EntityName=' ||strip(entityName) || '));';
		call execute(command);
		call execute("run;");
	run;

	/*SCD2 loading and check row amount */
	%if &LoadCSV eq Y %then %LoadWatchList(list_name=&list_name.,file_name=&SCD_file_name,full_load=&FullLoad.);
	data ccc;
	 	set &RowAmountDS;	
	 	command = '%nrstr(%CheckRowAmount(list_name=&list_name.,tableName=' ||strip(tableName) ||',ExpectedAmount='|| SCDCNT ||',EntityName=' ||strip(entityName) || '));';
		call execute(command);
		call execute("run;");
	run;

	/*Check if SCD 2 changes are loaded into database correctly.*/
	data ddd;
		set &DataExitDS;	
		command = '%nrstr(%CheckDataExist(list_name=&list_name.,tablename='||strip(tablename)||',wherestm='||strip(wherestm)||',expected='||strip(expected)||'));';
		call execute(command);
	run;
%mend;


/*WCHK watch list
*%include "/home/centos/rts/watchlist_etl/sas/rts_wchk_import_loctype.sas";
*%include "/home/centos/rts/watchlist_etl/sas/rts_wchk_import_keyword.sas";
*/

%let entity_watch_list_key = 16;
%VerifyLoadWatchList(
			list_name=WCHK,
			Full_file_name=&wchk_path./premium-world-check-day.csv,
			Ins_file_name=&wchk_path./premium-world-check-day_ins.csv,
			SCD_file_name=&wchk_path./premium-world-check-day_SCD.csv,
			RowAmountDS=WCHK_RowAmount,
			DataExitDS=WCHK_DataExist,
			LoadCSV=Y
);


/*Dow Jones watch list*/
%let entity_watch_list_key = 14947;
%let entity_watch_list_number = '11090015';
%VerifyLoadWatchList(
			list_name=DWJN,
			Full_file_name=&dwjn_path./CSV_PFA_11090001_11100000_F.csv,
			Ins_file_name=&dwjn_path./CSV_PFA_11090001_11100000_F_ins.csv,
			SCD_file_name=&dwjn_path./CSV_PFA_11090001_11100000_F_scd.csv,
			RowAmountDS=DWJN_RowAmount,
			DataExitDS=DWJN_DataExist,
			LoadCSV=Y
);

%put Total check amount = &CheckAmount;
%put Total Error Amount=====&ErrorAmount;

data _null_;
	a =symget( "ErrorAmount");
	if a>"0" then put"Error: please check log, error found";
run;


