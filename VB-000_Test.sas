PROC IMPORT					
DATAFILE	= "D:\00. SAS Data Export\05. Setting_VBA\Setting_VBA.csv"
OUT			= Test_Import
DBMS		= csv REPLACE;
GETNAMES	= yes;
/*SHEET		= &Sheet_name;*/
RUN;

PROC EXPORT 
OUTFILE = 	"D:\00. SAS Data Export\Run_Test_VB.xlsx"
DATA	=	Test_Import dbms=xlsx replace;
SHEET	=	"Export_VBA";
RUN;
