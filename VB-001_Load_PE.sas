
LIBNAME LPS			'D:\00. SAS Data Export\MASTER DATA\03. LPS';

%MACRO importpay (Sas_table, Sheet_name);
PROC IMPORT					
	DATAFILE	= "D:\03. DBS Data\Raw_table\Payment_table_2024.xlsx"
	OUT			= &Sas_table
	DBMS		= xlsx REPLACE;
	GETNAMES	= yes;
	SHEET		= &Sheet_name;
RUN;
%MEND;

%importpay(Request_2024			, Request);
%importpay(Processsequence_2024	, Processsequence);
%importpay(Popayment_2024		, Popayment);
%importpay(Distribution_2024	, Distribution);
%importpay(Trxn_pay_2024		, TRANSACTION_PAYMENT);
%importpay(Trxn_state_2024		, TRANSACTION_STATEMENT);

/*-------------------------02. REQUEST ------------------------*/
DATA Request_2024; Set Request_2024; DROP PAYEEID PAYEEISSDATE DELEGATEID DELEGATEISSDATE EXT06 EXT07 EXT08 EXT09; RUN;
DATA Request_2024; SET Request_2024; YEAR = year(datepart(MODIFIEDDATE)); RUN;
DATA Request_BK; SET LPS.Request; WHERE Year not in (2024, .); RUN;
DATA LPS.Request; SET Request_BK Request_2024; RUN;

/*-------------------------03. PROCESSSEQUENCE ------------------------*/
DATA Processsequence_2024; SET Processsequence_2024;
DROP  ext03 ext04 ext05 ext06 ext07 ext08 ext09;
YEAR = year(datepart(MODIFIEDDATE)); RUN;
DATA Processsequence_bk; SET LPS.processsequence; WHERE Year not in (2024, .); RUN;
DATA LPS.processsequence; SET Processsequence_bk Processsequence_2024 ; RUN;

/*-------------------------04. POPAYMENT------------------------*/
DATA Popayment_2024; 	SET Popayment_2024; 
DROP DELEGATEID; YEAR = YEAR(datepart(MODIFIEDDATE)) ;RUN;
DATA POPAYMENT_bk; 		SET LPS.POPAYMENT; WHERE Year not in (2024, .); RUN;
DATA LPS.POPAYMENT; 	SET POPAYMENT_bk Popayment_2024 ; RUN;

/*-------------------------05. DISTRIBUTION ------------------------*/
DATA Distribution_20242; SET Distribution_2024; YEAR = year(datepart(T_MODIFIEDDATE)); S_ACCCODE2 = INPUT(S_ACCCODE, comma10.); RUN;
DATA Distribution_2024; SET Distribution_20242; DROP S_ACCCODE; RENAME S_ACCCODE2=S_ACCCODE; RUN;
DATA Distribution_bk; SET LPS.Distribution; WHERE Year not in (2024, .); RUN;
DATA LPS.Distribution; SET Distribution_bk Distribution_2024; RUN;

/*-------------------------06. TRANSACTION_PAYMENT ------------------------*/
DATA Trxn_pay_20242; SET Trxn_pay_2024;
DROP payment_key retry_time delegate_dob delegate_email aia_account_number _rid _self _etag	_attachments _ts;
trans_amount2=INPUT(trans_amount,best12.);
bank_aia_code2=INPUT(bank_aia_code,best12.);
bank_partner_code2=INPUT(bank_partner_code,best12.);
delegate_id2=INPUT(delegate_id,best12.);
delegate_telephone2=INPUT(delegate_telephone,best12.);
created_date2= DATEPART(INPUT(created_date,anydtdtm40.));
updated_date2= DATEPART(INPUT(updated_date,anydtdtm40.));
final_payment_time2= DATEPART(INPUT(final_payment_time,anydtdtm40.));
format created_date2 updated_date2 final_payment_time2 datetime.;
YEAR = year(datepart(INPUT(updated_date,anydtdtm40.)));
RUN;

DATA Trxn_pay_20243; SET Trxn_pay_20242;
DROP trans_amount bank_aia_code bank_partner_code delegate_id created_date updated_date final_payment_time delegate_telephone;
RENAME trans_amount2=trans_amount bank_aia_code2=bank_aia_code bank_partner_code2=bank_partner_code delegate_id2=delegate_id
created_date2=created_date updated_date2=updated_date final_payment_time2=final_payment_time delegate_telephone2=delegate_telephone;
; RUN;
DATA Trxn_pay_bk; SET LPS.TRANSACTION_PAYMENT; WHERE Year not in (2024, .); RUN;
DATA LPS.Transaction_Payment; SET Trxn_pay_bk Trxn_pay_20243 ; RUN;


/*-------------------------07. TRANSACTION_STATEMENT ------------------------*/

DATA Trxn_state_20242; SET Trxn_state_2024; 
citad_reference_code2=INPUT(citad_reference_code,best12.);
transaction_amount2=INPUT(transaction_amount,best12.);
created_date2= DATEPART(INPUT(created_date,anydtdtm40.));
payment_date2= DATEPART(INPUT(payment_date,anydtdtm40.));
updated_date2= DATEPART(INPUT(updated_date,anydtdtm40.));
return_date2= DATEPART(INPUT(return_date,anydtdtm40.));
statement_time2= DATEPART(INPUT(statement_time,anydtdtm40.));
format created_date2 updated_date2 payment_date2 return_date2 statement_time2 YYMMDD10.;
YEAR = year(datepart(INPUT(updated_date,anydtdtm40.))); RUN;

DATA Trxn_state_20243; SET Trxn_state_20242; 
DROP citad_reference_code created_date payment_date updated_date return_date statement_time transaction_amount;
RENAME citad_reference_code2=citad_reference_code created_date2=created_date payment_date2=payment_date updated_date2=updated_date
return_date2=return_date statement_time2=statement_time transaction_amount2=transaction_amount;
RUN;

DATA Trxn_state_bk; SET LPS.TRANSACTION_STATEMENT; WHERE Year not in (2024, .); RUN;
DATA LPS.TRANSACTION_STATEMENT; SET Trxn_state_bk Trxn_state_20243 ; RUN;

/*-------------------------CHECK RECORDS ------------------------*/

PROC SQL; CREATE TABLE count_record AS 
	  SELECT 'REQUEST' as tb_nm, year, count(*) as vol FROM lps.REQUEST WHERE YEAR = 2024 GROUP BY year 
UNION SELECT 'PROCESSSEQUENCE' as tb_nm, year, count(*) as vol  FROM lps.PROCESSSEQUENCE WHERE YEAR = 2024 group by year 
UNION SELECT 'Distribution' as tb_nm, year, count(*) as vol  FROM lps.DISTRIBUTION WHERE YEAR = 2024 group by year
UNION SELECT 'POPAYMENT' as tb_nm, year, count(*) as vol  FROM lps.POPAYMENT  WHERE YEAR = 2024 group by year 
UNION SELECT 'Trnx_pay' as tb_nm, year, count(*) as vol  FROM lps.TRANSACTION_PAYMENT WHERE YEAR = 2024 group by year 
UNION SELECT 'trxn_state' as tb_nm, year, count(*) as vol  FROM lps.TRANSACTION_STATEMENT WHERE YEAR = 2024 group by year
;QUIT;

PROC EXPORT 
OUTFILE = 	"D:\00. SAS Data Export\Load_PE_log.xlsx"
DATA	=	count_record dbms=xlsx replace;
SHEET	=	"Total_records_load";
RUN;
