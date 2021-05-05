-- FUNCTION: rtapi.sp_get_member_insurance_quote_demandscore_by_card(character varying, character varying)

-- DROP FUNCTION rtapi.sp_get_member_insurance_quote_demandscore_by_card(character varying, character varying);

CREATE OR REPLACE FUNCTION rtapi.sp_get_member_insurance_quote_demandscore_by_card(
	p_card character varying,
	p_product character varying)
    RETURNS TABLE(crn character varying, isvalidandactive character varying, cardtype character varying, crnriskscore numeric, crndiscountscore numeric, created timestamp without time zone, modified timestamp without time zone, created_system integer, created_by character varying, modified_by character varying, status numeric) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE SECURITY DEFINER PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE

/***********************************************************************************
** Proc Name : public.sp_get_member_insurance_quote_demandscore_by_card
** Desc      : Procedure to get the member insurance quote demand score by providing the carda
** Auth      : Hari V
** Date      : 26-Jun-2020
**************************
** Change History
**************************
** Ver     Date                  Author                     Description
** --      --------               -------           ------------------------------------
** 1.0       26-Jun-2020         Hari V                Initial Draft
** 1.1       26-Oct-2020         Anil K                Fix the field mapping from cstmr_acct_type_model_score_val to cstmr_acct_type_model_dscnt_pctfor crnriskscore
      Add Distinct in the final SQL to remove exact duplicate
************************************************************************************/

isValid  character varying(3);
Cst_Chk character varying;
cardType character varying(3);
p_product_cd smallint;
a_stat_cd  smallint;
T_a_stat_cd  smallint;
T_cardType character varying(3);
countRow integer Default 0;
lv_n_rtn           INTEGER;
lv_t_err_msg       TEXT;
lv_t_err_dtl       TEXT;  

BEGIN

SELECT lcdm_type_data_cd INTO p_product_cd FROM   lcdm_main.cnsltd_rfrnc_table
WHERE  type_table_cd = 2 and btrim(upper(SUBSTR(lcdm_type_data_nm,1, POSITION(' ' IN lcdm_type_data_nm)))) = upper(p_product) AND lcdm_type_Data_cd in (3001,3002,3003,3004,3005,3006,3007);

 
 
IF p_product_cd IS NOT NULL  THEN -- Scenario#10

 RAISE NOTICE '2';
-- RETURN QUERY select concat( error_msg_cd , error_msg_nm )::character varying ,'N/A'::character varying, 'N/A'::character varying, NULL::numeric,NULL::numeric, NULL::timestamp without time zone, NULL::timestamp without time zone,
-- NULL::integer, NULL::character varying, 'NULL'::character varying  from lcdm_main.error  where error_Cndtn_nm ='INPUT_PARAM_ERROR';  

--ELSE

IF p_card IS NULL  THEN  

RAISE NOTICE '3';
 
RETURN QUERY select NULL::character varying  ,'NO'::character varying, NULL::character varying, NULL::numeric,NULL::numeric, NULL::timestamp without time zone,
NULL::timestamp without time zone, NULL::integer, NULL::character varying, NULL:: character varying, 0::numeric  ;
 
ELSE

RAISE NOTICE '4';
-- Used Account Table instead of Customer as to maintain good customer experience , RI DQ issue is to be fixed internally.
SELECT src_cstmr_num , CASE WHEN Acct_Type_Cd = 1002 THEN 'EDR' WHEN Acct_Type_Cd = 1003 THEN 'SDC' END, acct_status_cd INTO Cst_Chk, cardType, a_stat_cd FROM lcdm_main.account
WHERE src_acct_num = p_card  and row_status_cd in ('I','U') and acct_type_cd in (1002,1003) order by acct_rgstrd_dttm desc Limit 1 ;

IF Cst_Chk IS NULL   THEN

RAISE NOTICE '5';

SELECT acct_status_cd,CASE WHEN acct_type_cd = 1002 THEN 'EDR' WHEN acct_type_cd = 1003 THEN 'SDC' END INTO  T_a_stat_cd,T_cardType
FROM lcdm_main.unidentified_account WHERE src_acct_num = p_card ;
 

IF  T_cardType IS NOT NULL THEN -- Scenario#4

RAISE NOTICE '6';

RETURN QUERY select NULL::character varying  ,'YES'::character varying, T_cardType::character varying, NULL::numeric,NULL::numeric,
NULL::timestamp without time zone, NULL::timestamp without time zone, NULL::integer, NULL::character varying, NULL:: character varying, 0::numeric  ;

ELSE    -- Scenario#7

RAISE NOTICE '7';

RETURN QUERY select NULL::character varying  ,'NO'::character varying, NULL::character varying, NULL::numeric,NULL::numeric, NULL::timestamp without time zone, NULL::timestamp without time zone,
NULL::integer, NULL::character varying, NULL:: character varying, 0::numeric   ;
END IF;

ELSE
IF a_stat_cd <> 1 THEN -- Scenario#5

RAISE NOTICE '8';
RETURN QUERY select NULL::character varying  ,'NO'::character varying, NULL::character varying, NULL::numeric,NULL::numeric, NULL::timestamp without time zone,
NULL::timestamp without time zone, NULL::integer, NULL::character varying, NULL:: character varying, 0::numeric  ;

ELSE

---- Scenario# 8
PERFORM a.CRN,a.isValidAndActive,cardType,a.crnRiskScore,a.crnDiscountScore,a.totalsavings,a.totalsavingsty,a.created,a.modified , a.created_system,a.created_by,a.modified_by, 0::numeric As Status
FROM (
SELECT DISTINCT
CATMS.src_Cstmr_Num AS CRN,
'YES'::Character Varying As isValidAndActive,
CATMS.cstmr_acct_type_model_dscnt_pct AS crnRiskScore ,
CATMS.Cstmr_Acct_Type_Model_Dscnt_Pct AS crnDiscountScore,
CATMS.total_Savings AS totalsavings,
CATMS.total_Savings_ty AS totalsavingsty,
CATMS.insert_dttm  AS created ,   CATMS.update_dttm AS modified , SSC.lcdm_type_data_cd::integer  AS created_system,
CATMS.insert_by_user_cd As created_by,CATMS.update_by_user_cd AS modified_by,
row_number() OVER (PARTITION BY CATMS.src_Cstmr_Num, CATMS.Latest_Acct_Type_cd, CATMS.Acct_Type_cd ORDER BY MR.model_run_dttm DESC ,CATMS.Cstmr_Acct_Type_Model_Score_Start_Dttm ) AS rn

FROM
lcdm_main.customer_Account_type_model_score   CATMS INNER JOIN lcdm_main.model_run MR
ON (CATMS.Model_Run_id = MR.Model_Run_Id)
INNER JOIN lcdm_main.analytical_model AM
ON (CATMS.Model_id = AM.anlytcl_model_id AND MR.Model_Id = AM.anlytcl_model_id)
LEFT JOIN lcdm_main.cnsltd_rfrnc_table SSC ON (SSC.type_table_cd = 80  and SSC.src_type_data_nm = BTRIM(CATMS.Src_Sys_Cd))   -- Assign Src_system_Cd)

WHERE CATMS.Src_Cstmr_Num = Cst_Chk AND  CATMS.Acct_Type_Cd  = p_product_cd AND
--CATMS.Src_Cstmr_Num = '1000000000000002061' AND   CATMS.Acct_Type_Cd  = p_product_cd AND
AM.anlytcl_model_id = 1000001 AND CATMS.Cstmr_Acct_Type_Model_Score_End_Dttm = to_timestamp('9999/12/31 23:59:59', 'YYYY/MM/DD HH24:MI:SS'))a Where a.rn = 1 ;
RAISE NOTICE '82';

GET DIAGNOSTICS countRow := ROW_COUNT;
RAISE NOTICE '83';
IF countRow = 0 then
RAISE NOTICE 'Scneria 8';

RETURN QUERY select Cst_Chk::character varying  ,'YES'::character varying, cardType::character varying, NULL::numeric,NULL::numeric, NULL::timestamp without time zone,
NULL::timestamp without time zone, NULL::integer, NULL::character varying, NULL:: character varying, 0::numeric   ;
ELSE
RAISE NOTICE '84';
RETURN QUERY
SELECT a.CRN,a.isValidAndActive,cardType,a.crnRiskScore,a.crnDiscountScore,a.totalsavings,a.totalsavingsty,a.created,a.modified , a.created_system,a.created_by,a.modified_by,0::numeric  As Status
FROM (
SELECT DISTINCT
CATMS.src_Cstmr_Num AS CRN,
'YES'::Character Varying As isValidAndActive,
CATMS.cstmr_acct_type_model_dscnt_pct  AS crnRiskScore ,
CATMS.Cstmr_Acct_Type_Model_Dscnt_Pct AS crnDiscountScore,
CATMS.total_Savings AS totalsavings,
CATMS.total_Savings_ty AS totalsavingsty,
CATMS.insert_dttm  AS created ,   CATMS.update_dttm AS modified , SSC.lcdm_type_data_cd::integer  AS created_system,
CATMS.insert_by_user_cd As created_by,CATMS.update_by_user_cd AS modified_by,
row_number() OVER (PARTITION BY CATMS.src_Cstmr_Num, CATMS.Latest_Acct_Type_cd, CATMS.Acct_Type_cd ORDER BY MR.model_run_dttm DESC ,CATMS.Cstmr_Acct_Type_Model_Score_Start_Dttm ) AS rn

FROM
lcdm_main.customer_Account_type_model_score   CATMS INNER JOIN lcdm_main.model_run MR
ON (CATMS.Model_Run_id = MR.Model_Run_Id)
INNER JOIN lcdm_main.analytical_model AM
ON (CATMS.Model_id = AM.anlytcl_model_id AND MR.Model_Id = AM.anlytcl_model_id)
LEFT JOIN lcdm_main.cnsltd_rfrnc_table SSC ON (SSC.type_table_cd = 80  and SSC.src_type_data_nm = BTRIM(CATMS.Src_Sys_Cd))   -- Assign Src_system_Cd)
 
WHERE CATMS.Src_Cstmr_Num = Cst_Chk AND  CATMS.Acct_Type_Cd  = p_product_cd AND
--CATMS.Src_Cstmr_Num = '1000000000000002061' AND   CATMS.Acct_Type_Cd  = p_product_cd AND
AM.anlytcl_model_id = 1000001 AND CATMS.Cstmr_Acct_Type_Model_Score_End_Dttm = to_timestamp('9999/12/31 23:59:59', 'YYYY/MM/DD HH24:MI:SS'))a Where a.rn =1 ;
END IF;

END IF;

RAISE NOTICE '81';

END IF;

 
END IF;

END IF;

EXCEPTION
   WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS
       lv_t_err_msg  = MESSAGE_TEXT,
       lv_t_err_dtl  = PG_EXCEPTION_DETAIL;
          raise notice 'error, %--%',lv_t_err_msg,lv_t_err_dtl;
       lv_n_rtn := lcdm_main.fn_log_error('get_member_insurance_quote_demandscore_by_crn', 'get_member_insurance_quote_demandscore_by_crn', lv_t_err_msg, lv_t_err_dtl);

        RETURN QUERY select NULL::character varying  ,NULL::character varying, NULL::character varying, NULL::numeric,NULL::numeric, NULL::timestamp without time zone, NULL::timestamp without time zone,
    NULL::integer, NULL::character varying, NULL:: character varying, 1::numeric  ;

       IF lv_n_rtn IS NULL THEN
          RAISE EXCEPTION 'Error Get failed for sp_get_member_preference_by_member_crn';
       END IF;
 
END;
$BODY$;
