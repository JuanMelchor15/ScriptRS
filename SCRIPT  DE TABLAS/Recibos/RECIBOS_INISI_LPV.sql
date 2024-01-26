-----RECIBOS INSUNIX
--23 TABLAS V 

---LPV

select 	p.ctid,
		p."POLICY_ID" ,
		p."INSR_TYPE" ,
		p."ATTR1",
		p."POLICY_NO" ,
		p."ATTR4" ,
		p."ATTR1" 
from USINSIV01."POLICY" p 

select 	cnp."PRODUCT_CODE" ,
		cnp."PRODUCT_LINK_ID" 
from usinsiv01."CFG_NL_PRODUCT" cnp 

select 	cnpc."PRODUCT_LINK_ID", 
		cnpc."PARAM_CPR_ID" 
from usinsiv01."CFG_NL_PRODUCT_CONDS" cnpc 

select cp."PARAM_CPR_ID" 
from usinsiv01."CPR_PARAMS" cp 

select 	pc."POLICY_ID" ,
		pc."COND_DIMENSION" ,
		pc."COND_TYPE"
from usinsiv01."POLICY_CONDITIONS" pc 

select 	cpv."PARAM_ID" ,
		cpv ."PARAM_VALUE" ,
		cpv."PARAM_VALUE_CPR_ID" ,
		cpv."DESCRIPTION" 
from usinsiv01."CPRS_PARAM_VALUE" cpv 

select 	tb."TECHNICAL_BRANCH" ,
		tb."TB_NAME" ,
		tb."INSR_TYPE" ,
		tb."AS_IS_PRODUCT"
from usinsiv01."CFGLPV_POLICY_TECHBRANCH_SBS" tb 

select 	i."COMPONENT" ,
		i."ITEM_ID"
from USINSIV01."BLC_ITEMS" i

select 	tr."ITEM_ID",
		tr."DOC_ID",
		tr."TRANSACTION_ID" ,
		tr."CURRENCY" ,
		tr."PAID_STATUS" ,
		tr."ATTRIB_6" ,
		tr."ATTRIB_7" ,
		tr."TRANSACTION_TYPE" ,
		tr."AMOUNT" 
from USINSIV01."BLC_TRANSACTIONS" tr

select 	bdo."DOC_ID" ,
        bdo."DOC_CLASS" ,
        bdo."DOC_NUMBER" ,
        bdo."DOC_TYPE_ID" ,
        bdo."REF_DOC_ID" ,
        bdo."ISSUE_DATE" ,
        bdo."DUE_DATE" ,
        bdo."DOC_SUFFIX" ,
from USINSIV01."BLC_DOCUMENTS" bdo


SELECT 	a."ACTION_TYPE" ,
		a."ID" ,
		a."DOC_ID" ,
		a."DOC_NUMBER" 
FROM USINSIV01."BLC_PROFORMA_GEN" a

select  b."ID" ,
		b."INTER_TYPE"
from USINSIV01."BLC_PROFORMA_ACC" b


SELECT 	RCC."CLAIM_ID",
	   	RCC."CLAIM_OBJ_SEQ" 
FROM USINSIV01."RI_CEDED_CLAIMS" RCC 

SELECT	"POLICY_ID"
 		"MASTER_POLICY_ID" ,
 		"ENG_POL_TYPE" 
FROM USINSIV01."POLICY_ENG_POLICIES"

SELECT	 "AV_CURRENCY",
		 "POLICY_ID" 
FROM	USINSIV01."INSURED_OBJECT"


select  C."CLAIM_ID",
		c."POLICY_ID" ,
		c."CLAIM_REGID" ,
		c."EVENT_DATE" 
from usinsiv01."CLAIM" c 

select 	co."CLAIM_ID" ,
		co."REQUEST_ID" ,
		co."CLAIM_OBJ_SEQ" ,
		co."MAN_ID" 
from usinsiv01."CLAIM_OBJECTS" co 

SELECT	"CLAIM_ID",
		"OP_TYPE",
		"REGISTRATION_DATE",
		"OP_TYPE" ,
		"REQUEST_ID" ,
		"RESERV_AMNT" ,
		"RESERV_SEQ" 
FROM	USINSIV01."CLAIM_RESERVE_HISTORY"

SELECT 	GRC."DISCOUNT",
		GRC."POLICY_ID",
		GRC."ANNEX_ID"
FROM usinsiv01."GEN_RISK_COVERED" GRC 

SELECT 	GA."ANNEX_ID",
		GA."POLICY_ID" 
FROM usinsiv01."GEN_ANNEX" GA
                        

SELECT  B."ACTION_DATE",
		B."DOCUMENT_ID",
		B."ATTRIB_0",
		B."ACTION_TYPE_ID"	
FROM usinsiv01."BLC_ACTIONS" B  

select 	bi."TRANSACTION_ID",
		bi."ITEM_ID" ,
		bi."ATTRIB_4" ,
		bi."ATTRIB_5",
		bi."AMOUNT" 
from usinsiv01."BLC_INSTALLMENTS" bi 

select 	pp."POLICY_ID" ,
		pp."PARTICPANT_ROLE" ,
		pp."ANNEX_ID" 
from  usinsiv01."POLICY_PARTICIPANTS" pp


