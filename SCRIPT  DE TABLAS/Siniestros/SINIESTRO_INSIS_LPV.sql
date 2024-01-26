---INSIS
--11 TABLAS

--LPV


select "MASTER_POLICY_ID" ,
	   "POLICY_ID"
from  usinsiv01."POLICY_ENG_POLICIES" 

select acc."OBJECT_ID", 
	   acc."MAN_ID",
	   acc."ACCINS_TYPE"
from   usinsiv01."O_ACCINSURED" acc

select  "MAN_ID" 
from usinsiv01."P_PEOPLE"

select  "AV_CURRENCY",
		"POLICY_ID" ,
		"OBJECT_ID" ,
		"INSURED_VALUE",
		"INSR_BEGIN",
		"INSR_END"
from	usinsiv01."INSURED_OBJECT"

select  ann."POLICY_ID",	
		ann."INSR_BEGIN",
		ann."ANNEX_TYPE",
		ann."POLICY_ID",
		ann."ANNEX_STATE"
from    usinsiv01."GEN_ANNEX" ann

select	"CLAIM_ID",
		"OP_TYPE",
		"REGISTRATION_DATE",
		"REQUEST_ID"
from	usinsiv01."CLAIM_RESERVE_HISTORY"

select 	c.ctid,
		c."CLAIM_ID" ,
		c."POLICY_ID" ,
		c."EVENT_DATE" ,
		c."CLAIM_REGID"
from  	usinsiv01."CLAIM" c 

select 	p.ctid,
		p."POLICY_ID" ,
		p."INSR_END", 
		p."ATTR1" 
from 	usinsiv01."POLICY" p 


select 	co."CLAIM_ID" ,
		co."REQUEST_ID",
		co."COVER_TYPE" 
from usinsiv01."CLAIM_OBJECTS" co 




