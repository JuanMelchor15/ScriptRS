---INSIS
--9 TABLAS

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
		cast (cast ("INSR_BEGIN" as date) as varchar),
		cast (cast ("INSR_END"as date) as varchar)
from	usinsiv01."INSURED_OBJECT"

select  ann."POLICY_ID",	
		cast (cast (ann."INSR_BEGIN" as date) as varchar),
		ann."ANNEX_TYPE",
		ann."ANNEX_STATE"
from    usinsiv01."GEN_ANNEX" ann

select	"CLAIM_ID",
		"OP_TYPE",
		cast (cast ("REGISTRATION_DATE" as date) as varchar),
		"REQUEST_ID"
from	usinsiv01."CLAIM_RESERVE_HISTORY"

select 	c.ctid,
		c."CLAIM_ID" ,
		c."POLICY_ID" ,
		cast (cast (c."EVENT_DATE"as date) as varchar) ,
		c."CLAIM_REGID"
from  	usinsiv01."CLAIM" c 

select 	p.ctid,
		p."POLICY_ID" ,
		cast (cast (p."INSR_END"as date) as varchar), 
		p."ATTR1" 
from 	usinsiv01."POLICY" p 


select 	co."CLAIM_ID" ,
		co."REQUEST_ID",
		co."COVER_TYPE" 
from usinsiv01."CLAIM_OBJECTS" co 




