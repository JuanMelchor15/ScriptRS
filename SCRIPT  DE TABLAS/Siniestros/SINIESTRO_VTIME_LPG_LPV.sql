--------SINIESTRO LPG VTIME
--12 TABLA LPG
--13 TABLAS LPV

---LPG 

select	cast (cast ( "DOPERDATE" as date ) as varchar) ,
		"NCLAIM",
		"NOPER_TYPE",
		"NCASE_NUM" ,
		"NDEMAN_TYPE" ,
		"NTRANSAC" 
from	usvtimg01."CLAIM_HIS"

select	"NOPER_TYPE"
from	usvtimg01."TABLE140"


select	"SVALUE",
		"NCONDITION"
from	usvtimg01."CONDITION_SERV" cs 

select	cer."NPAYFREQ",
		cer."SCERTYPE",
		cer."NBRANCH",
		cer."NPOLICY",
		cer."NCERTIF",
		cer."NDIGIT"
from 	usvtimg01."CERTIFICAT" cer

select	cov."NCAPITAL",
		cov."SCERTYPE",
		cov."NBRANCH",
		cov."NPRODUCT",
		cov."NPOLICY",
		cov."NCERTIF",
		cast (cast (cov."DEFFECDATE" as date) as varchar),
		cast (cast ( cov."DNULLDATE" as date) as varchar) ,
		cov."NCOVER",
		cov."NMODULEC"
from    usvtimg01."COVER" cov

select  gen."NCOVER",
		gen."NPRODUCT",
		gen."NMODULEC",
		gen."NBRANCH",
		cast (cast (gen."DEFFECDATE" as date) as varchar),
		cast (cast ( gen."DNULLDATE" as date) as varchar),
		gen."SSTATREGT",
		gen."SADDSUINI"
from 	usvtimg01."LIFE_COVER" gen

select 	coi."NSHARE",
        coi."SCERTYPE",
        coi."NBRANCH",
        coi."NPRODUCT",
        coi."NPOLICY",
        coi."NCOMPANY",
        cast (cast (coi."DEFFECDATE"as date ) as varchar),
        cast (cast (coi."DNULLDATE" as date ) as varchar)
from	usvtimg01."COINSURAN" coi

select  cpl."NCURRENCY",
		cpl."SCERTYPE",
		cpl."NBRANCH" ,
		cpl."NPRODUCT" ,
		cpl."NPOLICY" ,
		cpl."NCERTIF"
from    usvtimg01."CURREN_POL" cpl

select 	pol.ctid,
		pol."SCERTYPE",
		pol."NPRODUCT",
		pol."SPOLITYPE",
		pol."SBUSSITYP",
		pol."NPOLICY" ,
		pol."NBRANCH" 
from 	usvtimg01."POLICY" pol

select 	cla.ctid,
		cla."NCLAIM",
		cla."SCERTYPE",
		cla."NPOLICY",
		cla."NBRANCH",
		cla."SSTACLAIM",
		cla."NPRODUCT",
		cla."NCERTIF",
		cast (cast (cla."DOCCURDAT"as date ) as varchar),
		cla."SCLIENT",
		cla."NCAUSECOD"
from 	usvtimg01."CLAIM" cla

select  "SCLIENT",
		"NCLAIM" ,
		"NCASE_NUM" ,
		"NDEMAN_TYPE" ,
		"NBENE_TYPE"
from    usvtimg01."CLAIMBENEF" 

select  clm."NCLAIM",
		clm."NCASE_NUM",
		clm."NDEMAN_TYPE" ,
		clm."NTRANSAC",
		clm."NCOVER",
		clm."NCURRENCY" ,
		clm."NAMOUNT",
		clm."NEXCHANGE",
		clm."NLOC_AMOUNT"
from  	usvtimg01."CL_M_COVER" clm

----LPV


select	cast (cast ("DOPERDATE" as date) as varchar),
		"NCLAIM",
		"NOPER_TYPE",
		"NCASE_NUM" ,
		"NDEMAN_TYPE" ,
		"NTRANSAC" 
from	usvtimv01."CLAIM_HIS"


select	"NOPER_TYPE"
from	usvtimv01."TABLE140"


select	"SVALUE",
		"NCONDITION" 		
from	usvtimv01."CONDITION_SERV" cs 

select	cer."NPAYFREQ",
		cer."SCERTYPE" ,
		cer."NBRANCH" ,
		cer."NPOLICY" ,
		cer."NCERTIF" ,
		cer."NDIGIT" 
from 	usvtimv01."CERTIFICAT" cer

select	cov."NCAPITAL",
		cov."SCERTYPE" ,
		cov."NBRANCH" ,
		cov."NPRODUCT" ,
		cov."NPOLICY" ,
		cov."NCERTIF" ,
		cast (cast (cov."DEFFECDATE" as date) as varchar) ,
		cast (cast (cov."DNULLDATE" as date) as varchar)
from    usvtimv01."COVER" cov

select 	lif."NCOVER",
		lif."NPRODUCT" ,
		lif."NMODULEC",
		lif."NBRANCH" ,
		cast (cast (lif."DEFFECDATE" as date) as varchar) ,
		cast (cast (lif."DNULLDATE" as date) as varchar) ,
		lif."SSTATREGT" ,
		lif."SADDSUINI"
from 	usvtimv01."LIFE_COVER" lif

select	gen."NCOVER" ,
		gen."NPRODUCT" ,
		gen."NMODULEC" ,
		gen."NBRANCH" ,
		cast (cast (gen."DEFFECDATE" as date) as varchar) ,
		cast (cast (gen."DNULLDATE" as date) as varchar) ,
		gen."SSTATREGT" ,
		gen."SADDSUINI" 
from    usvtimv01."GEN_COVER" gen   


select 	coi."NSHARE",
        coi."SCERTYPE" ,
        coi."NBRANCH" ,
        coi."NPRODUCT" ,
        coi."NPOLICY" ,
        coi."NCOMPANY" ,
        cast (cast (coi."DEFFECDATE" as date) as varchar)  ,
        cast (cast (coi."DNULLDATE"as date) as varchar)
from	usvtimv01."COINSURAN" coi

select  cpl."NCURRENCY",
		cpl."SCERTYPE" ,
		cpl."NBRANCH" ,
		cpl."NPRODUCT",
		cpl."NPOLICY" ,
		cpl."NCERTIF" 
from    usvtimv01."CURREN_POL" cpl

select 	pol.ctid,
		pol."SCERTYPE",
		pol."NPRODUCT",
		pol."SPOLITYPE",
		pol."SBUSSITYP",
		pol."NPOLICY" ,
		pol."NBRANCH" 
from 	usvtimv01."POLICY" pol

select 	cla.ctid,
		cla."NCLAIM",
		cla."SCERTYPE",
		cla."NPOLICY",
		cla."NBRANCH",
		cla."SSTACLAIM",
		cla."NPRODUCT",
		cla."NCERTIF",
		cast (cast (cla."DOCCURDAT" as date) as varchar),
		cla."SCLIENT",
		cla."NCAUSECOD"
from 	usvtimv01."CLAIM" cla

select  "SCLIENT",
		"NCLAIM" ,
		"NCASE_NUM" ,
		"NDEMAN_TYPE" ,
		"NBENE_TYPE"
from    usvtimv01."CLAIMBENEF" 

select  clm."NCLAIM",
		clm."NCASE_NUM",
		clm."NDEMAN_TYPE" ,
		clm."NTRANSAC",
		clm."NCOVER",
		clm."NCURRENCY" ,
		clm."NAMOUNT",
		clm."NEXCHANGE",
		clm."NLOC_AMOUNT"
from  	usvtimv01."CL_M_COVER" clm
