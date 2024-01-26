
-----RECIBOS VTIME 
--21 TABLAS G
--15 TABLAS V

---LPG 

select	dp0."NPREMIUM",
		dp0."NRECEIPT",
		dp0."NBILL_ITEM" ,
		dp0."NBRANCH_LED" ,
		dp0."NDIGIT" ,
		dp0."NDET_CODE" ,
		dp0."STYPE_DETAI" ,
		dp0."NCAPITAL",
		dp0."SADDSUINI" ,
		dp0."NMODULEC" ,
		dp0 ."NBRANCH_REI" 
from	usvtimg01."DETAIL_PRE" dp0

select 	p.ctid,
		p."NRECEIPT" ,
		p."NDIGIT" ,
		p."DEXPIRDAT" ,
		p."DNULLDATE" ,
		p."SSTATUSVA" ,
		p."SCERTYPE" ,
		p."NPOLICY" ,
		p."DEFFECDATE",
		p."NPRODUCT" ,
		p."NBRANCH" 
from usvtimg01."PREMIUM" p 

select 	p.ctid,
		p."SCERTYPE" ,
		p."NPRODUCT" ,
		p."NBRANCH" 
from usvtimg01."POLICY" p 

select 	c."SCERTYPE" ,
		c."NBRANCH" ,
		c."NPOLICY" ,
		c."NCERTIF" ,
		c."DOCCURDAT" ,
		c."SSTACLAIM" ,
		c."NBRANCH" ,
		c."NPOLICY" 
from usvtimg01."CLAIM" c 

select 	cmc."NCLAIM" ,
		cmc."NCASE_NUM" ,
		cmc."NDEMAN_TYPE" ,
		cmc."NTRANSAC" ,
		cmc ."NAMOUNT" ,
		cmc."NCURRENCY" ,
		cmc ."NEXCHANGE" ,
		cmc ."NLOC_AMOUNT" ,
		cmc ."NCOVER" 
from usvtimg01."CL_M_COVER" cmc

select 	c."SCERTYPE" ,
		c."NBRANCH" ,
		c."NPRODUCT" ,
		c."NPOLICY" ,
		c."NCOMPANY" ,
		c."DEFFECDATE" ,
		c."DNULLDATE" ,
		c."DCOMPDATE" ,
		c."NCOMPANY" ,
		c."NSHARE" 
from usvtimg01."COINSURAN" c 

select 	p."NPRODUCT" ,
		p."NLEADSHARE" ,
		p."DCOMPDATE" ,
		p."SCERTYPE" ,
		p."SBUSSITYP" ,
		p."NPOLICY" ,
		p."NBRANCH" 
from usvtimg01."POLICY" p 

select	cs."SVALUE"  ,
		cs."NCONDITION"
from	usvtimg01."CONDITION_SERV" cs

select 	ch."NCLAIM" ,
		ch."NOPER_TYPE" ,
		ch."DOPERDATE" ,
		ch."NCASE_NUM" ,
		ch."NDEMAN_TYPE" ,
		ch."NTRANSAC" ,
		ch."SCLIENT" 
from usvtimg01."CLAIM_HIS" ch 

select 	csp."NTRANSAC",
		csp."NCLAIM" ,
		csp."NCASE_NUM" ,
		csp."NCASE_NUM" ,
		csp."NTRANSAC" 
from	usvtimg01."WBSTBLCLAIM_DOC_SAP"  csp

select  cpl."NCURRENCY",
		cpl."SCERTYPE" ,
		cpl."NBRANCH" ,
		cpl."NPRODUCT" ,
		cpl."NPOLICY" ,
		cpl."NCERTIF" 
from    usvtimg01."CURREN_POL" cpl

select	REI."NSHARE",
		REI."SCERTYPE" ,
		REI."NBRANCH" ,
		REI."NPRODUCT" ,
		REI."NPOLICY" ,
		REI."NCERTIF" ,
		REI."NBRANCH_REI" ,
		REI."NTYPE_REIN" ,
		REI."DEFFECDATE",
		REI."DNULLDATE"
from	usvtimg01."REINSURAN" REI

select 	cov."NPREMIUM" ,
		cov."NCOVER" ,
		cov."NPRODUCT" ,
		cov."NMODULEC" ,
		cov."NBRANCH" ,
		cov."SCERTYPE" ,
		cov."NCERTIF" ,
		cov."DEFFECDATE" ,
		cov."DNULLDATE" 
from   usvtimg01."COVER" cov

select 	lc."NCOVER" ,
		lc."NPRODUCT" ,
		lc."NMODULEC" ,
		lc."NBRANCH" ,
		lc."DEFFECDATE" ,
		lc."DNULLDATE" ,
		lc."SSTATREGT" ,
		lc."NBRANCH_LED" ,
		lc."NBRANCH_REI" 
from usvtimg01."LIFE_COVER" lc 

select 	gen."NCOVER" ,
		gen ."NPRODUCT" ,
		gen."NMODULEC" ,
		gen."NBRANCH" ,
		gen."DEFFECDATE" ,
		gen."DNULLDATE" ,
		gen."SSTATREGT" ,
		gen."NBRANCH_LED" ,
		gen."NCOVERGEN" 
from usvtimg01."GEN_COVER" gen

select	cer."DDATE_ORIGI" ,
		cer."SCERTYPE" ,
		cer."NBRANCH" ,
		cer."NPOLICY" ,
		cer."NCERTIF" ,
		cer."NDIGIT" 
from 	usvtimg01."CERTIFICAT" cer

select	"SCLIENT_VGT",
		"SCLIENT_OLD" 
from	usvtimg01."WBSTBLCLIDEPEQUI" 

select	"DSTATDATE",
		"NRECEIPT",
		"NDIGIT" ,
		"NTYPE" 
from	usvtimg01."PREMIUM_MO"

select 	dp1."NRECEIPT" ,
		dp1."NDIGIT" ,
		dp1."NBRANCH_REI" ,
		dp1."NMODULEC" ,
		dp1."STYPE_DETAI" ,
		dp1."NBILL_ITEM" ,
		dp1."SCERTYPE" ,
		dp1."NBRANCH" ,
		dp1."NPOLICY" ,
		dp1."NCERTIF" 
from	usvtimg01."PREMIUM_CE" dp1

select  cnm."NNUMBER" ,
		cnm ."NBRANCH" ,
		cnm ."NTYPE" ,
		cnm ."NTYPE_REL" ,
		cnm ."DSTARTDATE" 
from usvtimg01."CONTRMASTER" cnm

select 	pcr."NTYPE_REL" ,
		pcr."NNUMBER" ,
		pcr ."NBRANCH" ,
		pcr."DSTARTDATE" ,
		pcr."DNULLDATE" ,
		pcr."NTYPE" ,
		pcr."NCOMPANY" ,
		pcr."NCOMISION" ,
		pcr."NSHARE" 
from 	usvtimg01."PART_CONTR" pcr

----------------------------LPV

select	dp0."NPREMIUM",
		dp0."NRECEIPT",
		dp0."NBILL_ITEM" ,
		dp0."NBRANCH_LED" ,
		dp0."NDIGIT" ,
		dp0."NDET_CODE" ,
		dp0."STYPE_DETAI" ,
		dp0."NCAPITAL",
		dp0."SADDSUINI" 
from	usvtimv01."DETAIL_PRE" dp0

select 	p.ctid,
		p."NRECEIPT" ,
		p."NDIGIT" ,
		p."DEXPIRDAT" ,
		p."DNULLDATE" ,
		p."SSTATUSVA" ,
		p."SCERTYPE" ,
		p."NPOLICY" ,
		p."DEFFECDATE" 
from usvtimv01."PREMIUM" p 


select 	p."SCERTYPE" ,
		p."NPRODUCT" ,
		p."NBRANCH" ,
		p."SPOLITYPE" 
from usvtimv01."POLICY" p 

select 	c."SCERTYPE" ,
		c."NBRANCH" ,
		c."NPOLICY" ,
		c."NCERTIF" ,
		c."DOCCURDAT" ,
		c."SSTACLAIM" ,
		c."NBRANCH" ,
		c."NPOLICY" 
from usvtimv01."CLAIM" c 

select 	cmc."NCLAIM" ,
		cmc."NCASE_NUM" ,
		cmc."NDEMAN_TYPE" ,
		cmc."NTRANSAC" ,
		cmc ."NAMOUNT" ,
		cmc."NCURRENCY" ,
		cmc ."NEXCHANGE" ,
		cmc ."NLOC_AMOUNT" ,
		cmc ."NCOVER" 
from usvtimv01."CL_M_COVER" cmc

select 	c."SCERTYPE" ,
		c."NBRANCH" ,
		c."NPRODUCT" ,
		c."NPOLICY" ,
		c."NCOMPANY" ,
		c."DEFFECDATE" ,
		c."DNULLDATE" ,
		c."DCOMPDATE" ,
		c."NCOMPANY" ,
		c."NSHARE" 
from usvtimv01."COINSURAN" c 

select 	p."NPRODUCT" ,
		p."NLEADSHARE" ,
		p."DCOMPDATE" ,
		p."SCERTYPE" ,
		p."SBUSSITYP" ,
		p."NPOLICY" ,
		p."NBRANCH" 
from usvtimv01."POLICY" p 

select	cs."SVALUE"  ,
		cs."NCONDITION"
from	usvtimv01."CONDITION_SERV" cs

select 	ch."NCLAIM" ,
		ch."NOPER_TYPE" ,
		ch."DOPERDATE" ,
		ch."NCASE_NUM" ,
		ch."NDEMAN_TYPE" ,
		ch."NTRANSAC" ,
		ch."SCLIENT" 
from usvtimv01."CLAIM_HIS" ch 

select 	csp."NTRANSAC",
		csp."NCLAIM" ,
		csp."NCASE_NUM" ,
		csp."NCASE_NUM" ,
		csp."NTRANSAC" 
from	usvtimv01."WBSTBLCLAIM_DOC_SAP"  csp

select  cpl."NCURRENCY",
		cpl."SCERTYPE" ,
		cpl."NBRANCH" ,
		cpl."NPRODUCT" ,
		cpl."NPOLICY" ,
		cpl."NCERTIF" 
from    usvtimv01."CURREN_POL" cpl

select	cer."DDATE_ORIGI" ,
		cer."SCERTYPE" ,
		cer."NBRANCH" ,
		cer."NPOLICY" ,
		cer."NCERTIF" ,
		cer."NDIGIT" 
from 	usvtimv01."CERTIFICAT" cer

select 	lc."NCOVER" ,
		lc."NPRODUCT" ,
		lc."NMODULEC" ,
		lc."NBRANCH" ,
		lc."DEFFECDATE" ,
		lc."DNULLDATE" ,
		lc."SSTATREGT" ,
		lc."NBRANCH_LED" ,
		lc."NBRANCH_REI" 
from usvtimv01."LIFE_COVER" lc 

select	"DSTATDATE",
		"NRECEIPT",
		"NDIGIT" ,
		"NTYPE" 
from	usvtimv01."PREMIUM_MO"

select 	dp1."NRECEIPT" ,
		dp1."NDIGIT" ,
		dp1."NBRANCH_REI" ,
		dp1."NMODULEC" ,
		dp1."STYPE_DETAI" ,
		dp1."NBILL_ITEM" ,
		dp1."SCERTYPE" ,
		dp1."NBRANCH" ,
		dp1."NPOLICY" ,
		dp1."NCERTIF" 
from	usvtimv01."PREMIUM_CE" dp1
