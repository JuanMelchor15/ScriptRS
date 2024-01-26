--------SINIESTRO LPG INX
--33 TABLAS LPG
--13 TABLAS LPV

----LPG

select	sub_product,
		usercomp,
		company,
		certype,
		branch,
		policy,
		product
from	usinsug01.pol_subproduct

select	operdate,
		claim,
		oper_type,
		exchange,
		transac
from	usinsug01.claim_his


select	operation,
		reserve,
		ajustes,
		pay_amount
from	usinsug01.tab_cl_ope

select	cpl.currency,
		cpl.usercomp,
		cpl.company,
		cpl.certype,
		cpl.branch,
		cpl.policy,
		cpl.certif
from	usinsug01.curren_pol cpl

select  cov.capital,
		cov.currency,
		cov.cover,
		cov.modulec,
		cov.usercomp,
		cov.company,
		cov.certype,
		cov.branch,
		cov.policy,
		cov.certif,
		cov.effecdate,
		cov.nulldate
from    usinsug01.cover cov

select 	ctid,
		usercomp,
		company,
		branch,
		product,
		currency,
		modulec,
		cover,
		effecdate,
		nulldate,
		statregt,
		addsuini
from   	usinsug01.gen_cover gco



select	exc.exchange,
		exc.usercomp,
		exc.company,
		exc.currency,
		exc.effecdate,
		exc.nulldate
from 	usinsug01.exchange exc


select tabname,
	   branch
from usinsug01.tab_name_b


select	ctid,
		usercomp,
		company,
		certype,
		branch,
		policy,
		certif,
		effecdate,
		nulldate,
		statusva
from	usinsug01.accident


select	ctid,
		usercomp,
		company,
		certype,
		branch,
		policy,
		certif,
		effecdate,
		nulldate,
		statusva
from	usinsug01.auto_peru

select	ctid,
		usercomp,
		company,
		certype,
		branch,
		policy,
		certif,
		effecdate,
		nulldate,
		statusva
from	usinsug01.civil 


select	ctid,
		usercomp,
		company,
		certype,
		branch,
		policy,
		certif,
		effecdate,
		nulldate,
		statusva
from	usinsug01.credit 

select	ctid,
		usercomp,
		company,
		certype,
		branch,
		policy,
		certif,
		effecdate,
		nulldate,
		statusva
from	usinsug01.deshones 


select	ctid,
		usercomp,
		company,
		certype,
		branch,
		policy,
		certif,
		effecdate,
		nulldate,
		statusva
from	usinsug01.eqele_peru 

select	ctid,
		usercomp,
		company,
		certype,
		branch,
		policy,
		certif,
		effecdate,
		nulldate,
		statusva
from	usinsug01.fire_lc 

select	ctid,
		usercomp,
		company,
		certype,
		branch,
		policy,
		certif,
		effecdate,
		nulldate,
		statusva
from	usinsug01.fire_peru 

select	ctid,
		usercomp,
		company,
		certype,
		branch,
		policy,
		certif,
		effecdate,
		nulldate,
		statusva
from	usinsug01.health 

select	ctid,
		usercomp,
		company,
		certype,
		branch,
		policy,
		certif,
		effecdate,
		nulldate,
		statusva
from	usinsug01.machine 

select	ctid,
		usercomp,
		company,
		certype,
		branch,
		policy,
		certif,
		effecdate,
		nulldate,
		statusva
from	usinsug01.machine_lc

select	ctid,
		usercomp,
		company,
		certype,
		branch,
		policy,
		certif,
		effecdate,
		nulldate,
		statusva
from	usinsug01.risk_3d

select	ctid,
		usercomp,
		company,
		certype,
		branch,
		policy,
		certif,
		effecdate,
		nulldate,
		statusva
from	usinsug01.ship

select	ctid,
		usercomp,
		company,
		certype,
		branch,
		policy,
		certif,
		effecdate,
		nulldate,
		statusva
from	usinsug01.theft

select	ctid,
		usercomp,
		company,
		certype,
		branch,
		policy,
		certif,
		effecdate,
		nulldate,
		statusva
from	usinsug01.transport


select	ctid,
		usercomp,
		company,
		certype,
		branch,
		policy,
		certif,
		effecdate,
		nulldate,
		statusva
from	usinsug01.trec

select	cer.statusva,
		cer.usercomp,
		cer.company,
		cer.certype ,
		cer.branch ,
		cer.policy ,
		cer.certif
from 	usinsug01.certificat cer


select	coi.share,
		coi.usercomp, 
		coi.company,
		coi.certype ,
		coi.branch ,
		coi.policy ,
		coi.effecdate ,
		coi.nulldate ,
		coi.companyc
from	usinsug01.coinsuran coi

select  rei.share,
		rei.usercomp,
		rei.company,
		rei.certype,
		rei.branch,
		rei.policy,
		rei.certif,
		rei.effecdate,
		rei.nulldate,
		rei.type
from    usinsug01.reinsuran rei


select 	pol.ctid,
		pol.certype,
		pol.bussityp,
		pol.leadshare,
		pol.usercomp,
        pol.company,
        pol.branch,
        pol.policy
from    usinsug01.policy pol


select 	cla.claim,
		cla.usercomp,
       	cla.company,
       	cla.branch,
       	cla.certif,
       	cla.client,
       	cla.policy,
       	cla.occurdat,
       	cla.staclaim,
       	cla.compdate,
       	cla.causecod
from usinsug01.claim cla 


select	codigint
from	usinsug01.table140

select 	operation,
		reserve,
		pay_amount,
		ajustes
from	usinsug01.tab_cl_ope

select  clm.cover ,
		clm.currency ,
		clm.amount ,
		clm.usercomp ,
		clm.company ,
		clm.claim ,
		clm.movement 
from 	usinsug01.cl_m_cover clm 	

select  bene_code,
		usercomp,
		company ,
		claim ,
		bene_type
from    usinsug01.claimbenef

-----LPV


select  claim,
		operdate,
		oper_type,
		exchange,
		transac
from	usinsuv01.claim_his

select	cpl.currency,
		cpl.usercomp,
		cpl.company,
		cpl.certype,
		cpl.branch,
		cpl.policy,
		cpl.certif
from	usinsuv01.curren_pol cpl

select  pro.brancht,
		pro.usercomp,
		pro.company,
		pro.branch,
		pro.product,
		pro.effecdate,
		pro.nulldate
from    usinsuv01.product pro

select  cov.capital,
		cov.cover,
		cov.modulec,
		cov.currency,
		cov.usercomp,
		cov.company,
		cov.certype,
		cov.branch,
		cov.policy,
		cov.certif,
		cov.effecdate,
		cov.nulldate
from    usinsuv01.cover cov

select  ctid,
		usercomp,
		company,
		branch,
		product,
		currency,
		modulec,
		cover,
		effecdate,
		nulldate,
		statregt,
		addsuini
from	usinsuv01.gen_cover

select  ctid,
		usercomp,
		company,
		branch,
		product,
		currency,
		cover,
		effecdate,
		nulldate,
		statregt,
		addcapii
from 	usinsuv01.life_cover

select	ctid,
		statusva
		usercomp,
		company,
		certype,
		branch,
		policy,
		certif,
		effecdate,
		nulldate
from	usinsuv01.life_prev


select	ctid,
		statusva
		usercomp,
		company,
		certype,
		branch,
		policy,
		certif,
		effecdate,
		nulldate
from	usinsuv01.life

select	ctid,
		statusva
		usercomp,
		company,
		certype,
		branch,
		policy,
		certif,
		effecdate,
		nulldate
from	usinsuv01.health

select	cer.statusva,
		cer.payfreq,
		cer.usercomp,
		cer.company,
		cer.certype,
		cer.branch,
		cer.policy,
		cer.certif
from 	usinsuv01.certificat cer

select	coi.share,
		coi.usercomp,
		coi.company,
		coi.certype,
		coi.branch,
		coi.policy,
		coi.effecdate,
		coi.nulldate,
		coi.companyc
from	usinsuv01.coinsuran coi


select 	pol.ctid,
		pol.certype,
		pol.bussityp,
		pol.leadshare,
		pol.usercomp,
        pol.company,
        pol.branch,
        pol.policy
from    usinsuv01.policy pol

select 	c.ctid,
		c.claim,
		c.certif,
		c.client,
		c.occurdat,
		c.staclaim,
		c.causecod,
		c.usercomp, 
	    c.company,  
	    c.branch, 
	    c.policy  
from 	usinsug01.claim c 