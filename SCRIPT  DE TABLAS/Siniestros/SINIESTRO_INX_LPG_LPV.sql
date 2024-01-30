
-----RECIBOS INSUNIX
--25 TABLAS G
--19 TABLAS V

----LPG

select	dp.commision,
		dp.usercomp,
		dp.company ,
		dp.receipt ,
		dp.certif ,
		dp.addsuini ,
		dp.code,
		dp.capital ,
		dp.premium ,
		dp.type_detai ,
		dp.bill_item 
from	usinsug01.detail_pre dp

select 	aa.ctid,
		aa.branch ,
		aa.branch_pyg ,
		aa.branch_bal ,
		aa.product ,
		aa.concept_fac 
from usinsug01.acc_autom2 aa 

select 	p.ctid,
		p.usercomp ,
		p.company ,
		p.receipt ,
		P.product ,
		p.branch ,
		p.expirdat ,
		p.nulldate ,
		p.effecdate ,
		p.statusva 
from usinsug01.premium p 

select 	ch.exchange ,
		ch.claim ,
		ch.transac,
		ch.operdate,
		ch.currency ,
		ch.amount 
from usinsug01.claim_his ch 

select  cpl.currency,
		cpl.usercomp ,
		cpl.branch ,
		cpl.certif,
		cpl.certype,
		cpl.company,
		cpl.policy
from    usinsug01.curren_pol cpl

select 	t10b.company ,
		t10b.branch 
from usinsug01.table10b t10b

select c.staclaim, 
	   c.branch ,
	   c.usercomp ,
	   c.company ,
	   c."policy" ,
	   c.certif ,
	   c.client ,
	   c.occurdat 
from usinsug01.claim c 

select 	t.codigint 
from usinsug01.table140 t 

select tco.operation , 
	   tco.pay_amount 	
from usinsug01.tab_cl_ope tco 

select 	p.certype ,
		p.usercomp ,
		p.company ,
		p.branch ,
		p."policy" ,
		p.bussityp ,
		p.date_origi ,
		p.compdate ,
		p.leadshare 
from usinsug01."policy" p 

select 	cmc.usercomp ,
		cmc .claim ,
		cmc .company ,
		cmc .movement ,
		cmc .amount ,
		cmc .currency 
from usinsug01.cl_m_cover cmc 

select 	c.usercomp ,
		c.company ,
		c.certype ,
		c.branch ,
		c.compdate ,
		c."policy" ,
		c.effecdate ,
		c.nulldate ,
		c.companyc ,
		c."share" ,
		c.compdate
from usinsug01.coinsuran c 

select	exc.exchange,
		exc.usercomp,
		exc.company ,
		exc.currency ,
		exc.effecdate ,
		exc.nulldate 
from 	usinsug01.exchange exc

select 	csp.transac,
		csp.claim,
		csp.transac_pay 
from	usinsug01.claim_pay_sap csp

select	sclient_vig,
		sclient_old 
from	usinsug01.wbstblclidepequi

select	evi.scod_vt,
		evi.scod_inx
from	usinsug01.equi_vt_inx evi 


select	cer.date_origi,
		cer.usercomp, 
		cer.company ,
		cer.certype ,
		cer.branch ,
		cer.policy ,
		cer.certif,
		cer.date_origi
from 	usinsug01.certificat cer  

select  rei.share,
		rei.usercomp ,
		rei.company ,
		rei.certype ,
		rei.branch ,
		rei.policy ,
		rei.certif ,
		rei.effecdate,
		rei.nulldate ,
		rei.type
from    usinsug01.reinsuran rei

select	sub_product,
		usercomp ,
		company,
		certype ,
		branch ,
		policy ,
		product
from	usinsug01.pol_subproduct

select 	num_case,
		usercomp,
		company ,
		claim 
from	usinsug01.claim_case

select	gco.ctid,
		gco.bill_item ,
		gco.usercomp,
		gco.company,
		gco.branch,
		gco.product,
		gco.currency,
		gco.modulec,
		gco.cover,
		gco.effecdate,
		gco.nulldate,
		gco.statregt
from	usinsug01.gen_cover gco

select	statdate,
		usercomp ,
		company ,
		receipt ,
		"type" 
from	usinsug01.premium_mo

select  tgc.usercomp ,
		tgc.company ,
		tgc.currency ,
		tgc.cover ,
		tgc.descript 
from  usinsug01.tab_gencov tgc

select t.codigint 
from usinsug01.table173 t 

select 	ctp.ctid,
		ctp.usercomp ,
		ctp.company ,
		ctp .companyc ,
		ctp .branch ,
		ctp .currency ,
		ctp."number" ,
		ctp.year_contr ,
		ctp ."type" ,
		ctp .nulldate ,
		ctp ."share" ,
		ctp .com_rate 
from usinsug01.contr_comp ctp

-----LPV

select	dp0.commision,
		dp0.usercomp,
		dp0.company ,
		dp0.receipt ,
		dp0.certif ,
		dp0.capital ,
		dp0.type_detai ,
		dp0.premium ,
		dp0.compdate ,
		dp0.code ,
		dp0.bill_item 
from	usinsuv01.detail_pre dp0

select 	aa.ctid,
		aa.branch ,
		aa.branch_pyg ,
		aa.branch_bal ,
		aa.product ,
		aa.concept_fac 
from usinsuv01.acc_autom2 aa 

select  ili.quantity,
		ili.usercomp,
		ili.company ,
		ili.certype ,
		ili.branch ,
		ili."policy" ,
		ili.certif ,
		ili.effecdate ,
		ili.nulldate ,
		ili.quantity 
from    usinsuv01.insured_li ili

select  sbs.cod_sbs_gyp,
		sbs.usercomp ,
		sbs.company ,
		sbs.branch ,
		sbs.product ,
		sbs.effecdate ,
		sbs.nulldate
from    usinsuv01.product_sbs sbs

select  as2.cod_sbs_bal ,
		as2.cod_sbs_gyp 
from usinsuv01.anexo1_sbs as2 

select 	p.titularc ,
		p.effecdate ,
		p.usercomp ,
		p.company ,
		p.certype ,
		p.compdate ,
		p.bussityp ,
		p.branch ,
		p.leadshare ,
		p.date_origi ,
		p."policy" 	
from usinsuv01."policy" p 

select 	ctid,
		type_cla,
		usercomp ,
		company ,
		certype ,
		branch ,
		certif ,
		effecdate ,
		statusva 
from usinsuv01.life_prev lp 

select 	p.ctid,
		p.product ,
		p.usercomp ,
		p.company ,
		p.receipt ,
		p.product ,
		p.branch ,
		p.expirdat ,
		p.nulldate ,
		p.statusva,
		p.effecdate 
from usinsuv01.premium p 


select  pro.brancht,
		pro.usercomp ,
		pro.company ,
		pro.branch ,
		pro.product ,
		pro.effecdate ,
		pro.nulldate 
from    usinsuv01.product pro

select  ctid,
		usercomp,
		company ,
		branch,
		product,
		modulec,
		cover,
		effecdate,
		nulldate,
		currency ,
		statregt,
		covergen 
from	usinsuv01.gen_cover

select  cov.cover ,
		cov.currency ,
		cov.usercomp ,
		cov.company ,
		cov.certype ,
		cov.branch ,
		cov."policy" ,
		cov.certif ,
		cov.effecdate ,
		cov.nulldate 
from    usinsuv01.cover cov

select  ctid,
		usercomp ,
		company ,
		branch ,
		product,
		currency,
		cover,
		effecdate,
		nulldate,
		statregt
from 	usinsuv01.life_cover

select 	ch.exchange ,
		ch.claim ,
		ch.transac 
from usinsuv01.claim_his ch 

select 	c.usercomp ,
		c.company ,
		c.branch ,
		c."policy" ,
		c.staclaim ,
		c.occurdat 
from usinsuv01.claim c 

select 	cmc .usercomp ,
		cmc .company ,
		cmc .claim ,
		cmc .movement ,
		cmc .cover ,
		cmc .amount ,
		cmc .currency 
from usinsuv01.cl_m_cover cmc 


select  c.usercomp ,
		c.company ,
		c.certype ,
		c.branch ,
		c."policy" ,
		c.effecdate ,
		c.effecdate ,
		c.nulldate ,
		c.companyc ,
		c.companyc, 
		c."share" 
from  usinsuv01.coinsuran c 

select 	csp.transac,
		csp.claim,
		csp.transac_pay 
from	usinsuv01.claim_pay_sap csp

select  cpl.currency,
		cpl.usercomp ,
		cpl.branch ,
		cpl.certif,
		cpl.certype,
		cpl.company,
		cpl.policy
from    usinsuv01.curren_pol cpl

select	cer.date_origi,
		cer.usercomp, 
		cer.company ,
		cer.certype ,
		cer.branch ,
		cer.policy ,
		cer.certif,
		cer.date_origi
from 	usinsuv01.certificat cer  

