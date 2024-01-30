def generate_siniestros_parquets(bucketName, config_dominio, glue_context, connection, s3_client, io):

    detail_pre = '''
                    (
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
                    ) AS TMP
                    '''

    acc_autom2 = '''
                    (
                        select 	aa.ctid,
                                aa.branch ,
                                aa.branch_pyg ,
                                aa.branch_bal ,
                                aa.product ,
                                aa.concept_fac 
                        from usinsug01.acc_autom2 aa 
                    ) AS TMP
                    '''

    premium = '''
                    (
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
                    ) AS TMP
                    '''

    claim_his = '''
                    (
                        select 	ch.exchange ,
                                ch.claim ,
                                ch.transac,
                                ch.operdate,
                                ch.currency ,
                                ch.amount 
                        from usinsug01.claim_his ch 
                    ) AS TMP
                    '''

    curren_pol = '''
                    (
                        select  cpl.currency,
                                cpl.usercomp ,
                                cpl.branch ,
                                cpl.certif,
                                cpl.certype,
                                cpl.company,
                                cpl.policy
                        from    usinsug01.curren_pol cpl
                    ) AS TMP
                    '''

    table10b = '''
                    (
                        select 	t10b.company ,
                                t10b.branch 
                        from usinsug01.table10b t10b
                    ) AS TMP
                    '''

    claim = '''
                    (
                        select c.staclaim, 
                            c.branch ,
                            c.usercomp ,
                            c.company ,
                            c."policy" ,
                            c.certif ,
                            c.client ,
                            c.occurdat 
                        from usinsug01.claim c 
                    ) AS TMP
                    '''

    table140 = '''
                    (
                        select 	t.codigint 
                        from usinsug01.table140 t 
                    ) AS TMP
                    '''

    tab_cl_ope = '''
                    (
                        select tco.operation , 
                            tco.pay_amount 	
                        from usinsug01.tab_cl_ope tco 
                    ) AS TMP
                    '''

    policy = '''
                    (
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
                    ) AS TMP
                    '''

    cl_m_cover = '''
                    (
                        select 	cmc.usercomp ,
                                cmc .claim ,
                                cmc .company ,
                                cmc .movement ,
                                cmc .amount ,
                                cmc .currency 
                        from usinsug01.cl_m_cover cmc 
                    ) AS TMP
                    '''

    coinsuran = '''
                    (
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
                    ) AS TMP
                    '''

    exchange = '''
                    (
                        select	exc.exchange,
                                exc.usercomp,
                                exc.company ,
                                exc.currency ,
                                exc.effecdate ,
                                exc.nulldate 
                        from 	usinsug01.exchange exc
                    ) AS TMP
                    '''

    wbstblclidepequi = '''
                    (
                        select	sclient_vig,
                                sclient_old 
                        from	usinsug01.wbstblclidepequi
                    ) AS TMP
                    '''

    equi_vt_inx = '''
                    (
                        select	evi.scod_vt,
                                evi.scod_inx
                        from	usinsug01.equi_vt_inx evi
                    ) AS TMP
                    '''

    certificat = '''
                    (
                        select	cer.date_origi,
                                cer.usercomp, 
                                cer.company ,
                                cer.certype ,
                                cer.branch ,
                                cer.policy ,
                                cer.certif,
                                cer.date_origi
                        from 	usinsug01.certificat cer 
                    ) AS TMP
                    '''

    reinsuran = '''
                    (
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
                    ) AS TMP
                    '''

    pol_subproduct = '''
                    (
                        select	sub_product,
                                usercomp ,
                                company,
                                certype ,
                                branch ,
                                policy ,
                                product
                        from	usinsug01.pol_subproduct
                    ) AS TMP
                    '''

    claim_case = '''
                    (
                        select 	num_case,
                                usercomp,
                                company ,
                                claim 
                        from	usinsug01.claim_case
                    ) AS TMP
                    '''

    gen_cover = '''
                    (
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
                    ) AS TMP
                    '''

    premium_mo = '''
                    (
                        select	statdate,
                                usercomp ,
                                company ,
                                receipt ,
                                "type" 
                        from	usinsug01.premium_mo
                    ) AS TMP
                    '''

    tab_gencov = '''
                    (
                        select  tgc.usercomp ,
                                tgc.company ,
                                tgc.currency ,
                                tgc.cover ,
                                tgc.descript 
                        from  usinsug01.tab_gencov tgc
                    ) AS TMP
                    '''

    table173 = '''
                    (
                        select t.codigint 
                        from usinsug01.table173 t
                    ) AS TMP
                    '''

    table173 = '''
                    (
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
                    ) AS TMP
                    '''
                        

# Iterate over tablas
    for tabla in config_dominio:
                
        df_result = glue_context.read.format('jdbc').options(**connection).option("dbtable", locals()[tabla['var']]).load() # read.execute_query(glue_context, connection, locals()[tabla['var']])
     
        # Verificar si el DataFrame está en caché antes de cachearlo
        if not df_result.is_cached:
            df_result.cache()
            
        #Trasformar a bit escrito en formato parquet
        L_BUFFER = io.BytesIO()
        df_result.toPandas().to_parquet(L_BUFFER, index=False)
        L_BUFFER.seek(0)
        
        # Escribir el objeto Parquet en S3
        s3_client.put_object(
            Bucket = bucketName,
            Key = tabla['name'],
            Body=L_BUFFER.read()
        )
        
        # Liberar la caché después de procesar la tabla
        df_result.unpersist()
   