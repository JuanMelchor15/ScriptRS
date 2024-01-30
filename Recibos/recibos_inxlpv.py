def generate_recibos_parquets(bucketName, config_dominio, glue_context, connection, s3_client, io):


    detail_pre = '''
                    (
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
                        from usinsuv01.acc_autom2 aa 
                    ) AS TMP
                    '''

    insured_li = '''
                    (
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
                    ) AS TMP
                    '''

    product_sbs = '''
                    (
                        select  sbs.cod_sbs_gyp,
                                sbs.usercomp ,
                                sbs.company ,
                                sbs.branch ,
                                sbs.product ,
                                sbs.effecdate ,
                                sbs.nulldate
                        from    usinsuv01.product_sbs sbs
                    ) AS TMP
                    '''

    anexo1_sbs = '''
                    (
                        select  as2.cod_sbs_bal ,
                                as2.cod_sbs_gyp 
                        from usinsuv01.anexo1_sbs as2 
                    ) AS TMP
                    '''

    policy = '''
                    (
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
                    ) AS TMP
                    '''

    life_prev = '''
                    (
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
                    ) AS TMP
                    '''

    premium = '''
                    (
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
                    ) AS TMP
                    '''

    product = '''
                    (
                        select  pro.brancht,
                                pro.usercomp ,
                                pro.company ,
                                pro.branch ,
                                pro.product ,
                                pro.effecdate ,
                                pro.nulldate 
                        from    usinsuv01.product pro
                    ) AS TMP
                    '''

    gen_cover = '''
                    (
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
                    ) AS TMP
                    '''

    cover = '''
                    (
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
                    ) AS TMP
                    '''

    life_cover = '''
                    (
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
                    ) AS TMP
                    '''

    claim_his = '''
                    (
                        select 	ch.exchange ,
                                ch.claim ,
                                ch.transac 
                        from usinsuv01.claim_his ch 
                    ) AS TMP
                    '''

    claim = '''
                    (
                        select 	c.usercomp ,
                                c.company ,
                                c.branch ,
                                c."policy" ,
                                c.staclaim ,
                                c.occurdat 
                        from usinsuv01.claim c 
                    ) AS TMP
                    '''

    cl_m_cover = '''
                    (
                        select 	cmc .usercomp ,
                                cmc .company ,
                                cmc .claim ,
                                cmc .movement ,
                                cmc .cover ,
                                cmc .amount ,
                                cmc .currency 
                        from usinsuv01.cl_m_cover cmc
                    ) AS TMP
                    '''

    coinsuran = '''
                    (
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
                    ) AS TMP
                    '''

    claim_pay_sap = '''
                    (
                        select 	csp.transac,
                                csp.claim,
                                csp.transac_pay 
                        from	usinsuv01.claim_pay_sap csp
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
                        from    usinsuv01.curren_pol cpl
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
                        from 	usinsuv01.certificat cer  
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
 
    
