def generate_siniestros_parquets(bucketName, config_dominio, glue_context, connection, s3_client, io):

    claim_his = '''
                    (
                        select  claim,
                                operdate,
                                oper_type,
                                exchange,
                                transac
                        from	usinsuv01.claim_his
                    ) AS TMP
                    '''

    curren_pol = '''
                    (
                        select	cpl.currency,
                                cpl.usercomp,
                                cpl.company,
                                cpl.certype,
                                cpl.branch,
                                cpl.policy,
                                cpl.certif
                        from	usinsuv01.curren_pol cpl
                    ) AS TMP
                    '''

    product = '''
                    (
                        select  pro.brancht,
                                pro.usercomp,
                                pro.company,
                                pro.branch,
                                pro.product,
                                pro.effecdate,
                                pro.nulldate
                        from    usinsuv01.product pro
                    ) AS TMP
                    '''

    cover = '''
                    (
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
                        limit 11000000
                    ) AS TMP
                    '''
                        
    gen_cover = '''
                    (
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
                    ) AS TMP
                    '''

    life_cover = '''
                    (
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
                    ) AS TMP
                    '''

    
    life_prev = '''
                    (
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
                    ) AS TMP
                    '''

    life = '''
                    (
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
                    ) AS TMP
                    '''

    health = '''
                    (
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
                    ) AS TMP
                    '''

    certificat = '''
                    (
                        select	cer.statusva,
                                cer.payfreq,
                                cer.usercomp,
                                cer.company,
                                cer.certype,
                                cer.branch,
                                cer.policy,
                                cer.certif
                        from 	usinsuv01.certificat cer
                    ) AS TMP
                    '''

    coinsuran = '''
                    (
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
                    ) AS TMP
                    '''

    policy = '''
                    (
                        select 	pol.ctid,
                                pol.certype,
                                pol.bussityp,
                                pol.leadshare,
                                pol.usercomp,
                                pol.company,
                                pol.branch,
                                pol.policy
                        from    usinsuv01.policy pol
                    ) AS TMP
                    '''

    claim = '''
                    (
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
                    ) AS TMP
                    '''

# Iterate over tablas
    for tabla in config_dominio:
                
        df_result = glue_context.read.format('jdbc').options(**connection).option("fetchsize", 10000).option("dbtable", locals()[tabla['var']]).load() # read.execute_query(glue_context, connection, locals()[tabla['var']])

        df_result = df_result.repartition(10)
     
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
   