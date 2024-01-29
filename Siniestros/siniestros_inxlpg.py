def generate_siniestros_parquets(bucketName, config_dominio, glue_context, connection, s3_client, io):
    
    pol_subproduct = '''
                    (
                        select	sub_product,
                                usercomp,
                                company,
                                certype,
                                branch,
                                policy,
                                product
                        from	usinsug01.pol_subproduct
                    ) AS TMP
                    '''  

    claim_his = '''
                   (
                        select	operdate,
                                claim,
                                oper_type,
                                exchange,
                                transac
                        from	usinsug01.claim_his
                   ) AS TMP
                   '''   

    table140 = '''
                  (
                    select codigint 
                    from	usinsug01.table140
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
                        from	usinsug01.curren_pol cpl
                    )  AS TMP
                    '''

    cover = '''
                (
                   select   cov.capital,
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
                )   AS TMP
                ''' 

    gen_cover = '''
                  (
                    select 	    ctid,
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
                  ) AS TMP
                  ''' 

    exchange = '''
                  (
                    select      exc.exchange,
                            exc.usercomp,
                            exc.company,
                            exc.currency,
                            exc.effecdate,
                            exc.nulldate
                    from 	usinsug01.exchange exc
                  ) AS TMP
                  '''

    tab_name_b = '''
                  (
                    select tabname,
                            branch
                    from usinsug01.tab_name_b
                  ) AS TMP
                  '''

    accident = '''
                  (
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
                  ) AS TMP
                  '''

    auto_peru = '''
                  (
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
                  ) AS TMP
                  ''' 

    civil = '''
              (
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
              ) AS TMP
              ''' 

    credit = ''' 
                (
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
                ) AS TMP
                '''

    deshones = '''
                  (
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
                  ) AS TMP
                  '''

    eqele_peru = '''
                    (
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
                    ) AS TMP
                    '''

    fire_lc  = '''
                  (
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
                  ) AS TMP
                  '''

    fire_peru = '''
                    (
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
                    ) AS TMP
                    '''

    health = ''' 
                (
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
                ) AS TMP
                '''

    machine = '''
                 (
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
                 )  AS TMP 
                 '''
    
    machine_lc = '''
                    (
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
                    )   AS TMP  
                    '''

    risk_3d  = '''
                    (
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
                    )   AS TMP  
                    '''

    ship = '''
                    (
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
                    )   AS TMP  
                    '''               

    theft = '''
                    (
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
                    )   AS TMP  
                    '''

    transport = '''
                    (
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
                    )  AS TMP  
                    '''

    trec = '''
                    (
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
                    )   AS TMP  
                    '''

    certificat = '''
                    (
                        select	cer.statusva,
                                cer.usercomp,
                                cer.company,
                                cer.certype ,
                                cer.branch ,
                                cer.policy ,
                                cer.certif
                        from 	usinsug01.certificat cer
                    )   AS TMP  
                    '''

    coinsuran = '''
                    (
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
                    )   AS TMP  
                    '''

    reinsuran = '''
                    (
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
                    )   AS TMP  
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
                        from    usinsug01.policy pol
                    )   AS TMP  
                    '''

    claim = '''
                    (
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
                    )   AS TMP  
                    '''

    tab_cl_ope = '''
                    (
                        select 	operation,
                                reserve,
                                pay_amount,
                                ajustes
                        from	usinsug01.tab_cl_ope
                    )   AS TMP  
                    '''

    cl_m_cover = '''
                    (
                        select  clm.cover ,
                                clm.currency ,
                                clm.amount ,
                                clm.usercomp ,
                                clm.company ,
                                clm.claim ,
                                clm.movement 
                        from 	usinsug01.cl_m_cover clm 
                    )   AS TMP  
                    '''

    claimbenef = '''
                    (
                        select  bene_code,
                                usercomp,
                                company ,
                                claim ,
                                bene_type
                        from    usinsug01.claimbenef
                    )   AS TMP  
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
    