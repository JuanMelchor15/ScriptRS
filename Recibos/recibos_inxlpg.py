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
   