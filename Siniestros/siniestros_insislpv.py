def generate_siniestros_parquets(bucketName, config_dominio, glue_context, connection, s3_client, io):


    policy_eng_policies = '''
                    (
                        select  "MASTER_POLICY_ID" ,
                                "POLICY_ID"
                        from  usinsiv01."POLICY_ENG_POLICIES" 
                    ) AS TMP
                    '''

    o_accinsured = '''
                    (
                        select acc."OBJECT_ID", 
                               acc."MAN_ID",
                               acc."ACCINS_TYPE"
                        from   usinsiv01."O_ACCINSURED" acc
                    ) AS TMP
                    '''

    p_people = '''
                    (
                        select  "MAN_ID" 
                        from usinsiv01."P_PEOPLE"
                    ) AS TMP
                    '''

    insured_object = '''
                    (
                        select  "AV_CURRENCY",
                                "POLICY_ID" ,
                                "OBJECT_ID" ,
                                "INSURED_VALUE",
                                cast (cast ("INSR_BEGIN" as date) as varchar),
                                cast (cast ("INSR_END"as date) as varchar)
                        from	usinsiv01."INSURED_OBJECT"
                    ) AS TMP
                    '''

    gen_annex = '''
                    (
                        select  ann."POLICY_ID",	
                                cast (cast (ann."INSR_BEGIN" as date) as varchar),
                                ann."ANNEX_TYPE",
                                ann."ANNEX_STATE"
                        from    usinsiv01."GEN_ANNEX" ann
                    ) AS TMP
                    '''

    claim_reserve_history = '''
                                (
                                    select	"CLAIM_ID",
                                            "OP_TYPE",
                                            cast (cast ("REGISTRATION_DATE" as date) as varchar),
                                            "REQUEST_ID"
                                    from	usinsiv01."CLAIM_RESERVE_HISTORY"           
                                ) AS TMP
                                '''

    claim = '''
                    (
                        select 	c.ctid,
                                c."CLAIM_ID" ,
                                c."POLICY_ID" ,
                                cast (cast (c."EVENT_DATE"as date) as varchar) ,
                                c."CLAIM_REGID"
                        from  	usinsiv01."CLAIM" c 
                    ) AS TMP
                    '''

    policy = '''
                    (
                        select 	p.ctid,
                                p."POLICY_ID" ,
                                cast (cast (p."INSR_END"as date) as varchar), 
                                p."ATTR1" 
                        from 	usinsiv01."POLICY" p 
                    ) AS TMP
                    '''



    claim_objects = '''
                    (
                        select 	co."CLAIM_ID" ,
                                co."REQUEST_ID",
                                co."COVER_TYPE" 
                        from usinsiv01."CLAIM_OBJECTS" co
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

