def generate_siniestros_parquets(bucketName, config_dominio, glue_context, connection, s3_client, io):

    claim_his = '''
                    (
                        select	"DOPERDATE",
                                "NCLAIM",
                                "NOPER_TYPE",
                                "NCASE_NUM" ,
                                "NDEMAN_TYPE" ,
                                "NTRANSAC" 
                        from	usvtimv01."CLAIM_HIS"
                    ) AS TMP
                    '''
    table140 = '''
                    (
                        select	"NOPER_TYPE"
                        from	usvtimv01."TABLE140"
                    ) AS TMP
                    '''

    condition_serv = '''
                    (
                        select	"SVALUE",
                                "NCONDITION" 		
                        from	usvtimv01."CONDITION_SERV" cs 
                    ) AS TMP
                    '''

    certificat = '''
                    (
                        select	cer."NPAYFREQ",
                                cer."SCERTYPE" ,
                                cer."NBRANCH" ,
                                cer."NPOLICY" ,
                                cer."NCERTIF" ,
                                cer."NDIGIT" 
                        from 	usvtimv01."CERTIFICAT" cer
                    ) AS TMP
                    '''

    cover = '''
                    (
                        select	cov."NCAPITAL",
                                cov."SCERTYPE" ,
                                cov."NBRANCH" ,
                                cov."NPRODUCT" ,
                                cov."NPOLICY" ,
                                cov."NCERTIF" ,
                                cov."DEFFECDATE" ,
                                cov."DNULLDATE"
                        from    usvtimv01."COVER" cov
                    ) AS TMP
                    '''

    life_cover = '''
                    (
                        select 	lif."NCOVER",
                                lif."NPRODUCT" ,
                                lif."NMODULEC",
                                lif."NBRANCH" ,
                                lif."DEFFECDATE" ,
                                lif."DNULLDATE" ,
                                lif."SSTATREGT" ,
                                lif."SADDSUINI"
                        from 	usvtimv01."LIFE_COVER" lif
                    ) AS TMP
                    '''

    gen_cover = '''
                    (
                        select	gen."NCOVER" ,
                                gen."NPRODUCT" ,
                                gen."NMODULEC" ,
                                gen."NBRANCH" ,
                                gen."DEFFECDATE" ,
                                gen."DNULLDATE" ,
                                gen."SSTATREGT" ,
                                gen."SADDSUINI" 
                        from    usvtimv01."GEN_COVER" gen   
                    ) AS TMP
                    '''

    coinsuran = '''
                    (
                        select 	coi."NSHARE",
                                coi."SCERTYPE" ,
                                coi."NBRANCH" ,
                                coi."NPRODUCT" ,
                                coi."NPOLICY" ,
                                coi."NCOMPANY" ,
                                coi."DEFFECDATE" ,
                                coi."DNULLDATE" ,
                                coi."NCOMPANY" 
                        from	usvtimv01."COINSURAN" coi  
                    ) AS TMP
                    '''

    curren_pol = '''
                    (
                        select  cpl."NCURRENCY",
                                cpl."SCERTYPE" ,
                                cpl."NBRANCH" ,
                                cpl."NPRODUCT",
                                cpl."NPOLICY" ,
                                cpl."NCERTIF" 
                        from    usvtimv01."CURREN_POL" cpl
                    ) AS TMP
                    '''

    policy = '''
                    (
                        select  cpl."NCURRENCY",
                                cpl."SCERTYPE" ,
                                cpl."NBRANCH" ,
                                cpl."NPRODUCT",
                                cpl."NPOLICY" ,
                                cpl."NCERTIF" 
                        from    usvtimv01."POLICY" cpl
                    ) AS TMP
                    '''

    claim = '''
                    (
                        select  cpl."NCURRENCY",
                                cpl."SCERTYPE" ,
                                cpl."NBRANCH" ,
                                cpl."NPRODUCT",
                                cpl."NPOLICY" ,
                                cpl."NCERTIF" 
                        from    usvtimv01."CLAIM" cpl
                    ) AS TMP
                    '''

    claimbenef = '''
                    (
                        select  "SCLIENT",
                                "NCLAIM" ,
                                "NCASE_NUM" ,
                                "NDEMAN_TYPE" ,
                                "NBENE_TYPE"
                        from    usvtimv01."CLAIMBENEF" 
                    ) AS TMP
                    '''

    cl_m_cover = '''
                    (
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