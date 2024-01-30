def generate_siniestros_parquets(bucketName, config_dominio, glue_context, connection, s3_client, io):

    claim_his = '''
                    (
                        select	cast (cast ( "DOPERDATE" as date ) as varchar) ,
                                "NCLAIM",
                                "NOPER_TYPE",
                                "NCASE_NUM" ,
                                "NDEMAN_TYPE" ,
                                "NTRANSAC" 
                        from	usvtimg01."CLAIM_HIS"
                    ) AS TMP
                    '''

    table140 = '''
                    (
                        select	"NOPER_TYPE"
                        from	usvtimg01."TABLE140"
                    ) AS TMP
                    '''

    condition_serv = '''
                    (
                        select	"SVALUE",
                                "NCONDITION"
                        from	usvtimg01."CONDITION_SERV" cs 
                    ) AS TMP
                    '''

    certificat = '''
                    (
                        select	cer."NPAYFREQ",
                                cer."SCERTYPE",
                                cer."NBRANCH",
                                cer."NPOLICY",
                                cer."NCERTIF",
                                cer."NDIGIT"
                        from 	usvtimg01."CERTIFICAT" cer
                    ) AS TMP
                    '''

    cover = '''
                    (
                        select	cov."NCAPITAL",
                                cov."SCERTYPE",
                                cov."NBRANCH",
                                cov."NPRODUCT",
                                cov."NPOLICY",
                                cov."NCERTIF",
                                cast (cast (cov."DEFFECDATE" as date) as varchar),
                                cast (cast ( cov."DNULLDATE" as date) as varchar) ,
                                cov."NCOVER",
                                cov."NMODULEC"
                        from    usvtimg01."COVER" cov
                    ) AS TMP
                    '''

    life_cover = '''
                    (
                        select  gen."NCOVER",
                                gen."NPRODUCT",
                                gen."NMODULEC",
                                gen."NBRANCH",
                                cast (cast (gen."DEFFECDATE" as date) as varchar),
                                cast (cast ( gen."DNULLDATE" as date) as varchar),
                                gen."SSTATREGT",
                                gen."SADDSUINI"
                        from 	usvtimg01."LIFE_COVER" gen
                    ) AS TMP
                    '''

    coinsuran = '''
                    (
                        select 	coi."NSHARE",
                                coi."SCERTYPE",
                                coi."NBRANCH",
                                coi."NPRODUCT",
                                coi."NPOLICY",
                                coi."NCOMPANY",
                                cast (cast (coi."DEFFECDATE"as date ) as varchar),
                                cast (cast (coi."DNULLDATE" as date ) as varchar)
                        from	usvtimg01."COINSURAN" coi
                    ) AS TMP
                    '''

    curren_pol = '''
                    (
                        select  cpl."NCURRENCY",
                                cpl."SCERTYPE",
                                cpl."NBRANCH" ,
                                cpl."NPRODUCT" ,
                                cpl."NPOLICY" ,
                                cpl."NCERTIF"
                        from    usvtimg01."CURREN_POL" cpl
                    ) AS TMP
                    '''

    policy = '''
                    (
                        select 	pol.ctid,
                                pol."SCERTYPE",
                                pol."NPRODUCT",
                                pol."SPOLITYPE",
                                pol."SBUSSITYP",
                                pol."NPOLICY" ,
                                pol."NBRANCH" 
                        from 	usvtimg01."POLICY" pol
                    ) AS TMP
                    '''

    claim = '''
                    (
                        select 	cla.ctid,
                                cla."NCLAIM",
                                cla."SCERTYPE",
                                cla."NPOLICY",
                                cla."NBRANCH",
                                cla."SSTACLAIM",
                                cla."NPRODUCT",
                                cla."NCERTIF",
                                cast (cast (cla."DOCCURDAT"as date ) as varchar),
                                cla."SCLIENT",
                                cla."NCAUSECOD"
                        from 	usvtimg01."CLAIM" cla
                    ) AS TMP
                    '''

    claimbenef = '''
                    (
                        select  "SCLIENT",
                                "NCLAIM" ,
                                "NCASE_NUM" ,
                                "NDEMAN_TYPE" ,
                                "NBENE_TYPE"
                        from    usvtimg01."CLAIMBENEF"
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
                        from  	usvtimg01."CL_M_COVER" clm
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
    
