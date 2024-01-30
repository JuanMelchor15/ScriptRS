def generate_recibos_parquets(bucketName, config_dominio, glue_context, connection, s3_client, io):

    detail_pre = '''
                    (
                        select	dp0."NPREMIUM",
                                dp0."NRECEIPT",
                                dp0."NBILL_ITEM" ,
                                dp0."NBRANCH_LED" ,
                                dp0."NDIGIT" ,
                                dp0."NDET_CODE" ,
                                dp0."STYPE_DETAI" ,
                                dp0."NCAPITAL",
                                dp0."SADDSUINI" 
                        from	usvtimv01."DETAIL_PRE" dp0
                    ) AS TMP
                    '''

    premium = '''
                    (
                        select 	p.ctid,
                                p."NRECEIPT" ,
                                p."NDIGIT" ,
                                cast (cast (p."DEXPIRDAT"  as date) as varchar),
                                cast (cast (p."DNULLDATE" as date) as varchar) ,
                                p."SSTATUSVA" ,
                                p."SCERTYPE" ,
                                p."NPOLICY" ,
                                cast (cast (p."DEFFECDATE" as date) as varchar)
                        from usvtimv01."PREMIUM" p
                    ) AS TMP
                    '''

    claim = '''
                    (
                        select 	c."SCERTYPE" ,
                                c."NBRANCH" ,
                                c."NPOLICY" ,
                                c."NCERTIF" ,
                                cast (cast (c."DOCCURDAT" as date) as varchar),
                                c."SSTACLAIM" ,
                                c."NBRANCH" ,
                                c."NPOLICY" 
                        from usvtimv01."CLAIM" c 
                    ) AS TMP
                    '''

    cl_m_cover = '''
                    (
                        select 	cmc."NCLAIM" ,
                                cmc."NCASE_NUM" ,
                                cmc."NDEMAN_TYPE" ,
                                cmc."NTRANSAC" ,
                                cmc ."NAMOUNT" ,
                                cmc."NCURRENCY" ,
                                cmc ."NEXCHANGE" ,
                                cmc ."NLOC_AMOUNT" ,
                                cmc ."NCOVER" 
                        from usvtimv01."CL_M_COVER" cmc
                    ) AS TMP
                    '''

    coinsuran = '''
                    (
                        select 	c."SCERTYPE" ,
                                c."NBRANCH" ,
                                c."NPRODUCT" ,
                                c."NPOLICY" ,
                                c."NCOMPANY" ,
                                cast (cast (c."DEFFECDATE"  as date) as varchar),
                                cast (cast (c."DNULLDATE" as date) as varchar),
                                cast (cast (c."DCOMPDATE"as date) as varchar) ,
                                c."NCOMPANY" ,
                                c."NSHARE" 
                        from usvtimv01."COINSURAN" c 
                    ) AS TMP
                    '''

    policy = '''
                    (
                        select 	p."NPRODUCT" ,
                                p."NLEADSHARE" ,
                                cast (cast (p."DCOMPDATE" as date) as varchar),
                                p."SCERTYPE" ,
                                p."SBUSSITYP" ,
                                p."SPOLITYPE" ,
                                p."NPOLICY" ,
                                p."NBRANCH" 
                        from usvtimv01."POLICY" p 
                    ) AS TMP
                    '''

    condition_serv = '''
                    (
                        select	cs."SVALUE"  ,
                                cs."NCONDITION"
                        from	usvtimv01."CONDITION_SERV" cs
                    ) AS TMP
                    '''

    claim_his = '''
                    (
                        select 	ch."NCLAIM" ,
                                ch."NOPER_TYPE" ,
                                cast (cast (ch."DOPERDATE" as date) as varchar),
                                ch."NCASE_NUM" ,
                                ch."NDEMAN_TYPE" ,
                                ch."NTRANSAC" ,
                                ch."SCLIENT" 
                        from usvtimv01."CLAIM_HIS" ch 
                    ) AS TMP
                    '''

    wbstblclaim_doc_sap = '''
                    (
                        select 	csp."NTRANSAC",
                                csp."NCLAIM" ,
                                csp."NCASE_NUM" ,
                                csp."NCASE_NUM" ,
                                csp."NTRANSAC" 
                        from	usvtimv01."WBSTBLCLAIM_DOC_SAP"  csp
                    ) AS TMP
                    '''

    curren_pol = '''
                    (
                        select  cpl."NCURRENCY",
                                cpl."SCERTYPE" ,
                                cpl."NBRANCH" ,
                                cpl."NPRODUCT" ,
                                cpl."NPOLICY" ,
                                cpl."NCERTIF" 
                        from    usvtimv01."CURREN_POL" cpl
                    ) AS TMP
                    '''

    certificat = '''
                    (
                        select	cast(cast (cer."DDATE_ORIGI"as date) as varchar) ,
                                cer."SCERTYPE" ,
                                cer."NBRANCH" ,
                                cer."NPOLICY" ,
                                cer."NCERTIF" ,
                                cer."NDIGIT" 
                        from 	usvtimv01."CERTIFICAT" cer
                    ) AS TMP
                    '''

    life_cover = '''
                    (
                        select 	lc."NCOVER" ,
                                lc."NPRODUCT" ,
                                lc."NMODULEC" ,
                                lc."NBRANCH" ,
                                cast(cast (lc."DEFFECDATE" as date) as varchar) ,
                                cast(cast (lc."DNULLDATE" as date) as varchar),
                                lc."SSTATREGT" ,
                                lc."NBRANCH_LED" ,
                                lc."NBRANCH_REI" 
                        from usvtimv01."LIFE_COVER" lc 
                    ) AS TMP
                    '''

    premium_mo = '''
                    (
                        select	cast(cast ( "DSTATDATE"as date) as varchar),
                                "NRECEIPT",
                                "NDIGIT" ,
                                "NTYPE" 
                        from	usvtimv01."PREMIUM_MO"
                    ) AS TMP
                    '''

    premium_ce = '''
                    (
                        select 	dp1."NRECEIPT" ,
                                dp1."NDIGIT" ,
                                dp1."NBRANCH_REI" ,
                                dp1."NMODULEC" ,
                                dp1."STYPE_DETAI" ,
                                dp1."NBILL_ITEM" ,
                                dp1."SCERTYPE" ,
                                dp1."NBRANCH" ,
                                dp1."NPOLICY" ,
                                dp1."NCERTIF" 
                        from	usvtimv01."PREMIUM_CE" dp1
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
 