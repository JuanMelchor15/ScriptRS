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
                                dp0."SADDSUINI" ,
                                dp0."NMODULEC" ,
                                dp0 ."NBRANCH_REI" 
                        from	usvtimg01."DETAIL_PRE" dp0
                    ) AS TMP
                    '''

    premium = '''
                    (
                        select 	p.ctid,
                                p."NRECEIPT" ,
                                p."NDIGIT" ,
                                cast (cast (p."DEXPIRDAT" as date) as varchar) ,
                                cast (cast (p."DNULLDATE"as date) as varchar) ,
                                p."SSTATUSVA" ,
                                p."SCERTYPE" ,
                                p."NPOLICY" ,
                                cast (cast (p."DEFFECDATE" as date) as varchar),
                                p."NPRODUCT" ,
                                p."NBRANCH" 
                        from usvtimg01."PREMIUM" p 
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
                        from usvtimg01."CLAIM" c 
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
                        from usvtimg01."CL_M_COVER" cmc
                    ) AS TMP
                    '''

    coinsuran = '''
                    (
                        select 	c."SCERTYPE" ,
                                c."NBRANCH" ,
                                c."NPRODUCT" ,
                                c."NPOLICY" ,
                                c."NCOMPANY" ,
                                cast (cast (c."DEFFECDATE" as date) as varchar),
                                cast (cast (c."DNULLDATE" as date) as varchar),
                                cast (cast (c."DCOMPDATE" as date) as varchar),
                                c."NCOMPANY" ,
                                c."NSHARE" 
                        from usvtimg01."COINSURAN" c 
                    ) AS TMP
                    '''

    policy = '''
                    (
                        select  P.ctid,
                                p."NPRODUCT" ,
                                p."NLEADSHARE" ,
                                cast (cast (p."DCOMPDATE"  as date) as varchar),
                                p."SCERTYPE" ,
                                p."SBUSSITYP" ,
                                p."NPOLICY" ,
                                p."NBRANCH" 
                        from usvtimg01."POLICY" p 
                    ) AS TMP
                    '''

    condition_serv = '''
                    (
                        select	cs."SVALUE"  ,
                                cs."NCONDITION"
                        from	usvtimg01."CONDITION_SERV" cs
                    ) AS TMP
                    '''

    claim_his = '''
                    (
                        select 	ch."NCLAIM" ,
                                ch."NOPER_TYPE" ,
                                cast (cast (ch."DOPERDATE"  as date) as varchar),
                                ch."NCASE_NUM" ,
                                ch."NDEMAN_TYPE" ,
                                ch."NTRANSAC" ,
                                ch."SCLIENT" 
                        from usvtimg01."CLAIM_HIS" ch 
                    ) AS TMP
                    '''

    wbstblclaim_doc_sap = '''
                    (
                        select 	csp."NTRANSAC",
                                csp."NCLAIM" ,
                                csp."NCASE_NUM" ,
                                csp."NCASE_NUM" ,
                                csp."NTRANSAC" 
                        from	usvtimg01."WBSTBLCLAIM_DOC_SAP"  csp
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
                        from    usvtimg01."CURREN_POL" cpl
                    ) AS TMP
                    '''

    reinsuran = '''
                    (
                        select	REI."NSHARE",
                                REI."SCERTYPE" ,
                                REI."NBRANCH" ,
                                REI."NPRODUCT" ,
                                REI."NPOLICY" ,
                                REI."NCERTIF" ,
                                REI."NBRANCH_REI" ,
                                REI."NTYPE_REIN" ,
                                cast (cast (REI."DEFFECDATE" as date) as varchar),
                                cast (cast (REI."DNULLDATE"as date) as varchar)
                        from	usvtimg01."REINSURAN" REI
                    ) AS TMP
                    '''

    cover = '''
                    (
                        select 	cov."NPREMIUM" ,
                                cov."NCOVER" ,
                                cov."NPRODUCT" ,
                                cov."NMODULEC" ,
                                cov."NBRANCH" ,
                                cov."SCERTYPE" ,
                                cov."NCERTIF" ,
                                cast (cast (cov."DEFFECDATE" as date) as varchar) ,
                                cast (cast (cov."DNULLDATE" as date) as varchar)
                        from   usvtimg01."COVER" cov
                    ) AS TMP
                    '''

    life_cover = '''
                    (
                        select 	lc."NCOVER" ,
                                lc."NPRODUCT" ,
                                lc."NMODULEC" ,
                                lc."NBRANCH" ,
                                cast (cast (lc."DEFFECDATE" as date) as varchar),
                                cast (cast (lc."DNULLDATE"as date) as varchar) ,
                                lc."SSTATREGT" ,
                                lc."NBRANCH_LED" ,
                                lc."NBRANCH_REI" 
                        from usvtimg01."LIFE_COVER" lc 
                    ) AS TMP
                    '''

    gen_cover = '''
                    (
                        select 	gen."NCOVER" ,
                                gen ."NPRODUCT" ,
                                gen."NMODULEC" ,
                                gen."NBRANCH" ,
                                cast (cast (gen."DEFFECDATE" as date) as varchar) ,
                                cast (cast (gen."DNULLDATE"as date) as varchar) ,
                                gen."SSTATREGT" ,
                                gen."NBRANCH_LED" ,
                                gen."NCOVERGEN" 
                        from usvtimg01."GEN_COVER" gen
                    ) AS TMP
                    '''

    certificat = '''
                    (
                        select	cast (cast (cer."DDATE_ORIGI"as date) as varchar) ,
                                cer."SCERTYPE" ,
                                cer."NBRANCH" ,
                                cer."NPOLICY" ,
                                cer."NCERTIF" ,
                                cer."NDIGIT" 
                        from 	usvtimg01."CERTIFICAT" cer
                    ) AS TMP
                    '''

    wbstblclidepequi = '''
                    (
                        select	"SCLIENT_VGT",
                                "SCLIENT_OLD" 
                        from	usvtimg01."WBSTBLCLIDEPEQUI" 
                    ) AS TMP
                    '''

    premium_mo = '''
                    (
                        select	cast (cast ("DSTATDATE" as date) as varchar),
                                "NRECEIPT",
                                "NDIGIT" ,
                                "NTYPE" 
                        from	usvtimg01."PREMIUM_MO"
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
                        from	usvtimg01."PREMIUM_CE" dp1
                    ) AS TMP
                    '''

    contrmaster = '''
                    (
                        select  cnm."NNUMBER" ,
                                cnm ."NBRANCH" ,
                                cnm ."NTYPE" ,
                                cnm ."NTYPE_REL" ,
                                cast (cast (cnm ."DSTARTDATE" as date) as varchar)
                        from usvtimg01."CONTRMASTER" cnm
                    ) AS TMP
                    '''

    part_contr = '''
                    (
                        select 	pcr."NTYPE_REL" ,
                                pcr."NNUMBER" ,
                                pcr ."NBRANCH" ,
                                cast (cast (pcr."DSTARTDATE" as date) as varchar) ,
                                cast (cast (pcr."DNULLDATE" as date) as varchar) ,
                                pcr."NTYPE" ,
                                pcr."NCOMPANY" ,
                                pcr."NCOMISION" ,
                                pcr."NSHARE" 
                        from 	usvtimg01."PART_CONTR" pcr
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
 
    