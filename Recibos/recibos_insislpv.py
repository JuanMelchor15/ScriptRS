def generate_recibos_parquets(bucketName, config_dominio, glue_context, connection, s3_client, io):

    policy = '''
                (
                    select 	p.ctid,
                            p."POLICY_ID" ,
                            p."INSR_TYPE" ,
                            p."ATTR1",
                            p."POLICY_NO" ,
                            p."ATTR4" ,
                            p."ATTR1" 
                    from USINSIV01."POLICY" p
                ) AS TMP
                '''

    cfg_nl_product = '''
                (
                    select 	cnp."PRODUCT_CODE" ,
                            cnp."PRODUCT_LINK_ID" 
                    from usinsiv01."CFG_NL_PRODUCT" cnp 
                ) AS TMP
                '''

    cfg_nl_product_conds = '''
                (
                    select 	cnpc."PRODUCT_LINK_ID", 
                            cnpc."PARAM_CPR_ID" 
                    from usinsiv01."CFG_NL_PRODUCT_CONDS" cnpc 
                ) AS TMP
                '''

    cpr_params = '''
                (
                    select cp."PARAM_CPR_ID" 
                    from usinsiv01."CPR_PARAMS" cp 
                ) AS TMP
                '''

    policy_conditions = '''
                (
                    select 	pc."POLICY_ID" ,
                            pc."COND_DIMENSION" ,
                            pc."COND_TYPE"
                    from usinsiv01."POLICY_CONDITIONS" pc 
                ) AS TMP
                '''

    cprs_param_value = '''
                (
                    select 	cpv."PARAM_ID" ,
                            cpv ."PARAM_VALUE" ,
                            cpv."PARAM_VALUE_CPR_ID" ,
                            cpv."DESCRIPTION" 
                    from usinsiv01."CPRS_PARAM_VALUE" cpv 
                ) AS TMP
                '''

    cfglpv_policy_techbranch_sbs = '''
                (
                    select 	tb."TECHNICAL_BRANCH" ,
                            tb."TB_NAME" ,
                            tb."INSR_TYPE" ,
                            tb."AS_IS_PRODUCT"
                    from usinsiv01."CFGLPV_POLICY_TECHBRANCH_SBS" tb 
                ) AS TMP
                '''

    blc_items = '''
                (
                    select 	i."COMPONENT" ,
                            i."ITEM_ID"
                    from USINSIV01."BLC_ITEMS" i
                ) AS TMP
                '''

    blc_transactions = '''
                (
                    select 	tr."ITEM_ID",
                            tr."DOC_ID",
                            tr."TRANSACTION_ID" ,
                            tr."CURRENCY" ,
                            tr."PAID_STATUS" ,
                            tr."ATTRIB_6" ,
                            tr."ATTRIB_7" ,
                            tr."TRANSACTION_TYPE" ,
                            tr."AMOUNT" 
                    from USINSIV01."BLC_TRANSACTIONS" tr
                ) AS TMP
                '''

    blc_documents = '''
                        (
                            select 	bdo."DOC_ID" ,
                                    bdo."DOC_CLASS" ,
                                    bdo."DOC_NUMBER" ,
                                    bdo."DOC_TYPE_ID" ,
                                    bdo."REF_DOC_ID" ,
                                    cast ( cast (bdo."ISSUE_DATE" as date) as varchar) ,
                                    cast ( cast (bdo."DUE_DATE" as date) as varchar),
                                    bdo."DOC_SUFFIX"
                            from USINSIV01."BLC_DOCUMENTS" bdo
                        ) AS TMP
                        '''

    blc_proforma_gen = '''
                        (
                            SELECT 	a."ACTION_TYPE" ,
                                    a."ID" ,
                                    a."DOC_ID" ,
                                    a."DOC_NUMBER" 
                            FROM USINSIV01."BLC_PROFORMA_GEN" a
                        ) AS TMP
                        '''

    blc_proforma_acc = '''
                        (
                            select  b."ID" ,
                                    b."INTER_TYPE"
                            from USINSIV01."BLC_PROFORMA_ACC" b
                        ) AS TMP
                        '''

    ri_ceded_claims = '''
                        (
                            SELECT 	RCC."CLAIM_ID",
                                    RCC."CLAIM_OBJ_SEQ" 
                            FROM USINSIV01."RI_CEDED_CLAIMS" RCC 
                        ) AS TMP
                        '''

    policy_eng_policies = '''
                        (
                            SELECT	"POLICY_ID"
                                    "MASTER_POLICY_ID" ,
                                    "ENG_POL_TYPE" 
                            FROM USINSIV01."POLICY_ENG_POLICIES"
                        ) AS TMP
                        '''

    insured_object = '''
                        (
                            SELECT	 "AV_CURRENCY",
                                    "POLICY_ID" 
                            FROM	USINSIV01."INSURED_OBJECT"
                        ) AS TMP
                        '''

    claim = '''
                        (
                            select  C."CLAIM_ID",
                                    c."POLICY_ID" ,
                                    c."CLAIM_REGID" ,
                                    c."EVENT_DATE" 
                            from usinsiv01."CLAIM" c 
                        ) AS TMP
                        '''

    claim_objects = '''
                        (
                            select 	co."CLAIM_ID" ,
                                    co."REQUEST_ID" ,
                                    co."CLAIM_OBJ_SEQ" ,
                                    co."MAN_ID" 
                            from usinsiv01."CLAIM_OBJECTS" co 
                        ) AS TMP
                        '''

    claim_reserve_history = '''
                        (
                            SELECT	"CLAIM_ID",
                                    "OP_TYPE",
                                    "REGISTRATION_DATE",
                                    "OP_TYPE" ,
                                    "REQUEST_ID" ,
                                    "RESERV_AMNT" ,
                                    "RESERV_SEQ" 
                            FROM	USINSIV01."CLAIM_RESERVE_HISTORY"
                        ) AS TMP
                        '''

    gen_risk_covered = '''
                        (
                            SELECT 	GRC."DISCOUNT",
                                    GRC."POLICY_ID",
                                    GRC."ANNEX_ID"
                            FROM usinsiv01."GEN_RISK_COVERED" GRC 
                        ) AS TMP
                        '''

    gen_annex = '''
                        (
                            SELECT 	GA."ANNEX_ID",
                                    GA."POLICY_ID" 
                            FROM usinsiv01."GEN_ANNEX" GA
                        ) AS TMP
                        '''

    blc_actions = '''
                        (
                            SELECT  cast (cast ( B."ACTION_DATE" as date) as varchar),
                                    B."DOCUMENT_ID",
                                    B."ATTRIB_0",
                                    B."ACTION_TYPE_ID"	
                            FROM usinsiv01."BLC_ACTIONS" B  
                        ) AS TMP
                        '''

    blc_installments = '''
                        (
                            select 	bi."TRANSACTION_ID",
                                    bi."ITEM_ID" ,
                                    bi."ATTRIB_4" ,
                                    bi."ATTRIB_5",
                                    bi."AMOUNT" 
                            from usinsiv01."BLC_INSTALLMENTS" bi  
                        ) AS TMP
                        '''

    policy_participants = '''
                        (
                            select 	pp."POLICY_ID" ,
                                    pp."PARTICPANT_ROLE" ,
                                    pp."ANNEX_ID" 
                            from  usinsiv01."POLICY_PARTICIPANTS" pp
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
 