'''
Created on Aug 6, 2018

@author: Lanie.Shannon
'''
import pyodbc
conn = pyodbc.connect(r'DSN=Workday_EIB;UID=sqllocal;PWD=IhPCIcbts!')
crsr = conn.cursor()

# create data
result = crsr.execute("""
SELECT 
     FIELD_1
  ,  SPREADSHEET_KEY_FIELD_2
  ,  ADD_ONLY_FIELD_3
  ,  CUSTOMER_FIELD_4
  ,  RTRIM(CUST_ID_FIELD_5) AS CUST_ID_FIELD_5
  ,  RTRIM(CUSTOMER_REFERENCE_ID_FIELD_6) AS CUSTOMER_REFERENCE_ID_FIELD_6
  ,  RTRIM(CUSTOMER_NAME_FIELD_7) AS CUSTOMER_NAME_FIELD_7
  ,  WORKTAG_ONLY_FIELD_8
  ,  CUSTOMER_CATEGORY_FIELD_9
  ,  ADDITIONAL_CUSTOMER_GROUP_FIELD_10
  ,  PAYMENT_TERMS_FIELD_11
  ,  DEFAULT_PAYMENT_TYPE_FIELD_12
  ,  RTRIM(BUSINESS_ENTITY_NAME_FIELD_13) AS BUSINESS_ENTITY_NAME_FIELD_13
  ,  BUSINESS_ENTITY_TAX_ID_FIELD_14
  ,  EXTERNAL_ENTITY_ID_FIELD_15
  ,  ROW_ID_1_FIELD_16
  ,  FORMATTED_ADDRESS_FIELD_17
  ,  ADDRESS_FORMAT_TYPE_FIELD_18
  ,  DEFAULTED_BUSINESS_SITE_ADDRESS_FIELD_19
  ,  DELETE_FIELD_20
  ,  DO_NOT_REPLACE_ALL_FIELD_21
  ,  EFFECTIVE_DATE_FIELD_22
  ,  RTRIM(COUNTRY_FIELD_23) AS COUNTRY_FIELD_23
  ,  LAST_MODIFIED_FIELD_24
  ,  ADDRESS_ROW_ID_FIELD_25
  ,  ADR_DESCRIPTOR_FIELD_26
  ,  ADR_TYPE_FIELD_27
  ,  RTRIM(ADDRESS_LINE_DATA_FIELD_28) AS ADDRESS_LINE_DATA_FIELD_28
  ,  RTRIM(MUNICIPALITY_FIELD_29) AS MUNICIPALITY_FIELD_29
  ,  COUNTRY_CITY_FIELD_30
  ,  ISO_3166_1_ALPHA_2_CODE_FIELD_31
  ,  SUBMUNI_ROW_ID_FIELD_32
  ,  ADDRESS_COMPONENT_NAME_FIELD_33
  ,  SUBMUNI_TYPE_FIELD_34
  ,  SUBMUNICIPALITY_DATA_FIELD_35
  ,  RTRIM(COUNTRY_REGION_FIELD_36) AS COUNTRY_REGION_FIELD_36
  ,  SUBREG_ROW_ID_FIELD_37
  ,  SUBREG_DESCRIPTOR_FIELD_38
  ,  SUBREG_TYPE_FIELD_39
  ,  SUBREGION_DATA_FIELD_40
  ,  RTRIM(POSTAL_CODE_FIELD_41) AS POSTAL_CODE_FIELD_41
  ,  USAGE_ROW_ID_FIELD_42
  ,  USAGE_PUBLIC_FIELD_43
  ,  TYPE_ROW_ID_FIELD_44
  ,  TYPE_PRIMARY_FIELD_45
  ,  TYPE_TYPE_FIELD_46
  ,  TYPE_USE_FOR_FIELD_47
  ,  TYPE_USE_FOR_TENANTED_FIELD_48
  ,  TYPE_COMMENTS_FIELD_49
  ,  MUNICIPALITY_LOCAL_FIELD_50
  ,  ADDRESS_FIELD_51
  ,  ADDRESS_ID_FIELD_52
  ,  RTRIM(PHONE_ROW_ID_FIELD_53) AS PHONE_ROW_ID_FIELD_53
  ,  FORMATTED_PHONE_FIELD_54
  ,  RTRIM(PH_COUNTRY_ISO_CODE_FIELD_55) AS PH_COUNTRY_ISO_CODE_FIELD_55
  ,  RTRIM(INTERNATIONAL_PHONE_CODE_FIELD_56) AS INTERNATIONAL_PHONE_CODE_FIELD_56
  ,  RTRIM(AREA_CODE_FIELD_57) AS AREA_CODE_FIELD_57
  ,  RTRIM(PHONE_NUMBER_FIELD_58) AS PHONE_NUMBER_FIELD_58
  ,  RTRIM(PHONE_EXTENSION_FIELD_59) AS PHONE_EXTENSION_FIELD_59
  ,  RTRIM(PHONE_DEVICE_TYPE_FIELD_60) AS PHONE_DEVICE_TYPE_FIELD_60
  ,  RTRIM(PH_USE_ROW_ID_FIELD_61) AS PH_USE_ROW_ID_FIELD_61
  ,  RTRIM(PH_USE_PUBLIC_FIELD_62) AS PH_USE_PUBLIC_FIELD_62
  ,  RTRIM(PH_USE_TYPE_ROW_ID_FIELD_63) AS PH_USE_TYPE_ROW_ID_FIELD_63
  ,  PH_USE_TYPE_PRIMARY_FIELD_64
  ,  RTRIM(PH_USE_TYPE_TYPE_FIELD_65) AS PH_USE_TYPE_TYPE_FIELD_65
  ,  RTRIM(PH_USE_TYPE_USE_FOR_FIELD_66) AS PH_USE_TYPE_USE_FOR_FIELD_66
  ,  PH_USE_TYPE_USE_FOR_TENANTED_FIELD_67
  ,  PH_USE_TYPE_COMMENTS_FIELD_68
  ,  EM_ROW_ID_FIELD_69
  ,  EMAIL_ADDRESS_FIELD_70
  ,  EMAIL_COMMENT_FIELD_71
  ,  EM_USE_ROW_ID_FIELD_72
  ,  EM_USE_PUBLIC_FIELD_73
  ,  EM_USE_TYPE_ROW_ID_FIELD_74
  ,  EM_USE_TYPE_PRIMARY_FIELD_75
  ,  EM_USE_TYPE_TYPE_FIELD_76
  ,  EM_USE_TYPE_USE_FOR_FIELD_77
  ,  EM_USE_TYPE_USE_FOR_TENANTED_FIELD_78
  ,  EM_USE_TYPE_COMMENTS_FIELD_79
  ,  WEB_ROW_ID_FIELD_80
  ,  WEB_ADDRESS_FIELD_81
  ,  WEB_ADDRESS_COMMENT_FIELD_82
  ,  WEB_USE_ROW_ID_FIELD_83
  ,  WEB_USE_PUBLIC_FIELD_84
  ,  WEB_USE_TYPE_ROW_ID_FIELD_85
  ,  WEB_USE_TYPE_PRIMARY_FIELD_86
  ,  WEB_USE_TYPE_TYPE_FIELD_87
  ,  WEB_USE_USE_FOR_FIELD_88
  ,  WEB_USE_USE_FOR_TENANTED_FIELD_89
  ,  WEB_USE_TYPE_COMMENTS_FIELD_90
  ,  CUST_STATUS_ROW_ID_FIELD_91
  ,  CUST_STATUS_VALUE_FIELD_92
  ,  REASON_FOR_CUST_STATUS_CHANGE_FIELD_93
  ,  CUST_STATUS_CHANGE_REASON_DESCRIP_FIELD_94
  ,  ALWAYS_SEPARATE_PAYMENTS_FIELD_95
  ,  INVOICE_DELIVERY_METHOD_FIELD_96
  ,  INVOICE_NOTIFI_EMAIL_RECIP_FIELD_97
  ,  STATEMENT_DELIVERY_METHOD_FIELD_98
  ,  STATEMENT_NOTIF_EMAIL_RECIP_FIELD_99
  ,  NOTE_ROW_ID_FIELD_100
  ,  CREATED_FIELD_101
  ,  LAST_UPDATED_FIELD_102
  ,  WORKER_FIELD_103
  ,  BUSINESS_ENTITY_CONTACT_FIELD_104
  ,  SYSTEM_USER_FIELD_105
  ,  NOTE_CONTENT_FIELD_106
  ,  FOLLOWUP_DATE_FIELD_107
  ,  WORKTAGS_FIELD_108
  ,  DATE_FILTER
FROM (
SELECT *
FROM (
SELECT 
    '10-' + (REPLICATE('0', 12-LEN(CUSTOMER_ID)) + CUSTOMER_ID) AS CUST_ID_FIELD_5
  , CASE
        WHEN AREA_CODE IS NOT NULL THEN '1'
        ELSE '9999'
    END AS PHONE_ROW_ID_SORT
  , CASE
        WHEN ADDRESS_1 IS NOT NULL THEN '1'
        ELSE '9999'
    END AS ADDRESS_ROW_ID_SORT
  ,  '' AS FIELD_1
  , ROW_NUMBER() OVER( ORDER BY CUSTOMER_ID) SPREADSHEET_KEY_FIELD_2
  , CASE
        WHEN ADS_DT_LST_UPDT = ADS_INITIAL_LOAD_DT THEN 'Y'
        ELSE 'N'
        END AS ADD_ONLY_FIELD_3
  , '' AS CUSTOMER_FIELD_4
  , CUSTOMER_ID AS CUSTOMER_REFERENCE_ID_FIELD_6
  , CUSTOMER_NAME AS CUSTOMER_NAME_FIELD_7
  , 'N' AS WORKTAG_ONLY_FIELD_8
  , 'Spay and Neuter' AS CUSTOMER_CATEGORY_FIELD_9
  , '' AS ADDITIONAL_CUSTOMER_GROUP_FIELD_10
  , 'Net30' AS PAYMENT_TERMS_FIELD_11
  , 'Check' AS DEFAULT_PAYMENT_TYPE_FIELD_12
  , CUSTOMER_NAME AS BUSINESS_ENTITY_NAME_FIELD_13
  , '' AS BUSINESS_ENTITY_TAX_ID_FIELD_14
  , '' AS EXTERNAL_ENTITY_ID_FIELD_15
  , '1' AS ROW_ID_1_FIELD_16
  , '' AS FORMATTED_ADDRESS_FIELD_17
  , 'Basic' AS ADDRESS_FORMAT_TYPE_FIELD_18
  , 'N' AS DEFAULTED_BUSINESS_SITE_ADDRESS_FIELD_19
  , '' AS DELETE_FIELD_20
  , '' AS DO_NOT_REPLACE_ALL_FIELD_21
  , '' AS EFFECTIVE_DATE_FIELD_22
  , SUBSTRING(COUNTRY_ISO_CODE, 1, 2) AS COUNTRY_FIELD_23
  , '' AS LAST_MODIFIED_FIELD_24
  , CASE
        WHEN ADDRESS_1 <> '' THEN '1'
        ELSE ''
    END AS ADDRESS_ROW_ID_FIELD_25
  , 'Address Line 1' AS ADR_DESCRIPTOR_FIELD_26
  , 'ADDRESS_LINE_1' AS ADR_TYPE_FIELD_27
  , ADDRESS_1 AS ADDRESS_LINE_DATA_FIELD_28
  , CITY AS MUNICIPALITY_FIELD_29
  , '' AS COUNTRY_CITY_FIELD_30
  , '' AS ISO_3166_1_ALPHA_2_CODE_FIELD_31
  , '' AS SUBMUNI_ROW_ID_FIELD_32
  , '' AS ADDRESS_COMPONENT_NAME_FIELD_33
  , '' AS SUBMUNI_TYPE_FIELD_34
  , '' AS SUBMUNICIPALITY_DATA_FIELD_35
  , (REPLACE(COUNTRY_ISO_CODE, ' ', '')) + '-' + COUNTRY_STATE AS COUNTRY_REGION_FIELD_36
  , '' AS SUBREG_ROW_ID_FIELD_37
  , '' AS SUBREG_DESCRIPTOR_FIELD_38
  , '' AS SUBREG_TYPE_FIELD_39
  , '' AS SUBREGION_DATA_FIELD_40
  , POSTAL_CODE AS POSTAL_CODE_FIELD_41
  , '1' AS USAGE_ROW_ID_FIELD_42
  , 'Y' AS USAGE_PUBLIC_FIELD_43
  , '1' AS TYPE_ROW_ID_FIELD_44
  , 'Y' AS TYPE_PRIMARY_FIELD_45
  , 'BUSINESS' AS TYPE_TYPE_FIELD_46
  , 'BILLING' AS TYPE_USE_FOR_FIELD_47
  , '' AS TYPE_USE_FOR_TENANTED_FIELD_48
  , '' AS TYPE_COMMENTS_FIELD_49
  , '' AS MUNICIPALITY_LOCAL_FIELD_50
  , '' AS ADDRESS_FIELD_51
  , '' AS ADDRESS_ID_FIELD_52
  , CASE
        WHEN AREA_CODE IS NOT NULL THEN '1'
        ELSE ''
    END AS PHONE_ROW_ID_FIELD_53
  , '' AS FORMATTED_PHONE_FIELD_54
  , CASE
        WHEN AREA_CODE IS NOT NULL THEN SUBSTRING(COUNTRY_ISO_CODE, 1, 3)
        ELSE ''
    END AS PH_COUNTRY_ISO_CODE_FIELD_55
  , CASE
        WHEN INTR_PHONE_CODE_NUMBER = '' THEN '0'
        ELSE COALESCE(INTR_PHONE_CODE_NUMBER,'0')
    END AS INTERNATIONAL_PHONE_CODE_FIELD_56
  , AREA_CODE
  , ADDRESS_1
  , CASE
        WHEN AREA_CODE = '' THEN '000'
        ELSE COALESCE(AREA_CODE,'000')
    END AS AREA_CODE_FIELD_57
  , CASE
        WHEN PHONE_NUMBER = '' THEN '0000'
        ELSE COALESCE(PHONE_NUMBER,'0000')
    END AS PHONE_NUMBER_FIELD_58
  , CASE
        WHEN AREA_CODE IS NOT NULL THEN PHONE_EXTN
        ELSE ''
    END AS PHONE_EXTENSION_FIELD_59
  , CASE
        WHEN AREA_CODE IS NOT NULL THEN 'TELEPHONE' 
        ELSE ''
    END AS PHONE_DEVICE_TYPE_FIELD_60
  , CASE
        WHEN AREA_CODE IS NOT NULL THEN '1'
        ELSE ''
    END AS PH_USE_ROW_ID_FIELD_61
  , CASE
        WHEN AREA_CODE IS NOT NULL THEN 'Y'
        ELSE ''
    END AS PH_USE_PUBLIC_FIELD_62
  , CASE
        WHEN AREA_CODE IS NOT NULL THEN '1'
        ELSE ''
    END AS PH_USE_TYPE_ROW_ID_FIELD_63
  , CASE
        WHEN AREA_CODE IS NOT NULL THEN 'Y'
        ELSE ''
    END AS PH_USE_TYPE_PRIMARY_FIELD_64
  , CASE
        WHEN AREA_CODE IS NOT NULL THEN 'BUSINESS'
        ELSE ''
    END AS PH_USE_TYPE_TYPE_FIELD_65
  , CASE
        WHEN AREA_CODE IS NOT NULL THEN 'BILLING'
        ELSE ''
    END AS PH_USE_TYPE_USE_FOR_FIELD_66
  , '' AS PH_USE_TYPE_USE_FOR_TENANTED_FIELD_67
  , '' AS PH_USE_TYPE_COMMENTS_FIELD_68
  , CASE
        WHEN EMAIL_ADDRESS <> '' THEN '1'
        ELSE ''
    END AS EM_ROW_ID_FIELD_69
  , CASE
        WHEN EMAIL_ADDRESS LIKE '%@%' 
        THEN (CASE
                  WHEN (CASE 
                            WHEN EMAIL_ADDRESS LIKE '%;%' THEN CHARINDEX ( ';' ,EMAIL_ADDRESS ) 
                            ELSE 0 END) > 0 
                  AND  (CASE 
                            WHEN EMAIL_ADDRESS LIKE '%;%' THEN CHARINDEX ( ';' ,EMAIL_ADDRESS ) 
                            ELSE 0 END) 
                  <    (CASE 
                            WHEN EMAIL_ADDRESS LIKE '%faxcore%' THEN CHARINDEX ( 'faxcore' ,EMAIL_ADDRESS ) 
                            ELSE 0 END) 
                  THEN SUBSTRING(EMAIL_ADDRESS,1, 
                       (CASE 
                            WHEN EMAIL_ADDRESS LIKE '%;%' THEN CHARINDEX ( ';' ,EMAIL_ADDRESS ) 
                            ELSE 0 END)-1)
                  WHEN (CASE 
                            WHEN EMAIL_ADDRESS LIKE '%;%' THEN CHARINDEX ( ';' ,EMAIL_ADDRESS ) 
                            ELSE 0 END) 
                  >    (CASE 
                            WHEN EMAIL_ADDRESS LIKE '%faxcore%' THEN CHARINDEX ( 'faxcore' ,EMAIL_ADDRESS )
                            ELSE 0 END) 
                  THEN SUBSTRING(EMAIL_ADDRESS, 
                       (CASE 
                            WHEN EMAIL_ADDRESS LIKE '%;%' THEN CHARINDEX ( ';' ,EMAIL_ADDRESS ) 
                            ELSE 0 END)+1, LEN(EMAIL_ADDRESS))
                  ELSE EMAIL_ADDRESS END)
        ELSE ''
    END AS EMAIL_ADDRESS_FIELD_70
  , '' AS EMAIL_COMMENT_FIELD_71
  , CASE
        WHEN ( CASE
                   WHEN EMAIL_ADDRESS LIKE '%@%' 
              THEN (CASE
                        WHEN (CASE 
                                  WHEN EMAIL_ADDRESS LIKE '%;%' THEN CHARINDEX ( ';' ,EMAIL_ADDRESS ) 
                              ELSE 0 END) > 0 
                        AND  (CASE 
                                  WHEN EMAIL_ADDRESS LIKE '%;%' THEN CHARINDEX ( ';' ,EMAIL_ADDRESS ) 
                              ELSE 0 END) 
                       <    (CASE 
                                 WHEN EMAIL_ADDRESS LIKE '%faxcore%' THEN CHARINDEX ( 'faxcore' ,EMAIL_ADDRESS ) 
                            ELSE 0 END) 
                       THEN SUBSTRING(EMAIL_ADDRESS,1, 
                            (CASE 
                                 WHEN EMAIL_ADDRESS LIKE '%;%' THEN CHARINDEX ( ';' ,EMAIL_ADDRESS ) 
                            ELSE 0 END)-1)
                       WHEN (CASE 
                                 WHEN EMAIL_ADDRESS LIKE '%;%' THEN CHARINDEX ( ';' ,EMAIL_ADDRESS ) 
                            ELSE 0 END) 
                       >    (CASE 
                                 WHEN EMAIL_ADDRESS LIKE '%faxcore%' THEN CHARINDEX ( 'faxcore' ,EMAIL_ADDRESS )
                            ELSE 0 END) 
                       THEN SUBSTRING(EMAIL_ADDRESS, 
                            (CASE 
                                 WHEN EMAIL_ADDRESS LIKE '%;%' THEN CHARINDEX ( ';' ,EMAIL_ADDRESS ) 
                            ELSE 0 END)+1, LEN(EMAIL_ADDRESS))
                       ELSE EMAIL_ADDRESS END)
              ELSE '' END)
        LIKE '%@%'
        THEN '1'
        ELSE ''
    END AS EM_USE_ROW_ID_FIELD_72
  , CASE
        WHEN ( CASE
                   WHEN EMAIL_ADDRESS LIKE '%@%' 
              THEN (CASE
                        WHEN (CASE 
                                  WHEN EMAIL_ADDRESS LIKE '%;%' THEN CHARINDEX ( ';' ,EMAIL_ADDRESS ) 
                              ELSE 0 END) > 0 
                        AND  (CASE 
                                  WHEN EMAIL_ADDRESS LIKE '%;%' THEN CHARINDEX ( ';' ,EMAIL_ADDRESS ) 
                              ELSE 0 END) 
                       <    (CASE 
                                 WHEN EMAIL_ADDRESS LIKE '%faxcore%' THEN CHARINDEX ( 'faxcore' ,EMAIL_ADDRESS ) 
                            ELSE 0 END) 
                       THEN SUBSTRING(EMAIL_ADDRESS,1, 
                            (CASE 
                                 WHEN EMAIL_ADDRESS LIKE '%;%' THEN CHARINDEX ( ';' ,EMAIL_ADDRESS ) 
                            ELSE 0 END)-1)
                       WHEN (CASE 
                                 WHEN EMAIL_ADDRESS LIKE '%;%' THEN CHARINDEX ( ';' ,EMAIL_ADDRESS ) 
                            ELSE 0 END) 
                       >    (CASE 
                                 WHEN EMAIL_ADDRESS LIKE '%faxcore%' THEN CHARINDEX ( 'faxcore' ,EMAIL_ADDRESS )
                            ELSE 0 END) 
                       THEN SUBSTRING(EMAIL_ADDRESS, 
                            (CASE 
                                 WHEN EMAIL_ADDRESS LIKE '%;%' THEN CHARINDEX ( ';' ,EMAIL_ADDRESS ) 
                            ELSE 0 END)+1, LEN(EMAIL_ADDRESS))
                       ELSE EMAIL_ADDRESS END)
              ELSE '' END)
        LIKE '%@%'
        THEN 'Y'
        ELSE ''
    END AS EM_USE_PUBLIC_FIELD_73
  , CASE
        WHEN ( CASE
                   WHEN EMAIL_ADDRESS LIKE '%@%' 
              THEN (CASE
                        WHEN (CASE 
                                  WHEN EMAIL_ADDRESS LIKE '%;%' THEN CHARINDEX ( ';' ,EMAIL_ADDRESS ) 
                              ELSE 0 END) > 0 
                        AND  (CASE 
                                  WHEN EMAIL_ADDRESS LIKE '%;%' THEN CHARINDEX ( ';' ,EMAIL_ADDRESS ) 
                              ELSE 0 END) 
                       <    (CASE 
                                 WHEN EMAIL_ADDRESS LIKE '%faxcore%' THEN CHARINDEX ( 'faxcore' ,EMAIL_ADDRESS ) 
                            ELSE 0 END) 
                       THEN SUBSTRING(EMAIL_ADDRESS,1, 
                            (CASE 
                                 WHEN EMAIL_ADDRESS LIKE '%;%' THEN CHARINDEX ( ';' ,EMAIL_ADDRESS ) 
                            ELSE 0 END)-1)
                       WHEN (CASE 
                                 WHEN EMAIL_ADDRESS LIKE '%;%' THEN CHARINDEX ( ';' ,EMAIL_ADDRESS ) 
                            ELSE 0 END) 
                       >    (CASE 
                                 WHEN EMAIL_ADDRESS LIKE '%faxcore%' THEN CHARINDEX ( 'faxcore' ,EMAIL_ADDRESS )
                            ELSE 0 END) 
                       THEN SUBSTRING(EMAIL_ADDRESS, 
                            (CASE 
                                 WHEN EMAIL_ADDRESS LIKE '%;%' THEN CHARINDEX ( ';' ,EMAIL_ADDRESS ) 
                            ELSE 0 END)+1, LEN(EMAIL_ADDRESS))
                       ELSE EMAIL_ADDRESS END)
              ELSE '' END)
        LIKE '%@%'
        THEN '1'
        ELSE ''
    END AS EM_USE_TYPE_ROW_ID_FIELD_74
  , CASE
        WHEN ( CASE
                   WHEN EMAIL_ADDRESS LIKE '%@%' 
              THEN (CASE
                        WHEN (CASE 
                                  WHEN EMAIL_ADDRESS LIKE '%;%' THEN CHARINDEX ( ';' ,EMAIL_ADDRESS ) 
                              ELSE 0 END) > 0 
                        AND  (CASE 
                                  WHEN EMAIL_ADDRESS LIKE '%;%' THEN CHARINDEX ( ';' ,EMAIL_ADDRESS ) 
                              ELSE 0 END) 
                       <    (CASE 
                                 WHEN EMAIL_ADDRESS LIKE '%faxcore%' THEN CHARINDEX ( 'faxcore' ,EMAIL_ADDRESS ) 
                            ELSE 0 END) 
                       THEN SUBSTRING(EMAIL_ADDRESS,1, 
                            (CASE 
                                 WHEN EMAIL_ADDRESS LIKE '%;%' THEN CHARINDEX ( ';' ,EMAIL_ADDRESS ) 
                            ELSE 0 END)-1)
                       WHEN (CASE 
                                 WHEN EMAIL_ADDRESS LIKE '%;%' THEN CHARINDEX ( ';' ,EMAIL_ADDRESS ) 
                            ELSE 0 END) 
                       >    (CASE 
                                 WHEN EMAIL_ADDRESS LIKE '%faxcore%' THEN CHARINDEX ( 'faxcore' ,EMAIL_ADDRESS )
                            ELSE 0 END) 
                       THEN SUBSTRING(EMAIL_ADDRESS, 
                            (CASE 
                                 WHEN EMAIL_ADDRESS LIKE '%;%' THEN CHARINDEX ( ';' ,EMAIL_ADDRESS ) 
                            ELSE 0 END)+1, LEN(EMAIL_ADDRESS))
                       ELSE EMAIL_ADDRESS END)
              ELSE '' END)
        LIKE '%@%'
        THEN 'Y'
        ELSE ''
    END AS EM_USE_TYPE_PRIMARY_FIELD_75
  , CASE
        WHEN ( CASE
                   WHEN EMAIL_ADDRESS LIKE '%@%' 
              THEN (CASE
                        WHEN (CASE 
                                  WHEN EMAIL_ADDRESS LIKE '%;%' THEN CHARINDEX ( ';' ,EMAIL_ADDRESS ) 
                              ELSE 0 END) > 0 
                        AND  (CASE 
                                  WHEN EMAIL_ADDRESS LIKE '%;%' THEN CHARINDEX ( ';' ,EMAIL_ADDRESS ) 
                              ELSE 0 END) 
                       <    (CASE 
                                 WHEN EMAIL_ADDRESS LIKE '%faxcore%' THEN CHARINDEX ( 'faxcore' ,EMAIL_ADDRESS ) 
                            ELSE 0 END) 
                       THEN SUBSTRING(EMAIL_ADDRESS,1, 
                            (CASE 
                                 WHEN EMAIL_ADDRESS LIKE '%;%' THEN CHARINDEX ( ';' ,EMAIL_ADDRESS ) 
                            ELSE 0 END)-1)
                       WHEN (CASE 
                                 WHEN EMAIL_ADDRESS LIKE '%;%' THEN CHARINDEX ( ';' ,EMAIL_ADDRESS ) 
                            ELSE 0 END) 
                       >    (CASE 
                                 WHEN EMAIL_ADDRESS LIKE '%faxcore%' THEN CHARINDEX ( 'faxcore' ,EMAIL_ADDRESS )
                            ELSE 0 END) 
                       THEN SUBSTRING(EMAIL_ADDRESS, 
                            (CASE 
                                 WHEN EMAIL_ADDRESS LIKE '%;%' THEN CHARINDEX ( ';' ,EMAIL_ADDRESS ) 
                            ELSE 0 END)+1, LEN(EMAIL_ADDRESS))
                       ELSE EMAIL_ADDRESS END)
              ELSE '' END)
        LIKE '%@%'
        THEN 'BUSINESS'
        ELSE ''
    END AS EM_USE_TYPE_TYPE_FIELD_76
  , CASE
        WHEN ( CASE
                   WHEN EMAIL_ADDRESS LIKE '%@%' 
              THEN (CASE
                        WHEN (CASE 
                                  WHEN EMAIL_ADDRESS LIKE '%;%' THEN CHARINDEX ( ';' ,EMAIL_ADDRESS ) 
                              ELSE 0 END) > 0 
                        AND  (CASE 
                                  WHEN EMAIL_ADDRESS LIKE '%;%' THEN CHARINDEX ( ';' ,EMAIL_ADDRESS ) 
                              ELSE 0 END) 
                       <    (CASE 
                                 WHEN EMAIL_ADDRESS LIKE '%faxcore%' THEN CHARINDEX ( 'faxcore' ,EMAIL_ADDRESS ) 
                            ELSE 0 END) 
                       THEN SUBSTRING(EMAIL_ADDRESS,1, 
                            (CASE 
                                 WHEN EMAIL_ADDRESS LIKE '%;%' THEN CHARINDEX ( ';' ,EMAIL_ADDRESS ) 
                            ELSE 0 END)-1)
                       WHEN (CASE 
                                 WHEN EMAIL_ADDRESS LIKE '%;%' THEN CHARINDEX ( ';' ,EMAIL_ADDRESS ) 
                            ELSE 0 END) 
                       >    (CASE 
                                 WHEN EMAIL_ADDRESS LIKE '%faxcore%' THEN CHARINDEX ( 'faxcore' ,EMAIL_ADDRESS )
                            ELSE 0 END) 
                       THEN SUBSTRING(EMAIL_ADDRESS, 
                            (CASE 
                                 WHEN EMAIL_ADDRESS LIKE '%;%' THEN CHARINDEX ( ';' ,EMAIL_ADDRESS ) 
                            ELSE 0 END)+1, LEN(EMAIL_ADDRESS))
                       ELSE EMAIL_ADDRESS END)
              ELSE '' END)
        LIKE '%@%'
        THEN 'BILLING'
        ELSE ''
    END AS EM_USE_TYPE_USE_FOR_FIELD_77
  , '' AS EM_USE_TYPE_USE_FOR_TENANTED_FIELD_78
  , '' AS EM_USE_TYPE_COMMENTS_FIELD_79
  , '' AS WEB_ROW_ID_FIELD_80
  , '' AS WEB_ADDRESS_FIELD_81
  , '' AS WEB_ADDRESS_COMMENT_FIELD_82
  , '' AS WEB_USE_ROW_ID_FIELD_83
  , '' AS WEB_USE_PUBLIC_FIELD_84
  , '' AS WEB_USE_TYPE_ROW_ID_FIELD_85
  , '' AS WEB_USE_TYPE_PRIMARY_FIELD_86
  , '' AS WEB_USE_TYPE_TYPE_FIELD_87
  , '' AS WEB_USE_USE_FOR_FIELD_88
  , '' AS WEB_USE_USE_FOR_TENANTED_FIELD_89
  , '' AS WEB_USE_TYPE_COMMENTS_FIELD_90
  , '1' AS CUST_STATUS_ROW_ID_FIELD_91
  , 'ACTIVE' AS CUST_STATUS_VALUE_FIELD_92
  , '' AS REASON_FOR_CUST_STATUS_CHANGE_FIELD_93
  , '' AS CUST_STATUS_CHANGE_REASON_DESCRIP_FIELD_94
  , 'N' AS ALWAYS_SEPARATE_PAYMENTS_FIELD_95
  , '' AS INVOICE_DELIVERY_METHOD_FIELD_96
  , '' AS INVOICE_NOTIFI_EMAIL_RECIP_FIELD_97
  , '' AS STATEMENT_DELIVERY_METHOD_FIELD_98
  , '' AS STATEMENT_NOTIF_EMAIL_RECIP_FIELD_99
  , '' AS NOTE_ROW_ID_FIELD_100
  , '' AS CREATED_FIELD_101
  , '' AS LAST_UPDATED_FIELD_102
  , '' AS WORKER_FIELD_103
  , '' AS BUSINESS_ENTITY_CONTACT_FIELD_104
  , '' AS SYSTEM_USER_FIELD_105
  , '' AS NOTE_CONTENT_FIELD_106
  , '' AS FOLLOWUP_DATE_FIELD_107
  , '' AS WORKTAGS_FIELD_108
  , CAST(ADS_DT_LST_UPDT as date) AS DATE_FILTER
FROM DBO.STG_SNO_CUSTOMER
WHERE ADS_DT_LST_UPDT >= DATEADD(day,-120,GETDATE()) 
AND CUSTOMER_ID <> ''
AND ADDRESS_1 <> '' 
OR  ADS_DT_LST_UPDT >= DATEADD(day,-120,GETDATE())
AND CUSTOMER_ID <> ''
AND AREA_CODE <> '' 
) y

UNION 

SELECT *
FROM (
SELECT 
    '10-' + (REPLICATE('0', 12-LEN(CUSTOMER_ID)) + CUSTOMER_ID) AS CUST_ID_FIELD_5
  , CASE
        WHEN FAX_AREA_CODE IS NOT NULL THEN '2'
        ELSE '9999'
    END AS PHONE_ROW_ID_SORT
  , CASE
        WHEN ADDRESS_2 IS NOT NULL THEN '2'
        ELSE '9999'
    END AS ADDRESS_ROW_ID_SORT
  , '' AS FIELD_1
  , ROW_NUMBER() OVER( ORDER BY CUSTOMER_ID) SPREADSHEET_KEY_FIELD_2
  , CASE
        WHEN ADS_DT_LST_UPDT = ADS_INITIAL_LOAD_DT THEN 'Y'
        ELSE 'N'
        END AS ADD_ONLY_FIELD_3
  , '' AS CUSTOMER_FIELD_4
  , '' AS CUSTOMER_REFERENCE_ID_FIELD_6
  , '' AS CUSTOMER_NAME_FIELD_7
  , '' AS WORKTAG_ONLY_FIELD_8
  , '' AS CUSTOMER_CATEGORY_FIELD_9
  , '' AS ADDITIONAL_CUSTOMER_GROUP_FIELD_10
  , '' AS PAYMENT_TERMS_FIELD_11
  , '' AS DEFAULT_PAYMENT_TYPE_FIELD_12
  , '' AS BUSINESS_ENTITY_NAME_FIELD_13
  , '' AS BUSINESS_ENTITY_TAX_ID_FIELD_14
  , '' AS EXTERNAL_ENTITY_ID_FIELD_15
  , CASE
        WHEN ADDRESS_2 <> '' THEN '1'
        ELSE ''
    END AS ROW_ID_1_FIELD_16
  , '' AS FORMATTED_ADDRESS_FIELD_17
  , '' AS ADDRESS_FORMAT_TYPE_FIELD_18
  , '' AS DEFAULTED_BUSINESS_SITE_ADDRESS_FIELD_19
  , '' AS DELETE_FIELD_20
  , '' AS DO_NOT_REPLACE_ALL_FIELD_21
  , '' AS EFFECTIVE_DATE_FIELD_22
  , '' AS COUNTRY_FIELD_23
  , '' AS LAST_MODIFIED_FIELD_24
  , CASE
        WHEN ADDRESS_2 <> '' THEN '2'
        ELSE ''
    END AS ADDRESS_ROW_ID_FIELD_25
  , CASE
        WHEN ADDRESS_2 <> '' THEN 'Address Line 2'
        ELSE ''
    END AS ADR_DESCRIPTOR_FIELD_26
  , CASE
        WHEN ADDRESS_2 <> '' THEN 'ADDRESS_LINE_2'
        ELSE ''
    END AS ADR_TYPE_FIELD_27
  , ADDRESS_2 AS ADDRESS_LINE_DATA_FIELD_28
  , '' AS MUNICIPALITY_FIELD_29
  , '' AS COUNTRY_CITY_FIELD_30
  , '' AS ISO_3166_1_ALPHA_2_CODE_FIELD_31
  , '' AS SUBMUNI_ROW_ID_FIELD_32
  , '' AS ADDRESS_COMPONENT_NAME_FIELD_33
  , '' AS SUBMUNI_TYPE_FIELD_34
  , '' AS SUBMUNICIPALITY_DATA_FIELD_35
  , '' AS COUNTRY_REGION_FIELD_36
  , '' AS SUBREG_ROW_ID_FIELD_37
  , '' AS SUBREG_DESCRIPTOR_FIELD_38
  , '' AS SUBREG_TYPE_FIELD_39
  , '' AS SUBREGION_DATA_FIELD_40
  , '' AS POSTAL_CODE_FIELD_41
  , '' AS USAGE_ROW_ID_FIELD_42
  , '' AS USAGE_PUBLIC_FIELD_43
  , '' AS TYPE_ROW_ID_FIELD_44
  , '' AS TYPE_PRIMARY_FIELD_45
  , '' AS TYPE_TYPE_FIELD_46
  , '' AS TYPE_USE_FOR_FIELD_47
  , '' AS TYPE_USE_FOR_TENANTED_FIELD_48
  , '' AS TYPE_COMMENTS_FIELD_49
  , '' AS MUNICIPALITY_LOCAL_FIELD_50
  , '' AS ADDRESS_FIELD_51
  , '' AS ADDRESS_ID_FIELD_52
  , CASE
        WHEN FAX_AREA_CODE <> '' THEN '2'
        ELSE ''
    END AS PHONE_ROW_ID_FIELD_53
  , '' AS FORMATTED_PHONE_FIELD_54
  , CASE
        WHEN FAX_AREA_CODE <> '' THEN SUBSTRING(COUNTRY_ISO_CODE, 1, 3)
        ELSE ''
    END AS PH_COUNTRY_ISO_CODE_FIELD_55
  , INTR_FAX_CODE_NUMBER AS INTERNATIONAL_PHONE_CODE_FIELD_56
  , FAX_AREA_CODE
  , ADDRESS_2
  , FAX_AREA_CODE AS AREA_CODE_FIELD_57
  , FAX_NUMBER AS PHONE_NUMBER_FIELD_58
  , '' AS PHONE_EXTENSION_FIELD_59
  , CASE
        WHEN FAX_AREA_CODE <> '' THEN 'FAX      '
        ELSE ''
    END AS PHONE_DEVICE_TYPE_FIELD_60
  , CASE
        WHEN FAX_AREA_CODE <> '' THEN '1'
        ELSE ''
    END AS PH_USE_ROW_ID_FIELD_61
  , CASE
        WHEN FAX_AREA_CODE <> '' THEN 'Y'
        ELSE ''
    END AS PH_USE_PUBLIC_FIELD_62
  , CASE
        WHEN FAX_AREA_CODE <> '' THEN '1'
        ELSE ''
    END AS PH_USE_TYPE_ROW_ID_FIELD_63
  , 'N' AS PH_USE_TYPE_PRIMARY_FIELD_64
  , CASE
        WHEN FAX_AREA_CODE <> '' THEN 'BUSINESS'
        ELSE ''
    END AS PH_USE_TYPE_TYPE_FIELD_65
  , CASE
        WHEN FAX_AREA_CODE <> '' THEN 'BILLING'
        ELSE ''
    END AS PH_USE_TYPE_USE_FOR_FIELD_66
  , '' AS PH_USE_TYPE_USE_FOR_TENANTED_FIELD_67
  , '' AS PH_USE_TYPE_COMMENTS_FIELD_68
  , '' AS EM_ROW_ID_FIELD_69
  , '' AS EMAIL_ADDRESS_FIELD_70
  , '' AS EMAIL_COMMENT_FIELD_71
  , '' AS EM_USE_ROW_ID_FIELD_72
  , '' AS EM_USE_PUBLIC_FIELD_73
  , '' AS EM_USE_TYPE_ROW_ID_FIELD_74
  , '' AS EM_USE_TYPE_PRIMARY_FIELD_75
  , '' AS EM_USE_TYPE_TYPE_FIELD_76
  , '' AS EM_USE_TYPE_USE_FOR_FIELD_77
  , '' AS EM_USE_TYPE_USE_FOR_TENANTED_FIELD_78
  , '' AS EM_USE_TYPE_COMMENTS_FIELD_79
  , '' AS WEB_ROW_ID_FIELD_80
  , '' AS WEB_ADDRESS_FIELD_81
  , '' AS WEB_ADDRESS_COMMENT_FIELD_82
  , '' AS WEB_USE_ROW_ID_FIELD_83
  , '' AS WEB_USE_PUBLIC_FIELD_84
  , '' AS WEB_USE_TYPE_ROW_ID_FIELD_85
  , '' AS WEB_USE_TYPE_PRIMARY_FIELD_86
  , '' AS WEB_USE_TYPE_TYPE_FIELD_87
  , '' AS WEB_USE_USE_FOR_FIELD_88
  , '' AS WEB_USE_USE_FOR_TENANTED_FIELD_89
  , '' AS WEB_USE_TYPE_COMMENTS_FIELD_90
  , '' AS CUST_STATUS_ROW_ID_FIELD_91
  , '' AS CUST_STATUS_VALUE_FIELD_92
  , '' AS REASON_FOR_CUST_STATUS_CHANGE_FIELD_93
  , '' AS CUST_STATUS_CHANGE_REASON_DESCRIP_FIELD_94
  , '' AS ALWAYS_SEPARATE_PAYMENTS_FIELD_95
  , '' AS INVOICE_DELIVERY_METHOD_FIELD_96
  , '' AS INVOICE_NOTIFI_EMAIL_RECIP_FIELD_97
  , '' AS STATEMENT_DELIVERY_METHOD_FIELD_98
  , '' AS STATEMENT_NOTIF_EMAIL_RECIP_FIELD_99
  , '' AS NOTE_ROW_ID_FIELD_100
  , '' AS CREATED_FIELD_101
  , '' AS LAST_UPDATED_FIELD_102
  , '' AS WORKER_FIELD_103
  , '' AS BUSINESS_ENTITY_CONTACT_FIELD_104
  , '' AS SYSTEM_USER_FIELD_105
  , '' AS NOTE_CONTENT_FIELD_106
  , '' AS FOLLOWUP_DATE_FIELD_107
  , '' AS WORKTAGS_FIELD_108
  , CAST(ADS_DT_LST_UPDT as date) AS DATE_FILTER
FROM DBO.STG_SNO_CUSTOMER
WHERE ADS_DT_LST_UPDT >= DATEADD(day,-120,GETDATE()) 
) x
WHERE( ADDRESS_2 <> '' or FAX_AREA_CODE <> '' )
) z
ORDER BY CUST_ID_FIELD_5, PHONE_ROW_ID_SORT, ADDRESS_ROW_ID_SORT""")

# Get all rows.
data = crsr.fetchall()



