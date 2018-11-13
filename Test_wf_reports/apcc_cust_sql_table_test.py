'''
Created on Oct 29, 2018

@author: Lanie.Shannon
'''
import pyodbc 
conn = pyodbc.connect(r'DSN=AnToxMRT_SQL;UID=db2admin;PWD=1pe567')
crsr = conn.cursor()

# create data
result = crsr.execute("""
SELECT 
     FIELD_1
  ,  SPREADSHEET_KEY_FIELD_2
  ,  ADD_ONLY_FIELD_3
  ,  CUSTOMER_FIELD_4
  ,  CUST_ID_FIELD_5
  ,  CUSTOMER_REFERENCE_ID_FIELD_6
  ,  CUSTOMER_NAME_FIELD_7
  ,  WORKTAG_ONLY_FIELD_8
  ,  CUSTOMER_CATEGORY_FIELD_9
  ,  ADDITIONAL_CUSTOMER_GROUP_FIELD_10
  ,  PAYMENT_TERMS_FIELD_11
  ,  DEFAULT_PAYMENT_TYPE_FIELD_12
  ,  BUSINESS_ENTITY_NAME_FIELD_13
  ,  BUSINESS_ENTITY_TAX_ID_FIELD_14
  ,  EXTERNAL_ENTITY_ID_FIELD_15
  ,  ROW_ID_1_FIELD_16
  ,  FORMATTED_ADDRESS_FIELD_17
  ,  ADDRESS_FORMAT_TYPE_FIELD_18
  ,  DEFAULTED_BUSINESS_SITE_ADDRESS_FIELD_19
  ,  DELETE_FIELD_20
  ,  DO_NOT_REPLACE_ALL_FIELD_21
  ,  EFFECTIVE_DATE_FIELD_22
  ,  COUNTRY_FIELD_23
  ,  LAST_MODIFIED_FIELD_24
  ,  ADDRESS_ROW_ID_FIELD_25
  ,  ADR_DESCRIPTOR_FIELD_26
  ,  ADR_TYPE_FIELD_27
  ,  ADDRESS_LINE_DATA_FIELD_28
  ,  MUNICIPALITY_FIELD_29
  ,  COUNTRY_CITY_FIELD_30
  ,  ISO_3166_1_ALPHA_2_CODE_FIELD_31
  ,  SUBMUNI_ROW_ID_FIELD_32
  ,  ADDRESS_COMPONENT_NAME_FIELD_33
  ,  SUBMUNI_TYPE_FIELD_34
  ,  SUBMUNICIPALITY_DATA_FIELD_35
  ,  COUNTRY_REGION_FIELD_36
  ,  SUBREG_ROW_ID_FIELD_37
  ,  SUBREG_DESCRIPTOR_FIELD_38
  ,  SUBREG_TYPE_FIELD_39
  ,  SUBREGION_DATA_FIELD_40
  ,  POSTAL_CODE_FIELD_41
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
  ,  PHONE_ROW_ID_FIELD_53
  ,  FORMATTED_PHONE_FIELD_54
  ,  PH_COUNTRY_ISO_CODE_FIELD_55
  ,  INTERNATIONAL_PHONE_CODE_FIELD_56
  ,  AREA_CODE_FIELD_57
  ,  PHONE_NUMBER_FIELD_58
  ,  PHONE_EXTENSION_FIELD_59
  ,  PHONE_DEVICE_TYPE_FIELD_60
  ,  PH_USE_ROW_ID_FIELD_61
  ,  PH_USE_PUBLIC_FIELD_62
  ,  PH_USE_TYPE_ROW_ID_FIELD_63
  ,  PH_USE_TYPE_PRIMARY_FIELD_64
  ,  PH_USE_TYPE_TYPE_FIELD_65
  ,  PH_USE_TYPE_USE_FOR_FIELD_66
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
    '12-' + (REPLICATE('0', 11-LEN(CUSTOMER_ID)) + CAST(CUSTOMER_ID AS VARCHAR)) AS CUST_ID_FIELD_5
  , CASE
  WHEN AREA_CODE <> '' THEN '1'
ELSE '9999'
END AS PHONE_ROW_ID_SORT
  , CASE
        WHEN ADDRESS_1 <> '' THEN '1'
ELSE '9999'
END AS ADDRESS_ROW_ID_SORT
  ,  '' AS FIELD_1
  , ROW_NUMBER() OVER( ORDER BY CUSTOMER_ID) SPREADSHEET_KEY_FIELD_2
  , CASE
        WHEN ADS_DT_LST_UPDT = ADS_INITIAL_LOAD_DT THEN 'Y'
        ELSE 'N'
        END AS ADD_ONLY_FIELD_3
  , '' AS CUSTOMER_FIELD_4
  , '12-' + (REPLICATE('0', 11-LEN(CUSTOMER_ID)) + CAST(CUSTOMER_ID AS VARCHAR)) AS CUSTOMER_REFERENCE_ID_FIELD_6
  , CUSTOMER_NAME AS CUSTOMER_NAME_FIELD_7
  , 'N' AS WORKTAG_ONLY_FIELD_8
  , 'APCC (Poison Control)' AS CUSTOMER_CATEGORY_FIELD_9
  , CASE
        WHEN (CUSTOMER_TYPE LIKE '%Discount%') OR (CUSTOMER_TYPE LIKE '%Corporate%') THEN 'Clinic - Discounted (Pricing)'
        WHEN (CUSTOMER_TYPE LIKE '%Regular%') THEN 'Clinic - Regular (Pricing)'
        WHEN (CUSTOMER_TYPE = 'Individual') THEN 'Individual'
        ELSE '' 
        END AS ADDITIONAL_CUSTOMER_GROUP_FIELD_10
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
  , CASE
        WHEN IsNumeric(POSTAL_CODE) = 1
        THEN (REPLICATE('0', 5-LEN(POSTAL_CODE)) + CAST(POSTAL_CODE AS VARCHAR))
        ELSE CAST(POSTAL_CODE AS VARCHAR)
        END AS POSTAL_CODE_FIELD_41
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
  WHEN AREA_CODE <> '' THEN '1'
ELSE ''
END AS PHONE_ROW_ID_FIELD_53
  , '' AS FORMATTED_PHONE_FIELD_54
  , CASE
  WHEN AREA_CODE <> '' THEN SUBSTRING(COUNTRY_ISO_CODE, 1, 3)
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
  WHEN AREA_CODE <> '' THEN PHONE_EXTN
ELSE ''
END AS PHONE_EXTENSION_FIELD_59
  , CASE
  WHEN AREA_CODE <> '' THEN 'TELEPHONE' 
ELSE ''
END AS PHONE_DEVICE_TYPE_FIELD_60
  , CASE
  WHEN AREA_CODE <> '' THEN '1'
ELSE ''
END AS PH_USE_ROW_ID_FIELD_61
  , CASE
  WHEN AREA_CODE <> '' THEN 'Y'
ELSE ''
END AS PH_USE_PUBLIC_FIELD_62
  , CASE
  WHEN AREA_CODE <> '' THEN '1'
ELSE ''
END AS PH_USE_TYPE_ROW_ID_FIELD_63
  , CASE
  WHEN AREA_CODE <> '' THEN 'Y'
ELSE ''
END AS PH_USE_TYPE_PRIMARY_FIELD_64
  , CASE
  WHEN AREA_CODE <> '' THEN 'BUSINESS'
ELSE ''
END AS PH_USE_TYPE_TYPE_FIELD_65
  , CASE
  WHEN AREA_CODE <> '' THEN 'BILLING'
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
FROM RPT.APCC_WD_CUSTOMER
WHERE ADS_DT_LST_UPDT BETWEEN '2018-05-01' AND '2018-05-31'
AND CUSTOMER_ID IS NOT NULL
AND ( ADDRESS_1 <> '' OR AREA_CODE <> '' ) 
/*OR ADS_DT_LST_UPDT > DATEADD(day,-30,GETDATE())
AND CUSTOMER_ID IS NOT NULL
AND AREA_CODE <> '' */
) y

UNION 

SELECT *
FROM (
SELECT 
    '12-' + (REPLICATE('0', 11-LEN(CUSTOMER_ID)) + CAST(CUSTOMER_ID AS VARCHAR)) AS CUST_ID_FIELD_5
  , CASE
  WHEN FAX_AREA_CODE <> '' THEN '2'
ELSE '9999'
END AS PHONE_ROW_ID_SORT
  , CASE
        WHEN ADDRESS_2 <> '' THEN '2'
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
FROM RPT.APCC_WD_CUSTOMER
WHERE ADS_DT_LST_UPDT BETWEEN '2018-05-01' AND '2018-05-31'
) x
WHERE( ADDRESS_2 <> '' or FAX_AREA_CODE <> '' )
) z
ORDER BY CUST_ID_FIELD_5, PHONE_ROW_ID_SORT, ADDRESS_ROW_ID_SORT
""")

# Get all rows.
rows = result.fetchall();

#print(rows)

# This will print the name of the columns, padding each name up
# to 22 characters. Note that comma at the end prevents new lines
# for desc in result.description:
#     print desc[0].rjust(22, ' '),


import xlsxwriter

# Create a workbook and add a worksheet.
workbook = xlsxwriter.Workbook('apcc_cust_test_hold.xlsx')
worksheet = workbook.add_worksheet()

row = 1 
col = 0

for row_data in rows:
    worksheet.write(row, col, row_data.FIELD_1)
    worksheet.write(row, col+1, row_data.SPREADSHEET_KEY_FIELD_2)
    worksheet.write(row, col+2, row_data.ADD_ONLY_FIELD_3)
    worksheet.write(row, col+3, row_data.CUSTOMER_FIELD_4)
    worksheet.write(row, col+4, row_data.CUST_ID_FIELD_5)
    worksheet.write(row, col+5, row_data.CUSTOMER_REFERENCE_ID_FIELD_6)
    worksheet.write(row, col+6, row_data.CUSTOMER_NAME_FIELD_7)
    worksheet.write(row, col+7, row_data.WORKTAG_ONLY_FIELD_8)
    worksheet.write(row, col+8, row_data.CUSTOMER_CATEGORY_FIELD_9)
    worksheet.write(row, col+9, row_data.ADDITIONAL_CUSTOMER_GROUP_FIELD_10)
    worksheet.write(row, col+10, row_data.PAYMENT_TERMS_FIELD_11)
    worksheet.write(row, col+11, row_data.DEFAULT_PAYMENT_TYPE_FIELD_12)
    worksheet.write(row, col+12, row_data.BUSINESS_ENTITY_NAME_FIELD_13)
    worksheet.write(row, col+13, row_data.BUSINESS_ENTITY_TAX_ID_FIELD_14)
    worksheet.write(row, col+14, row_data.EXTERNAL_ENTITY_ID_FIELD_15)
    worksheet.write(row, col+15, row_data.ROW_ID_1_FIELD_16)
    worksheet.write(row, col+16, row_data.FORMATTED_ADDRESS_FIELD_17)
    worksheet.write(row, col+17, row_data.ADDRESS_FORMAT_TYPE_FIELD_18)
    worksheet.write(row, col+18, row_data.DEFAULTED_BUSINESS_SITE_ADDRESS_FIELD_19)
    worksheet.write(row, col+19, row_data.DELETE_FIELD_20)
    worksheet.write(row, col+20, row_data.DO_NOT_REPLACE_ALL_FIELD_21)
    worksheet.write(row, col+21, row_data.EFFECTIVE_DATE_FIELD_22)
    worksheet.write(row, col+22, row_data.COUNTRY_FIELD_23)
    worksheet.write(row, col+23, row_data.LAST_MODIFIED_FIELD_24)
    worksheet.write(row, col+24, row_data.ADDRESS_ROW_ID_FIELD_25)
    worksheet.write(row, col+25, row_data.ADR_DESCRIPTOR_FIELD_26)
    worksheet.write(row, col+26, row_data.ADR_TYPE_FIELD_27)
    worksheet.write(row, col+27, row_data.ADDRESS_LINE_DATA_FIELD_28)
    worksheet.write(row, col+28, row_data.MUNICIPALITY_FIELD_29)
    worksheet.write(row, col+29, row_data.COUNTRY_CITY_FIELD_30)
    worksheet.write(row, col+30, row_data.ISO_3166_1_ALPHA_2_CODE_FIELD_31)
    worksheet.write(row, col+31, row_data.SUBMUNI_ROW_ID_FIELD_32)
    worksheet.write(row, col+32, row_data.ADDRESS_COMPONENT_NAME_FIELD_33)
    worksheet.write(row, col+33, row_data.SUBMUNI_TYPE_FIELD_34)
    worksheet.write(row, col+34, row_data.SUBMUNICIPALITY_DATA_FIELD_35)
    worksheet.write(row, col+35, row_data.COUNTRY_REGION_FIELD_36)
    worksheet.write(row, col+36, row_data.SUBREG_ROW_ID_FIELD_37)
    worksheet.write(row, col+37, row_data.SUBREG_DESCRIPTOR_FIELD_38)
    worksheet.write(row, col+38, row_data.SUBREG_TYPE_FIELD_39)
    worksheet.write(row, col+39, row_data.SUBREGION_DATA_FIELD_40)
    worksheet.write(row, col+40, row_data.POSTAL_CODE_FIELD_41)
    worksheet.write(row, col+41, row_data.USAGE_ROW_ID_FIELD_42)
    worksheet.write(row, col+42, row_data.USAGE_PUBLIC_FIELD_43)
    worksheet.write(row, col+43, row_data.TYPE_ROW_ID_FIELD_44)
    worksheet.write(row, col+44, row_data.TYPE_PRIMARY_FIELD_45)
    worksheet.write(row, col+45, row_data.TYPE_TYPE_FIELD_46)
    worksheet.write(row, col+46, row_data.TYPE_USE_FOR_FIELD_47)
    worksheet.write(row, col+47, row_data.TYPE_USE_FOR_TENANTED_FIELD_48)
    worksheet.write(row, col+48, row_data.TYPE_COMMENTS_FIELD_49)
    worksheet.write(row, col+49, row_data.MUNICIPALITY_LOCAL_FIELD_50)
    worksheet.write(row, col+50, row_data.ADDRESS_FIELD_51)
    worksheet.write(row, col+51, row_data.ADDRESS_ID_FIELD_52)
    worksheet.write(row, col+52, row_data.PHONE_ROW_ID_FIELD_53)
    worksheet.write(row, col+53, row_data.FORMATTED_PHONE_FIELD_54)
    worksheet.write(row, col+54, row_data.PH_COUNTRY_ISO_CODE_FIELD_55)
    worksheet.write(row, col+55, row_data.INTERNATIONAL_PHONE_CODE_FIELD_56)
    worksheet.write(row, col+56, row_data.AREA_CODE_FIELD_57)
    worksheet.write(row, col+57, row_data.PHONE_NUMBER_FIELD_58)
    worksheet.write(row, col+58, row_data.PHONE_EXTENSION_FIELD_59)
    worksheet.write(row, col+59, row_data.PHONE_DEVICE_TYPE_FIELD_60)
    worksheet.write(row, col+60, row_data.PH_USE_ROW_ID_FIELD_61)
    worksheet.write(row, col+61, row_data.PH_USE_PUBLIC_FIELD_62)
    worksheet.write(row, col+62, row_data.PH_USE_TYPE_ROW_ID_FIELD_63)
    worksheet.write(row, col+63, row_data.PH_USE_TYPE_PRIMARY_FIELD_64)
    worksheet.write(row, col+64, row_data.PH_USE_TYPE_TYPE_FIELD_65)
    worksheet.write(row, col+65, row_data.PH_USE_TYPE_USE_FOR_FIELD_66)
    worksheet.write(row, col+66, row_data.PH_USE_TYPE_USE_FOR_TENANTED_FIELD_67)
    worksheet.write(row, col+67, row_data.PH_USE_TYPE_COMMENTS_FIELD_68)
    worksheet.write(row, col+68, row_data.EM_ROW_ID_FIELD_69)
    worksheet.write(row, col+69, row_data.EMAIL_ADDRESS_FIELD_70)
    worksheet.write(row, col+70, row_data.EMAIL_COMMENT_FIELD_71)
    worksheet.write(row, col+71, row_data.EM_USE_ROW_ID_FIELD_72)
    worksheet.write(row, col+72, row_data.EM_USE_PUBLIC_FIELD_73)
    worksheet.write(row, col+73, row_data.EM_USE_TYPE_ROW_ID_FIELD_74)
    worksheet.write(row, col+74, row_data.EM_USE_TYPE_PRIMARY_FIELD_75)
    worksheet.write(row, col+75, row_data.EM_USE_TYPE_TYPE_FIELD_76)
    worksheet.write(row, col+76, row_data.EM_USE_TYPE_USE_FOR_FIELD_77)
    worksheet.write(row, col+77, row_data.EM_USE_TYPE_USE_FOR_TENANTED_FIELD_78)
    worksheet.write(row, col+78, row_data.EM_USE_TYPE_COMMENTS_FIELD_79)
    worksheet.write(row, col+79, row_data.WEB_ROW_ID_FIELD_80)
    worksheet.write(row, col+80, row_data.WEB_ADDRESS_FIELD_81)
    worksheet.write(row, col+81, row_data.WEB_ADDRESS_COMMENT_FIELD_82)
    worksheet.write(row, col+82, row_data.WEB_USE_ROW_ID_FIELD_83)
    worksheet.write(row, col+83, row_data.WEB_USE_PUBLIC_FIELD_84)
    worksheet.write(row, col+84, row_data.WEB_USE_TYPE_ROW_ID_FIELD_85)
    worksheet.write(row, col+85, row_data.WEB_USE_TYPE_PRIMARY_FIELD_86)
    worksheet.write(row, col+86, row_data.WEB_USE_TYPE_TYPE_FIELD_87)
    worksheet.write(row, col+87, row_data.WEB_USE_USE_FOR_FIELD_88)
    worksheet.write(row, col+88, row_data.WEB_USE_USE_FOR_TENANTED_FIELD_89)
    worksheet.write(row, col+89, row_data.WEB_USE_TYPE_COMMENTS_FIELD_90)
    worksheet.write(row, col+90, row_data.CUST_STATUS_ROW_ID_FIELD_91)
    worksheet.write(row, col+91, row_data.CUST_STATUS_VALUE_FIELD_92)
    worksheet.write(row, col+92, row_data.REASON_FOR_CUST_STATUS_CHANGE_FIELD_93)
    worksheet.write(row, col+93, row_data.CUST_STATUS_CHANGE_REASON_DESCRIP_FIELD_94)
    worksheet.write(row, col+94, row_data.ALWAYS_SEPARATE_PAYMENTS_FIELD_95)
    worksheet.write(row, col+95, row_data.INVOICE_DELIVERY_METHOD_FIELD_96)
    worksheet.write(row, col+96, row_data.INVOICE_NOTIFI_EMAIL_RECIP_FIELD_97)
    worksheet.write(row, col+97, row_data.STATEMENT_DELIVERY_METHOD_FIELD_98)
    worksheet.write(row, col+98, row_data.STATEMENT_NOTIF_EMAIL_RECIP_FIELD_99)
    worksheet.write(row, col+99, row_data.NOTE_ROW_ID_FIELD_100)
    worksheet.write(row, col+100, row_data.CREATED_FIELD_101)
    worksheet.write(row, col+101, row_data.LAST_UPDATED_FIELD_102)
    worksheet.write(row, col+102, row_data.WORKER_FIELD_103)
    worksheet.write(row, col+103, row_data.BUSINESS_ENTITY_CONTACT_FIELD_104)
    worksheet.write(row, col+104, row_data.SYSTEM_USER_FIELD_105)
    worksheet.write(row, col+105, row_data.NOTE_CONTENT_FIELD_106)
    worksheet.write(row, col+106, row_data.FOLLOWUP_DATE_FIELD_107)
    worksheet.write(row, col+107, row_data.WORKTAGS_FIELD_108)
    worksheet.write(row, col+108, row_data.DATE_FILTER)
    row += 1

workbook.close()

import combine2xlsx

combine2xlsx.combine(['put_customer_headers.xlsx', 'apcc_cust_test_hold.xlsx'], 'apcc_cust_test.xlsx')