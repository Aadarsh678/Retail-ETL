def get_merge_query_categories_union() -> str:
    return """
    MERGE INTO ods.categories AS ods
    USING (
        SELECT
            CATEGORY_NAME,
            PARENT_CATEGORY_ID,
            CATEGORY_PATH,
            NULL AS CATEGORY_NAME_LOCAL,
            FALSE AS IS_GDPR_SENSITIVE,
            IS_ACTIVE,
            CREATED_AT::TIMESTAMP_NTZ AS CREATED_AT,
            UPDATED_AT::TIMESTAMP_NTZ AS UPDATED_AT,
            'ASIA' AS SOURCE_REGION,
            _SOURCE AS SOURCE_SYSTEM
        FROM staging_asia.categories

        UNION ALL

        SELECT
            CATEGORY_NAME,
            PARENT_CATEGORY_ID,
            CATEGORY_PATH,
            CATEGORY_NAME_LOCAL,
            GDPR_SENSITIVE AS IS_GDPR_SENSITIVE,
            IS_ACTIVE,
            CREATED_AT::TIMESTAMP_NTZ AS CREATED_AT,
            UPDATED_AT::TIMESTAMP_NTZ AS UPDATED_AT,
            'EU' AS SOURCE_REGION,
            _SOURCE AS SOURCE_SYSTEM
        FROM staging_eu.categories

        UNION ALL

        SELECT
            CATEGORY_NAME,
            PARENT_CATEGORY_ID,
            CATEGORY_PATH,
            NULL AS CATEGORY_NAME_LOCAL,
            FALSE AS IS_GDPR_SENSITIVE,
            IS_ACTIVE,
            CREATED_AT::TIMESTAMP_NTZ AS CREATED_AT,
            UPDATED_AT::TIMESTAMP_NTZ AS UPDATED_AT,
            'US' AS SOURCE_REGION,
            _SOURCE AS SOURCE_SYSTEM
        FROM staging_us.categories
    ) AS src
    ON ods.CATEGORY_NAME = src.CATEGORY_NAME
       AND ods.SOURCE_REGION = src.SOURCE_REGION
    WHEN MATCHED THEN UPDATE SET
        ods.PARENT_CATEGORY_ID = src.PARENT_CATEGORY_ID,
        ods.CATEGORY_PATH = src.CATEGORY_PATH,
        ods.CATEGORY_NAME_LOCAL = src.CATEGORY_NAME_LOCAL,
        ods.IS_GDPR_SENSITIVE = src.IS_GDPR_SENSITIVE,
        ods.IS_ACTIVE = src.IS_ACTIVE,
        ods.UPDATED_AT = src.UPDATED_AT,
        ods.SOURCE_SYSTEM = src.SOURCE_SYSTEM
    WHEN NOT MATCHED THEN INSERT (
        CATEGORY_NAME,
        PARENT_CATEGORY_ID,
        CATEGORY_PATH,
        CATEGORY_NAME_LOCAL,
        IS_GDPR_SENSITIVE,
        IS_ACTIVE,
        CREATED_AT,
        UPDATED_AT,
        SOURCE_REGION,
        SOURCE_SYSTEM
    ) VALUES (
        src.CATEGORY_NAME,
        src.PARENT_CATEGORY_ID,
        src.CATEGORY_PATH,
        src.CATEGORY_NAME_LOCAL,
        src.IS_GDPR_SENSITIVE,
        src.IS_ACTIVE,
        src.CREATED_AT,
        src.UPDATED_AT,
        src.SOURCE_REGION,
        src.SOURCE_SYSTEM
    );
    """
