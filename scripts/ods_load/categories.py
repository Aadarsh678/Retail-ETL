# def get_merge_query_categories_union() -> str:
#     return """
#     MERGE INTO ods.categories AS ods
#     USING (
#         SELECT
#             category_name,
#             parent_category_id,
#             category_path,
#             NULL AS category_name_local,
#             FALSE AS is_gdpr_sensitive,
#             is_active,
#             created_at,
#             updated_at,
#             'ASIA' AS source_region,
#             _source AS source_system
#         FROM staging_asia.categories

#         UNION ALL

#         SELECT
#             category_name,
#             parent_category_id,
#             category_path,
#             category_name_local,
#             is_gdpr_sensitive,
#             is_active,
#             created_at,
#             updated_at,
#             'EU' AS source_region,
#             _source AS source_system
#         FROM staging_eu.categories

#         UNION ALL

#         SELECT
#             category_name,
#             parent_category_id,
#             category_path,
#             NULL AS category_name_local,
#             FALSE AS is_gdpr_sensitive,
#             is_active,
#             created_at,
#             updated_at,
#             'US' AS source_region,
#             _source AS source_system
#         FROM staging_us.categories
#     ) src
#     ON ods.category_name = src.category_name AND ods.source_region = src.source_region
#     WHEN MATCHED THEN UPDATE SET
#         parent_category_id = src.parent_category_id,
#         category_path = src.category_path,
#         category_name_local = src.category_name_local,
#         is_gdpr_sensitive = src.is_gdpr_sensitive,
#         is_active = src.is_active,
#         updated_at = src.updated_at,
#         source_system = src.source_system
#     WHEN NOT MATCHED THEN INSERT (
#         category_name, parent_category_id, category_path,
#         category_name_local, is_gdpr_sensitive, is_active,
#         created_at, updated_at, source_region, source_system
#     ) VALUES (
#         src.category_name, src.parent_category_id, src.category_path,
#         src.category_name_local, src.is_gdpr_sensitive, src.is_active,
#         src.created_at, src.updated_at, src.source_region, src.source_system
#     );
#     """



def get_merge_query_categories_union() -> str:
    return """
    MERGE INTO ods.categories AS ods
    USING (
        SELECT
            category_name,
            parent_category_id,
            category_path,
            NULL AS category_name_local,
            FALSE AS is_gdpr_sensitive,
            is_active,
            created_at::TIMESTAMP_NTZ AS created_at,
            updated_at::TIMESTAMP_NTZ AS updated_at,
            'ASIA' AS source_region,
            _source AS source_system
        FROM staging_asia.categories

        UNION ALL

        SELECT
            category_name,
            parent_category_id,
            category_path,
            category_name_local,
            is_gdpr_sensitive,
            is_active,
            created_at::TIMESTAMP_NTZ AS created_at,
            updated_at::TIMESTAMP_NTZ AS updated_at,
            'EU' AS source_region,
            _source AS source_system
        FROM staging_eu.categories

        UNION ALL

        SELECT
            category_name,
            parent_category_id,
            category_path,
            NULL AS category_name_local,
            FALSE AS is_gdpr_sensitive,
            is_active,
            created_at::TIMESTAMP_NTZ AS created_at,
            updated_at::TIMESTAMP_NTZ AS updated_at,
            'US' AS source_region,
            _source AS source_system
        FROM staging_us.categories
    ) AS src
    ON ods.category_name = src.category_name
       AND ods.source_region = src.source_region
    WHEN MATCHED THEN UPDATE SET
        ods.parent_category_id = src.parent_category_id,
        ods.category_path = src.category_path,
        ods.category_name_local = src.category_name_local,
        ods.is_gdpr_sensitive = src.is_gdpr_sensitive,
        ods.is_active = src.is_active,
        ods.updated_at = src.updated_at,
        ods.source_system = src.source_system
    WHEN NOT MATCHED THEN INSERT (
        category_name,
        parent_category_id,
        category_path,
        category_name_local,
        is_gdpr_sensitive,
        is_active,
        created_at,
        updated_at,
        source_region,
        source_system
    ) VALUES (
        src.category_name,
        src.parent_category_id,
        src.category_path,
        src.category_name_local,
        src.is_gdpr_sensitive,
        src.is_active,
        src.created_at,
        src.updated_at,
        src.source_region,
        src.source_system
    );
    """