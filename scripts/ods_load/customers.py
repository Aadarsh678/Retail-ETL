def get_merge_query_customers_union() -> str:
    return """
    MERGE INTO ods.customers AS ods
    USING (
        -- ASIA region
        SELECT
            customer_id,
            email,
            first_name,
            last_name,
            phone,
            CAST(birth_date AS DATE) AS birth_date,
            gender,
            NULL AS country_code,
            registration_date,
            last_login,
            customer_segment,
            acquisition_channel,
            TRUE AS is_active,
            NULL AS gdpr_consent_date,
            NULL AS data_retention_until,
            created_at,
            created_at AS updated_at,
            'ASIA' AS source_region,
            _source AS source_system
        FROM staging_asia.customers

        UNION ALL

        -- EU region
        SELECT
            customer_id,
            email,
            first_name,
            last_name,
            phone,
            NULL AS birth_date,
            NULL AS gender,
            country_code,
            registration_date,
            last_login,
            customer_segment,
            acquisition_channel,
            is_active,
            gdpr_consent_date,
            data_retention_until,
            created_at,
            updated_at,
            'EU' AS source_region,
            _source AS source_system
        FROM staging_eu.customers

        UNION ALL

        -- US region
        SELECT
            customer_id,
            email,
            first_name,
            last_name,
            phone,
            birth_date,
            gender,
            NULL AS country_code,
            registration_date,
            last_login,
            customer_segment,
            acquisition_channel,
            is_active,
            NULL AS gdpr_consent_date,
            NULL AS data_retention_until,
            created_at,
            updated_at,
            'US' AS source_region,
            _source AS source_system
        FROM staging_us.customers
    ) src
    ON ods.customer_id = src.customer_id AND ods.source_region = src.source_region
    WHEN MATCHED THEN UPDATE SET
        email = src.email,
        first_name = src.first_name,
        last_name = src.last_name,
        phone = src.phone,
        birth_date = src.birth_date,
        gender = src.gender,
        country_code = src.country_code,
        registration_date = src.registration_date,
        last_login = src.last_login,
        customer_segment = src.customer_segment,
        acquisition_channel = src.acquisition_channel,
        is_active = src.is_active,
        gdpr_consent_date = src.gdpr_consent_date,
        data_retention_until = src.data_retention_until,
        created_at = src.created_at,
        updated_at = src.updated_at,
        source_system = src.source_system

    WHEN NOT MATCHED THEN INSERT (
        customer_id, email, first_name, last_name, phone, birth_date, gender,
        country_code, registration_date, last_login, customer_segment,
        acquisition_channel, is_active, gdpr_consent_date, data_retention_until,
        created_at, updated_at, source_region, source_system
    ) VALUES (
        src.customer_id, src.email, src.first_name, src.last_name, src.phone, src.birth_date, src.gender,
        src.country_code, src.registration_date, src.last_login, src.customer_segment,
        src.acquisition_channel, src.is_active, src.gdpr_consent_date, src.data_retention_until,
        src.created_at, src.updated_at, src.source_region, src.source_system
    );
    """
