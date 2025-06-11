def get_merge_query_payments_union() -> str:
    return """
    
    MERGE INTO ods.payments AS ods
    USING (
        -- ASIA
        SELECT
            payment_id,
            order_id,
            payment_method,
            payment_status,
            CAST(payment_amount_usd AS DECIMAL(10,2)) As payment_amount_usd , 
            payment_timestamp,
            transaction_id,
            gateway_response,
            NULL AS psd2_compliant,
            created_at,
            'ASIA' AS source_region,
            _source AS source_system
        FROM staging_asia.payments

        UNION ALL

        -- EU
        SELECT
            payment_id,
            order_id,
            payment_method,
            payment_status,
            CAST(payment_amount_usd AS DECIMAL(10,2)) As payment_amount_usd, 
            payment_timestamp,
            transaction_id,
            gateway_response,
            psd2_compliant,
            created_at,
            'EU' AS source_region,
            _source AS source_system
        FROM staging_eu.payments

        UNION ALL

        -- US
        SELECT
            payment_id,
            order_id,
            payment_method,
            payment_status,
            payment_amount_usd,
            payment_timestamp,
            transaction_id,
            gateway_response,
            NULL AS psd2_compliant,
            created_at,
            'US' AS source_region,
            _source AS source_system
        FROM staging_us.payments
    ) src
    ON ods.payment_id = src.payment_id AND ods.source_region = src.source_region
    WHEN MATCHED THEN UPDATE SET
        order_id = src.order_id,
        payment_method = src.payment_method,
        payment_status = src.payment_status,
        payment_amount_usd = src.payment_amount_usd,
        payment_timestamp = src.payment_timestamp,
        transaction_id = src.transaction_id,
        gateway_response = src.gateway_response,
        psd2_compliant = src.psd2_compliant,
        created_at = src.created_at,
        source_system = src.source_system
    WHEN NOT MATCHED THEN INSERT (
        payment_id,
        order_id,
        payment_method,
        payment_status,
        payment_amount_usd,
        payment_timestamp,
        transaction_id,
        gateway_response,
        psd2_compliant,
        created_at,
        source_region,
        source_system
    ) VALUES (
        src.payment_id,
        src.order_id,
        src.payment_method,
        src.payment_status,
        src.payment_amount_usd,
        src.payment_timestamp,
        src.transaction_id,
        src.gateway_response,
        src.psd2_compliant,
        src.created_at,
        src.source_region,
        src.source_system
    );

    """
