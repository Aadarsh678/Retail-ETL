def get_merge_query_returns_union() -> str:
    return """
    MERGE INTO ods.returns AS ods
    USING (
        -- ASIA
        SELECT
            return_id,
            order_id,
            return_reason,
            return_status,
            return_timestamp,
            refund_amount,
            refund_timestamp,
            created_at,
            NULL AS cooling_off_period_days,
            'ASIA' AS source_region,
            _source AS source_system
        FROM staging_asia.returns

        UNION ALL

        -- EU
        SELECT
            return_id,
            order_id,
            return_reason,
            return_status,
            return_timestamp,
            refund_amount,
            refund_timestamp,
            created_at,
            cooling_off_period_days,
            'EU' AS source_region,
            _source AS source_system
        FROM staging_eu.returns

        UNION ALL

        -- US
        SELECT
            return_id,
            order_id,
            return_reason,
            return_status,
            return_timestamp,
            refund_amount,
            refund_timestamp,
            created_at,
            NULL AS cooling_off_period_days,
            'US' AS source_region,
            _source AS source_system
        FROM staging_us.returns
    ) src
    ON ods.return_id = src.return_id AND ods.source_region = src.source_region
    WHEN MATCHED THEN UPDATE SET
        order_id = src.order_id,
        return_reason = src.return_reason,
        return_status = src.return_status,
        return_timestamp = src.return_timestamp,
        refund_amount = src.refund_amount,
        refund_timestamp = src.refund_timestamp,
        created_at = src.created_at,
        cooling_off_period_days = src.cooling_off_period_days,
        source_system = src.source_system
    WHEN NOT MATCHED THEN INSERT (
        return_id,
        order_id,
        return_reason,
        return_status,
        return_timestamp,
        refund_amount,
        refund_timestamp,
        created_at,
        cooling_off_period_days,
        source_region,
        source_system
    ) VALUES (
        src.return_id,
        src.order_id,
        src.return_reason,
        src.return_status,
        src.return_timestamp,
        src.refund_amount,
        src.refund_timestamp,
        src.created_at,
        src.cooling_off_period_days,
        src.source_region,
        src.source_system
    );

    
    """
