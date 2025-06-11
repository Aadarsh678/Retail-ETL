def get_merge_query_shipments_union() -> str:
    return """
    MERGE INTO ods.shipments AS ods
    USING (
        -- ASIA
        SELECT
            shipment_id,
            order_id,
            tracking_number,
            carrier,
            shipping_method,
            shipped_timestamp,
            estimated_delivery_date,
            actual_delivery_timestamp,
            shipment_status,
            created_at,
            updated_at,
            'ASIA' AS source_region,
            _source AS source_system
        FROM staging_asia.shipments

        UNION ALL

        -- EU
        SELECT
            shipment_id,
            order_id,
            tracking_number,
            carrier,
            shipping_method,
            shipped_timestamp,
            CAST(estimated_delivery_date AS TIMESTAMP) AS estimated_delivery_date,
            actual_delivery_timestamp,
            shipment_status,
            created_at,
            updated_at,
            'EU' AS source_region,
            _source AS source_system
        FROM staging_eu.shipments

        UNION ALL

        -- US
        SELECT
            shipment_id,
            order_id,
            tracking_number,
            carrier,
            shipping_method,
            shipped_timestamp,
            estimated_delivery_date,
            actual_delivery_timestamp,
            shipment_status,
            created_at,
            updated_at,
            'US' AS source_region,
            _source AS source_system
        FROM staging_us.shipments
    ) src
    ON ods.shipment_id = src.shipment_id AND ods.source_region = src.source_region
    WHEN MATCHED THEN UPDATE SET
        order_id = src.order_id,
        tracking_number = src.tracking_number,
        carrier = src.carrier,
        shipping_method = src.shipping_method,
        shipped_timestamp = src.shipped_timestamp,
        estimated_delivery_date = src.estimated_delivery_date,
        actual_delivery_timestamp = src.actual_delivery_timestamp,
        shipment_status = src.shipment_status,
        created_at = src.created_at,
        updated_at = src.updated_at,
        source_system = src.source_system
    WHEN NOT MATCHED THEN INSERT (
        shipment_id,
        order_id,
        tracking_number,
        carrier,
        shipping_method,
        shipped_timestamp,
        estimated_delivery_date,
        actual_delivery_timestamp,
        shipment_status,
        created_at,
        updated_at,
        source_region,
        source_system
    ) VALUES (
        src.shipment_id,
        src.order_id,
        src.tracking_number,
        src.carrier,
        src.shipping_method,
        src.shipped_timestamp,
        src.estimated_delivery_date,
        src.actual_delivery_timestamp,
        src.shipment_status,
        src.created_at,
        src.updated_at,
        src.source_region,
        src.source_system
    );
    """
