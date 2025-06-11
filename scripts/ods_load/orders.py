def get_merge_query_orders_union() -> str:
    return """
    MERGE INTO ods.orders AS ods
    USING (
        SELECT
            order_id,
            order_reference,
            customer_id,
            order_status,
            order_timestamp,
            CAST(subtotal_usd AS DECIMAL(10,2)) AS subtotal_usd,
            CAST(tax_amount_usd AS DECIMAL(10,2)) AS tax_amount_usd,
            CAST(shipping_amount_usd AS DECIMAL(10,2)) AS shipping_amount_usd,
            CAST(discount_amount_usd AS DECIMAL(10,2)) AS discount_amount_usd,
            CAST(total_amount_usd AS DECIMAL(10,2)) AS total_amount_usd,
            billing_address_id,
            shipping_address_id,
            campaign_id,
            discount_id,
            NULL AS invoice_required,
            created_at,
            created_at AS updated_at,
            'ASIA' AS source_region,
            _source AS source_system
        FROM staging_asia.orders

        UNION ALL

        SELECT
            order_id,
            order_reference,
            customer_id,
            order_status,
            order_timestamp,
            CAST(subtotal_usd AS DECIMAL(10,2)) AS subtotal_usd,
            CAST(tax_amount_usd AS DECIMAL(10,2)) AS tax_amount_usd,
            CAST(shipping_amount_usd AS DECIMAL(10,2)) AS shipping_amount_usd,
            CAST(discount_amount_usd AS DECIMAL(10,2)) AS discount_amount_usd,
            CAST(total_amount_usd AS DECIMAL(10,2)) AS total_amount_usd,
            billing_address_id,
            shipping_address_id,
            campaign_id,
            discount_id,
            invoice_required,
            created_at,
            created_at AS updated_at,
            'EU' AS source_region,
            _source AS source_system
        FROM staging_eu.orders

        UNION ALL

        SELECT
            order_id,
            order_reference,
            customer_id,
            order_status,
            order_timestamp,
            CAST(subtotal_usd AS DECIMAL(10,2)) AS subtotal_usd,
            CAST(tax_amount_usd AS DECIMAL(10,2)) AS tax_amount_usd,
            CAST(shipping_amount_usd AS DECIMAL(10,2)) AS shipping_amount_usd,
            CAST(discount_amount_usd AS DECIMAL(10,2)) AS discount_amount_usd,
            CAST(total_amount_usd AS DECIMAL(10,2)) AS total_amount_usd,
            billing_address_id,
            shipping_address_id,
            campaign_id,
            discount_id,
            NULL AS invoice_required,
            created_at,
            created_at AS updated_at,
            'US' AS source_region,
            _source AS source_system
        FROM staging_us.orders
    ) src
    ON ods.order_id = src.order_id AND ods.source_region = src.source_region
    WHEN MATCHED THEN UPDATE SET
        order_reference = src.order_reference,
        customer_id = src.customer_id,
        order_status = src.order_status,
        order_timestamp = src.order_timestamp,
        subtotal_usd = src.subtotal_usd,
        tax_amount_usd = src.tax_amount_usd,
        shipping_amount_usd = src.shipping_amount_usd,
        discount_amount_usd = src.discount_amount_usd,
        total_amount_usd = src.total_amount_usd,
        billing_address_id = src.billing_address_id,
        shipping_address_id = src.shipping_address_id,
        campaign_id = src.campaign_id,
        discount_id = src.discount_id,
        invoice_required = src.invoice_required,
        created_at = src.created_at,
        updated_at = src.updated_at,
        source_system = src.source_system
    WHEN NOT MATCHED THEN INSERT (
        order_id,
        order_reference,
        customer_id,
        order_status,
        order_timestamp,
        subtotal_usd,
        tax_amount_usd,
        shipping_amount_usd,
        discount_amount_usd,
        total_amount_usd,
        billing_address_id,
        shipping_address_id,
        campaign_id,
        discount_id,
        invoice_required,
        created_at,
        updated_at,
        source_region,
        source_system
    ) VALUES (
        src.order_id,
        src.order_reference,
        src.customer_id,
        src.order_status,
        src.order_timestamp,
        src.subtotal_usd,
        src.tax_amount_usd,
        src.shipping_amount_usd,
        src.discount_amount_usd,
        src.total_amount_usd,
        src.billing_address_id,
        src.shipping_address_id,
        src.campaign_id,
        src.discount_id,
        src.invoice_required,
        src.created_at,
        src.updated_at,
        src.source_region,
        src.source_system
    );
    """