def get_merge_query_marketing_campaigns_union() -> str:
    return """
    MERGE INTO ODS.MARKETING_CAMPAIGNS AS ods
    USING (
        -- ASIA
        SELECT
            campaign_id,
            campaign_name,
            campaign_type,
            channel,
            start_date AS start_date,
            end_date AS end_date,
            CAST(budget_usd AS DECIMAL(10,2)) As budget_usd, 
            target_audience,
            campaign_status,
            NULL AS is_gdpr_compliant,
            created_at AS created_at,
            NULL AS updated_at,
            'ASIA' AS source_region,
            _source AS source_system
        FROM STAGING_ASIA.MARKETING_CAMPAIGNS asia

        UNION ALL

        -- EU
        SELECT
            campaign_id,
            campaign_name,
            campaign_type,
            channel,
            start_date,
            end_date,
            CAST(budget_usd AS DECIMAL(10,2)) As budget_usd, 
            target_audience,
            campaign_status,
            is_gdpr_compliant,
            created_at,
            updated_at,
            'EU' AS source_region,
            _source AS source_system
        FROM STAGING_EU.MARKETING_CAMPAIGNS eu

        UNION ALL

        -- US
        SELECT
            campaign_id,
            campaign_name,
            campaign_type,
            channel,
            start_date,
            end_date,
            budget_usd,
            target_audience,
            campaign_status,
            NULL AS is_gdpr_compliant,
            created_at,
            updated_at,
            'US' AS source_region,
            _source AS source_system
        FROM STAGING_US.MARKETING_CAMPAIGNS
    ) AS src
    ON ods.campaign_id = src.campaign_id AND ods.source_region = src.source_region

    WHEN MATCHED THEN UPDATE SET
        campaign_name = src.campaign_name,
        campaign_type = src.campaign_type,
        channel = src.channel,
        start_date = src.start_date,
        end_date = src.end_date,
        budget_usd = src.budget_usd,
        target_audience = src.target_audience,
        campaign_status = src.campaign_status,
        is_gdpr_compliant = src.is_gdpr_compliant,
        created_at = src.created_at,
        updated_at = src.updated_at,
        source_system = src.source_system

    WHEN NOT MATCHED THEN INSERT (
        campaign_id, campaign_name, campaign_type, channel, start_date, end_date,
        budget_usd, target_audience, campaign_status, is_gdpr_compliant,
        created_at, updated_at, source_region, source_system
    ) VALUES (
        src.campaign_id, src.campaign_name, src.campaign_type, src.channel, src.start_date, src.end_date,
        src.budget_usd, src.target_audience, src.campaign_status, src.is_gdpr_compliant,
        src.created_at, src.updated_at, src.source_region, src.source_system
    );
    """
