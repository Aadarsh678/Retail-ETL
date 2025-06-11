def dim_campaign_sql() -> str:
    return """
MERGE INTO star.dim_marketing_campaign AS target
USING (
    SELECT 
        mc.campaign_id,
        mc.campaign_name,
        COALESCE(mc.campaign_type, 'General') AS campaign_type,
        COALESCE(mc.channel, 'Direct') AS channel,
        DATE(mc.start_date) AS start_date,
        DATE(mc.end_date) AS end_date,
        mc.budget_usd,
        mc.target_audience,
        COALESCE(mc.campaign_status, 'Unknown') AS campaign_status,
        mc.source_region,
        COALESCE(mc.is_gdpr_compliant, 
            CASE WHEN mc.source_region = 'EU' THEN TRUE ELSE FALSE END
        ) AS is_gdpr_compliant
    FROM ods.marketing_campaigns mc
) AS src
ON target.campaign_id = src.campaign_id AND target.source_region = src.source_region

WHEN MATCHED THEN UPDATE SET
    campaign_name = src.campaign_name,
    campaign_type = src.campaign_type,
    channel = src.channel,
    start_date = src.start_date,
    end_date = src.end_date,
    budget_usd = src.budget_usd,
    target_audience = src.target_audience,
    campaign_status = src.campaign_status,
    is_gdpr_compliant = src.is_gdpr_compliant

WHEN NOT MATCHED THEN INSERT (
    campaign_id,
    campaign_name,
    campaign_type,
    channel,
    start_date,
    end_date,
    budget_usd,
    target_audience,
    campaign_status,
    source_region,
    is_gdpr_compliant
) VALUES (
    src.campaign_id,
    src.campaign_name,
    src.campaign_type,
    src.channel,
    src.start_date,
    src.end_date,
    src.budget_usd,
    src.target_audience,
    src.campaign_status,
    src.source_region,
    src.is_gdpr_compliant
);
"""