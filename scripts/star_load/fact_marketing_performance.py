def fact_marketing_performance_sql() -> str:
    return """
MERGE INTO fact_marketing_performance AS target
USING (
    WITH marketing_data AS (
        SELECT 
            mp.campaign_id,
            mp.source_region,
            mp.record_date,
            
            -- Aggregate measures by campaign, region, and date
            SUM(mp.impressions) AS total_impressions,
            SUM(mp.clicks) AS total_clicks,
            SUM(mp.conversions) AS total_conversions,
            SUM(mp.spend_usd) AS total_spend_usd,
            SUM(mp.conversion_value_usd) AS total_revenue_usd,
            
            -- Get campaign details
            mc.campaign_name,
            mc.campaign_type,
            mc.channel,
            mc.target_audience,
            mc.campaign_status
            
        FROM ods.marketing_performance mp
        JOIN ods.marketing_campaigns mc 
          ON mp.campaign_id = mc.campaign_id 
         AND mp.source_region = mc.source_region
        
        WHERE LOWER(mc.campaign_status) = 'active'   -- Only active campaigns
          AND mp.record_date IS NOT NULL
        
        GROUP BY 
            mp.campaign_id, mp.source_region, mp.record_date,
            mc.campaign_name, mc.campaign_type, mc.channel, 
            mc.target_audience, mc.campaign_status
    ),
    
    dimension_keys AS (
        SELECT 
            md.*,
            
            -- Get dimension keys
            dt.time_key AS date_key,
            dmc.campaign_key,
            dg.geography_key,
            
            -- Calculate new customer acquisitions from sales data
            COALESCE(new_customers.new_customer_count, 0) AS new_customers,
            
            -- Calculate derived metrics
            CASE 
                WHEN md.total_impressions > 0 
                THEN ROUND(md.total_clicks * 1.0 / md.total_impressions, 4)
                ELSE 0 
            END AS click_through_rate,
            
            CASE 
                WHEN md.total_clicks > 0 
                THEN ROUND(md.total_conversions * 1.0 / md.total_clicks, 4)
                ELSE 0 
            END AS conversion_rate,
            
            CASE 
                WHEN md.total_clicks > 0 
                THEN ROUND(md.total_spend_usd / md.total_clicks, 2)
                ELSE 0 
            END AS cost_per_click,
            
            CASE 
                WHEN md.total_conversions > 0 
                THEN ROUND(md.total_spend_usd / md.total_conversions, 2)
                ELSE 0 
            END AS cost_per_acquisition,
            
            CASE 
                WHEN md.total_spend_usd > 0 
                THEN ROUND(md.total_revenue_usd / md.total_spend_usd, 2)
                ELSE 0 
            END AS return_on_ad_spend
            
        FROM marketing_data md
        
        -- Join with dimension tables to get keys
        JOIN dim_time dt 
          ON TO_NUMBER(TO_CHAR(md.record_date, 'YYYYMMDD')) = dt.time_key
        
        JOIN dim_marketing_campaign dmc 
          ON md.campaign_id = dmc.campaign_id 
         AND md.source_region = dmc.source_region
        
        -- Get geography key based on campaign's source region
        JOIN dim_geography dg ON (
            (md.source_region = 'US' AND dg.region = 'US') OR
            (md.source_region = 'EU' AND dg.region = 'EU') OR
            (md.source_region = 'ASIA' AND dg.region = 'ASIA')
        )
        
        -- Calculate new customers acquired through this campaign on this date
        LEFT JOIN (
            SELECT 
                fs.campaign_key,
                dt.date_actual,
                COUNT(DISTINCT fs.customer_key) AS new_customer_count
            FROM fact_sales fs
            JOIN dim_time dt ON fs.order_date_key = dt.time_key
            WHERE fs.is_first_purchase = TRUE
            GROUP BY fs.campaign_key, dt.date_actual
        ) new_customers 
          ON dmc.campaign_key = new_customers.campaign_key 
         AND md.record_date = new_customers.date_actual
    ),
    
    final_marketing_data AS (
        SELECT 
            dk.date_key,
            dk.campaign_key,
            dk.geography_key,
            dk.total_impressions AS impressions,
            dk.total_clicks AS clicks,
            dk.total_conversions AS conversions,
            dk.total_spend_usd AS spend_usd,
            dk.total_revenue_usd AS revenue_usd,
            dk.new_customers,
            dk.click_through_rate,
            dk.conversion_rate,
            dk.cost_per_click,
            dk.cost_per_acquisition,
            dk.return_on_ad_spend,
            dk.source_region
        FROM dimension_keys dk
    )
    
    SELECT * FROM final_marketing_data
) AS source
ON target.date_key = source.date_key
AND target.campaign_key = source.campaign_key
AND target.geography_key = source.geography_key

WHEN MATCHED THEN UPDATE SET
    impressions = source.impressions,
    clicks = source.clicks,
    conversions = source.conversions,
    spend_usd = source.spend_usd,
    revenue_usd = source.revenue_usd,
    new_customers = source.new_customers,
    click_through_rate = source.click_through_rate,
    conversion_rate = source.conversion_rate,
    cost_per_click = source.cost_per_click,
    cost_per_acquisition = source.cost_per_acquisition,
    return_on_ad_spend = source.return_on_ad_spend,
    source_region = source.source_region

WHEN NOT MATCHED THEN INSERT (
    date_key,
    campaign_key,
    geography_key,
    impressions,
    clicks,
    conversions,
    spend_usd,
    revenue_usd,
    new_customers,
    click_through_rate,
    conversion_rate,
    cost_per_click,
    cost_per_acquisition,
    return_on_ad_spend,
    source_region
) VALUES (
    source.date_key,
    source.campaign_key,
    source.geography_key,
    source.impressions,
    source.clicks,
    source.conversions,
    source.spend_usd,
    source.revenue_usd,
    source.new_customers,
    source.click_through_rate,
    source.conversion_rate,
    source.cost_per_click,
    source.cost_per_acquisition,
    source.return_on_ad_spend,
    source.source_region
);
"""

