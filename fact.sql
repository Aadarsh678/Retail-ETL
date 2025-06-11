CREATE TABLE fact_customer (
    customer_behavior_key INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- Dimension Keys
    date_key INTEGER NOT NULL,
    customer_key INTEGER NOT NULL,
    geography_key INTEGER NOT NULL,
    
    -- Measures (actual data only, no web analytics)
    cart_additions INTEGER DEFAULT 0,
    cart_abandonments INTEGER DEFAULT 0,
    wishlist_additions INTEGER DEFAULT 0,
    reviews_written INTEGER DEFAULT 0,
    
    -- Calculated measures
    cart_abandonment_rate DECIMAL(5,4),
    engagement_score DECIMAL(8,2),
    
    source_region VARCHAR(10) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign Keys
    FOREIGN KEY (date_key) REFERENCES dim_time(time_key),
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (geography_key) REFERENCES dim_geography(geography_key)
);
CREATE TABLE IF NOT EXISTS fact_sales (
    sales_key INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- Dimension Keys
    order_date_key INTEGER NOT NULL,
    customer_key INTEGER NOT NULL,
    product_key INTEGER NOT NULL,
    geography_key INTEGER NOT NULL,
    campaign_key INTEGER,
    payment_method_key INTEGER,
    
    -- Degenerate Dimensions (IDs kept for drill-down)
    order_id INTEGER NOT NULL,
    order_reference VARCHAR(60),
    source_region VARCHAR(10) NOT NULL,
    
    -- Measures
    quantity INTEGER NOT NULL,
    unit_price_usd DECIMAL(12,4) NOT NULL,
    total_price_usd DECIMAL(14,4) NOT NULL,
    cost_usd DECIMAL(12,4),
    gross_profit_usd DECIMAL(14,4),
    discount_amount_usd DECIMAL(12,2) DEFAULT 0,
    tax_amount_usd DECIMAL(12,2) DEFAULT 0,
    shipping_amount_usd DECIMAL(12,2) DEFAULT 0,
    
    -- Flags for analysis
    is_first_purchase BOOLEAN DEFAULT FALSE,
    is_returned BOOLEAN DEFAULT FALSE,
    days_to_ship INTEGER,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign Keys
    FOREIGN KEY (order_date_key) REFERENCES dim_time(time_key),
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    FOREIGN KEY (geography_key) REFERENCES dim_geography(geography_key),
    FOREIGN KEY (campaign_key) REFERENCES dim_marketing_campaign(campaign_key),
    FOREIGN KEY (payment_method_key) REFERENCES dim_payment_method(payment_method_key)
);
-- Marketing Performance Fact Table
CREATE TABLE IF NOT EXISTS fact_marketing_performance (
    marketing_key INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- Dimension Keys
    date_key INTEGER NOT NULL,
    campaign_key INTEGER NOT NULL,
    geography_key INTEGER NOT NULL,
    
    -- Measures
    impressions INTEGER DEFAULT 0,
    clicks INTEGER DEFAULT 0,
    conversions INTEGER DEFAULT 0,
    spend_usd DECIMAL(12,2) DEFAULT 0,
    revenue_usd DECIMAL(14,2) DEFAULT 0,
    new_customers INTEGER DEFAULT 0,
    
    -- Calculated measures
    click_through_rate DECIMAL(6,4),
    conversion_rate DECIMAL(6,4),
    cost_per_click DECIMAL(8,2),
    cost_per_acquisition DECIMAL(10,2),
    return_on_ad_spend DECIMAL(8,2),
    
    source_region VARCHAR(10) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign Keys
    FOREIGN KEY (date_key) REFERENCES dim_time(time_key),
    FOREIGN KEY (campaign_key) REFERENCES dim_marketing_campaign(campaign_key),
    FOREIGN KEY (geography_key) REFERENCES dim_geography(geography_key)
);

-- Product Performance Fact Table
CREATE TABLE IF NOT EXISTS fact_product_performance (
    product_performance_key INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- Dimension Keys
    date_key INTEGER NOT NULL,
    product_key INTEGER NOT NULL,
    geography_key INTEGER NOT NULL,
    
    -- Measures (daily aggregated)
    units_sold INTEGER DEFAULT 0,
    revenue_usd DECIMAL(14,2) DEFAULT 0,
    cost_usd DECIMAL(12,2) DEFAULT 0,
    gross_profit_usd DECIMAL(14,2) DEFAULT 0,
    returns INTEGER DEFAULT 0,
    return_value_usd DECIMAL(12,2) DEFAULT 0,
    reviews_count INTEGER DEFAULT 0,
    average_rating DECIMAL(3,2),
    inventory_turns DECIMAL(8,2),
    stockout_days INTEGER DEFAULT 0,
    
    -- Calculated measures
    return_rate DECIMAL(5,4),
    profit_margin DECIMAL(5,4),
    
    source_region VARCHAR(10) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign Keys
    FOREIGN KEY (date_key) REFERENCES dim_time(time_key),
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    FOREIGN KEY (geography_key) REFERENCES dim_geography(geography_key)
);