-- ===============================================
-- COMPREHENSIVE STAR SCHEMA FOR ANALYTICS
-- Designed for: Sales, Marketing, Product, Executive Teams
-- ===============================================

-- ===============================================
-- DIMENSION TABLES
-- ===============================================

-- Time Dimension - Critical for all analysis
CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,  -- YYYYMMDD format
    full_date DATE NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    quarter_name VARCHAR(6) NOT NULL, -- 'Q1 2024'
    month INTEGER NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    month_year VARCHAR(10) NOT NULL, -- 'Jan 2024'
    week_of_year INTEGER NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN DEFAULT FALSE,
    holiday_name VARCHAR(50),
    fiscal_year INTEGER,
    fiscal_quarter INTEGER,
    season VARCHAR(10), -- Spring, Summer, Fall, Winter
    is_month_end BOOLEAN DEFAULT FALSE,
    is_quarter_end BOOLEAN DEFAULT FALSE,
    is_year_end BOOLEAN DEFAULT FALSE
);

-- Customer Dimension - Flattened for easy analysis
CREATE TABLE dim_customer (
    customer_key INTEGER PRIMARY KEY AUTOINCREMENT, -- Surrogate key
    customer_id INTEGER NOT NULL,
    source_region VARCHAR(10) NOT NULL,
    
    -- Basic customer info
    email_hash VARCHAR(255), -- Masked for GDPR
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(200), -- Concatenated for reports
    phone VARCHAR(50),
    birth_date DATE,
    age_group VARCHAR(20), -- '18-24', '25-34', etc.
    gender VARCHAR(20),
    
    -- Geographic info (flattened from addresses)
    country_code VARCHAR(2),
    country_name VARCHAR(100),
    region VARCHAR(100),
    city VARCHAR(100),
    postal_code VARCHAR(20),
    
    -- Customer behavior attributes
    registration_date DATE,
    first_order_date DATE,
    last_order_date DATE,
    customer_tenure_days INTEGER,
    customer_segment VARCHAR(50), -- VIP, Regular, New, At-Risk, etc.
    customer_tier VARCHAR(20), -- Bronze, Silver, Gold, Platinum
    acquisition_channel VARCHAR(100),
    acquisition_campaign VARCHAR(100),
    
    -- Customer lifetime metrics (slowly changing)
    lifetime_orders INTEGER DEFAULT 0,
    lifetime_value_usd DECIMAL(12,2) DEFAULT 0,
    average_order_value DECIMAL(10,2) DEFAULT 0,
    days_since_last_order INTEGER,
    
    -- Status flags
    is_active BOOLEAN DEFAULT TRUE,
    is_vip BOOLEAN DEFAULT FALSE,
    is_b2b BOOLEAN DEFAULT FALSE,
    has_gdpr_consent BOOLEAN DEFAULT FALSE,
    
    -- SCD tracking
    effective_date DATE NOT NULL,
    expiry_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    
    -- Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(customer_id, source_region, effective_date)
);

-- Product Dimension - Comprehensive product hierarchy
CREATE TABLE dim_product (
    product_key INTEGER PRIMARY KEY AUTOINCREMENT, -- Surrogate key
    product_id INTEGER NOT NULL,
    source_region VARCHAR(10) NOT NULL,
    
    -- Product identification
    product_sku VARCHAR(30) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    product_description TEXT,
    
    -- Product hierarchy (flattened from categories)
    category_l1_id INTEGER, -- Top level category
    category_l1_name VARCHAR(100),
    category_l2_id INTEGER, -- Sub category
    category_l2_name VARCHAR(100),
    category_l3_id INTEGER, -- Detailed category
    category_l3_name VARCHAR(100),
    category_path VARCHAR(500), -- Full breadcrumb
    
    -- Variant information (flattened)
    variant_id BIGINT,
    variant_name VARCHAR(255),
    variant_type VARCHAR(100), -- Color, Size, Model, etc.
    variant_value VARCHAR(100),
    
    -- Pricing and costs
    current_price_usd DECIMAL(10,2),
    original_price_usd DECIMAL(10,2),
    cost_usd DECIMAL(10,2),
    margin_usd DECIMAL(10,2),
    margin_percent DECIMAL(5,2),
    
    -- Physical attributes
    weight_kg DECIMAL(8,3),
    length_cm DECIMAL(8,2),
    width_cm DECIMAL(8,2),
    height_cm DECIMAL(8,2),
    volume_cm3 DECIMAL(12,2), -- Calculated
    
    -- Product characteristics
    brand VARCHAR(100),
    manufacturer VARCHAR(100),
    energy_rating VARCHAR(10),
    vat_rate DECIMAL(5,2),
    
    -- Product performance indicators
    avg_rating DECIMAL(3,2), -- From reviews
    review_count INTEGER DEFAULT 0,
    return_rate DECIMAL(5,2) DEFAULT 0,
    
    -- Product lifecycle
    launch_date DATE,
    discontinue_date DATE,
    product_age_days INTEGER,
    lifecycle_stage VARCHAR(20), -- New, Growth, Mature, Decline
    
    -- Status flags
    is_active BOOLEAN DEFAULT TRUE,
    is_featured BOOLEAN DEFAULT FALSE,
    is_seasonal BOOLEAN DEFAULT FALSE,
    is_gdpr_sensitive BOOLEAN DEFAULT FALSE,
    
    -- SCD tracking
    effective_date DATE NOT NULL,
    expiry_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    
    -- Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(product_id, variant_id, source_region, effective_date)
);

-- Marketing Campaign Dimension
CREATE TABLE dim_campaign (
    campaign_key INTEGER PRIMARY KEY AUTOINCREMENT,
    campaign_id INTEGER NOT NULL,
    source_region VARCHAR(10) NOT NULL,
    
    -- Campaign identification
    campaign_name VARCHAR(255) NOT NULL,
    campaign_code VARCHAR(50),
    campaign_type VARCHAR(50), -- Email, Social, PPC, Display, etc.
    
    -- Channel information
    primary_channel VARCHAR(100), -- Email, Facebook, Google, etc.
    channel_category VARCHAR(50), -- Paid, Organic, Social, Email
    
    -- Campaign details
    start_date DATE,
    end_date DATE,
    campaign_duration_days INTEGER,
    budget_usd DECIMAL(12,2),
    target_audience TEXT,
    campaign_objective VARCHAR(100), -- Awareness, Conversion, Retention
    
    -- Campaign status
    campaign_status VARCHAR(20), -- Active, Paused, Completed, Cancelled
    is_gdpr_compliant BOOLEAN DEFAULT TRUE,
    
    -- Performance metrics (updated periodically)
    total_impressions BIGINT DEFAULT 0,
    total_clicks BIGINT DEFAULT 0,
    click_rate DECIMAL(5,4) DEFAULT 0,
    conversion_rate DECIMAL(5,4) DEFAULT 0,
    cost_per_click DECIMAL(8,4) DEFAULT 0,
    
    -- Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(campaign_id, source_region)
);

-- Geography Dimension
CREATE TABLE dim_geography (
    geography_key INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- Geographic hierarchy
    country_code VARCHAR(2) NOT NULL,
    country_name VARCHAR(100) NOT NULL,
    region VARCHAR(100),
    state_province VARCHAR(100),
    city VARCHAR(100),
    postal_code VARCHAR(20),
    
    -- Business geography
    sales_territory VARCHAR(100),
    sales_region VARCHAR(50),
    warehouse_location VARCHAR(100),
    
    -- Geographic attributes
    timezone VARCHAR(50),
    currency_code VARCHAR(3),
    language_code VARCHAR(5),
    
    -- Economic indicators
    gdp_per_capita DECIMAL(12,2),
    population BIGINT,
    market_maturity VARCHAR(20), -- Emerging, Growing, Mature
    
    -- Regulatory
    requires_gdpr_compliance BOOLEAN DEFAULT FALSE,
    tax_rate DECIMAL(5,2),
    
    UNIQUE(country_code, region, city, postal_code)
);

-- Channel Dimension (for omnichannel analysis)
CREATE TABLE dim_channel (
    channel_key INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_name VARCHAR(100) NOT NULL,
    channel_type VARCHAR(50) NOT NULL, -- Online, Retail, Mobile, Partner
    channel_category VARCHAR(30), -- Direct, Indirect
    is_digital BOOLEAN DEFAULT TRUE,
    commission_rate DECIMAL(5,2) DEFAULT 0
);

-- ===============================================
-- FACT TABLES
-- ===============================================

-- Primary Sales Fact Table
CREATE TABLE fact_sales (
    -- Surrogate keys to dimensions
    date_key INTEGER NOT NULL,
    customer_key INTEGER NOT NULL,
    product_key INTEGER NOT NULL,
    campaign_key INTEGER,
    geography_key INTEGER NOT NULL,
    channel_key INTEGER NOT NULL,
    
    -- Natural keys for drill-through
    order_id INTEGER NOT NULL,
    order_item_id INTEGER NOT NULL,
    source_region VARCHAR(10) NOT NULL,
    
    -- Quantitative measures - Financial
    gross_sales_amount DECIMAL(14,4) NOT NULL,
    discount_amount DECIMAL(14,4) DEFAULT 0,
    tax_amount DECIMAL(14,4) DEFAULT 0,
    shipping_amount DECIMAL(14,4) DEFAULT 0,
    net_sales_amount DECIMAL(14,4) NOT NULL,
    
    product_cost DECIMAL(14,4) DEFAULT 0,
    gross_profit DECIMAL(14,4) DEFAULT 0,
    gross_margin_percent DECIMAL(5,2) DEFAULT 0,
    
    -- Quantitative measures - Units
    quantity_ordered INTEGER NOT NULL,
    quantity_shipped INTEGER DEFAULT 0,
    quantity_returned INTEGER DEFAULT 0,
    quantity_net INTEGER DEFAULT 0,
    
    unit_price DECIMAL(12,4) NOT NULL,
    unit_cost DECIMAL(12,4) DEFAULT 0,
    
    -- Order characteristics
    order_line_number INTEGER,
    is_first_order BOOLEAN DEFAULT FALSE,
    is_repeat_order BOOLEAN DEFAULT FALSE,
    payment_method VARCHAR(50),
    
    -- Time stamps (for detailed analysis)
    order_timestamp TIMESTAMP,
    shipped_timestamp TIMESTAMP,
    delivered_timestamp TIMESTAMP,
    
    -- Status flags
    is_cancelled BOOLEAN DEFAULT FALSE,
    is_returned BOOLEAN DEFAULT FALSE,
    is_exchanged BOOLEAN DEFAULT FALSE,
    
    -- Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key constraints
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    FOREIGN KEY (campaign_key) REFERENCES dim_campaign(campaign_key),
    FOREIGN KEY (geography_key) REFERENCES dim_geography(geography_key),
    FOREIGN KEY (channel_key) REFERENCES dim_channel(channel_key),
    
    -- Composite primary key
    PRIMARY KEY (order_id, order_item_id, source_region)
);

-- Marketing Performance Fact Table
CREATE TABLE fact_marketing (
    -- Dimension keys
    date_key INTEGER NOT NULL,
    campaign_key INTEGER NOT NULL,
    geography_key INTEGER NOT NULL,
    channel_key INTEGER NOT NULL,
    
    -- Campaign performance metrics
    impressions BIGINT DEFAULT 0,
    clicks BIGINT DEFAULT 0,
    conversions INTEGER DEFAULT 0,
    leads_generated INTEGER DEFAULT 0,
    
    -- Financial metrics
    spend_amount DECIMAL(12,2) DEFAULT 0,
    revenue_attributed DECIMAL(14,2) DEFAULT 0,
    
    -- Calculated metrics
    click_through_rate DECIMAL(5,4) DEFAULT 0,
    conversion_rate DECIMAL(5,4) DEFAULT 0,
    cost_per_click DECIMAL(8,4) DEFAULT 0,
    cost_per_conversion DECIMAL(8,2) DEFAULT 0,
    return_on_ad_spend DECIMAL(8,4) DEFAULT 0,
    
    -- Attribution metrics
    first_touch_conversions INTEGER DEFAULT 0,
    last_touch_conversions INTEGER DEFAULT 0,
    assisted_conversions INTEGER DEFAULT 0,
    
    -- Engagement metrics
    email_opens INTEGER DEFAULT 0,
    email_opens_unique INTEGER DEFAULT 0,
    social_shares INTEGER DEFAULT 0,
    social_likes INTEGER DEFAULT 0,
    
    -- Audience metrics
    new_customers_acquired INTEGER DEFAULT 0,
    existing_customers_reached INTEGER DEFAULT 0,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (campaign_key) REFERENCES dim_campaign(campaign_key),
    FOREIGN KEY (geography_key) REFERENCES dim_geography(geography_key),
    FOREIGN KEY (channel_key) REFERENCES dim_channel(channel_key),
    
    PRIMARY KEY (date_key, campaign_key, geography_key, channel_key)
);

-- Customer Behavior Fact Table (for customer analysis)
CREATE TABLE fact_customer_behavior (
    -- Dimension keys
    date_key INTEGER NOT NULL,
    customer_key INTEGER NOT NULL,
    geography_key INTEGER NOT NULL,
    channel_key INTEGER NOT NULL,
    
    -- Behavioral metrics
    website_visits INTEGER DEFAULT 0,
    page_views INTEGER DEFAULT 0,
    session_duration_minutes DECIMAL(8,2) DEFAULT 0,
    bounce_rate DECIMAL(5,4) DEFAULT 0,
    
    -- Shopping behavior
    products_viewed INTEGER DEFAULT 0,
    products_added_to_cart INTEGER DEFAULT 0,
    cart_abandonment_flag BOOLEAN DEFAULT FALSE,
    wishlist_additions INTEGER DEFAULT 0,
    
    -- Communication engagement
    emails_opened INTEGER DEFAULT 0,
    emails_clicked INTEGER DEFAULT 0,
    sms_opened INTEGER DEFAULT 0,
    push_notifications_opened INTEGER DEFAULT 0,
    
    -- Customer service
    support_tickets_created INTEGER DEFAULT 0,
    support_calls_made INTEGER DEFAULT 0,
    chat_sessions INTEGER DEFAULT 0,
    
    -- Reviews and feedback
    reviews_written INTEGER DEFAULT 0,
    review_rating DECIMAL(3,2),
    surveys_completed INTEGER DEFAULT 0,
    nps_score INTEGER,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (geography_key) REFERENCES dim_geography(geography_key),
    FOREIGN KEY (channel_key) REFERENCES dim_channel(channel_key),
    
    PRIMARY KEY (date_key, customer_key, geography_key, channel_key)
);

-- Product Performance Fact Table
CREATE TABLE fact_product_performance (
    -- Dimension keys
    date_key INTEGER NOT NULL,
    product_key INTEGER NOT NULL,
    geography_key INTEGER NOT NULL,
    channel_key INTEGER NOT NULL,
    
    -- Sales performance
    units_sold INTEGER DEFAULT 0,
    revenue DECIMAL(14,2) DEFAULT 0,
    profit DECIMAL(14,2) DEFAULT 0,
    
    -- Inventory metrics
    stock_level INTEGER DEFAULT 0,
    stock_outs INTEGER DEFAULT 0,
    restock_count INTEGER DEFAULT 0,
    inventory_turnover_rate DECIMAL(8,4) DEFAULT 0,
    
    -- Customer engagement
    product_views INTEGER DEFAULT 0,
    add_to_cart_count INTEGER DEFAULT 0,
    wishlist_additions INTEGER DEFAULT 0,
    
    -- Quality metrics
    returns_count INTEGER DEFAULT 0,
    return_rate DECIMAL(5,4) DEFAULT 0,
    defect_count INTEGER DEFAULT 0,
    
    -- Review metrics
    new_reviews INTEGER DEFAULT 0,
    average_rating DECIMAL(3,2),
    rating_1_star INTEGER DEFAULT 0,
    rating_2_star INTEGER DEFAULT 0,
    rating_3_star INTEGER DEFAULT 0,
    rating_4_star INTEGER DEFAULT 0,
    rating_5_star INTEGER DEFAULT 0,
    
    -- Competitive metrics
    price_rank INTEGER,
    market_share_percent DECIMAL(5,2) DEFAULT 0,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    FOREIGN KEY (geography_key) REFERENCES dim_geography(geography_key),
    FOREIGN KEY (channel_key) REFERENCES dim_channel(channel_key),
    
    PRIMARY KEY (date_key, product_key, geography_key, channel_key)
);

-- ===============================================
-- AGGREGATE/SUMMARY TABLES FOR PERFORMANCE
-- ===============================================

-- Daily Sales Summary (for executive dashboards)
CREATE TABLE agg_daily_sales_summary (
    date_key INTEGER NOT NULL,
    geography_key INTEGER NOT NULL,
    channel_key INTEGER NOT NULL,
    
    -- High-level metrics
    total_orders INTEGER DEFAULT 0,
    total_customers INTEGER DEFAULT 0,
    new_customers INTEGER DEFAULT 0,
    returning_customers INTEGER DEFAULT 0,
    
    total_revenue DECIMAL(16,2) DEFAULT 0,
    total_profit DECIMAL(16,2) DEFAULT 0,
    avg_order_value DECIMAL(12,2) DEFAULT 0,
    avg_items_per_order DECIMAL(6,2) DEFAULT 0,
    
    conversion_rate DECIMAL(5,4) DEFAULT 0,
    return_rate DECIMAL(5,4) DEFAULT 0,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (geography_key) REFERENCES dim_geography(geography_key),
    FOREIGN KEY (channel_key) REFERENCES dim_channel(channel_key),
    
    PRIMARY KEY (date_key, geography_key, channel_key)
);

-- Monthly Customer Cohort Analysis
CREATE TABLE agg_monthly_customer_cohorts (
    cohort_month INTEGER NOT NULL, -- YYYYMM format
    geography_key INTEGER NOT NULL,
    months_since_first_order INTEGER NOT NULL,
    
    cohort_size INTEGER DEFAULT 0,
    active_customers INTEGER DEFAULT 0,
    retention_rate DECIMAL(5,4) DEFAULT 0,
    
    total_revenue DECIMAL(16,2) DEFAULT 0,
    avg_revenue_per_customer DECIMAL(12,2) DEFAULT 0,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (geography_key) REFERENCES dim_geography(geography_key),
    
    PRIMARY KEY (cohort_month, geography_key, months_since_first_order)
);

-- Product Category Performance Summary
CREATE TABLE agg_product_category_performance (
    date_key INTEGER NOT NULL,
    category_l1_name VARCHAR(100) NOT NULL,
    category_l2_name VARCHAR(100) NOT NULL,
    geography_key INTEGER NOT NULL,
    
    total_revenue DECIMAL(16,2) DEFAULT 0,
    total_profit DECIMAL(16,2) DEFAULT 0,
    units_sold INTEGER DEFAULT 0,
    avg_rating DECIMAL(3,2),
    return_rate DECIMAL(5,4) DEFAULT 0,
    
    market_share_percent DECIMAL(5,2) DEFAULT 0,
    growth_rate_percent DECIMAL(5,2) DEFAULT 0,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (geography_key) REFERENCES dim_geography(geography_key),
    
    PRIMARY KEY (date_key, category_l1_name, category_l2_name, geography_key)
);

-- ===============================================
-- INDEXES FOR PERFORMANCE OPTIMIZATION
-- ===============================================

-- Fact table indexes
CREATE INDEX idx_fact_sales_date ON fact_sales(date_key);
CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_key);
CREATE INDEX idx_fact_sales_product ON fact_sales(product_key);
CREATE INDEX idx_fact_sales_geography ON fact_sales(geography_key);
CREATE INDEX idx_fact_sales_composite ON fact_sales(date_key, geography_key, channel_key);

CREATE INDEX idx_fact_marketing_date ON fact_marketing(date_key);
CREATE INDEX idx_fact_marketing_campaign ON fact_marketing(campaign_key);
CREATE INDEX idx_fact_marketing_composite ON fact_marketing(date_key, campaign_key, geography_key);

-- Dimension table indexes
CREATE INDEX idx_dim_customer_segment ON dim_customer(customer_segment);
CREATE INDEX idx_dim_customer_geography ON dim_customer(country_code, region);
CREATE INDEX idx_dim_customer_tenure ON dim_customer(customer_segment, customer_tier);

CREATE INDEX idx_dim_product_category ON dim_product(category_l1_name, category_l2_name);
CREATE INDEX idx_dim_product_brand ON dim_product(brand);
CREATE INDEX idx_dim_product_lifecycle ON dim_product(lifecycle_stage);

-- ===============================================
-- VIEWS FOR DIFFERENT USER GROUPS
-- ===============================================

-- Executive Dashboard View
-- CREATE VIEW vw_executive_kpis AS
-- SELECT 
--     d.year,
--     d.quarter_name,
--     d.month_name,
--     g.country_name,
--     g.sales_region,
    
--     SUM(fs.net_sales_amount) as total_revenue,
--     SUM(fs.gross_profit) as total_profit,
--     AVG(fs.gross_margin_percent) as avg_margin_percent,
--     COUNT(DISTINCT fs.customer_key) as unique_customers,
--     COUNT(fs.order_id) as total_orders,
--     SUM(fs.net_sales_amount) / COUNT(fs.order_id) as avg_order_value
    
-- FROM fact_sales fs
-- JOIN dim_date d ON fs.date_key = d.date_key
-- JOIN dim_geography g ON fs.geography_key = g.geography_key
-- WHERE d.year >= YEAR(CURRENT_DATE) - 2
-- GROUP BY d.year, d.quarter_name, d.month_name, g.country_name, g.sales_region;

-- -- Sales Team Performance View
-- CREATE VIEW vw_sales_performance AS
-- SELECT 
--     d.full_date,
--     d.month_name,
--     g.sales_territory,
--     g.country_name,
--     ch.channel_name,
    
--     p.category_l1_name,
--     p.brand,
    
--     SUM(fs.quantity_ordered) as units_sold,
--     SUM(fs.net_sales_amount) as revenue,
--     SUM(fs.gross_profit) as profit,
--     COUNT(DISTINCT fs.customer_key) as customers_reached,
--     COUNT(fs.order_id) as orders_count,
    
--     AVG(fs.unit_price) as avg_selling_price,
--     SUM(fs.net_sales_amount) / COUNT(fs.order_id) as avg_order_value,
--     SUM(fs.gross_profit) / SUM(fs.net_sales_amount) * 100 as margin_percent
    
-- FROM fact_sales fs
-- JOIN dim_date d ON fs.date_key = d.date_key
-- JOIN dim_geography g ON fs.geography_key = g.geography_key
-- JOIN dim_product p ON fs.product_key = p.product_key
-- JOIN dim_channel ch ON fs.channel_key = ch.channel_key
-- WHERE d.full_date >= DATE_SUB(CURRENT_DATE, INTERVAL 12 MONTH)
-- GROUP BY d.full_date, d.month_name, g.sales_territory, g.country_name, 
--          ch.channel_name, p.category_l1_name, p.brand;

-- -- Marketing Performance View
-- CREATE VIEW vw_marketing_performance AS
-- SELECT 
--     d.month_name,
--     d.year,
--     cam.campaign_name,
--     cam.campaign_type,
--     cam.primary_channel,
--     g.country_name,
    
--     SUM(fm.spend_amount) as total_spend,
--     SUM(fm.impressions) as total_impressions,
--     SUM(fm.clicks) as total_clicks,
--     SUM(fm.conversions) as total_conversions,
--     SUM(fm.revenue_attributed) as attributed_revenue,
    
--     AVG(fm.click_through_rate) as avg_ctr,
--     AVG(fm.conversion_rate) as avg_conversion_rate,
--     AVG(fm.cost_per_click) as avg_cpc,
--     AVG(fm.return_on_ad_spend) as avg_roas,
    
--     SUM(fm.revenue_attributed) / NULLIF(SUM(fm.spend_amount), 0) as roi_ratio
    
-- FROM fact_marketing fm
-- JOIN dim_date d ON fm.date_key = d.date_key
-- JOIN dim_campaign cam ON fm.campaign_key = cam.campaign_key
-- JOIN dim_geography g ON fm.geography_key = g.geography_key
-- WHERE d.full_date >= DATE_SUB(CURRENT_DATE, INTERVAL 6 MONTH)
-- GROUP BY d.month_name, d.year, cam.campaign_name, cam.campaign_type, 
--          cam.primary_channel, g.country_name;

-- -- Customer Analysis View
-- CREATE VIEW vw_customer_analysis AS
-- SELECT 
--     c.customer_segment,
--     c.customer_tier,
--     c.age_group,
--     c.country_name,
--     c.acquisition_channel,
    
--     COUNT(DISTINCT c.customer_key) as customer_count,
--     AVG(c.lifetime_value_usd) as avg_lifetime_value,
--     AVG(c.average_order_value) as avg_order_value,
--     AVG(c.lifetime_orders) as avg_orders_per_customer,
    
--     -- Recent behavior metrics
--     SUM(CASE WHEN fs.date_key >= 
--         (SELECT date_key FROM dim_date WHERE full_date = DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY))
--         THEN fs.net_sales_amount ELSE 0 END) as revenue_last_30_days,
    
--     COUNT(CASE WHEN fs.date_key >= 
--         (SELECT date_key FROM dim_date WHERE full_date = DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY))
--         THEN fs.order_id END) as orders_last_30_days
    
-- FROM dim_customer c
-- LEFT JOIN fact_sales fs ON c.customer_key = fs.customer_key
-- WHERE c.is_current = TRUE
-- GROUP BY c.customer_segment, c.customer_tier, c.age_group, 
--          c.country_name, c.acquisition_channel;