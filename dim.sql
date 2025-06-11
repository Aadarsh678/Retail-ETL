-- Create Star Schema star.dimensions for Analytics

-- Time star.dimension
CREATE TABLE IF NOT EXISTS star.dim_time (
    time_key INTEGER PRIMARY KEY,
    date_actual DATE NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    week INTEGER NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN DEFAULT FALSE,
    fiscal_year INTEGER,
    fiscal_quarter INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Customer star.dimension 
CREATE TABLE IF NOT EXISTS star.dim_customer (
    customer_key INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER NOT NULL,
    source_region VARCHAR(10) NOT NULL,
    email_hash VARCHAR(255), -- For privacy compliance
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(200),
    gender VARCHAR(20),
    birth_date DATE,
    age_group VARCHAR(20),
    country_code VARCHAR(2),
    country_name VARCHAR(100),
    customer_segment VARCHAR(50),
    acquisition_channel VARCHAR(100),
    registration_date DATE,
    customer_lifetime_value_usd DECIMAL(12,2),
    total_orders INTEGER DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE,
    gdpr_compliant BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Unique constraint to prevent duplicates
    UNIQUE(customer_id, source_region)
);

-- Product star.dimension
CREATE TABLE IF NOT EXISTS star.dim_product (
    product_key INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id INTEGER NOT NULL,
    product_sku VARCHAR(30),
    product_name VARCHAR(255),
    product_description TEXT,
    category_id INTEGER,
    category_name VARCHAR(100),
    category_path TEXT,
    parent_category VARCHAR(100),
    price_usd DECIMAL(10,2),
    cost_usd DECIMAL(10,2),
    margin_usd DECIMAL(10,2),
    margin_percentage DECIMAL(5,2),
    weight_kg DECIMAL(8,3),
    star.dimensions_cm VARCHAR(50), -- Combined length x width x height
    vat_rate DECIMAL(5,2),
    energy_rating VARCHAR(10),
    source_region VARCHAR(10),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Geography star.dimension
CREATE TABLE IF NOT EXISTS star.dim_geography (
    geography_key INTEGER PRIMARY KEY AUTOINCREMENT,
    country_code VARCHAR(2) NOT NULL,
    country_name VARCHAR(100) NOT NULL,
    region VARCHAR(50), -- ASIA, EU, US
    continent VARCHAR(50),
    currency_code VARCHAR(3),
    timezone VARCHAR(50),
    gdpr_applicable BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Marketing Campaign star.dimension
CREATE TABLE IF NOT EXISTS star.dim_marketing_campaign (
    campaign_key INTEGER PRIMARY KEY AUTOINCREMENT,
    campaign_id INTEGER NOT NULL,
    campaign_name VARCHAR(255),
    campaign_type VARCHAR(50),
    channel VARCHAR(100),
    start_date DATE,
    end_date DATE,
    budget_usd DECIMAL(12,2),
    target_audience TEXT,
    campaign_status VARCHAR(20),
    source_region VARCHAR(10),
    is_gdpr_compliant BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Payment Method star.dimension
CREATE TABLE IF NOT EXISTS star.dim_payment_method (
    payment_method_key INTEGER PRIMARY KEY AUTOINCREMENT,
    payment_method VARCHAR(50) NOT NULL,
    payment_category VARCHAR(30), -- Credit Card, Digital Wallet, Bank Transfer, etc.
    is_digital BOOLEAN DEFAULT FALSE,
    processing_fee_percentage DECIMAL(5,4),
    psd2_required BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_dim_customer_id_region ON star.dim_customer(customer_id, source_region);
CREATE INDEX IF NOT EXISTS idx_dim_customer_current ON star.dim_customer(is_current);
CREATE INDEX IF NOT EXISTS idx_dim_product_id_region ON star.dim_product(product_id, source_region);
CREATE INDEX IF NOT EXISTS idx_dim_time_date ON star.dim_time(date_actual);
CREATE INDEX IF NOT EXISTS idx_dim_geography_country ON star.dim_geography(country_code);
CREATE INDEX IF NOT EXISTS idx_dim_campaign_id_region ON star.dim_marketing_campaign(campaign_id, source_region);
