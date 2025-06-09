-- =============================================
-- ASIA REGION DATABASE SCHEMA
-- Single vendor e-commerce platform for Asian markets with legacy system integration
-- Features: JPY currency, mixed data quality, legacy naming conventions
-- Inconsistent data types, missing fields, dirty data scenarios
-- Represents a system that evolved organically over time
-- =============================================

-- Product Categories - Legacy system with inconsistent naming
-- ASIA specific: Different column names, string status flags, missing fields

\connect retail_etl;


CREATE TABLE IF NOT EXISTS asia.categories (
    cat_id SERIAL PRIMARY KEY, -- Legacy naming convention
    name VARCHAR(100) NOT NULL, -- Simplified field name
    parent_id INTEGER REFERENCES asia.categories(cat_id), -- Different reference name
    path TEXT, -- Missing category_path prefix
    active_flag VARCHAR(1) DEFAULT 'Y', -- String flag: 'Y'/'N' instead of boolean
    create_time VARCHAR(50), -- Different naming
    modify_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    -- Missing updated_at field (legacy system didn't track updates properly)
);

-- Products - Mixed units, inconsistent data, JPY pricing
-- ASIA specific: JPY currency (no decimals), grams for weight, free-text dimensions
CREATE TABLE IF NOT EXISTS asia.products (
    product_id SERIAL PRIMARY KEY,
    item_code VARCHAR(30), -- Inconsistent format, some NULL values allowed
    name VARCHAR(255), -- Product name, some may be NULL
    description TEXT,
    cat_id INTEGER REFERENCES asia.categories(cat_id), -- Different column name
    price_jpy DECIMAL(12,0), -- Japanese Yen (no decimal places): 89999
    cost_jpy DECIMAL(12,0), -- Cost in Yen
    weight_g DECIMAL(10,1), -- Weight in grams: 160.5g
    size_info TEXT, -- Free text instead of structured: "155mm x 76mm x 8mm"
    active_status VARCHAR(20), -- Inconsistent values: 'ACTIVE', 'DISCONTINUED', 'OUT_OF_STOCK'
    create_date VARCHAR(50)
    -- Missing updated_at field
);

-- Product Variants - Simplified structure with inconsistencies
-- ASIA specific: JPY pricing, string status, simplified fields
CREATE TABLE IF NOT EXISTS asia.product_variants (
    variant_id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES asia.products(product_id),
    sku VARCHAR(100), -- No UNIQUE constraint (allows duplicates!)
    variant_name VARCHAR(100),
    variant_type VARCHAR(50),
    variant_value VARCHAR(100),
    price_diff_jpy DECIMAL(10,0) DEFAULT 0, -- Price difference in Yen (no decimals)
    status VARCHAR(10) DEFAULT 'ACTIVE', -- String status
    created VARCHAR(50)
    -- Missing is_active boolean
);

-- Inventory Management - Simplified warehouse system
-- ASIA specific: Simplified warehouse naming, different field names
CREATE TABLE IF NOT EXISTS asia.inventory (
    inventory_id SERIAL PRIMARY KEY,
    variant_id INTEGER REFERENCES asia.product_variants(variant_id),
    warehouse VARCHAR(100), -- Simplified: "TOKYO-01", "OSAKA-02"
    qty_available INTEGER NOT NULL DEFAULT 0, -- Shortened field name
    qty_reserved INTEGER NOT NULL DEFAULT 0,
    reorder_point INTEGER DEFAULT 10, -- Different field name
    last_restock VARCHAR(50), -- Simplified field name
    updated VARCHAR(50) -- Simplified field name
);

-- Customer Database - Dirty data, missing validation, inconsistent formats
-- ASIA specific: Missing fields, dirty phone numbers, inconsistent data
-- CREATE TABLE IF NOT EXISTS asia.customers (
--     cust_id SERIAL PRIMARY KEY, -- Legacy naming
--     email VARCHAR(255), -- No UNIQUE constraint, allows duplicates and NULLs
--     fname VARCHAR(100), -- Shortened field names
--     lname VARCHAR(100),
--     phone VARCHAR(50), -- Longer field to accommodate malformed data
--     birth_date DATE, -- May contain invalid dates
--     sex VARCHAR(20), -- Inconsistent values: 'M', 'F', 'Male', 'Female', 'Other', NULL
--     reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
--     login_time TIMESTAMP, -- Different field name
--     -- Missing is_active field entirely (legacy system didn't track status)
--     segment VARCHAR(50), -- Customer segment
--     source VARCHAR(100), -- Acquisition source
--     created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
--     -- Missing updated_at field
-- );
CREATE TABLE IF NOT EXISTS asia.customers (
    cust_id SERIAL PRIMARY KEY, -- Legacy naming
    email VARCHAR(255), -- No UNIQUE constraint, allows duplicates and NULLs
    fname VARCHAR(100), -- Shortened field names
    lname VARCHAR(100),
    phone VARCHAR(50), -- Longer field to accommodate malformed data
    birth_date VARCHAR(50), -- May contain invalid dates
    sex VARCHAR(20), -- Inconsistent values: 'M', 'F', 'Male', 'Female', 'Other', NULL
    reg_date VARCHAR(50),
    login_time VARCHAR(50), -- Different field name
    -- Missing is_active field entirely (legacy system didn't track status)
    segment VARCHAR(50), -- Customer segment
    source VARCHAR(100), -- Acquisition source
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    -- Missing updated_at field
);

-- Customer Addresses - Different field names, inconsistent data
-- ASIA specific: Prefecture instead of state, different field naming
CREATE TABLE IF NOT EXISTS asia.customer_addresses (
    addr_id SERIAL PRIMARY KEY, -- Shortened field name
    cust_id INTEGER REFERENCES asia.customers(cust_id), -- Different reference name
    addr_type VARCHAR(20), -- Address type
    street TEXT NOT NULL,
    city VARCHAR(100) NOT NULL,
    prefecture VARCHAR(100), -- Japanese prefecture instead of state
    postal VARCHAR(20), -- Simplified field name, various formats
    country VARCHAR(100) NOT NULL,
    is_default VARCHAR(1) DEFAULT 'N', -- String flag instead of boolean
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Marketing Campaigns - Simplified structure
-- ASIA specific: JPY budget, simplified field names, missing features
CREATE TABLE IF NOT EXISTS asia.marketing_campaigns (
    campaign_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL, -- Simplified field name
    type VARCHAR(50), -- Simplified
    channel VARCHAR(100),
    start_dt VARCHAR(50), -- Different field name format
    end_dt VARCHAR(50),
    budget_jpy DECIMAL(12,0), -- JPY budget (no decimals)
    target TEXT, -- Simplified field name
    status VARCHAR(20),
    created VARCHAR(50)
);

-- Discount System - Inconsistent structure
-- ASIA specific: JPY amounts, string active flag, missing constraints
CREATE TABLE IF NOT EXISTS asia.discounts (
    discount_id SERIAL PRIMARY KEY,
    code VARCHAR(50), -- No UNIQUE constraint
    name VARCHAR(255),
    type VARCHAR(20),
    value DECIMAL(10,2), -- Percentage or amount
    min_order_jpy DECIMAL(12,0), -- JPY minimum
    max_discount_jpy DECIMAL(12,0), -- JPY maximum
    usage_limit INTEGER,
    usage_count INTEGER DEFAULT 0,
    start_dt VARCHAR(50), -- Different field naming
    end_dt VARCHAR(50),
    active VARCHAR(1) DEFAULT 'Y', -- String flag
    created VARCHAR(50)
);

-- Shopping Cart - Missing fields, simplified structure
-- ASIA specific: Missing abandonment tracking
CREATE TABLE IF NOT EXISTS asia.shopping_carts (
    cart_id SERIAL PRIMARY KEY,
    cust_id INTEGER REFERENCES asia.customers(cust_id), -- Different reference name
    session_id VARCHAR(255),
    created VARCHAR,
    updated VARCHAR(50),
    abandoned VARCHAR(50) -- Simplified field name, missing is_abandoned flag
);

-- Cart Items - JPY pricing
CREATE TABLE IF NOT EXISTS asia.cart_items (
    cart_item_id SERIAL PRIMARY KEY,
    cart_id INTEGER REFERENCES asia.shopping_carts(cart_id),
    variant_id INTEGER REFERENCES asia.product_variants(variant_id),
    qty INTEGER NOT NULL, -- Simplified field name
    unit_price_jpy DECIMAL(10,0) NOT NULL, -- JPY pricing (no decimals)
    added VARCHAR(50) , -- Simplified field name
    updated VARCHAR(50) 
);

-- Orders - Inconsistent date formats, missing fields, dirty data
-- ASIA specific: String date fields, JPY currency, missing audit fields
CREATE TABLE IF NOT EXISTS asia.orders (
    order_id SERIAL PRIMARY KEY,
    order_no VARCHAR(60), -- Different format, longer field, some NULLs allowed
    cust_id INTEGER REFERENCES asia.customers(cust_id), -- Different reference name
    status VARCHAR(30), -- Longer field for inconsistent status values
    order_time VARCHAR(25), -- STRING instead of TIMESTAMP! Various formats
    subtotal_jpy DECIMAL(12,0), -- JPY amounts (no decimals)
    tax_jpy DECIMAL(12,0), -- Japanese consumption tax
    shipping_jpy DECIMAL(12,0),
    discount_jpy DECIMAL(12,0),
    total_jpy DECIMAL(12,0),
    billing_addr_id INTEGER REFERENCES asia.customer_addresses(addr_id), -- Different field name
    shipping_addr_id INTEGER REFERENCES asia.customer_addresses(addr_id),
    campaign_id INTEGER REFERENCES asia.marketing_campaigns(campaign_id),
    discount_id INTEGER REFERENCES asia.discounts(discount_id),
    -- Missing created_at and updated_at fields
    processed_date VARCHAR(50)  -- Different field name
);

-- Order Items - JPY pricing, simplified structure
CREATE TABLE IF NOT EXISTS asia.order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES asia.orders(order_id),
    variant_id INTEGER REFERENCES asia.product_variants(variant_id),
    qty INTEGER NOT NULL, -- Simplified field name
    unit_price_jpy DECIMAL(10,0) NOT NULL, -- JPY (no decimals)
    total_price_jpy DECIMAL(10,0) NOT NULL,
    created VARCHAR(50) 
);

-- Payment Processing - Different field names, JPY amounts
-- ASIA specific: Simplified field names, JPY currency, missing features
CREATE TABLE IF NOT EXISTS asia.payments (
    payment_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES asia.orders(order_id),
    method VARCHAR(50), -- Simplified field name: "credit_card", "konbini", "bank_transfer"
    status VARCHAR(20),
    amount_jpy DECIMAL(12,0) NOT NULL, -- JPY amount
    payment_time VARCHAR(50), -- Different field name
    transaction_ref VARCHAR(255), -- Different field name
    gateway_resp TEXT, -- Simplified field name
    created VARCHAR(50) 
);

-- Shipping Management - Simplified structure
-- ASIA specific: Japanese carriers, simplified field names
CREATE TABLE IF NOT EXISTS asia.shipments (
    shipment_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES asia.orders(order_id),
    tracking_no VARCHAR(100), -- Simplified field name
    carrier VARCHAR(100), -- "Yamato", "Sagawa", "Japan Post"
    method VARCHAR(100), -- Simplified field name
    shipped_time VARCHAR(50) , -- Different field name
    est_delivery VARCHAR(50) , -- Simplified field name
    actual_delivery VARCHAR(50) , -- Simplified field name
    status VARCHAR(20),
    created VARCHAR(50) ,
    updated VARCHAR(50) 
);

-- Returns and Refunds - Simplified structure
-- ASIA specific: JPY refunds, simplified field names
CREATE TABLE IF NOT EXISTS asia.returns (
    return_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES asia.orders(order_id),
    reason VARCHAR(255), -- Simplified field name
    status VARCHAR(20),
    return_time VARCHAR(50) , -- Different field name
    refund_jpy DECIMAL(10,0), -- JPY refund amount
    refund_time VARCHAR(50) , -- Different field name
    created VARCHAR(50) 
);

-- Customer Interactions - Basic tracking, no privacy restrictions
-- ASIA specific: Basic structure, full data collection, simplified fields
-- CREATE TABLE IF NOT EXISTS asia.customer_interactions (
--     interaction_id SERIAL PRIMARY KEY,
--     cust_id INTEGER REFERENCES asia.customers(cust_id), -- Different reference name
--     session_id VARCHAR(255),
--     type VARCHAR(50), -- Simplified field name
--     product_id INTEGER REFERENCES asia.products(product_id),
--     variant_id INTEGER REFERENCES asia.product_variants(variant_id),
--     page_url TEXT, -- Full URL stored
--     referrer TEXT, -- Simplified field name
--     user_agent TEXT,
--     ip_addr INET, -- Simplified field name
--     timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Simplified field name
--     extra_data TEXT -- Plain text instead of JSONB
-- );

-- Product Reviews - Missing features, simplified structure
-- ASIA specific: String verified flag, missing moderation
CREATE TABLE IF NOT EXISTS asia.product_reviews (
    review_id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES asia.products(product_id),
    cust_id INTEGER REFERENCES asia.customers(cust_id), -- Different reference name
    order_id INTEGER REFERENCES asia.orders(order_id),
    rating INTEGER CHECK (rating >= 1 AND rating <= 5),
    title VARCHAR(255), -- Simplified field name
    review_text TEXT,
    verified VARCHAR(1) DEFAULT 'N', -- String flag instead of boolean
    helpful_count INTEGER DEFAULT 0, -- Different field name
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    -- Missing moderation features
);

-- Wishlist - Simplified structure
CREATE TABLE IF NOT EXISTS asia.wishlists (
    wishlist_id SERIAL PRIMARY KEY,
    cust_id INTEGER REFERENCES asia.customers(cust_id), -- Different reference name
    product_id INTEGER REFERENCES asia.products(product_id),
    added VARCHAR(50) -- Simplified field name
);

-- A/B Testing Framework - Basic implementation
-- ASIA specific: Simplified structure, missing advanced features
-- CREATE TABLE IF NOT EXISTS asia.ab_test_campaigns (
--     test_id SERIAL PRIMARY KEY,
--     name VARCHAR(255) NOT NULL, -- Simplified field name
--     description TEXT,
--     start_dt TIMESTAMP, -- Different field naming
--     end_dt TIMESTAMP,
--     status VARCHAR(20),
--     created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
--     -- Missing GDPR considerations, impact assessments
-- );

-- -- A/B Test Variants - Basic structure
-- CREATE TABLE IF NOT EXISTS asia.ab_test_variants (
--     variant_id SERIAL PRIMARY KEY,
--     test_id INTEGER REFERENCES asia.ab_test_campaigns(test_id),
--     name VARCHAR(100), -- Simplified field name
--     description TEXT,
--     traffic_pct DECIMAL(5,2), -- Different field name
--     created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
-- );

-- -- A/B Test Assignments - Basic assignment tracking
-- CREATE TABLE IF NOT EXISTS asia.ab_test_assignments (
--     assignment_id SERIAL PRIMARY KEY,
--     test_id INTEGER REFERENCES asia.ab_test_campaigns(test_id),
--     variant_id INTEGER REFERENCES asia.ab_test_variants(variant_id),
--     cust_id INTEGER REFERENCES asia.customers(cust_id), -- Different reference name
--     session_id VARCHAR(255),
--     assigned TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Simplified field name
--     -- Missing consent tracking
-- );

-- Performance Indexes - Fewer indexes due to legacy system constraints
-- ASIA specific: Basic indexing, some missing for performance
CREATE INDEX IF NOT EXISTS idx_products_cat ON asia.products(cat_id); -- Different naming
CREATE INDEX IF NOT EXISTS idx_variants_product ON asia.product_variants(product_id);
CREATE INDEX IF NOT EXISTS idx_orders_customer ON asia.orders(cust_id); -- Different reference
-- CREATE INDEX IF NOT EXISTS idx_interactions_customer ON asia.customer_interactions(cust_id);
-- Missing many indexes that would be present in modern systems
-- Missing timestamp indexes due to string date fields
-- Missing composite indexes for complex queries