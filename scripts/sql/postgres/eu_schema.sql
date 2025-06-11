-- =============================================
-- EU REGION DATABASE SCHEMA
-- Single vendor e-commerce platform compliant with GDPR and EU regulations
-- Features: GDPR compliance, VAT handling, metric units, EUR currency
-- Multi-language support, data protection, privacy by design
-- =============================================

-- Product Categories - GDPR compliant with localization
-- EU specific: Integer status codes, timezone awareness, GDPR sensitivity flags
\connect retail_etl;
SET search_path TO eu;


CREATE TABLE IF NOT EXISTS eu.categories (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL, -- English name: "Electronics"
    category_name_local VARCHAR(100), -- Local language: "Elektronik" (German)
    parent_category_id INTEGER REFERENCES eu.categories(category_id),
    category_path TEXT, -- "/Electronics/Smartphones"
    is_active INTEGER DEFAULT 1, -- 1=active, 0=inactive (EU prefers integers)
    gdpr_sensitive BOOLEAN DEFAULT FALSE, -- Does this category contain sensitive data?
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, -- EU timezone awareness
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Products - EU market specific with regulatory compliance
-- EU specific: EUR pricing, metric measurements, VAT rates, energy ratings
CREATE TABLE IF NOT EXISTS eu.products (
    product_id SERIAL PRIMARY KEY,
    product_sku VARCHAR(25) NOT NULL UNIQUE, -- EU format: "EU-SKU-ABC123"
    product_name VARCHAR(255) NOT NULL,
    product_description TEXT,
    category_id INTEGER REFERENCES eu.categories(category_id),
    base_price_eur DECIMAL(10,2) NOT NULL, -- Euro pricing: 899.99
    vat_rate DECIMAL(5,2) DEFAULT 20.00, -- VAT rate varies by country (19-25%)
    cost_price_eur DECIMAL(10,2),
    weight_kg DECIMAL(8,3), -- Metric: kilograms (1.5 kg)
    length_cm DECIMAL(8,2), -- Metric: centimeters
    width_cm DECIMAL(8,2),
    height_cm DECIMAL(8,2),
    energy_rating VARCHAR(10), -- EU energy labels: "A+++", "A++", "A+", "A", "B", "C", "D"
    is_active INTEGER DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Product Variants - EU specific pricing and compliance
-- EU specific: EUR adjustments, integer status codes
CREATE TABLE IF NOT EXISTS eu.product_variants (
    variant_id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES eu.products(product_id),
    sku VARCHAR(100) UNIQUE NOT NULL, -- "EU-SKU-ABC123-RED-L"
    variant_name VARCHAR(100),
    variant_type VARCHAR(50),
    variant_value VARCHAR(100),
    price_adjustment_eur DECIMAL(10,2) DEFAULT 0, -- EUR price difference
    is_active INTEGER DEFAULT 1, -- Integer status
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Inventory Management - EU warehouse system
-- EU specific: European warehouse codes, metric units
CREATE TABLE IF NOT EXISTS eu.inventory (
    inventory_id SERIAL PRIMARY KEY,
    variant_id INTEGER REFERENCES eu.product_variants(variant_id),
    warehouse_location VARCHAR(100), -- "EU-DE-01" (Germany), "EU-NL-02" (Netherlands)
    quantity_available INTEGER NOT NULL DEFAULT 0,
    quantity_reserved INTEGER NOT NULL DEFAULT 0,
    reorder_level INTEGER DEFAULT 10,
    last_restocked_at TIMESTAMP WITH TIME ZONE, -- Timezone aware
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Customer Database - GDPR Compliant with data minimization
-- EU specific: Hashed/masked data, consent tracking, data retention limits
CREATE TABLE IF NOT EXISTS eu.customers (
    customer_id SERIAL PRIMARY KEY,
    email_hash VARCHAR(64), -- SHA-256 hash instead of plain email for privacy
    first_name_masked VARCHAR(100), -- Masked: "J***" or "John" (based on consent)
    last_name_masked VARCHAR(100), -- Masked: "D***" or "Doe"
    phone_masked VARCHAR(20), -- Masked: "+49-***-***-1234"
    country_code VARCHAR(2), -- ISO country code: "DE", "FR", "IT"
    registration_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_login_at TIMESTAMP WITH TIME ZONE,
    status_code INTEGER DEFAULT 1, -- 1=active, 0=inactive, 2=suspended, 3=deleted
    customer_tier VARCHAR(50), -- "standard", "premium", "enterprise"
    gdpr_consent_date TIMESTAMP WITH TIME ZONE, -- When GDPR consent was given
    data_retention_until DATE, -- When data must be deleted (GDPR Article 17)
    acquisition_channel VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Customer Addresses - EU address format with GDPR compliance
-- EU specific: Region instead of state, country codes, postal code formats
CREATE TABLE IF NOT EXISTS eu.customer_addresses (
    address_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES eu.customers(customer_id),
    address_type VARCHAR(20), -- "billing", "shipping"
    street_address TEXT NOT NULL,
    city VARCHAR(100) NOT NULL,
    region VARCHAR(100), -- "Bavaria", "Île-de-France", "Lombardy"
    postal_code VARCHAR(20), -- Various EU formats: "10115", "75001", "20121"
    country_code VARCHAR(2) NOT NULL, -- ISO codes: "DE", "FR", "IT"
    is_default BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Marketing Campaigns - GDPR compliant marketing
-- EU specific: GDPR compliance flags, EUR budgets
CREATE TABLE IF NOT EXISTS eu.marketing_campaigns (
    campaign_id SERIAL PRIMARY KEY,
    campaign_name VARCHAR(255) NOT NULL,
    campaign_type VARCHAR(50),
    channel VARCHAR(100),
    start_date DATE,
    end_date DATE,
    budget_eur DECIMAL(12,2), -- Euro budget
    target_audience TEXT,
    campaign_status VARCHAR(20),
    gdpr_compliant BOOLEAN DEFAULT TRUE, -- Must be GDPR compliant
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Discount System - EU specific with VAT considerations
-- EU specific: EUR amounts, VAT-inclusive pricing
CREATE TABLE IF NOT EXISTS eu.discounts (
    discount_id SERIAL PRIMARY KEY,
    discount_code VARCHAR(50) UNIQUE,
    discount_name VARCHAR(255),
    discount_type VARCHAR(20),
    discount_value DECIMAL(10,2),
    minimum_order_amount_eur DECIMAL(10,2), -- EUR minimum
    maximum_discount_amount_eur DECIMAL(10,2), -- EUR maximum
    usage_limit INTEGER,
    usage_count INTEGER DEFAULT 0,
    start_date TIMESTAMP WITH TIME ZONE,
    end_date TIMESTAMP WITH TIME ZONE,
    is_active INTEGER DEFAULT 1, -- Integer status
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Shopping Cart - GDPR compliant session management
-- EU specific: Integer abandonment flag, timezone awareness
CREATE TABLE IF NOT EXISTS eu.shopping_carts (
    cart_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES eu.customers(customer_id),
    session_id VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    abandoned_at TIMESTAMP WITH TIME ZONE,
    is_abandoned INTEGER DEFAULT 0 -- 0=active, 1=abandoned
);

-- Cart Items - EUR pricing
CREATE TABLE IF NOT EXISTS eu.cart_items (
    cart_item_id SERIAL PRIMARY KEY,
    cart_id INTEGER REFERENCES eu.shopping_carts(cart_id),
    variant_id INTEGER REFERENCES eu.product_variants(variant_id),
    quantity INTEGER NOT NULL,
    unit_price_eur DECIMAL(10,2) NOT NULL, -- EUR pricing
    added_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Orders - EU specific with VAT handling and invoicing
-- EU specific: EUR currency, VAT breakdown, invoice requirements
CREATE TABLE IF NOT EXISTS eu.orders (
    order_id SERIAL PRIMARY KEY,
    order_reference VARCHAR(50) UNIQUE NOT NULL, -- EU format: "EU-2024-001234"
    customer_id INTEGER REFERENCES eu.customers(customer_id),
    order_status VARCHAR(20),
    order_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, -- More precise naming
    subtotal_eur DECIMAL(12,2) NOT NULL, -- Pre-VAT total
    vat_amount_eur DECIMAL(10,2) DEFAULT 0, -- VAT amount (varies by country)
    shipping_eur DECIMAL(10,2) DEFAULT 0,
    discount_eur DECIMAL(10,2) DEFAULT 0,
    total_eur DECIMAL(12,2) NOT NULL, -- VAT-inclusive total
    billing_address_id INTEGER REFERENCES eu.customer_addresses(address_id),
    shipping_address_id INTEGER REFERENCES eu.customer_addresses(address_id),
    campaign_id INTEGER REFERENCES eu.marketing_campaigns(campaign_id),
    discount_id INTEGER REFERENCES eu.discounts(discount_id),
    invoice_required BOOLEAN DEFAULT TRUE, -- EU customers often need invoices
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Order Items - EU specific with VAT tracking
-- EU specific: VAT rate tracking per item (varies by product type)
CREATE TABLE IF NOT EXISTS eu.order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES eu.orders(order_id),
    variant_id INTEGER REFERENCES eu.product_variants(variant_id),
    quantity INTEGER NOT NULL,
    unit_price_eur DECIMAL(10,2) NOT NULL, -- Pre-VAT price
    vat_rate DECIMAL(5,2) NOT NULL, -- VAT rate at time of purchase
    total_price_eur DECIMAL(10,2) NOT NULL, -- VAT-inclusive total
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Payment Processing - EU specific with PSD2 compliance
-- EU specific: PSD2 compliance, EUR amounts, European payment methods
CREATE TABLE IF NOT EXISTS eu.payments (
    payment_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES eu.orders(order_id),
    payment_method VARCHAR(50), -- "sepa_debit", "ideal", "sofort", "credit_card"
    payment_status VARCHAR(20),
    payment_amount_eur DECIMAL(12,2) NOT NULL,
    payment_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    transaction_id VARCHAR(255),
    gateway_response TEXT,
    psd2_compliant BOOLEAN DEFAULT TRUE, -- PSD2 compliance flag
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Shipping Management - EU specific carriers and regulations
-- EU specific: European carriers, cross-border shipping
CREATE TABLE IF NOT EXISTS eu.shipments (
    shipment_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES eu.orders(order_id),
    tracking_number VARCHAR(100), -- DHL, DPD, Hermes formats
    carrier VARCHAR(100), -- "DHL", "DPD", "Hermes", "PostNL"
    shipping_method VARCHAR(100), -- "Standard", "Express", "Same Day"
    shipped_timestamp TIMESTAMP WITH TIME ZONE, -- Timezone aware
    estimated_delivery_date DATE,
    actual_delivery_timestamp TIMESTAMP WITH TIME ZONE,
    shipment_status VARCHAR(20),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Marketing Campaigns - GDPR compliant multi-currency marketing
CREATE TABLE IF NOT EXISTS eu.marketing_campaigns (
    campaign_id SERIAL PRIMARY KEY,
    campaign_name VARCHAR(255) NOT NULL,
    campaign_type VARCHAR(50),
    channel VARCHAR(100),
    start_date DATE,
    end_date DATE,
    budget_eur DECIMAL(12,2), -- Budget in EUR
    target_audience TEXT,
    campaign_status VARCHAR(20),
    gdpr_compliant BOOLEAN DEFAULT TRUE, -- Must be GDPR compliant
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Returns and Refunds - EU consumer protection laws
-- EU specific: 14-day cooling off period, consumer rights
CREATE TABLE IF NOT EXISTS eu.returns (
    return_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES eu.orders(order_id),
    return_reason VARCHAR(255),
    return_status VARCHAR(20),
    return_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    refund_amount_eur DECIMAL(10,2),
    refund_timestamp TIMESTAMP WITH TIME ZONE,
    cooling_off_period_days INTEGER DEFAULT 14, -- EU consumer protection
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Customer Interactions - GDPR compliant analytics
-- EU specific: Hashed/masked data, data retention limits, consent tracking
-- CREATE TABLE IF NOT EXISTS eu.customer_interactions (
--     interaction_id SERIAL PRIMARY KEY,
--     customer_id INTEGER REFERENCES eu.customers(customer_id),
--     session_id_hash VARCHAR(64), -- Hashed session ID for privacy
--     interaction_type VARCHAR(50),
--     product_id INTEGER REFERENCES eu.products(product_id),
--     variant_id INTEGER REFERENCES eu.product_variants(variant_id),
--     page_url_hash VARCHAR(64), -- Hashed URL for privacy
--     referrer_hash VARCHAR(64), -- Hashed referrer
--     user_agent_hash VARCHAR(64), -- Hashed user agent
--     ip_address_masked VARCHAR(20), -- Masked IP: "192.168.1.***"
--     interaction_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
--     additional_data JSONB, -- Anonymized additional data
--     data_retention_until DATE -- GDPR data retention limit
-- );

-- Product Reviews - EU specific with moderation
-- EU specific: Content moderation for EU regulations
CREATE TABLE IF NOT EXISTS eu.product_reviews (
    review_id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES eu.products(product_id),
    customer_id INTEGER REFERENCES eu.customers(customer_id),
    order_id INTEGER REFERENCES eu.orders(order_id),
    rating INTEGER CHECK (rating >= 1 AND rating <= 5),
    review_title VARCHAR(255),
    review_text TEXT,
    is_verified_purchase BOOLEAN DEFAULT FALSE,
    helpful_votes INTEGER DEFAULT 0,
    moderation_status VARCHAR(20) DEFAULT 'pending', -- EU content moderation
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Wishlist - GDPR compliant
CREATE TABLE IF NOT EXISTS eu.wishlists (
    wishlist_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES eu.customers(customer_id),
    product_id INTEGER REFERENCES eu.products(product_id),
    added_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- A/B Testing Framework - GDPR compliant experimentation
-- EU specific: GDPR impact assessment, consent requirements
-- CREATE TABLE IF NOT EXISTS eu.ab_test_campaigns (
--     test_id SERIAL PRIMARY KEY,
--     test_name VARCHAR(255) NOT NULL,
--     test_description TEXT,
--     start_date TIMESTAMP WITH TIME ZONE,
--     end_date TIMESTAMP WITH TIME ZONE,
--     test_status VARCHAR(20),
--     gdpr_impact_assessment TEXT, -- Required GDPR assessment
--     created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
-- );

-- -- A/B Test Variants
-- CREATE TABLE IF NOT EXISTS eu.ab_test_variants (
--     variant_id SERIAL PRIMARY KEY,
--     test_id INTEGER REFERENCES eu.ab_test_campaigns(test_id),
--     variant_name VARCHAR(100),
--     variant_description TEXT,
--     traffic_allocation DECIMAL(5,2),
--     created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
-- );

-- -- A/B Test Assignments - GDPR compliant with consent tracking
-- CREATE TABLE IF NOT EXISTS eu.ab_test_assignments (
--     assignment_id SERIAL PRIMARY KEY,
--     test_id INTEGER REFERENCES eu.ab_test_campaigns(test_id),
--     variant_id INTEGER REFERENCES eu.ab_test_variants(variant_id),
--     customer_id INTEGER REFERENCES eu.customers(customer_id),
--     session_id_hash VARCHAR(64), -- Hashed session ID
--     assigned_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
--     consent_given BOOLEAN DEFAULT FALSE -- Explicit consent for testing
-- );

-- Performance Indexes - Optimized for EU query patterns and GDPR compliance
CREATE INDEX IF NOT EXISTS idx_products_category ON eu.products(category_id);
CREATE INDEX IF NOT EXISTS idx_variants_product ON eu.product_variants(product_id);
CREATE INDEX IF NOT EXISTS idx_inventory_variant ON eu.inventory(variant_id);
CREATE INDEX IF NOT EXISTS idx_orders_customer ON eu.orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_timestamp ON eu.orders(order_timestamp); -- Timezone aware
CREATE INDEX IF NOT EXISTS idx_order_items_order ON eu.order_items(order_id);
-- CREATE INDEX IF NOT EXISTS idx_interactions_customer ON eu.customer_interactions(customer_id);
-- CREATE INDEX IF NOT EXISTS idx_interactions_timestamp ON eu.customer_interactions(interaction_timestamp);
-- CREATE INDEX IF NOT EXISTS idx_interactions_type ON eu.customer_interactions(interaction_type);
CREATE INDEX IF NOT EXISTS idx_customers_retention ON eu.customers(data_retention_until); -- GDPR cleanup