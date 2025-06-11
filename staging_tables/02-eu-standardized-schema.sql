-- =============================================
-- EU REGION DATABASE SCHEMA - STANDARDIZED
-- GDPR-compliant system with EUR currency and privacy features
-- Standardized naming conventions while maintaining GDPR compliance
-- =============================================

\connect retail_etl;
SET search_path TO eu;

CREATE TABLE IF NOT EXISTS staging_eu.categories (
    category_id INTEGER AUTOINCREMENT PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL,
    category_name_local VARCHAR(100), -- Local language support
    parent_category_id INTEGER REFERENCES staging_eu.categories(category_id),
    category_path TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    is_gdpr_sensitive BOOLEAN DEFAULT FALSE, -- GDPR sensitivity flag
    created_at TIMESTAMP  DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP  DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'EU' NOT NULL,
    _source VARCHAR(50) DEFAULT 'postgres' NOT NULL
);

-- Products - EUR pricing, metric measurements, energy ratings
CREATE TABLE IF NOT EXISTS staging_eu.products (
    product_id INTEGER AUTOINCREMENT PRIMARY KEY,
    product_sku VARCHAR(25) NOT NULL UNIQUE, -- "EU-SKU-ABC123"
    product_name VARCHAR(255) NOT NULL,
    product_description TEXT,
    category_id INTEGER REFERENCES staging_eu.categories(category_id),
    price_usd DECIMAL(10,2) NOT NULL,
    cost_usd DECIMAL(10,2),
    vat_rate DECIMAL(5,2) DEFAULT 20.00, -- VAT varies by country
    weight_kg DECIMAL(8,3), -- Metric: kilograms
    length_cm DECIMAL(8,2), -- Metric: centimeters
    width_cm DECIMAL(8,2),
    height_cm DECIMAL(8,2),
    energy_rating VARCHAR(10), -- EU energy labels: "A+++", "A++", etc.
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'EU' NOT NULL,
    _source VARCHAR(50) DEFAULT 'postgres' NOT NULL
);

-- Product Variants
CREATE TABLE IF NOT EXISTS staging_eu.product_variants (
    variant_id INTEGER AUTOINCREMENT PRIMARY KEY,
    product_id INTEGER REFERENCES staging_eu.products(product_id),
    product_sku VARCHAR(100) UNIQUE NOT NULL,
    variant_name VARCHAR(100),
    variant_type VARCHAR(50),
    variant_value VARCHAR(100),
    price_diff_usd DECIMAL(10,2) DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'EU' NOT NULL,
    _source VARCHAR(50) DEFAULT 'postgres' NOT NULL
);

-- Inventory Management
CREATE TABLE IF NOT EXISTS staging_eu.inventory (
    inventory_id INTEGER AUTOINCREMENT PRIMARY KEY,
    variant_id INTEGER REFERENCES staging_eu.product_variants(variant_id),
    warehouse_location VARCHAR(100), -- "EU-DE-01", "EU-NL-02"
    quantity_available INTEGER NOT NULL DEFAULT 0,
    quantity_reserved INTEGER NOT NULL DEFAULT 0,
    reorder_level INTEGER DEFAULT 10,
    last_restocked_at TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'EU' NOT NULL,
    _source VARCHAR(50) DEFAULT 'postgres' NOT NULL
);

-- Customer Database - GDPR Compliant with data masking
CREATE TABLE IF NOT EXISTS staging_eu.customers (
    customer_id INTEGER AUTOINCREMENT PRIMARY KEY,
    email VARCHAR(64), -- SHA-256 hash for privacy
    first_name VARCHAR(100), -- Masked based on consent
    last_name VARCHAR(100),
    phone VARCHAR(20),
    country_code VARCHAR(2), -- ISO country codes
    registration_date TIMESTAMP,
    last_login TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    customer_segment VARCHAR(50),
    gdpr_consent_date TIMESTAMP, -- GDPR consent tracking
    data_retention_until TIMESTAMP, -- GDPR Article 17 compliance
    acquisition_channel VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'EU' NOT NULL,
    _source VARCHAR(50) DEFAULT 'postgres' NOT NULL
);

-- Customer Addresses
CREATE TABLE IF NOT EXISTS staging_eu.customer_addresses (
    address_id INTEGER AUTOINCREMENT PRIMARY KEY,
    customer_id INTEGER REFERENCES staging_eu.customers(customer_id),
    address_type VARCHAR(20),
    street_address TEXT NOT NULL,
    city VARCHAR(100) NOT NULL,
    region VARCHAR(100), -- European regions
    postal_code VARCHAR(20), -- Various EU formats
    country VARCHAR(2) NOT NULL, -- ISO codes
    is_default BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP  DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'EU' NOT NULL,
    _source VARCHAR(50) DEFAULT 'postgres' NOT NULL
);

-- Marketing Campaigns - GDPR compliant
CREATE TABLE IF NOT EXISTS staging_eu.marketing_campaigns (
    campaign_id INTEGER AUTOINCREMENT PRIMARY KEY,
    campaign_name VARCHAR(255) NOT NULL,
    campaign_type VARCHAR(50),
    channel VARCHAR(100),
    start_date DATE,
    end_date DATE,
    budget_usd DECIMAL(12,2),
    target_audience TEXT,
    campaign_status VARCHAR(20),
    is_gdpr_compliant BOOLEAN DEFAULT TRUE, -- GDPR compliance flag
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'EU' NOT NULL,
    _source VARCHAR(50) DEFAULT 'postgres' NOT NULL
);

-- Discount System
CREATE TABLE IF NOT EXISTS staging_eu.discounts (
    discount_id INTEGER AUTOINCREMENT PRIMARY KEY,
    discount_code VARCHAR(50) UNIQUE,
    discount_name VARCHAR(255),
    discount_type VARCHAR(20),
    discount_value DECIMAL(10,2),
    minimum_order_amount DECIMAL(10,2),
    maximum_discount_amount DECIMAL(10,2),
    usage_limit INTEGER,
    usage_count INTEGER DEFAULT 0,
    start_date TIMESTAMP,
    end_date TIMESTAMP ,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'EU' NOT NULL,
    _source VARCHAR(50) DEFAULT 'postgres' NOT NULL
);

-- Shopping Cart
CREATE TABLE IF NOT EXISTS staging_eu.shopping_carts (
    cart_id INTEGER AUTOINCREMENT PRIMARY KEY,
    customer_id INTEGER REFERENCES staging_eu.customers(customer_id),
    session_id VARCHAR(255),
    created_at TIMESTAMP  DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP  DEFAULT CURRENT_TIMESTAMP,
    abandoned_at TIMESTAMP ,
    is_abandoned BOOLEAN DEFAULT FALSE,
    _region VARCHAR(10) DEFAULT 'EU' NOT NULL,
    _source VARCHAR(50) DEFAULT 'postgres' NOT NULL
);

-- Cart Items
CREATE TABLE IF NOT EXISTS staging_eu.cart_items (
    cart_item_id INTEGER AUTOINCREMENT PRIMARY KEY,
    cart_id INTEGER REFERENCES staging_eu.shopping_carts(cart_id),
    variant_id INTEGER REFERENCES staging_eu.product_variants(variant_id),
    quantity INTEGER NOT NULL,
    unit_price_usd DECIMAL(10,2) NOT NULL,
    added_at TIMESTAMP  DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP  DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'EU' NOT NULL,
    _source VARCHAR(50) DEFAULT 'postgres' NOT NULL
);

-- Orders - EU specific with VAT handling
CREATE TABLE IF NOT EXISTS staging_eu.orders (
    order_id INTEGER AUTOINCREMENT PRIMARY KEY,
    order_reference  VARCHAR(50) UNIQUE NOT NULL, -- "EU-2024-001234"
    customer_id INTEGER REFERENCES staging_eu.customers(customer_id),
    order_status VARCHAR(20),
    order_timestamp TIMESTAMP  DEFAULT CURRENT_TIMESTAMP,
    subtotal_usd DECIMAL(12,2) NOT NULL, -- Pre-VAT total
    tax_amount_usd DECIMAL(10,2) DEFAULT 0,
    shipping_amount_usd DECIMAL(10,2) DEFAULT 0,
    discount_amount_usd DECIMAL(10,2) DEFAULT 0,
    total_amount_usd DECIMAL(12,2) NOT NULL, -- VAT-inclusive total
    billing_address_id INTEGER REFERENCES staging_eu.customer_addresses(address_id),
    shipping_address_id INTEGER REFERENCES staging_eu.customer_addresses(address_id),
    campaign_id INTEGER REFERENCES staging_eu.marketing_campaigns(campaign_id),
    discount_id INTEGER REFERENCES staging_eu.discounts(discount_id),
    invoice_required BOOLEAN DEFAULT TRUE, -- EU invoice requirements
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'EU' NOT NULL,
    _source VARCHAR(50) DEFAULT 'postgres' NOT NULL
);

-- Order Items - VAT tracking per item
CREATE TABLE IF NOT EXISTS staging_eu.order_items (
    order_item_id INTEGER AUTOINCREMENT PRIMARY KEY,
    order_id INTEGER REFERENCES staging_eu.orders(order_id),
    variant_id INTEGER REFERENCES staging_eu.product_variants(variant_id),
    quantity INTEGER NOT NULL,
    unit_price_usd DECIMAL(10,2) NOT NULL, -- Pre-VAT price
    vat_rate DECIMAL(5,2) NOT NULL, -- VAT rate at purchase time
    total_price_usd DECIMAL(10,2) NOT NULL, -- VAT-inclusive total
    created_at TIMESTAMP  DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'EU' NOT NULL,
    _source VARCHAR(50) DEFAULT 'postgres' NOT NULL
);

-- Payment Processing - PSD2 compliant
CREATE TABLE IF NOT EXISTS staging_eu.payments (
    payment_id INTEGER AUTOINCREMENT PRIMARY KEY,
    order_id INTEGER REFERENCES staging_eu.orders(order_id),
    payment_method VARCHAR(50), -- "sepa_debit", "ideal", "sofort"
    payment_status VARCHAR(20),
    payment_amount_usd DECIMAL(12,2) NOT NULL,
    payment_timestamp TIMESTAMP  DEFAULT CURRENT_TIMESTAMP,
    transaction_id VARCHAR(255),
    gateway_response TEXT,
    psd2_compliant BOOLEAN DEFAULT TRUE, -- PSD2 compliance
    created_at TIMESTAMP  DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'EU' NOT NULL,
    _source VARCHAR(50) DEFAULT 'postgres' NOT NULL
);

-- Shipping Management
CREATE TABLE IF NOT EXISTS staging_eu.shipments (
    shipment_id INTEGER AUTOINCREMENT PRIMARY KEY,
    order_id INTEGER REFERENCES staging_eu.orders(order_id),
    tracking_number VARCHAR(100),
    carrier VARCHAR(100), -- "DHL", "DPD", "Hermes"
    shipping_method VARCHAR(100),
    shipped_timestamp TIMESTAMP,
    estimated_delivery_date DATE,
    actual_delivery_timestamp TIMESTAMP,
    shipment_status VARCHAR(20),
    created_at TIMESTAMP  DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP  DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'EU' NOT NULL,
    _source VARCHAR(50) DEFAULT 'postgres' NOT NULL
);

-- Returns and Refunds - EU consumer protection
CREATE TABLE IF NOT EXISTS staging_eu.returns (
    return_id INTEGER AUTOINCREMENT PRIMARY KEY,
    order_id INTEGER REFERENCES staging_eu.orders(order_id),
    return_reason VARCHAR(255),
    return_status VARCHAR(20),
    return_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    refund_amount DECIMAL(10,2),
    refund_timestamp TIMESTAMP,
    created_at TIMESTAMP  DEFAULT CURRENT_TIMESTAMP,
    cooling_off_period_days INTEGER DEFAULT 14, -- EU consumer protection
    _region VARCHAR(10) DEFAULT 'EU' NOT NULL,
    _source VARCHAR(50) DEFAULT 'postgres' NOT NULL
);

-- Product Reviews - Content moderation for EU regulations
CREATE TABLE IF NOT EXISTS staging_eu.product_reviews (
    review_id INTEGER AUTOINCREMENT PRIMARY KEY,
    product_id INTEGER REFERENCES staging_eu.products(product_id),
    customer_id INTEGER REFERENCES staging_eu.customers(customer_id),
    order_id INTEGER REFERENCES staging_eu.orders(order_id),
    rating INTEGER ,
    review_title VARCHAR(255),
    review_text TEXT,
    is_verified_purchase BOOLEAN DEFAULT FALSE,
    helpful_votes INTEGER DEFAULT 0,
    moderation_status VARCHAR(20) DEFAULT 'pending', -- EU content moderation
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'EU' NOT NULL,
    _source VARCHAR(50) DEFAULT 'postgres' NOT NULL
);

-- Wishlist
CREATE TABLE IF NOT EXISTS staging_eu.wishlists (
    wishlist_id INTEGER AUTOINCREMENT PRIMARY KEY,
    customer_id INTEGER REFERENCES staging_eu.customers(customer_id),
    product_id INTEGER REFERENCES staging_eu.products(product_id),
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'EU' NOT NULL,
    _source VARCHAR(50) DEFAULT 'postgres' NOT NULL
);

-- Performance Indexes
CREATE INDEX IF NOT EXISTS idx_eu_products_category ON eu.products(category_id);
CREATE INDEX IF NOT EXISTS idx_eu_variants_product ON eu.product_variants(product_id);
CREATE INDEX IF NOT EXISTS idx_eu_inventory_variant ON eu.inventory(variant_id);
CREATE INDEX IF NOT EXISTS idx_eu_orders_customer ON eu.orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_eu_orders_date ON eu.orders(order_date);
CREATE INDEX IF NOT EXISTS idx_eu_customers_retention ON eu.customers(data_retention_until); -- GDPR cleanup


-- Performance Indexes
CREATE INDEX IF NOT EXISTS idx_eu_products_category ON eu.products(category_id);
CREATE INDEX IF NOT EXISTS idx_eu_variants_product ON eu.product_variants(product_id);
CREATE INDEX IF NOT EXISTS idx_eu_inventory_variant ON eu.inventory(variant_id);
CREATE INDEX IF NOT EXISTS idx_eu_orders_customer ON eu.orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_eu_orders_date ON eu.orders(order_date);
CREATE INDEX IF NOT EXISTS idx_eu_customers_retention ON eu.customers(data_retention_until); -- GDPR cleanup

CREATE INDEX IF NOT EXISTS idx_eu_customers_region_source ON eu.customers(_region, _source);
CREATE INDEX IF NOT EXISTS idx_eu_orders_region_source ON eu.orders(_region, _source);
