-- =============================================
-- US REGION DATABASE SCHEMA - STANDARDIZED
-- Standard system with USD currency and imperial measurements
-- Standardized naming conventions for US market
-- =============================================

\connect retail_etl;
SET search_path TO us;

-- Product Categories
CREATE TABLE IF NOT EXISTS us.categories (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL,
    parent_category_id INTEGER REFERENCES us.categories(category_id),
    category_path TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'US' NOT NULL,
    _source VARCHAR(50) DEFAULT 'us_standard_system' NOT NULL
);

-- Products - USD pricing, imperial measurements
CREATE TABLE IF NOT EXISTS us.products (
    product_id SERIAL PRIMARY KEY,
    product_sku VARCHAR(20) NOT NULL UNIQUE, -- "PRD-12345"
    product_name VARCHAR(255) NOT NULL,
    product_description TEXT,
    category_id INTEGER REFERENCES us.categories(category_id),
    price_usd DECIMAL(10,2) NOT NULL,
    cost_usd DECIMAL(10,2),
    weight_kg DECIMAL(8,3), -- Imperial: pounds
    length_cm DECIMAL(8,2), -- Metric: centimeters
    width_cm DECIMAL(8,2),
    height_cm DECIMAL(8,2),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'US' NOT NULL,
    _source VARCHAR(50) DEFAULT 'us_standard_system' NOT NULL
);

-- Product Variants
CREATE TABLE IF NOT EXISTS us.product_variants (
    variant_id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES us.products(product_id),
    product_sku VARCHAR(100) UNIQUE NOT NULL,
    variant_name VARCHAR(100),
    variant_type VARCHAR(50),
    variant_value VARCHAR(100),
    price_diff_usd DECIMAL(10,2) DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'US' NOT NULL,
    _source VARCHAR(50) DEFAULT 'us_standard_system' NOT NULL
);

-- Inventory Management
CREATE TABLE IF NOT EXISTS us.inventory (
    inventory_id SERIAL PRIMARY KEY,
    variant_id INTEGER REFERENCES us.product_variants(variant_id),
    warehouse_location VARCHAR(100), -- "US-WEST-01", "US-EAST-02"
    quantity_available INTEGER NOT NULL DEFAULT 0,
    quantity_reserved INTEGER NOT NULL DEFAULT 0,
    reorder_level INTEGER DEFAULT 10,
    last_restocked_at TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'US' NOT NULL,
    _source VARCHAR(50) DEFAULT 'us_standard_system' NOT NULL
);

-- Customer Database - Full data collection
CREATE TABLE IF NOT EXISTS us.customers (
    customer_id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    phone VARCHAR(20), -- US format: "+1-555-123-4567"
    birth_date DATE,
    gender VARCHAR(10), -- "Male", "Female", "Other"
    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    customer_segment VARCHAR(50), -- "new", "regular", "vip", "churned"
    acquisition_channel VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'US' NOT NULL,
    _source VARCHAR(50) DEFAULT 'us_standard_system' NOT NULL
);

-- Customer Addresses
CREATE TABLE IF NOT EXISTS us.customer_addresses (
    address_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES us.customers(customer_id),
    address_type VARCHAR(20),
    street_address TEXT NOT NULL,
    city VARCHAR(100) NOT NULL,
    region VARCHAR(100), -- US states
    postal_code VARCHAR(10), -- "10001" or "10001-1234"
    country VARCHAR(100) NOT NULL DEFAULT 'USA',
    is_default BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'US' NOT NULL,
    _source VARCHAR(50) DEFAULT 'us_standard_system' NOT NULL
);

-- Marketing Campaigns
-- CREATE TABLE IF NOT EXISTS us.marketing_campaigns (
--     campaign_id SERIAL PRIMARY KEY,
--     campaign_name VARCHAR(255) NOT NULL,
--     campaign_type VARCHAR(50),
--     channel VARCHAR(100),
--     start_date DATE,
--     end_date DATE,
--     budget_usd DECIMAL(12,2),
--     target_audience TEXT,
--     campaign_status VARCHAR(20),
--     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
--     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
--     _region VARCHAR(10) DEFAULT 'US' NOT NULL,
--     _source VARCHAR(50) DEFAULT 'us_standard_system' NOT NULL
-- );

-- Discount System
CREATE TABLE IF NOT EXISTS us.discounts (
    discount_id SERIAL PRIMARY KEY,
    discount_code VARCHAR(50) UNIQUE,
    discount_name VARCHAR(255),
    discount_type VARCHAR(20),
    discount_value DECIMAL(10,2),
    minimum_order_amount DECIMAL(10,2),
    maximum_discount_amount DECIMAL(10,2),
    usage_limit INTEGER,
    usage_count INTEGER DEFAULT 0,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'US' NOT NULL,
    _source VARCHAR(50) DEFAULT 'us_standard_system' NOT NULL
);

-- Shopping Cart
CREATE TABLE IF NOT EXISTS us.shopping_carts (
    cart_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES us.customers(customer_id),
    session_id VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    abandoned_at TIMESTAMP,
    is_abandoned BOOLEAN DEFAULT FALSE,
    _region VARCHAR(10) DEFAULT 'US' NOT NULL,
    _source VARCHAR(50) DEFAULT 'us_standard_system' NOT NULL
);

-- Cart Items
CREATE TABLE IF NOT EXISTS us.cart_items (
    cart_item_id SERIAL PRIMARY KEY,
    cart_id INTEGER REFERENCES us.shopping_carts(cart_id),
    variant_id INTEGER REFERENCES us.product_variants(variant_id),
    quantity INTEGER NOT NULL,
    unit_price_usd DECIMAL(10,2) NOT NULL,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'US' NOT NULL,
    _source VARCHAR(50) DEFAULT 'us_standard_system' NOT NULL
);

-- Orders
CREATE TABLE IF NOT EXISTS us.orders (
    order_id SERIAL PRIMARY KEY,
    order_reference VARCHAR(50) UNIQUE NOT NULL, -- "US-ORD-123456"
    customer_id INTEGER REFERENCES us.customers(customer_id),
    order_status VARCHAR(20),
    order_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    subtotal_usd DECIMAL(12,2) NOT NULL,
    tax_amount_usd DECIMAL(10,2) DEFAULT 0, -- Sales tax varies by state
    shipping_amount_usd DECIMAL(10,2) DEFAULT 0,
    discount_amount_usd DECIMAL(10,2) DEFAULT 0,
    total_amount_usd DECIMAL(12,2) NOT NULL,
    billing_address_id INTEGER REFERENCES us.customer_addresses(address_id),
    shipping_address_id INTEGER REFERENCES us.customer_addresses(address_id),
    campaign_id INTEGER REFERENCES us.marketing_campaigns(campaign_id),
    discount_id INTEGER REFERENCES us.discounts(discount_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'US' NOT NULL,
    _source VARCHAR(50) DEFAULT 'us_standard_system' NOT NULL
);

-- Order Items
CREATE TABLE IF NOT EXISTS us.order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES us.orders(order_id),
    variant_id INTEGER REFERENCES us.product_variants(variant_id),
    quantity INTEGER NOT NULL,
    unit_price_usd DECIMAL(10,2) NOT NULL,
    total_price_usd DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'US' NOT NULL,
    _source VARCHAR(50) DEFAULT 'us_standard_system' NOT NULL
);

-- Payment Processing
CREATE TABLE IF NOT EXISTS us.payments (
    payment_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES us.orders(order_id),
    payment_method VARCHAR(50), -- "credit_card", "paypal", "apple_pay"
    payment_status VARCHAR(20),
    payment_amount_usd DECIMAL(12,2) NOT NULL,
    payment_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    transaction_id VARCHAR(255),
    gateway_response TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'US' NOT NULL,
    _source VARCHAR(50) DEFAULT 'us_standard_system' NOT NULL
);

-- Shipping Management
CREATE TABLE IF NOT EXISTS us.shipments (
    shipment_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES us.orders(order_id),
    tracking_number VARCHAR(100),
    carrier VARCHAR(100), -- "UPS", "FedEx", "USPS"
    shipping_method VARCHAR(100),
    shipped_timestamp TIMESTAMP,
    estimated_delivery_date TIMESTAMP,
    actual_delivery_timestamp TIMESTAMP,
    shipment_status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'US' NOT NULL,
    _source VARCHAR(50) DEFAULT 'us_standard_system' NOT NULL
);

-- Returns and Refunds
CREATE TABLE IF NOT EXISTS us.returns (
    return_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES us.orders(order_id),
    return_reason VARCHAR(255),
    return_status VARCHAR(20),
    return_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    refund_amount DECIMAL(10,2),
    refund_timestamp TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'US' NOT NULL,
    _source VARCHAR(50) DEFAULT 'us_standard_system' NOT NULL
);

-- Product Reviews
CREATE TABLE IF NOT EXISTS us.product_reviews (
    review_id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES us.products(product_id),
    customer_id INTEGER REFERENCES us.customers(customer_id),
    order_id INTEGER REFERENCES us.orders(order_id),
    rating INTEGER CHECK (rating >= 1 AND rating <= 5),
    review_title VARCHAR(255),
    review_text TEXT,
    is_verified_purchase BOOLEAN DEFAULT FALSE,
    helpful_votes INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'US' NOT NULL,
    _source VARCHAR(50) DEFAULT 'us_standard_system' NOT NULL
);

-- Wishlist
CREATE TABLE IF NOT EXISTS us.wishlists (
    wishlist_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES us.customers(customer_id),
    product_id INTEGER REFERENCES us.products(product_id),
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'US' NOT NULL,
    _source VARCHAR(50) DEFAULT 'us_standard_system' NOT NULL
);

-- Performance Indexes
CREATE INDEX IF NOT EXISTS idx_us_products_category ON us.products(category_id);
CREATE INDEX IF NOT EXISTS idx_us_variants_product ON us.product_variants(product_id);
CREATE INDEX IF NOT EXISTS idx_us_inventory_variant ON us.inventory(variant_id);
CREATE INDEX IF NOT EXISTS idx_us_orders_customer ON us.orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_us_orders_date ON us.orders(order_date);
CREATE INDEX IF NOT EXISTS idx_us_order_items_order ON us.order_items(order_id);

-- Performance Indexes
CREATE INDEX IF NOT EXISTS idx_us_products_category ON us.products(category_id);
CREATE INDEX IF NOT EXISTS idx_us_variants_product ON us.product_variants(product_id);
CREATE INDEX IF NOT EXISTS idx_us_inventory_variant ON us.inventory(variant_id);
CREATE INDEX IF NOT EXISTS idx_us_orders_customer ON us.orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_us_orders_date ON us.orders(order_date);
CREATE INDEX IF NOT EXISTS idx_us_order_items_order ON us.order_items(order_id);

CREATE INDEX IF NOT EXISTS idx_us_customers_region_source ON us.customers(_region, _source);
CREATE INDEX IF NOT EXISTS idx_us_orders_region_source ON us.orders(_region, _source);