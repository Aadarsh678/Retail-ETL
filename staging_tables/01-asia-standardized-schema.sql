-- Product Categories
CREATE TABLE IF NOT EXISTS staging_asia.categories (
    category_id INTEGER AUTOINCREMENT PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL,
    parent_category_id INTEGER,
    category_path VARCHAR,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'asia' NOT NULL,
    _source VARCHAR(50) DEFAULT 'postgres' NOT NULL
);

-- Products
CREATE TABLE IF NOT EXISTS staging_asia.products (
    product_id INTEGER AUTOINCREMENT PRIMARY KEY,
    product_sku VARCHAR(30) UNIQUE,
    product_name VARCHAR(255),
    product_description VARCHAR,
    category_id INTEGER,
    price_usd NUMBER(10,2),
    cost_usd NUMBER(10,2),
    weight_kg NUMBER(8,3),
    length_cm NUMBER(8,2),
    width_cm NUMBER(8,2),
    height_cm NUMBER(8,2),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'asia' NOT NULL,
    _source VARCHAR(50) DEFAULT 'postgres' NOT NULL
);

-- Product Variants
CREATE TABLE IF NOT EXISTS staging_asia.product_variants (
    variant_id INTEGER AUTOINCREMENT PRIMARY KEY,
    product_id INTEGER,
    product_sku VARCHAR(100),
    variant_name VARCHAR(100),
    variant_type VARCHAR(50),
    variant_value VARCHAR(100),
    price_diff_usd NUMBER(10,2) DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'asia' NOT NULL,
    _source VARCHAR(50) DEFAULT 'postgres' NOT NULL
);

-- Inventory
CREATE TABLE IF NOT EXISTS staging_asia.inventory (
    inventory_id INTEGER AUTOINCREMENT PRIMARY KEY,
    variant_id INTEGER,
    warehouse_location VARCHAR(100),
    quantity_available INTEGER DEFAULT 0,
    quantity_reserved INTEGER DEFAULT 0,
    reorder_level INTEGER DEFAULT 10,
    last_restocked_at TIMESTAMP_LTZ,
    updated_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'asia' NOT NULL,
    _source VARCHAR(50) DEFAULT 'postgres' NOT NULL
);

-- Customers
CREATE TABLE IF NOT EXISTS staging_asia.customers (
    customer_id INTEGER AUTOINCREMENT PRIMARY KEY,
    email VARCHAR(255),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    phone VARCHAR(50),
    birth_date TIMESTAMP_LTZ,
    gender VARCHAR(20),
    registration_date TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP_LTZ,
    customer_segment VARCHAR(50),
    acquisition_channel VARCHAR(100),
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'asia' NOT NULL,
    _source VARCHAR(50) DEFAULT 'postgres' NOT NULL
);

-- Customer Addresses
CREATE TABLE IF NOT EXISTS staging_asia.customer_addresses (
    address_id INTEGER AUTOINCREMENT PRIMARY KEY,
    customer_id INTEGER,
    address_type VARCHAR(20),
    street_address VARCHAR NOT NULL,
    city VARCHAR(100) NOT NULL,
    region VARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(100) NOT NULL,
    is_default BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'asia' NOT NULL,
    _source VARCHAR(50) DEFAULT 'postgres' NOT NULL
);

-- Discounts
CREATE TABLE IF NOT EXISTS staging_asia.discounts (
    discount_id INTEGER AUTOINCREMENT PRIMARY KEY,
    discount_code VARCHAR(50),
    discount_name VARCHAR(255),
    discount_type VARCHAR(20),
    discount_value NUMBER(10,2),
    minimum_order_amount NUMBER(10,2),
    maximum_discount_amount NUMBER(10,2),
    usage_limit INTEGER,
    usage_count INTEGER DEFAULT 0,
    start_date TIMESTAMP_LTZ,
    end_date TIMESTAMP_LTZ,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'asia' NOT NULL,
    _source VARCHAR(50) DEFAULT 'postgres' NOT NULL
);

-- Shopping Carts
CREATE TABLE IF NOT EXISTS staging_asia.shopping_carts (
    cart_id INTEGER AUTOINCREMENT PRIMARY KEY,
    customer_id INTEGER,
    session_id VARCHAR(255),
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
    abandoned_at TIMESTAMP_LTZ,
    _region VARCHAR(10) DEFAULT 'asia' NOT NULL,
    _source VARCHAR(50) DEFAULT 'postgres' NOT NULL
);

-- Cart Items
CREATE TABLE IF NOT EXISTS staging_asia.cart_items (
    cart_item_id INTEGER AUTOINCREMENT PRIMARY KEY,
    cart_id INTEGER,
    variant_id INTEGER,
    quantity INTEGER NOT NULL,
    unit_price_usd NUMBER(10,2) NOT NULL,
    added_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'asia' NOT NULL,
    _source VARCHAR(50) DEFAULT 'postgres' NOT NULL
);

-- Orders
CREATE TABLE IF NOT EXISTS staging_asia.orders (
    order_id INTEGER AUTOINCREMENT PRIMARY KEY,
    order_reference VARCHAR(60),
    customer_id INTEGER,
    order_status VARCHAR(30),
    order_timestamp TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
    subtotal_usd NUMBER(12,2),
    tax_amount_usd NUMBER(12,2),
    shipping_amount_usd NUMBER(12,2),
    discount_amount_usd NUMBER(12,2),
    total_amount_usd NUMBER(12,2),
    billing_address_id INTEGER,
    shipping_address_id INTEGER,
    campaign_id INTEGER,
    discount_id INTEGER,
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'asia' NOT NULL,
    _source VARCHAR(50) DEFAULT 'postgres' NOT NULL
);

-- Order Items
CREATE TABLE IF NOT EXISTS staging_asia.order_items (
    order_item_id INTEGER AUTOINCREMENT PRIMARY KEY,
    order_id INTEGER,
    variant_id INTEGER,
    quantity INTEGER NOT NULL,
    unit_price_usd NUMBER(10,2) NOT NULL,
    total_price_usd NUMBER(10,2) NOT NULL,
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'asia' NOT NULL,
    _source VARCHAR(50) DEFAULT 'postgres' NOT NULL
);

-- Payments
CREATE TABLE IF NOT EXISTS staging_asia.payments (
    payment_id INTEGER AUTOINCREMENT PRIMARY KEY,
    order_id INTEGER,
    payment_method VARCHAR(50),
    payment_status VARCHAR(20),
    payment_amount_usd NUMBER(12,2) NOT NULL,
    payment_timestamp TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
    transaction_id VARCHAR(255),
    gateway_response VARCHAR,
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'asia' NOT NULL,
    _source VARCHAR(50) DEFAULT 'postgres' NOT NULL
);

-- Shipments
CREATE TABLE IF NOT EXISTS staging_asia.shipments (
    shipment_id INTEGER AUTOINCREMENT PRIMARY KEY,
    order_id INTEGER,
    tracking_number VARCHAR(100),
    carrier VARCHAR(100),
    shipping_method VARCHAR(100),
    shipped_timestamp TIMESTAMP_LTZ,
    estimated_delivery_date TIMESTAMP_LTZ,
    actual_delivery_timestamp TIMESTAMP_LTZ,
    shipment_status VARCHAR(20),
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'asia' NOT NULL,
    _source VARCHAR(50) DEFAULT 'postgres' NOT NULL
);

-- Returns
CREATE TABLE IF NOT EXISTS staging_asia.returns (
    return_id INTEGER AUTOINCREMENT PRIMARY KEY,
    order_id INTEGER,
    return_reason VARCHAR(255),
    return_status VARCHAR(20),
    return_timestamp TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
    refund_amount NUMBER(10,2),
    refund_timestamp TIMESTAMP_LTZ,
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'asia' NOT NULL,
    _source VARCHAR(50) DEFAULT 'postgres' NOT NULL
);

-- Product Reviews
CREATE TABLE IF NOT EXISTS staging_asia.product_reviews (
    review_id INTEGER AUTOINCREMENT PRIMARY KEY,
    product_id INTEGER,
    customer_id INTEGER,
    order_id INTEGER,
    rating INTEGER CHECK (rating BETWEEN 1 AND 5),
    review_title VARCHAR(255),
    review_text VARCHAR,
    is_verified_purchase BOOLEAN DEFAULT FALSE,
    helpful_votes INTEGER DEFAULT 0,
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'asia' NOT NULL,
    _source VARCHAR(50) DEFAULT 'postgres' NOT NULL
);

-- Wishlists
CREATE TABLE IF NOT EXISTS staging_asia.wishlists (
    wishlist_id INTEGER AUTOINCREMENT PRIMARY KEY,
    customer_id INTEGER,
    product_id INTEGER,
    added_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
    _region VARCHAR(10) DEFAULT 'asia' NOT NULL,
    _source VARCHAR(50) DEFAULT 'postgres' NOT NULL
);

-- Indexes
CREATE INDEX idx_staging_asia_products_category ON staging_asia.products(category_id);
CREATE INDEX idx_staging_asia_variants_product ON staging_asia.product_variants(product_id);
CREATE INDEX idx_staging_asia_orders_customer ON staging_asia.orders(customer_id);
CREATE INDEX idx_staging_asia_orders_region_source ON staging_asia.orders(_region, _source);
CREATE INDEX idx_staging_asia_customers_region_source ON staging_asia.customers(_region, _source);
