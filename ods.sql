
-- Product Categories
CREATE TABLE IF NOT EXISTS ods.categories (
    category_id INTEGER AUTOINCREMENT,
    category_name VARCHAR(100) NOT NULL,
    parent_category_id INTEGER,
    category_path TEXT,
    category_name_local VARCHAR(100), -- From EU schema
    is_gdpr_sensitive BOOLEAN DEFAULT FALSE, -- From EU schema
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_region VARCHAR(10) NOT NULL, -- 'ASIA', 'EU', 'US'
    source_system VARCHAR(50) NOT NULL,
    primary key (CATEGORY_ID)
);



-- Products
CREATE TABLE IF NOT EXISTS ods.products (
    product_id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_sku VARCHAR(30) ,
    product_name VARCHAR(255) ,
    product_description TEXT,
    category_id INTEGER REFERENCES ods.categories(category_id),
    price_usd DECIMAL(10,2) ,
    cost_usd DECIMAL(10,2),
    weight_kg DECIMAL(8,3),
    length_cm DECIMAL(8,2),
    width_cm DECIMAL(8,2),
    height_cm DECIMAL(8,2),
    vat_rate DECIMAL(5,2), -- From EU schema
    energy_rating VARCHAR(10), -- From EU schema
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_region VARCHAR(10) NOT NULL, -- 'ASIA', 'EU', 'US'
    source_system VARCHAR(50) NOT NULL,
    CONSTRAINT uq_product_id_region UNIQUE (product_id, source_region);

);



-- Product Variants
CREATE TABLE IF NOT EXISTS ods.product_variants (
    variant_id BIGINT AUTOINCREMENT,
    product_id INT NOT NULL,
    product_sku VARCHAR NOT NULL,
    variant_name VARCHAR,
    variant_type VARCHAR,
    variant_value VARCHAR,
    price_diff_usd DECIMAL(10,2),
    is_active BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    source_region VARCHAR(10),
    source_system VARCHAR(50),
    PRIMARY KEY (variant_id, source_region),
    UNIQUE (product_sku, source_region)
);


-- Inventory
CREATE TABLE IF NOT EXISTS ods.inventory (
    inventory_id BIGINT AUTOINCREMENT PRIMARY KEY,  -- optional surrogate key
    variant_id INTEGER NOT NULL,
    warehouse_location VARCHAR(100) NOT NULL,
    quantity_available INTEGER DEFAULT 0,
    quantity_reserved INTEGER DEFAULT 0,
    reorder_level INTEGER DEFAULT 10,
    last_restocked_at TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_region VARCHAR(10) NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    UNIQUE (variant_id, source_region, warehouse_location)
);



-- Customers - Handling GDPR compliance
CREATE TABLE IF NOT EXISTS ods.customers (
    customer_id INTEGER NOT NULL,
    email VARCHAR(255), -- Hashed in EU region
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    phone VARCHAR(50),
    birth_date DATE,
    gender VARCHAR(20),
    country_code VARCHAR(2), -- ISO country code
    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP,
    customer_segment VARCHAR(50),
    acquisition_channel VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE,
    gdpr_consent_date TIMESTAMP, -- From EU schema
    data_retention_until TIMESTAMP, -- From EU schema
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_region VARCHAR(10) NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    
    -- Composite primary key ensures uniqueness per region
    PRIMARY KEY (customer_id, source_region)
);


-- Customer Addresses
CREATE TABLE IF NOT EXISTS ods.customer_addresses (

    address_id INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    address_type VARCHAR(20),
    street_address TEXT NOT NULL,
    city VARCHAR(100) NOT NULL,
    region VARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(100) NOT NULL,
    is_default BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_region VARCHAR(10) NOT NULL,     -- 'ASIA', 'EU', 'US'
    source_system VARCHAR(50) NOT NULL,     -- 'postgres', 'eu_gdpr_system', etc.
    
    CONSTRAINT uq_address_region UNIQUE (address_id, source_region),

    FOREIGN KEY (customer_id, source_region) REFERENCES ods.customers(customer_id, source_region)
);


-- Discounts
CREATE TABLE IF NOT EXISTS ods.discounts (
    discount_id INTEGER PRIMARY KEY AUTOINCREMENT,
    discount_code VARCHAR(50),
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
    source_region VARCHAR(10) NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    CONSTRAINT uq_discount_region UNIQUE (discount_id, source_region)
);

-- Shopping Carts
CREATE TABLE IF NOT EXISTS ods.shopping_carts (
    cart_id INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    session_id VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    abandoned_at TIMESTAMP,
    is_abandoned BOOLEAN DEFAULT FALSE,
    source_region VARCHAR(10) NOT NULL,
    source_system VARCHAR(50) NOT NULL,

    CONSTRAINT uq_cart_region UNIQUE (cart_id, source_region),
    FOREIGN KEY (customer_id, source_region) REFERENCES ods.customers(customer_id, source_region)
);


-- Cart Items
CREATE TABLE IF NOT EXISTS ods.cart_items (
    cart_item_id INTEGER NOT NULL,
    cart_id INTEGER NOT NULL,
    variant_id BIGINT NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price_usd DECIMAL(10,2) NOT NULL,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_region VARCHAR(10) NOT NULL,
    source_system VARCHAR(50) NOT NULL,

    -- Ensure uniqueness per region for compatibility with multi-source ingestion
    CONSTRAINT pk_cart_items_region PRIMARY KEY (cart_item_id, source_region),

    -- Enforce FK on composite region-aware cart reference
    FOREIGN KEY (cart_id, source_region)
        REFERENCES ods.shopping_carts(cart_id, source_region),

    -- Standard FK (product_variants are globally unique by variant_id)
    FOREIGN KEY (variant_id)
        REFERENCES ods.product_variants(variant_id)
);


-- Orders
CREATE TABLE IF NOT EXISTS ods.orders (
    order_id INTEGER NOT NULL AUTOINCREMENT,
    order_reference VARCHAR(60) NOT NULL,
    customer_id INTEGER NOT NULL,
    order_status VARCHAR(30),
    order_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    subtotal_usd DECIMAL(12,2) NOT NULL,
    tax_amount_usd DECIMAL(12,2) DEFAULT 0,
    shipping_amount_usd DECIMAL(12,2) DEFAULT 0,
    discount_amount_usd DECIMAL(12,2) DEFAULT 0,
    total_amount_usd DECIMAL(12,2) NOT NULL,

    billing_address_id INTEGER,
    shipping_address_id INTEGER,

    campaign_id INTEGER,
    discount_id INTEGER,

    invoice_required BOOLEAN, -- from EU schema only, nullable for others

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP,

    source_region VARCHAR(10) NOT NULL,
    source_system VARCHAR(50) NOT NULL,

    PRIMARY KEY (order_id, source_region),

    -- Composite foreign key constraints as table-level constraints:
    CONSTRAINT fk_customer FOREIGN KEY (customer_id, source_region)
        REFERENCES ods.customers(customer_id, source_region),

    CONSTRAINT fk_billing_address FOREIGN KEY (billing_address_id, source_region)
        REFERENCES ods.customer_addresses(address_id, source_region),

    CONSTRAINT fk_shipping_address FOREIGN KEY (shipping_address_id, source_region)
        REFERENCES ods.customer_addresses(address_id, source_region),

    CONSTRAINT fk_campaign FOREIGN KEY (campaign_id, source_region)
        REFERENCES ods.marketing_campaigns(campaign_id, source_region),

    CONSTRAINT fk_discount FOREIGN KEY (discount_id, source_region)
        REFERENCES ods.discounts(discount_id, source_region)
);

CREATE TABLE IF NOT EXISTS ods.marketing_campaigns (
    campaign_id INTEGER NOT NULL,
    campaign_name VARCHAR(255) NOT NULL,
    campaign_type VARCHAR(50),
    channel VARCHAR(100),
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    budget_usd DECIMAL(12,2),
    target_audience TEXT,
    campaign_status VARCHAR(20),
    is_gdpr_compliant BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP,
    source_region VARCHAR(10) NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    CONSTRAINT uq_campaign_region UNIQUE (campaign_id, source_region)
);

CREATE TABLE IF NOT EXISTS ods.marketing_performance (
    campaign_id INT,
    ad_platform VARCHAR(50) ,
    ad_id VARCHAR(100),
    ad_group_id VARCHAR(100) ,
    record_date DATE ,
    impressions INT ,
    clicks INT ,
    spend_usd DECIMAL(10, 2),
    conversions INT ,
    conversion_value_usd DECIMAL(10, 2),
    source_region VARCHAR(50) ,
    source_system VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() ,
    updated_at TIMESTAMP,
    PRIMARY KEY (campaign_id, source_region)
);


-- Order Items
CREATE TABLE IF NOT EXISTS ods.order_items (
    order_item_id INTEGER NOT NULL,
    order_id INTEGER NOT NULL,
    variant_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price_usd DECIMAL(12, 4) NOT NULL,
    total_price_usd DECIMAL(14, 4) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    vat_rate DECIMAL(5, 2), -- Only applicable to EU
    source_region VARCHAR(10) NOT NULL,
    source_system VARCHAR(50) NOT NULL,

    PRIMARY KEY (order_item_id, source_region),

    FOREIGN KEY (order_id, source_region) REFERENCES ods.orders(order_id, source_region),
    FOREIGN KEY (variant_id, source_region) REFERENCES ods.product_variants(variant_id, source_region)
);

-- Payments
CREATE TABLE IF NOT EXISTS ods.payments (
    payment_id INTEGER NOT NULL,
    order_id INTEGER NOT NULL,
    payment_method VARCHAR(50),
    payment_status VARCHAR(20),
    payment_amount_usd DECIMAL(14, 4) NOT NULL,
    payment_timestamp TIMESTAMP,
    transaction_id VARCHAR(255),
    gateway_response TEXT,
    psd2_compliant BOOLEAN,  -- NULL for ASIA and US
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_region VARCHAR(10) NOT NULL,
    source_system VARCHAR(50) NOT NULL,

    PRIMARY KEY (payment_id, source_region),
    FOREIGN KEY (order_id, source_region) REFERENCES ods.orders(order_id, source_region)
);


-- Shipments
CREATE TABLE IF NOT EXISTS ods.shipments (
    shipment_id INTEGER NOT NULL,
    order_id INTEGER NOT NULL,
    tracking_number VARCHAR(100),
    carrier VARCHAR(100),
    shipping_method VARCHAR(100),
    shipped_timestamp TIMESTAMP,
    estimated_delivery_date TIMESTAMP,
    actual_delivery_timestamp TIMESTAMP,
    shipment_status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_region VARCHAR(10) NOT NULL,
    source_system VARCHAR(50) NOT NULL,

    PRIMARY KEY (shipment_id, source_region),
    FOREIGN KEY (order_id, source_region) REFERENCES ods.orders(order_id, source_region)
);

-- Returns
CREATE TABLE IF NOT EXISTS ods.returns (
    return_id INTEGER NOT NULL,
    order_id INTEGER NOT NULL,
    return_reason VARCHAR(255),
    return_status VARCHAR(20),
    return_timestamp TIMESTAMP,
    refund_amount DECIMAL(10,2),
    refund_timestamp TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    cooling_off_period_days INTEGER, -- EU only
    source_region VARCHAR(10) NOT NULL,
    source_system VARCHAR(50) NOT NULL,

    PRIMARY KEY (return_id, source_region),
    FOREIGN KEY (order_id, source_region) REFERENCES ods.orders(order_id, source_region)
);

-- Product Reviews
CREATE TABLE IF NOT EXISTS ods.product_reviews (
    review_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    order_id INTEGER,
    rating INTEGER ,
    review_title VARCHAR(255),
    review_text TEXT,
    is_verified_purchase BOOLEAN DEFAULT FALSE,
    helpful_votes INTEGER DEFAULT 0,
    moderation_status VARCHAR(20), -- Only for EU; NULL elsewhere
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_region VARCHAR(10) NOT NULL,
    source_system VARCHAR(50) NOT NULL,

    PRIMARY KEY (review_id, source_region),
    FOREIGN KEY (product_id, source_region) REFERENCES ods.products(product_id, source_region),
    FOREIGN KEY (customer_id, source_region) REFERENCES ods.customers(customer_id, source_region),
    FOREIGN KEY (order_id, source_region) REFERENCES ods.orders(order_id, source_region)
);

-- Wishlists
CREATE TABLE IF NOT EXISTS ods.wishlists (
    wishlist_id INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_region VARCHAR(10) NOT NULL,
    source_system VARCHAR(50) NOT NULL,

    CONSTRAINT uq_wishlist_region UNIQUE (wishlist_id, source_region),
    FOREIGN KEY (customer_id, source_region) REFERENCES ods.customers(customer_id, source_region),
    FOREIGN KEY (product_id) REFERENCES ods.products(product_id)
);



