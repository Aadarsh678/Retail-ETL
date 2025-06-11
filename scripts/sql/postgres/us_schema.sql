-- =============================================
-- US REGION DATABASE SCHEMA
-- Single vendor e-commerce platform optimized for US market
-- Features: Imperial units, USD currency, full customer data
-- =============================================

-- Product Categories - Hierarchical structure for product organization
-- US specific: Standard boolean fields, simple structure
\connect retail_etl;
SET search_path TO us;


CREATE TABLE IF NOT EXISTS us.categories (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL, -- "Electronics", "Clothing", etc.
    parent_category_id INTEGER REFERENCES us.categories(category_id), -- For hierarchy: Electronics > Smartphones
    category_path TEXT, -- Computed path: "/Electronics/Smartphones" for easy queries
    is_active BOOLEAN DEFAULT TRUE, -- Standard boolean in US
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMp, -- UTC timestamps
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Products - Core product catalog for single vendor
-- US specific: USD pricing, imperial measurements, simple product codes
CREATE TABLE IF NOT EXISTS us.products (
    product_id SERIAL PRIMARY KEY,
    product_code VARCHAR(20) NOT NULL UNIQUE, -- US format: "PRD-12345"
    product_name VARCHAR(255) NOT NULL, -- "iPhone 15 Pro", "Wireless Headphones"
    product_description TEXT, -- Full product description
    category_id INTEGER REFERENCES us.categories(category_id),
    base_price_usd DECIMAL(10,2) NOT NULL, -- US dollars: 999.99
    cost_price_usd DECIMAL(10,2), -- Internal cost for margin calculation
    weight_lbs DECIMAL(8,3), -- Imperial: pounds (2.5 lbs)
    dimensions_inches VARCHAR(50), -- "6.1 x 3.0 x 0.3" (L x W x H)
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Product Variants - Size, color, style variations
-- US specific: USD price adjustments, simple SKU format
CREATE TABLE IF NOT EXISTS us.product_variants (
    variant_id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES us.products(product_id),
    sku VARCHAR(100) UNIQUE NOT NULL, -- "PRD-12345-RED-L"
    variant_name VARCHAR(100), -- "Red Large", "128GB Space Gray"
    variant_type VARCHAR(50), -- "color", "size", "storage"
    variant_value VARCHAR(100), -- "Red", "Large", "128GB"
    price_adjustment_usd DECIMAL(10,2) DEFAULT 0, -- +50.00 for larger storage
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Inventory Management - Stock levels per variant
-- US specific: Simple warehouse naming, imperial reorder levels
CREATE TABLE IF NOT EXISTS us.inventory (
    inventory_id SERIAL PRIMARY KEY,
    variant_id INTEGER REFERENCES us.product_variants(variant_id),
    warehouse_location VARCHAR(100), -- "US-WEST-01", "US-EAST-02"
    quantity_available INTEGER NOT NULL DEFAULT 0, -- Current stock
    quantity_reserved INTEGER NOT NULL DEFAULT 0, -- Reserved for pending orders
    reorder_level INTEGER DEFAULT 10, -- Trigger reorder when below this
    last_restocked_at TIMESTAMP, -- When inventory was last replenished
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Customer Database - Full customer information (no privacy restrictions)
-- US specific: Complete data collection, boolean active status
CREATE TABLE IF NOT EXISTS us.customers (
    customer_id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL, -- Primary identifier
    first_name VARCHAR(100), -- Full first name stored
    last_name VARCHAR(100), -- Full last name stored
    phone VARCHAR(20), -- US format: "+1-555-123-4567"
    date_of_birth DATE, -- For age verification, marketing
    gender VARCHAR(10), -- "Male", "Female", "Other"
    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login_at TIMESTAMP, -- Track engagement
    is_active BOOLEAN DEFAULT TRUE, -- Simple boolean status
    customer_segment VARCHAR(50), -- "new", "regular", "vip", "churned"
    acquisition_channel VARCHAR(100), -- "google_ads", "facebook", "referral"
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Customer Addresses - Billing and shipping addresses
-- US specific: State field, ZIP codes, defaulted to USA
CREATE TABLE IF NOT EXISTS us.customer_addresses (
    address_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES us.customers(customer_id),
    address_type VARCHAR(20), -- "billing", "shipping"
    street_address TEXT NOT NULL, -- "123 Main St, Apt 4B"
    city VARCHAR(100) NOT NULL, -- "New York"
    state VARCHAR(100), -- "New York", "California" - US states
    zip_code VARCHAR(10), -- "10001" or "10001-1234"
    country VARCHAR(100) NOT NULL DEFAULT 'USA', -- Defaulted to USA
    is_default BOOLEAN DEFAULT FALSE, -- Primary address flag
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Marketing Campaigns - Promotional campaign tracking
-- US specific: USD budgets, US-focused channels
CREATE TABLE IF NOT EXISTS us.marketing_campaigns (
    campaign_id SERIAL PRIMARY KEY,
    campaign_name VARCHAR(255) NOT NULL, -- "Black Friday 2024", "Summer Sale"
    campaign_type VARCHAR(50), -- "email", "social", "ppc", "display"
    channel VARCHAR(100), -- "google_ads", "facebook", "instagram"
    start_date DATE, -- Campaign start
    end_date DATE, -- Campaign end
    budget_usd DECIMAL(12,2), -- Campaign budget in USD
    target_audience TEXT, -- Audience description
    campaign_status VARCHAR(20), -- "active", "paused", "completed"
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Discount System - Coupons and promotional codes
-- US specific: USD amounts, simple boolean active flag
CREATE TABLE IF NOT EXISTS us.discounts (
    discount_id SERIAL PRIMARY KEY,
    discount_code VARCHAR(50) UNIQUE, -- "SAVE20", "BLACKFRIDAY"
    discount_name VARCHAR(255), -- "20% Off Everything"
    discount_type VARCHAR(20), -- "percentage", "fixed_amount", "free_shipping"
    discount_value DECIMAL(10,2), -- 20.00 (for 20%) or 50.00 (for $50 off)
    minimum_order_amount_usd DECIMAL(10,2), -- Minimum order to qualify
    maximum_discount_amount_usd DECIMAL(10,2), -- Cap on discount amount
    usage_limit INTEGER, -- How many times code can be used
    usage_count INTEGER DEFAULT 0, -- Current usage count
    start_date TIMESTAMP, -- When discount becomes active
    end_date TIMESTAMP, -- When discount expires
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Shopping Cart - Temporary storage for customer selections
-- US specific: Simple abandonment tracking
CREATE TABLE IF NOT EXISTS us.shopping_carts (
    cart_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES us.customers(customer_id),
    session_id VARCHAR(255), -- For guest users before login
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    abandoned_at TIMESTAMP, -- When cart was abandoned
    is_abandoned BOOLEAN DEFAULT FALSE -- Abandonment flag for marketing
);

-- Cart Items - Individual products in shopping cart
-- US specific: USD pricing
CREATE TABLE IF NOT EXISTS us.cart_items (
    cart_item_id SERIAL PRIMARY KEY,
    cart_id INTEGER REFERENCES us.shopping_carts(cart_id),
    variant_id INTEGER REFERENCES us.product_variants(variant_id),
    quantity INTEGER NOT NULL, -- How many of this item
    unit_price_usd DECIMAL(10,2) NOT NULL, -- Price at time of adding to cart
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Orders - Completed purchase transactions
-- US specific: USD currency, standard order numbering, sales tax
CREATE TABLE IF NOT EXISTS us.orders (
    order_id SERIAL PRIMARY KEY,
    order_number VARCHAR(50) UNIQUE NOT NULL, -- "US-ORD-123456"
    customer_id INTEGER REFERENCES us.customers(customer_id),
    order_status VARCHAR(20), -- "pending", "confirmed", "shipped", "delivered", "cancelled"
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    subtotal_usd DECIMAL(12,2) NOT NULL, -- Pre-tax total
    tax_amount_usd DECIMAL(10,2) DEFAULT 0, -- Sales tax (varies by state)
    shipping_amount_usd DECIMAL(10,2) DEFAULT 0, -- Shipping cost
    discount_amount_usd DECIMAL(10,2) DEFAULT 0, -- Applied discounts
    total_amount_usd DECIMAL(12,2) NOT NULL, -- Final total
    billing_address_id INTEGER REFERENCES us.customer_addresses(address_id),
    shipping_address_id INTEGER REFERENCES us.customer_addresses(address_id),
    campaign_id INTEGER REFERENCES us.marketing_campaigns(campaign_id), -- Attribution
    discount_id INTEGER REFERENCES us.discounts(discount_id), -- Applied discount
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Order Items - Individual products within an order
-- US specific: USD pricing, simple structure
CREATE TABLE IF NOT EXISTS us.order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES us.orders(order_id),
    variant_id INTEGER REFERENCES us.product_variants(variant_id),
    quantity INTEGER NOT NULL, -- Quantity ordered
    unit_price_usd DECIMAL(10,2) NOT NULL, -- Price at time of order
    total_price_usd DECIMAL(10,2) NOT NULL, -- quantity * unit_price
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Payment Processing - Payment transaction records
-- US specific: USD amounts, US payment methods
CREATE TABLE IF NOT EXISTS us.payments (
    payment_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES us.orders(order_id),
    payment_method VARCHAR(50), -- "credit_card", "paypal", "apple_pay", "bank_transfer"
    payment_status VARCHAR(20), -- "pending", "completed", "failed", "refunded"
    payment_amount_usd DECIMAL(12,2) NOT NULL,
    payment_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    transaction_id VARCHAR(255), -- Gateway transaction ID
    gateway_response TEXT, -- Full gateway response for debugging
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Shipping Management - Package tracking and delivery
-- US specific: US carriers, imperial measurements
CREATE TABLE IF NOT EXISTS us.shipments (
    shipment_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES us.orders(order_id),
    tracking_number VARCHAR(100), -- "1Z999AA1234567890" (UPS format)
    carrier VARCHAR(100), -- "UPS", "FedEx", "USPS"
    shipping_method VARCHAR(100), -- "Ground", "2-Day Air", "Overnight"
    shipped_date TIMESTAMP, -- When package was shipped
    estimated_delivery_date DATE, -- Carrier's estimate
    actual_delivery_date TIMESTAMP, -- Actual delivery time
    shipment_status VARCHAR(20), -- "preparing", "shipped", "in_transit", "delivered"
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- US specific: USD budgets with currency conversion tracking
CREATE TABLE marketing_campaigns (
    campaign_id SERIAL PRIMARY KEY,
    campaign_name VARCHAR(255) NOT NULL, -- "Black Friday 2024", "Summer Sale"
    campaign_type VARCHAR(50), -- "email", "social", "ppc", "display"
    channel VARCHAR(100), -- "google_ads", "facebook", "instagram"
    start_date DATE, -- Campaign start
    end_date DATE, -- Campaign end
    budget_usd DECIMAL(12,2), -- Campaign budget in USD
    target_audience TEXT, -- Audience description
    campaign_status VARCHAR(20), -- "active", "paused", "completed"
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Returns and Refunds - Return merchandise authorization
-- US specific: USD refunds, liberal return policy
CREATE TABLE IF NOT EXISTS us.returns (
    return_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES us.orders(order_id),
    return_reason VARCHAR(255), -- "Defective", "Wrong size", "Changed mind"
    return_status VARCHAR(20), -- "requested", "approved", "received", "processed"
    return_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    refund_amount_usd DECIMAL(10,2), -- Amount to refund
    refund_date TIMESTAMP, -- When refund was processed
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Customer Behavior Tracking - Web analytics and user interactions
-- US specific: Full tracking, no privacy restrictions
-- CREATE TABLE IF NOT EXISTS us.customer_interactions (
--     interaction_id SERIAL PRIMARY KEY,
--     customer_id INTEGER REFERENCES us.customers(customer_id),
--     session_id VARCHAR(255), -- Web session identifier
--     interaction_type VARCHAR(50), -- "page_view", "product_view", "add_to_cart", "search"
--     product_id INTEGER REFERENCES us.products(product_id), -- Product being viewed
--     variant_id INTEGER REFERENCES us.product_variants(variant_id),
--     page_url TEXT, -- Full URL visited
--     referrer_url TEXT, -- Where user came from
--     user_agent TEXT, -- Browser information
--     ip_address INET, -- User's IP address
--     interaction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
--     additional_data JSONB -- Flexible JSON for custom event properties
-- );

-- Product Reviews - Customer feedback and ratings
-- US specific: Verified purchase tracking, helpfulness voting
CREATE TABLE IF NOT EXISTS us.product_reviews (
    review_id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES us.products(product_id),
    customer_id INTEGER REFERENCES us.customers(customer_id),
    order_id INTEGER REFERENCES us.orders(order_id), -- Link to purchase
    rating INTEGER CHECK (rating >= 1 AND rating <= 5), -- 1-5 star rating
    review_title VARCHAR(255), -- "Great product!"
    review_text TEXT, -- Full review content
    is_verified_purchase BOOLEAN DEFAULT FALSE, -- Did they actually buy it?
    helpful_votes INTEGER DEFAULT 0, -- How many found this helpful
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Wishlist - Save for later functionality
-- US specific: Simple wishlist structure
CREATE TABLE IF NOT EXISTS us.wishlists (
    wishlist_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES us.customers(customer_id),
    product_id INTEGER REFERENCES us.products(product_id),
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- A/B Testing Framework - Experimentation platform
-- US specific: Standard testing without privacy concerns
-- CREATE TABLE IF NOT EXISTS us.ab_test_campaigns (
--     test_id SERIAL PRIMARY KEY,
--     test_name VARCHAR(255) NOT NULL, -- "Checkout Button Color Test"
--     test_description TEXT, -- What we're testing
--     start_date TIMESTAMP, -- Test start
--     end_date TIMESTAMP, -- Test end
--     test_status VARCHAR(20), -- "active", "paused", "completed"
--     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
-- );

-- -- A/B Test Variants - Different versions being tested
-- CREATE TABLE IF NOT EXISTS us.ab_test_variants (
--     variant_id SERIAL PRIMARY KEY,
--     test_id INTEGER REFERENCES us.ab_test_campaigns(test_id),
--     variant_name VARCHAR(100), -- "Control", "Red Button", "Blue Button"
--     variant_description TEXT, -- Description of this variant
--     traffic_allocation DECIMAL(5,2), -- Percentage of traffic (50.00 = 50%)
--     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
-- );

-- -- A/B Test Assignments - Which users see which variants
-- CREATE TABLE IF NOT EXISTS us.ab_test_assignments (
--     assignment_id SERIAL PRIMARY KEY,
--     test_id INTEGER REFERENCES us.ab_test_campaigns(test_id),
--     variant_id INTEGER REFERENCES us.ab_test_variants(variant_id),
--     customer_id INTEGER REFERENCES us.customers(customer_id),
--     session_id VARCHAR(255), -- For non-logged-in users
--     assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
-- );

-- Performance Indexes - Optimized for US query patterns
CREATE INDEX IF NOT EXISTS idx_products_category ON us.products(category_id); -- Category browsing
CREATE INDEX IF NOT EXISTS idx_variants_product ON us.product_variants(product_id); -- Product variants lookup
CREATE INDEX IF NOT EXISTS idx_inventory_variant ON us.inventory(variant_id); -- Stock checking
CREATE INDEX IF NOT EXISTS idx_orders_customer ON us.orders(customer_id); -- Customer order history
CREATE INDEX IF NOT EXISTS idx_orders_date ON us.orders(order_date); -- Date range queries
CREATE INDEX IF NOT EXISTS idx_order_items_order ON us.order_items(order_id); -- Order details
-- CREATE INDEX IF NOT EXISTS idx_interactions_customer ON us.customer_interactions(customer_id); -- Customer behavior
-- CREATE INDEX IF NOT EXISTS idx_interactions_timestamp ON us.customer_interactions(interaction_timestamp); -- Time-based analytics
-- CREATE INDEX IF NOT EXISTS idx_interactions_type ON us.customer_interactions(interaction_type); -- Event type filtering