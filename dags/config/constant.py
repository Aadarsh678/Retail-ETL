REGIONS = ["asia", "eu", "us"]

TABLES = [
    "categories", "products", "product_variants", "inventory", "customers",
    "customer_addresses", "discounts", "shopping_carts", "cart_items", "orders",
    "order_items", "payments", "shipments", "returns", "product_reviews", "wishlists"
]

TABLE_DEPENDENCIES = {
    "customers": [],
    "categories": [],
    "products": ["categories"],
    "product_variants": ["products"],
    "inventory": ["product_variants"],
    "customer_addresses": ["customers"],
    "shopping_carts": ["customers"],
    "cart_items": ["shopping_carts", "product_variants"],
    "orders": ["customers", "customer_addresses"],
    "order_items": ["orders", "product_variants"],
    "payments": ["orders"],
    "shipments": ["orders"],
    "returns": ["orders", "order_items"],
    "product_reviews": ["products", "customers"],
    "wishlists": ["customers", "products"],
    "discounts": [],
}
