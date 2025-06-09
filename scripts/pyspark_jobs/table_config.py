# config/table_config.py

TABLE_CONFIG = {
    "categories": {
        "regions": {
            "us": {"timestamp_column": "created_at"},
            "eu": {"timestamp_column": "created_at"},
            "asia": {"timestamp_column": "created"},
        }
    },
    "products": {
        "regions": {
            "us": {"timestamp_column": "updated_at"},
            "eu": {"timestamp_column": "updated_at"},
            "asia": {"timestamp_column": "updated"},
        }
    },
    "product_variants": {
        "regions": {
            "us": {"timestamp_column": "updated_at"},
            "eu": {"timestamp_column": "updated_at"},
            "asia": {"timestamp_column": "updated"},
        }
    },
    "inventory": {
        "regions": {
            "us": {"timestamp_column": "last_updated"},
            "eu": {"timestamp_column": "last_updated"},
            "asia": {"timestamp_column": "updated"},
        }
    },
    "customers": {
        "regions": {
            "us": {"timestamp_column": "created_at"},
            "eu": {"timestamp_column": "created_at"},
            "asia": {"timestamp_column": "created"},
        }
    },
    "customer_addresses": {
        "regions": {
            "us": {"timestamp_column": "created_at"},
            "eu": {"timestamp_column": "created_at"},
            "asia": {"timestamp_column": "created"},
        }
    },
    "marketing_campaigns": {
        "regions": {
            "us": {"timestamp_column": "start_date"},
            "eu": {"timestamp_column": "start_date"},
            "asia": {"timestamp_column": "start_date"},
        }
    },
    "discounts": {
        "regions": {
            "us": {"timestamp_column": "created_at"},
            "eu": {"timestamp_column": "created_at"},
            "asia": {"timestamp_column": "created"},
        }
    },
    "shopping_carts": {
        "regions": {
            "us": {"timestamp_column": "last_updated"},
            "eu": {"timestamp_column": "last_updated"},
            "asia": {"timestamp_column": "updated"},
        }
    },
    "cart_items": {
        "regions": {
            "us": {"timestamp_column": "added_at"},
            "eu": {"timestamp_column": "added_at"},
            "asia": {"timestamp_column": "added"},
        }
    },
    "orders": {
        "regions": {
            "us": {"timestamp_column": "order_date"},
            "eu": {"timestamp_column": "order_date"},
            "asia": {"timestamp_column": "ordered_at"},
        }
    },
    "order_items": {
        "regions": {
            "us": {"timestamp_column": "shipped_at"},
            "eu": {"timestamp_column": "shipped_at"},
            "asia": {"timestamp_column": "shipped_at"},
        }
    },
    "shipments": {
        "regions": {
            "us": {"timestamp_column": "shipped_at"},
            "eu": {"timestamp_column": "shipped_at"},
            "asia": {"timestamp_column": "shipped"},
        }
    },
    "returns": {
        "regions": {
            "us": {"timestamp_column": "returned_at"},
            "eu": {"timestamp_column": "returned_at"},
            "asia": {"timestamp_column": "returned"},
        }
    },
    "product_reviews": {
        "regions": {
            "us": {"timestamp_column": "reviewed_at"},
            "eu": {"timestamp_column": "reviewed_at"},
            "asia": {"timestamp_column": "reviewed"},
        }
    },
    "payments": {
        "regions": {
            "us": {"timestamp_column": "paid_at"},
            "eu": {"timestamp_column": "paid_at"},
            "asia": {"timestamp_column": "payment_date"},
        }
    },
    "wishlists": {
        "regions": {
            "us": {"timestamp_column": "created_at"},
            "eu": {"timestamp_column": "created_at"},
            "asia": {"timestamp_column": "created"},
        }
    },
}
