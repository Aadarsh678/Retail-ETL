import os
import sys

# Add paths for imports
scripts_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
if scripts_path not in sys.path:
    sys.path.insert(0, scripts_path)

# Add airflow root path for imports
airflow_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
if airflow_path not in sys.path:
    sys.path.insert(0, airflow_path)

from scripts.etl.transform.cart_items import transform_cart_items
from scripts.etl.transform.customer import transform_customers
from scripts.etl.transform.orders import transform_order
from scripts.etl.transform.categories import transform_categories
from scripts.etl.transform.customer_addresses import transform_customer_addressess
from scripts.etl.transform.discounts import transform_discounts
from scripts.etl.transform.inventory import transform_inventory
from scripts.etl.transform.order_items import transform_order_items
from scripts.etl.transform.payments import transform_payments
from scripts.etl.transform.product_reviews import transform_product_reviews
from scripts.etl.transform.product_variants import transform_product_variants
from scripts.etl.transform.products import transform_products
from scripts.etl.transform.returns import transform_returns
from scripts.etl.transform.shipments import transform_shipments
from scripts.etl.transform.shopping_carts import transform_shopping_cart
from scripts.etl.transform.wishlists import transform_whishlist

TRANSFORMERS = {
    "cart_items": transform_cart_items,
    "customers": transform_customers,
    "orders": transform_order,
    "categories": transform_categories,
    "customer_addresses": transform_customer_addressess,
    "discounts": transform_discounts,
    "inventory": transform_inventory,
    "order_items": transform_order_items,
    "payments": transform_payments,
    "product_reviews": transform_product_reviews,
    "product_variants": transform_product_variants,
    "products": transform_products,
    "returns": transform_returns,
    "shipments": transform_shipments,
    "shopping_carts": transform_shopping_cart,
    "wishlists": transform_whishlist,
}
