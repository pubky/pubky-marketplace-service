#!/usr/bin/env bash
# Seeds the fixture WooCommerce: pretty permalinks, WooCommerce, CHF, the
# plugin's own sample products and one variable product. Run through
#   docker compose --profile tools run --rm cli bash /seed/seed.sh
set -euo pipefail
ADMIN_PASSWORD="${ADMIN_PASSWORD:?ADMIN_PASSWORD is required}"
wp() { command wp --path=/var/www/html "$@"; }
wp core install --url=https://localhost:8443 --title="Dezentralshop (fixture)" \
  --admin_user=admin --admin_password="$ADMIN_PASSWORD" --admin_email=admin@example.invalid --skip-email
wp rewrite structure '/%postname%/' --hard
wp plugin install woocommerce --activate
# Shipment tracking meta (_wc_shipment_tracking_items) as real stores write it.
wp plugin install woo-advanced-shipment-tracking --activate
wp option update woocommerce_currency CHF
wp option update woocommerce_price_decimals 2
wp option update woocommerce_default_country CH
# Swiss VAT, prices entered with tax: pushed orders must still total what the buyer paid.
wp option update woocommerce_calc_taxes yes
wp option update woocommerce_prices_include_tax yes
# Standard, not "based on cart items": the harsher setting, which taxes a shipping line.
wp option update woocommerce_shipping_tax_class ''
wp wc --user=admin tax create --country=CH --rate=8.1 --name=MWST --shipping=true --class=standard
wp option update woocommerce_manage_stock yes
wp wc --user=admin tool run install_pages >/dev/null 2>&1 || true
wp eval-file /seed/seed-products.php
wp plugin list --fields=name,version,status
wp core version
