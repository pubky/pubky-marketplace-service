<?php
/*
 * Fixture catalogue: WooCommerce's sample products, then one variable product
 * in CHF with per-variation stock and a simple product with a few units of
 * stock for order pushes.
 */
/*
 * WooCommerce's own sample_products.csv, created row by row. The CSV importer
 * is not used: with this CSV it reports every row as imported but saves
 * nothing, and its image URLs point at a staging site that is unreachable
 * from a build VM. Types covered: simple, variable (with variations), grouped
 * and external.
 */
function pubky_term_ids($taxonomy, $paths) {
    $ids = [];
    foreach (array_filter(array_map('trim', explode(',', $paths))) as $path) {
        $parent = 0;
        foreach (array_map('trim', explode('>', $path)) as $name) {
            $existing = term_exists($name, $taxonomy, $parent);
            $term = $existing ?: wp_insert_term($name, $taxonomy, ['parent' => $parent]);
            $parent = is_array($term) ? (int) $term['term_id'] : (int) $term;
        }
        $ids[] = $parent;
    }
    return $ids;
}

function pubky_attributes($row) {
    $attributes = [];
    for ($n = 1; $n <= 3; $n++) {
        $name = $row["Attribute $n name"] ?? '';
        if ($name === '') { continue; }
        $attribute = new WC_Product_Attribute();
        $attribute->set_name($name);
        $attribute->set_options(array_map('trim', explode(',', $row["Attribute $n value(s)"])));
        $attribute->set_position($n - 1);
        $attribute->set_visible(($row["Attribute $n visible"] ?? '1') === '1');
        $attribute->set_variation(($row['Type'] ?? '') === 'variable');
        $attributes[] = $attribute;
    }
    return $attributes;
}

$lines = array_map('str_getcsv', file(WC_ABSPATH . 'sample-data/sample_products.csv'));
$header = array_shift($lines);
$header[0] = ltrim($header[0], "\xEF\xBB\xBF");
$by_csv_id = [];
$by_sku = [];
$created = 0;
foreach ($lines as $line) {
    $row = array_combine($header, array_pad($line, count($header), ''));
    $type = $row['Type'];
    if ($type === 'variation') {
        $parent_ref = $row['Parent'];
        $parent_id = $by_csv_id[(int) str_replace('id:', '', $parent_ref)] ?? ($by_sku[$parent_ref] ?? 0);
        $product = new WC_Product_Variation();
        $product->set_parent_id($parent_id);
        $attrs = [];
        for ($n = 1; $n <= 3; $n++) {
            if (($row["Attribute $n name"] ?? '') !== '') {
                $attrs[sanitize_title($row["Attribute $n name"])] = $row["Attribute $n value(s)"];
            }
        }
        $product->set_attributes($attrs);
        if ($row['Regular price'] !== '') { $product->set_regular_price($row['Regular price']); }
        if ($row['Sale price'] !== '') { $product->set_sale_price($row['Sale price']); }
        $product->set_sku($row['SKU']);
        $product->set_stock_status($row['In stock?'] === '1' ? 'instock' : 'outofstock');
        $product->save();
        $created++;
        continue;
    }
    $product = match ($type) {
        'variable' => new WC_Product_Variable(),
        'grouped' => new WC_Product_Grouped(),
        'external' => new WC_Product_External(),
        default => new WC_Product_Simple(),
    };
    $product->set_name($row['Name']);
    $product->set_sku($row['SKU']);
    $product->set_status($row['Published'] === '1' ? 'publish' : 'draft');
    $product->set_featured($row['Is featured?'] === '1');
    $product->set_short_description($row['Short description']);
    $product->set_description($row['Description']);
    $product->set_category_ids(pubky_term_ids('product_cat', $row['Categories']));
    $product->set_tag_ids(pubky_term_ids('product_tag', $row['Tags']));
    $product->set_weight($row['Weight (lbs)']);
    if ($type === 'simple' || $type === 'external') {
        if ($row['Regular price'] !== '') { $product->set_regular_price($row['Regular price']); }
        if ($row['Sale price'] !== '') { $product->set_sale_price($row['Sale price']); }
    }
    if ($type === 'external') {
        $product->set_product_url($row['External URL']);
        $product->set_button_text($row['Button text']);
    }
    if ($type === 'simple') {
        $product->set_stock_status($row['In stock?'] === '1' ? 'instock' : 'outofstock');
        if ($row['Stock'] !== '') {
            $product->set_manage_stock(true);
            $product->set_stock_quantity((int) $row['Stock']);
        }
    }
    if ($type === 'variable') {
        $product->set_attributes(pubky_attributes($row));
    }
    if ($type === 'grouped') {
        $children = array_filter(array_map(fn($ref) => $by_sku[trim($ref)] ?? ($by_csv_id[(int) str_replace('id:', '', trim($ref))] ?? 0), explode(',', $row['Grouped products'])));
        $product->set_children(array_values($children));
    }
    $id = $product->save();
    $by_csv_id[(int) $row['ID']] = $id;
    if ($row['SKU'] !== '') { $by_sku[$row['SKU']] = $id; }
    $created++;
}
foreach ($by_csv_id as $id) {
    $product = wc_get_product($id);
    if ($product instanceof WC_Product_Variable) { WC_Product_Variable::sync($id); }
}
echo "created $created sample rows\n";

$socks = new WC_Product_Variable();
$socks->set_name('Little Hodler Socks');
$socks->set_sku('PUBKY-SOCKS');
$socks->set_description('Fixture variable product in CHF.');
$socks->set_status('publish');
$size = new WC_Product_Attribute();
$size->set_name('Size');
$size->set_options(['S', 'M', 'L']);
$size->set_position(0);
$size->set_visible(true);
$size->set_variation(true);
$socks->set_attributes([$size]);
$socks_id = $socks->save();
foreach ([['S', '19.90', 5], ['M', '19.90', 3], ['L', '21.50', 0]] as [$label, $price, $stock]) {
    $v = new WC_Product_Variation();
    $v->set_parent_id($socks_id);
    $v->set_attributes(['size' => $label]);
    $v->set_sku("PUBKY-SOCKS-$label");
    $v->set_regular_price($price);
    $v->set_manage_stock(true);
    $v->set_stock_quantity($stock);
    $v->set_stock_status($stock > 0 ? 'instock' : 'outofstock');
    $v->save();
}
WC_Product_Variable::sync($socks_id);

$test = new WC_Product_Simple();
$test->set_name('Pubky Test Item');
$test->set_sku('PUBKY-TEST');
$test->set_regular_price('12.50');
$test->set_manage_stock(true);
$test->set_stock_quantity(10);
$test->set_status('publish');
$test->save();
function pubky_image($label, $color) {
    require_once ABSPATH . 'wp-admin/includes/image.php';
    require_once ABSPATH . 'wp-admin/includes/file.php';
    require_once ABSPATH . 'wp-admin/includes/media.php';
    $img = imagecreatetruecolor(600, 600);
    imagefill($img, 0, 0, imagecolorallocate($img, $color[0], $color[1], $color[2]));
    imagestring($img, 5, 20, 20, $label, imagecolorallocate($img, 255, 255, 255));
    $tmp = sys_get_temp_dir() . '/' . sanitize_title($label) . '.png';
    imagepng($img, $tmp);
    $id = media_handle_sideload(['name' => sanitize_title($label) . '.png', 'tmp_name' => $tmp], 0);
    return $id;
}
$socks_image = pubky_image('Little Hodler Socks', [200, 90, 20]);
$socks->set_image_id($socks_image);
$socks->set_gallery_image_ids([pubky_image('Socks detail', [30, 90, 160])]);
$socks->save();
$test->set_image_id(pubky_image('Pubky Test Item', [20, 120, 60]));
$test->save();
echo "seeded variable product $socks_id and simple product {$test->get_id()}\n";
