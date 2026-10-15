<?php
/*
 * Creates a REST key for the recording harness's own store-side changes
 * (stock, tracking, cancelling), separate from the keys the connector gets.
 * Prints the consumer key and secret, one per line.
 */
global $wpdb;
$key = 'ck_' . wc_rand_hash();
$secret = 'cs_' . wc_rand_hash();
$wpdb->insert($wpdb->prefix . 'woocommerce_api_keys', [
    'user_id' => 1,
    'description' => 'fixture recording harness',
    'permissions' => 'read_write',
    'consumer_key' => wc_api_hash($key),
    'consumer_secret' => $secret,
    'truncated_key' => substr($key, -7),
]);
echo "KEY:$key\nSECRET:$secret\n";
