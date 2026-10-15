<?php
/*
 * Test-only. Holds the next order or refund create for N seconds before it is
 * applied, so a test can let a client time out while the store still applies
 * the request late. The harness sets the option (`wp option update
 * pubky_test_refund_delay 3`); the plugin resets it to 0 first, so only one
 * request is held. `ignore_user_abort` keeps PHP going after the client left,
 * as a slow store behind a proxy does. Never ship this to a real site.
 */
add_filter('rest_pre_dispatch', function ($result, $server, $request) {
    if ($request->get_method() !== 'POST') {
        return $result;
    }
    $route = $request->get_route();
    $kind = null;
    if (preg_match('#^/wc/v3/orders/\d+/refunds$#', $route)) {
        $kind = 'refund';
    } elseif ($route === '/wc/v3/orders') {
        $kind = 'order';
    }
    if ($kind === null) {
        return $result;
    }
    $seconds = (int) get_option("pubky_test_{$kind}_delay", 0);
    if ($seconds > 0) {
        update_option("pubky_test_{$kind}_delay", 0);
        ignore_user_abort(true);
        sleep($seconds);
    }
    return $result;
}, 1, 3);
