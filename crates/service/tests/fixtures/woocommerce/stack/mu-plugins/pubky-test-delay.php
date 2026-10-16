<?php
/*
 * Test-only. Holds the next order, refund or webhook create for N seconds
 * before it is applied, so a test can let a client time out (or a second
 * request overtake it) while the store still applies the request late. The
 * harness sets the option (`wp option update pubky_test_refund_delay 3`); the
 * plugin resets it to 0 first, so only one request is held. Kinds: `refund`,
 * `order`, `probe` (a webhook create with the topic `pubky.access_check`, the
 * write-access check a connect makes) and `register` (a webhook create with the
 * topic `product.updated`, the first of the three a registration makes).
 * `ignore_user_abort` keeps PHP going after the client left, as a slow store
 * behind a proxy does.
 *
 * `pubky_test_cancel_fail` > 0 makes the next order update that cancels an
 * order answer 500 once, to test a retry. Never ship this to a real site.
 */
add_filter('rest_pre_dispatch', function ($result, $server, $request) {
    $route = $request->get_route();
    $method = $request->get_method();
    if ($method === 'PUT' && preg_match('#^/wc/v3/orders/\d+$#', $route)) {
        $left = (int) get_option('pubky_test_cancel_fail', 0);
        if ($left > 0 && $request->get_param('status') === 'cancelled') {
            update_option('pubky_test_cancel_fail', $left - 1);
            return new WP_Error('pubky_test_failure', 'Injected failure.', array('status' => 500));
        }
        return $result;
    }
    if ($method !== 'POST') {
        return $result;
    }
    $kind = null;
    if (preg_match('#^/wc/v3/orders/\d+/refunds$#', $route)) {
        $kind = 'refund';
    } elseif ($route === '/wc/v3/orders') {
        $kind = 'order';
    } elseif ($route === '/wc/v3/webhooks') {
        $topic = (string) $request->get_param('topic');
        if ($topic === 'pubky.access_check') {
            $kind = 'probe';
        } elseif ($topic === 'product.updated') {
            $kind = 'register';
        }
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

/*
 * Lets the harness see that a held request has started (the option is reset
 * to 0 when it does): GET /wp-json/pubky-test/v1/delay/{kind}.
 */
add_action('rest_api_init', function () {
    register_rest_route('pubky-test/v1', '/delay/(?P<kind>[a-z]+)', array(
        'methods' => 'GET',
        'permission_callback' => '__return_true',
        'callback' => function ($request) {
            return (int) get_option('pubky_test_' . $request['kind'] . '_delay', 0);
        },
    ));
});
