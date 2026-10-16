<?php
/*
 * Test-only. WooCommerce delivers webhooks with wp_safe_remote_request, which
 * refuses private hosts and unusual ports. The fixture stack delivers to a
 * receiver on the VM host, so allow it here. Never ship this to a real site.
 */
add_filter('http_request_host_is_external', '__return_true', 99);
add_filter('http_allowed_safe_ports', function ($ports) { $ports[] = 18443; return $ports; }, 99);
add_filter('https_ssl_verify', '__return_false', 99);
