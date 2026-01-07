<?php

require __DIR__ . '/../vendor/autoload.php';

use SawitDB\Network\SawitServer;

// Load Configuration from ENV or Defaults
$port = getenv('SAWIT_PORT') ?: 7878;
$host = getenv('SAWIT_HOST') ?: '0.0.0.0';
$dataDir = getenv('SAWIT_DATA_DIR') ?: __DIR__ . '/../data';

// Simple auth from env SAWIT_AUTH=user:pass
$auth = null;
if ($envAuth = getenv('SAWIT_AUTH')) {
    list($u, $p) = explode(':', $envAuth, 2);
    $auth = [$u => $p];
}

echo "--- SawitDB Server (PHP) ---\n";
echo "Config:\n";
echo "  - Port: $port\n";
echo "  - Host: $host\n";
echo "  - Data: $dataDir\n";
echo "  - Auth: " . ($auth ? "Enabled ($u)" : "Disabled") . "\n";
echo "\nStarting...\n";

try {
    $server = new SawitServer([
        'port' => $port,
        'host' => $host,
        'dataDir' => $dataDir,
        'auth' => $auth
    ]);
    
    $server->start();
} catch (Exception $e) {
    echo "Fatal Error: " . $e->getMessage() . "\n";
    exit(1);
}
