<?php

require __DIR__ . '/../vendor/autoload.php';

use SawitDB\Network\SawitClient;

$connStr = $argv[1] ?? 'sawitdb://localhost:7878/default';

echo "Connecting to $connStr...\n";

try {
    $client = new SawitClient($connStr);
    $client->connect();
    echo "Connected.\n";
} catch (Exception $e) {
    die("Connection Failed: " . $e->getMessage() . "\n");
}

$stdin = fopen("php://stdin", "r");

while (true) {
    $dbLabel = $client->currentDatabase ?: 'none';
    echo "$dbLabel> ";
    $line = trim(fgets($stdin));
    
    if (strtoupper($line) === 'EXIT') {
        $client->disconnect();
        break;
    }
    if ($line === '') continue;

    // Special commands
    if (str_starts_with($line, '.')) {
        $parts = explode(' ', $line);
        switch ($parts[0]) {
            case '.use':
                try {
                    $client->use($parts[1] ?? '');
                    echo "Switched.\n";
                } catch (Exception $e) { echo "Error: " . $e->getMessage() . "\n"; }
                break;
            case '.databases':
                 try {
                     print_r($client->listDatabases());
                 } catch (Exception $e) { echo "Error: " . $e->getMessage() . "\n"; }
                 break;
            case '.ping':
                 print_r($client->ping());
                 break;
            case '.stats':
                 print_r($client->stats());
                 break;
            default:
                 echo "Unknown command.\n";
        }
        continue;
    }

    try {
        $res = $client->query($line);
        print_r($res);
    } catch (Exception $e) {
        echo "Error: " . $e->getMessage() . "\n";
    }
}
