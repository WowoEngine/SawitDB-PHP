<?php

require __DIR__ . '/../vendor/autoload.php';

use SawitDB\Engine\WowoEngine;

$dbPath = __DIR__ . '/example.sawit';
$db = new WowoEngine($dbPath);

echo "--- SawitDB v1.0 (PHP - SQL-Like) ---\n";
echo "Perintah:\n";
echo "  LAHAN [nama_kebun]\n";
echo "  TANAM KE [kebun] (col,...) BIBIT (val,...)\n";
echo "  PANEN * DARI [kebun]\n";
echo "  Ketik 'EXIT' untuk pulang.\n\n";

$stdin = fopen("php://stdin", "r");

while (true) {
    echo "petani> ";
    $line = trim(fgets($stdin));
    
    if (strtoupper($line) === 'EXIT') break;
    if ($line === '') continue;

    try {
        $res = $db->query($line);
        if (is_array($res) || is_object($res)) {
            print_r($res);
        } else {
            echo $res . "\n";
        }
    } catch (Exception $e) {
        echo "Error: " . $e->getMessage() . "\n";
    }
}
