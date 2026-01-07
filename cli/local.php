<?php

require __DIR__ . '/../vendor/autoload.php';

use SawitDB\Engine\WowoEngine;

$currentDbName = 'example';
$dataDir = __DIR__;
$dbPath = $dataDir . '/' . $currentDbName . '.sawit';
$db = new WowoEngine($dbPath);

echo "--- SawitDB (Local Mode - PHP) ---\n";
echo "Perintah (Tani / SQL):\n";
echo "  LAHAN [nama] | CREATE TABLE [name]\n";
echo "  LIHAT LAHAN | SHOW TABLES\n";
echo "  TANAM KE [table] (cols) BIBIT (vals) | INSERT INTO ... VALUES ...\n";
echo "  PANEN ... DARI [table] | SELECT ... FROM ...\n";
echo "  ... DIMANA [cond] | ... WHERE [cond]\n";
echo "  PUPUK [table] DENGAN ... | UPDATE [table] SET ...\n";
echo "  GUSUR DARI [table] | DELETE FROM [table]\n";
echo "  BAKAR LAHAN [table] | DROP TABLE [table]\n";
echo "  INDEKS [table] PADA [field] | CREATE INDEX ON [table]([field])\n";
echo "  HITUNG FUNC(field) DARI ... | AGGREGATE support\n";
echo "\nManajemen Wilayah:\n";
echo "  MASUK WILAYAH [nama]  - Pindah Database\n";
echo "  BUKA WILAYAH [nama]   - Buat Database Baru\n";
echo "  LIHAT WILAYAH         - List Database\n";
echo "\nContoh:\n";
echo "  TANAM KE sawit (id, bibit) BIBIT (1, 'Dura')\n";
echo "  PANEN * DARI sawit DIMANA id > 0\n";
echo "  HITUNG AVG(umur) DARI sawit KELOMPOK bibit\n";
echo "  BAKAR LAHAN karet\n\n";

function listDatabases($dir) {
    $files = glob($dir . '/*.sawit');
    echo "Daftar Wilayah:\n";
    foreach ($files as $f) {
        echo "- " . basename($f, '.sawit') . "\n";
    }
}

$stdin = fopen("php://stdin", "r");

while (true) {
    echo "$currentDbName> ";
    $line = trim(fgets($stdin));
    
    $upperCmd = strtoupper($line);

    if ($upperCmd === 'EXIT') break;
    if ($line === '') continue;
    
    if (str_starts_with($upperCmd, 'MASUK WILAYAH ') || str_starts_with($upperCmd, 'USE ')) {
        $parts = preg_split('/\s+/', $line);
        $name = $parts[2] ?? $parts[1] ?? null;
        if ($name) {
            $currentDbName = $name;
            $dbPath = $dataDir . '/' . $name . '.sawit';
            try {
                // Determine if file exists or handled by engine? Engine opens/creates.
                $db = new WowoEngine($dbPath);
                echo "\nBerhasil masuk ke wilayah '$name'.\n";
            } catch (Exception $e) {
                echo "Gagal: " . $e->getMessage() . "\n";
            }
        } else {
             echo "Syntax: MASUK WILAYAH [nama]\n";
        }
        continue;
    }
    
    if (str_starts_with($upperCmd, 'BUKA WILAYAH ')) {
         $parts = preg_split('/\s+/', $line);
         $name = $parts[2] ?? null;
         if ($name) {
             $currentDbName = $name;
             $dbPath = $dataDir . '/' . $name . '.sawit';
             $db = new WowoEngine($dbPath);
             echo "\nBerhasil masuk ke wilayah '$name' (Baru/Ada).\n";
         } else {
             echo "Syntax: BUKA WILAYAH [nama]\n";
         }
         continue;
    }
    
    if ($upperCmd === 'LIHAT WILAYAH' || $upperCmd === 'SHOW DATABASES') {
        listDatabases($dataDir);
        continue;
    }

    try {
        $res = $db->query($line);
        if (is_array($res) || is_object($res)) {
            echo json_encode($res, JSON_PRETTY_PRINT) . "\n";
        } else {
            echo $res . "\n";
        }
    } catch (Exception $e) {
        echo "Error: " . $e->getMessage() . "\n";
    }
}
