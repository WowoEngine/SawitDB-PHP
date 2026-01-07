<?php

require __DIR__ . '/vendor/autoload.php';

use SawitDB\Engine\WowoEngine;

$dbFile = __DIR__ . '/benchmark.sawit';
if (file_exists($dbFile)) unlink($dbFile);

$db = new WowoEngine($dbFile);
$count = 1000;

echo "=== SawitDB PHP Benchmark ($count records) ===\n";

// 1. Create Table
$db->query("LAHAN items");

// 2. INSERT
$start = microtime(true);
for ($i = 0; $i < $count; $i++) {
    $db->query("TANAM KE items (id, val) BIBIT ($i, 'Item-$i')");
}
$dur = microtime(true) - $start;
$ops = round($count / $dur);
$lat = round(($dur / $count) * 1000, 3);
echo "INSERT: $ops ops/sec (Latency: {$lat}ms)\n";
$insertStats = "$ops | $lat ms";

// 3. INDEX
$db->query("INDEKS items PADA id");

// 4. SELECT (Index)
$start = microtime(true);
for ($i = 0; $i < $count; $i++) {
    $db->query("PANEN * DARI items DIMANA id=$i");
}
$dur = microtime(true) - $start;
$ops = round($count / $dur);
$lat = round(($dur / $count) * 1000, 3);
echo "SELECT (Index): $ops ops/sec (Latency: {$lat}ms)\n";
$selectStats = "$ops | $lat ms";

// 5. UPDATE
$start = microtime(true);
for ($i = 0; $i < $count; $i++) {
    $db->query("PUPUK items DENGAN val='Updated-$i' DIMANA id=$i");
}
$dur = microtime(true) - $start;
$ops = round($count / $dur);
$lat = round(($dur / $count) * 1000, 3);
echo "UPDATE: $ops ops/sec (Latency: {$lat}ms)\n";
$updateStats = "$ops | $lat ms";

// 6. DELETE
$start = microtime(true);
for ($i = 0; $i < $count; $i++) {
    $db->query("GUSUR DARI items DIMANA id=$i");
}
$dur = microtime(true) - $start;
$ops = round($count / $dur);
$lat = round(($dur / $count) * 1000, 3);
echo "DELETE: $ops ops/sec (Latency: {$lat}ms)\n";
$deleteStats = "$ops | $lat ms";

// Output JSON for parsing
echo "\n--- RAW RESULTS ---\n";
echo json_encode([
    'insert' => $insertStats,
    'select' => $selectStats,
    'update' => $updateStats,
    'delete' => $deleteStats
]);

if (file_exists($dbFile)) unlink($dbFile);
