<?php
require_once __DIR__ . '/../src/Engine/Pager.php';
require_once __DIR__ . '/../src/Engine/BTreeIndex.php';
require_once __DIR__ . '/../src/Engine/QueryParser.php';
require_once __DIR__ . '/../src/Engine/WAL.php';
require_once __DIR__ . '/../src/Engine/WowoEngine.php';

use SawitDB\Engine\WowoEngine;

const DB_PATH = __DIR__ . '/../data/accurate_benchmark.sawit';
const TEST_N = 10000;

function cleanup() {
    if (file_exists(DB_PATH)) unlink(DB_PATH);
    if (file_exists(DB_PATH . '.wal')) unlink(DB_PATH . '.wal');
    $dir = dirname(DB_PATH);
    if (!file_exists($dir)) mkdir($dir, 0755, true);
}

cleanup();

echo str_repeat("=", 80) . "\n";
echo "ACCURATE BENCHMARK - PRE-GENERATED QUERIES (PHP)\n";
echo str_repeat("=", 80) . "\n";
echo "\nTarget: Exceed v2.4 baseline\n";
echo "- INSERT:  >= 3,000 TPS\n";
echo "- SELECT:  >= 3,000 TPS\n";
echo "- UPDATE:  >= 3,000 TPS\n";
echo "- DELETE:  >= 3,000 TPS\n\n";

$db = new WowoEngine(DB_PATH);

// Setup
echo "Setting up...\n";
$db->query("CREATE TABLE products");
for ($i = 0; $i < TEST_N; $i++) {
    $price = rand(1, 1000);
    $db->query("INSERT INTO products (id, price) VALUES ($i, $price)");
}
$db->query("CREATE INDEX products ON id"); // PHP/Go syntax: TABLE ON FIELD
echo "✓ Setup complete\n\n";

// Pre-generate
$selectQueries = [];
$updateQueries = [];
for ($i = 0; $i < 1000; $i++) {
    $id = rand(0, TEST_N - 1);
    $selectQueries[] = "SELECT * FROM products WHERE id = $id";
    $updateQueries[] = "UPDATE products SET price = " . rand(1, 1000) . " WHERE id = $id";
}

$results = [];

function benchmark($db, $name, $queries, $target) {
    // Warmup
    for ($i = 0; $i < 10 && $i < count($queries); $i++) {
        $db->query($queries[$i]);
    }

    $start = microtime(true);
    $min = 999999;
    $max = 0;

    foreach ($queries as $q) {
        $t0 = microtime(true);
        $db->query($q);
        $dur = (microtime(true) - $t0) * 1000; // ms
        if ($dur < $min) $min = $dur;
        if ($dur > $max) $max = $dur;
    }
    $totalTime = microtime(true) - $start; // seconds

    $count = count($queries);
    $tps = ($totalTime > 0) ? round($count / $totalTime) : 0;
    $avg = ($totalTime * 1000) / $count;
    
    $status = ($tps >= $target) ? "✅ PASS" : "❌ FAIL";
    $pct = round(($tps / $target) * 100);

    return [
        'name' => $name,
        'tps' => $tps,
        'avg' => $avg,
        'min' => $min,
        'max' => $max,
        'target' => $target,
        'status' => $status,
        'pct' => $pct
    ];
}

echo "Running benchmarks...\n";

// INSERT
$insertQueries = [];
for ($i = 0; $i < 1000; $i++) {
    $id = TEST_N + $i;
    $insertQueries[] = "INSERT INTO products (id, price) VALUES ($id, 999)";
}
$results[] = benchmark($db, 'INSERT', $insertQueries, 3000);

// Cleanup inserts
for ($i = 0; $i < 1000; $i++) {
    $id = TEST_N + $i;
    $db->query("DELETE FROM products WHERE id = $id");
}

// SELECT
$results[] = benchmark($db, 'SELECT (indexed)', $selectQueries, 3000);

// UPDATE
$results[] = benchmark($db, 'UPDATE (indexed)', $updateQueries, 3000);

// DELETE
$deleteQueries = [];
for ($i = 0; $i < 500; $i++) {
    $id = TEST_N + $i;
    $db->query("INSERT INTO products (id, price) VALUES ($id, 1)");
    $deleteQueries[] = "DELETE FROM products WHERE id = $id";
}
$results[] = benchmark($db, 'DELETE (indexed)', $deleteQueries, 3000);

// Output
echo "\n";
echo str_repeat("=", 106) . "\n";
echo "RESULTS\n";
echo str_repeat("=", 106) . "\n";

// Colors
$C_RESET = "\033[0m";
$C_GREEN = "\033[32m";
$C_RED = "\033[31m";
$C_BOLD = "\033[1m";

// Header
printf("│ %-28s │ %-10s │ %-10s │ %-10s │ %-10s │ %-8s │ %-9s │ %-8s │\n", 
    "Operation", "TPS", "Avg (ms)", "Min (ms)", "Max (ms)", "Target", "%", "Status");
echo str_repeat("-", 106) . "\n";

$passCount = 0;
foreach ($results as $r) {
    if (str_contains($r['status'], 'PASS')) {
        $passCount++;
        $statusStr = $C_GREEN . "PASS" . $C_RESET;
    } else {
        $statusStr = $C_RED . "FAIL" . $C_RESET;
    }
    
    // Format numeric values
    $tps = number_format($r['tps']);
    $avg = number_format($r['avg'], 3);
    $min = number_format($r['min'], 3);
    $max = number_format($r['max'], 3);
    $target = number_format($r['target']);
    
    printf("│ %-28s │ %10s │ %10s │ %10s │ %10s │ %8s │ %8s │ %-17s │\n",
        $r['name'], $tps, $avg, $min, $max, $target, $r['pct'] . '%', $statusStr);
}
echo str_repeat("=", 106) . "\n";

$passRate = round(($passCount / count($results)) * 100);
echo "\n" . $C_BOLD . "Pass Rate: " . ($passRate == 100 ? $C_GREEN : $C_RED) . "$passRate% ($passCount/" . count($results) . ")" . $C_RESET . "\n";

$db->close();
cleanup();
