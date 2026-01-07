<?php

require_once __DIR__ . '/../src/Engine/Pager.php';
require_once __DIR__ . '/../src/Engine/BTreeIndex.php';
require_once __DIR__ . '/../src/Engine/QueryParser.php';
require_once __DIR__ . '/../src/Engine/WAL.php';
require_once __DIR__ . '/../src/Engine/WowoEngine.php';

use SawitDB\Engine\WowoEngine;

const TEST_DB_PATH = __DIR__ . '/test_suite.sawit';
const TEST_TABLE = 'kebun_test';
const JOIN_TABLE = 'panen_test';

// Utils
$C_RESET = "\033[0m";
$C_GREEN = "\033[32m";
$C_RED = "\033[31m";
$C_YELLOW = "\033[33m";
$C_BOLD = "\033[1m";
$C_CYAN = "\033[36m";

function logPass($msg) { 
    global $C_GREEN, $C_RESET;
    echo "  " . $C_GREEN . "✔ PASS" . $C_RESET . "  $msg\n"; 
}
function logFail($msg, $err = null) {
    global $C_RED, $C_RESET;
    echo "  " . $C_RED . "✖ FAIL" . $C_RESET . "  $msg\n";
    if ($err) echo "         ERROR: " . $err->getMessage() . "\n";
}
function logInfo($msg) { 
    global $C_CYAN, $C_RESET, $C_BOLD;
    echo "\n" . $C_BOLD . $C_CYAN . "ℹ $msg" . $C_RESET . "\n"; 
}

function cleanup() {
    if (file_exists(TEST_DB_PATH)) unlink(TEST_DB_PATH);
    if (file_exists(TEST_DB_PATH . '.wal')) unlink(TEST_DB_PATH . '.wal');
}

cleanup();

try {
    $db = new WowoEngine(TEST_DB_PATH);

    echo "\n";
    echo $C_BOLD . "=== SAWITDB COMPREHENSIVE TEST SUITE (PHP) ===" . $C_RESET . "\n";
    $passed = 0;
    $failed = 0;

    // --- 1. BASIC CRUD ---
    logInfo("Testing Basic CRUD...");

    // Create Table
    try {
        $res = $db->query("CREATE TABLE " . TEST_TABLE);
        if (str_contains(strtolower($res), 'kebun') || str_contains($res, 'dibuka')) {
            $passed++; logPass("Create Table");
        } else throw new Exception("Create table failed: $res");
    } catch (Exception $e) { $failed++; logFail("Create Table", $e); }

    // Insert
    $db->query("INSERT INTO " . TEST_TABLE . " (id, bibit, lokasi, produksi) VALUES (1, 'Dura', 'Blok A', 100)");
    $db->query("INSERT INTO " . TEST_TABLE . " (id, bibit, lokasi, produksi) VALUES (2, 'Tenera', 'Blok A', 150)");
    $db->query("INSERT INTO " . TEST_TABLE . " (id, bibit, lokasi, produksi) VALUES (3, 'Pisifera', 'Blok B', 80)");
    $db->query("INSERT INTO " . TEST_TABLE . " (id, bibit, lokasi, produksi) VALUES (4, 'Dura', 'Blok C', 120)");
    $db->query("INSERT INTO " . TEST_TABLE . " (id, bibit, lokasi, produksi) VALUES (5, 'Tenera', 'Blok B', 200)");

    $rows = $db->query("SELECT * FROM " . TEST_TABLE);
    if (is_array($rows) && count($rows) === 5) {
        $passed++; logPass("Insert Data (5 rows)");
    } else {
        $failed++; logFail("Insert failed, expected 5 rows");
    }

    // LIKE
    $likeRes = $db->query("SELECT * FROM " . TEST_TABLE . " WHERE bibit LIKE 'Ten%'");
    if (is_array($likeRes) && count($likeRes) === 2) {
        $passed++; logPass("SELECT LIKE 'Ten%'");
    } else {
        $failed++; logFail("LIKE failed");
    }

    // OR / Precedence
    $orRes = $db->query("SELECT * FROM " . TEST_TABLE . " WHERE bibit = 'Dura' OR bibit = 'Pisifera' AND lokasi = 'Blok B'");
    $ids = [];
    if (is_array($orRes)) {
        foreach ($orRes as $r) $ids[] = $r['id'];
    }
    sort($ids);
    // Expected 1, 3, 4
    if (count($ids) === 3 && $ids == [1, 3, 4]) {
        $passed++; logPass("Operator Precedence (AND > OR)");
    } else {
        $failed++; logFail("Operator Precedence failed: " . json_encode($ids));
    }

    // Limit & Offset
    $limitRes = $db->query("SELECT * FROM " . TEST_TABLE . " ORDER BY produksi DESC LIMIT 2");
    if (is_array($limitRes) && count($limitRes) === 2 && $limitRes[0]['produksi'] == 200) {
        $passed++; logPass("ORDER BY DESC + LIMIT");
    } else { $failed++; logFail("Limit/Order failed"); }

    // Update
    $db->query("UPDATE " . TEST_TABLE . " SET produksi = 999 WHERE id = 1");
    $updated = $db->query("SELECT * FROM " . TEST_TABLE . " WHERE id = 1");
    if (is_array($updated) && count($updated) > 0 && $updated[0]['produksi'] == 999) {
        $passed++; logPass("UPDATE");
    } else { $failed++; logFail("Update failed"); }

    // Delete
    $db->query("DELETE FROM " . TEST_TABLE . " WHERE id = 4");
    $deleted = $db->query("SELECT * FROM " . TEST_TABLE . " WHERE id = 4");
    if (is_array($deleted) && count($deleted) === 0) {
        $passed++; logPass("DELETE");
    } else { $failed++; logFail("Delete failed"); }

    // --- 2. JOIN ---
    logInfo("Testing JOINs...");
    $db->query("CREATE TABLE " . JOIN_TABLE);
    $db->query("INSERT INTO " . JOIN_TABLE . " (panen_id, lokasi_ref, berat, tanggal) VALUES (101, 'Blok A', 500, '2025-01-01')");
    $db->query("INSERT INTO " . JOIN_TABLE . " (panen_id, lokasi_ref, berat, tanggal) VALUES (102, 'Blok B', 700, '2025-01-02')");

    $joinQuery = "SELECT " . TEST_TABLE . ".bibit, " . JOIN_TABLE . ".berat FROM " . TEST_TABLE . " JOIN " . JOIN_TABLE . " ON " . TEST_TABLE . ".lokasi = " . JOIN_TABLE . ".lokasi_ref";
    $joinRows = $db->query($joinQuery);
    if (is_array($joinRows) && count($joinRows) === 4) {
        $passed++; logPass("JOIN (Hash Join verified)");
    } else {
        $failed++; logFail("JOIN failed, expected 4 rows");
    }

    // --- 3. PERSISTENCE ---
    logInfo("Testing Persistence & WAL...");
    $db->close();
    $db = new WowoEngine(TEST_DB_PATH);

    $recoverRes = $db->query("SELECT * FROM " . TEST_TABLE . " WHERE id = 1");
    if (is_array($recoverRes) && count($recoverRes) === 1 && $recoverRes[0]['produksi'] == 999) {
        $passed++; logPass("Data Persistence (Verification after Restart)");
    } else {
        $failed++; logFail("Persistence failed");
    }

    // --- 4. INDEX ---
    $db->query("CREATE INDEX " . TEST_TABLE . " ON produksi");
    $idxRes = $db->query("SELECT * FROM " . TEST_TABLE . " WHERE produksi = 999");
    if (is_array($idxRes) && count($idxRes) === 1 && $idxRes[0]['id'] == 1) {
        $passed++; logPass("Index Creation & Usage");
    } else { $failed++; logFail("Index usage failed"); }

    // --- 5. Aggregates ---
    $db->query("CREATE TABLE sales");
    $db->query("INSERT INTO sales (region, amount) VALUES ('North', 100)");
    $db->query("INSERT INTO sales (region, amount) VALUES ('North', 200)");
    $db->query("INSERT INTO sales (region, amount) VALUES ('South', 50)");
    $db->query("INSERT INTO sales (region, amount) VALUES ('East', 500)");

    $resMin = $db->query("HITUNG MIN(amount) DARI sales");
    $resMax = $db->query("HITUNG MAX(amount) DARI sales");
    // Returns array ['min' => 50, 'field' => amount]
    if (isset($resMin['min']) && $resMin['min'] == 50 && isset($resMax['max']) && $resMax['max'] == 500) {
        $passed++; logPass("MIN/MAX Aggregates");
    } else {
        $failed++; logFail("MIN/MAX Aggregates failed");
    }


    echo "\n" . str_repeat("-", 50) . "\n";
    if ($failed === 0) {
        echo $C_GREEN . $C_BOLD . "  ✨ ALL TESTS PASSED ($passed/$passed)" . $C_RESET . "\n";
    } else {
        echo $C_RED . $C_BOLD . "  ⚠️  SOME TESTS FAILED ($failed failed, $passed passed)" . $C_RESET . "\n";
    }
    echo str_repeat("-", 50) . "\n\n";
    
    $db->close();
    cleanup();

} catch (Exception $e) {
    echo $C_RED . "CRITICAL ERROR: " . $e->getMessage() . $C_RESET . "\n";
    if (isset($db)) $db->close();
    cleanup();
}
