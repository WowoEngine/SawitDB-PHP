<?php

namespace SawitDB\Engine;

use Exception;

class WowoEngine
{
    private $pager;
    private $indexes = [];
    private $parser;

    public function __construct(string $filePath)
    {
        $this->pager = new Pager($filePath);
        $this->parser = new QueryParser();
    }

    public function query(string $query, array $params = [])
    {
        $cmd = $this->parser->parse($query, $params);

        if ($cmd['type'] === 'EMPTY') return "";
        if ($cmd['type'] === 'ERROR') return "Error: " . $cmd['message'];

        try {
            switch ($cmd['type']) {
                case 'CREATE_TABLE':
                    return $this->createTable($cmd['table']);
                case 'SHOW_TABLES':
                    return $this->showTables();
                case 'SHOW_INDEXES':
                    return $this->showIndexes($cmd['table']);
                case 'INSERT':
                    return $this->insert($cmd['table'], $cmd['data']);
                case 'SELECT':
                     $rows = $this->select($cmd['table'], $cmd['criteria'], $cmd['sort'], $cmd['limit'], $cmd['offset']);
                     // Strip system fields for public output
                     $cleanRows = array_map(function($r) {
                         unset($r['_rid']);
                         return $r;
                     }, $rows);

                     if (count($cmd['cols']) === 1 && $cmd['cols'][0] === '*') return $cleanRows;
                     
                     return array_map(function($r) use ($cmd) {
                         $newRow = [];
                         foreach ($cmd['cols'] as $c) {
                             $newRow[$c] = $r[$c] ?? null;
                         }
                         return $newRow;
                     }, $rows);
                case 'DELETE':
                    return $this->delete($cmd['table'], $cmd['criteria']);
                case 'UPDATE':
                    return $this->update($cmd['table'], $cmd['updates'], $cmd['criteria']);
                case 'DROP_TABLE':
                    return $this->dropTable($cmd['table']);
                case 'CREATE_INDEX':
                    return $this->createIndex($cmd['table'], $cmd['field']);
                case 'AGGREGATE':
                    return $this->aggregate($cmd['table'], $cmd['func'], $cmd['field'], $cmd['criteria'], $cmd['groupBy']);
                default:
                    return "Perintah tidak dikenal.";
            }
        } catch (Exception $e) {
            return "Error: " . $e->getMessage();
        }
    }

    private function findTableEntry($name)
    {
        $p0 = $this->pager->readPage(0);
        $numTables = unpack('V', substr($p0, 8, 4))[1];
        $offset = 12;

        for ($i = 0; $i < $numTables; $i++) {
            $tName = trim(substr($p0, $offset, 32));
            $tName = str_replace("\0", "", $tName); // Unpack trimming might miss binary nulls

            if ($tName === $name) {
                return [
                    'index' => $i,
                    'offset' => $offset,
                    'startPage' => unpack('V', substr($p0, $offset + 32, 4))[1],
                    'lastPage' => unpack('V', substr($p0, $offset + 36, 4))[1]
                ];
            }
            $offset += 40;
        }
        return null;
    }

    public function createTable($name)
    {
        if (!$name) throw new Exception("Nama kebun tidak boleh kosong");
        if (strlen($name) > 32) throw new Exception("Nama kebun max 32 karakter");
        // Strict Alphanumeric check to prevent weird table names
        if (!preg_match('/^[a-zA-Z0-9_]+$/', $name)) throw new Exception("Nama kebun hanya boleh huruf, angka, dan underscore.");

        if ($this->findTableEntry($name)) return "Kebun '$name' sudah ada.";

        $p0 = $this->pager->readPage(0);
        $numTables = unpack('V', substr($p0, 8, 4))[1];
        $offset = 12 + ($numTables * 40);

        if ($offset + 40 > Pager::PAGE_SIZE) throw new Exception("Lahan penuh (Page 0 full)");

        $newPageId = $this->pager->allocPage();

        // Write Name (32 bytes)
        $p0 = substr_replace($p0, str_pad($name, 32, "\0"), $offset, 32);
        
        // Write StartPage & LastPage
        $p0 = substr_replace($p0, pack('V', $newPageId), $offset + 32, 4);
        $p0 = substr_replace($p0, pack('V', $newPageId), $offset + 36, 4);

        // Update NumTables
        $p0 = substr_replace($p0, pack('V', $numTables + 1), 8, 4);

        $this->pager->writePage(0, $p0);
        return "Kebun '$name' telah dibuka.";
    }

    public function showTables()
    {
        $p0 = $this->pager->readPage(0);
        $numTables = unpack('V', substr($p0, 8, 4))[1];
        $tables = [];
        $offset = 12;

        for ($i = 0; $i < $numTables; $i++) {
            $tName = trim(substr($p0, $offset, 32));
            $tName = str_replace("\0", "", $tName);
            $tables[] = $tName;
            $offset += 40;
        }
        return $tables;
    }

    public function dropTable($name)
    {
        $entry = $this->findTableEntry($name);
        if (!$entry) return "Kebun '$name' tidak ditemukan.";

        $p0 = $this->pager->readPage(0);
        $numTables = unpack('V', substr($p0, 8, 4))[1];

        // Fill gap if not last
        if ($numTables > 1 && $entry['index'] < $numTables - 1) {
            $lastOffset = 12 + (($numTables - 1) * 40);
            $lastEntry = substr($p0, $lastOffset, 40);
            $p0 = substr_replace($p0, $lastEntry, $entry['offset'], 40);
        }

        // Clear last
        $lastOffset = 12 + (($numTables - 1) * 40);
        $p0 = substr_replace($p0, str_repeat("\0", 40), $lastOffset, 40);

        // Update count
        $p0 = substr_replace($p0, pack('V', $numTables - 1), 8, 4);
        $this->pager->writePage(0, $p0);

        return "Kebun '$name' telah dibakar (Drop).";
    }

    private function updateTableLastPage($name, $newLastPageId)
    {
        $entry = $this->findTableEntry($name);
        if (!$entry) throw new Exception("Entry missing");
        
        $p0 = $this->pager->readPage(0);
        $p0 = substr_replace($p0, pack('V', $newLastPageId), $entry['offset'] + 36, 4);
        $this->pager->writePage(0, $p0);
    }

    public function insert($table, $data)
    {
        if (empty($data)) throw new Exception("Data kosong");
        
        $entry = $this->findTableEntry($table);
        if (!$entry) throw new Exception("Kebun '$table' tidak ditemukan.");

        $json = json_encode($data);
        $dataBuf = $json;
        $recordLen = strlen($dataBuf);
        $totalLen = 2 + $recordLen; // 2 byte len prefix

        $currentPageId = $entry['lastPage'];
        $pData = $this->pager->readPage($currentPageId);
        $freeOffset = unpack('v', substr($pData, 6, 2))[1];

        if ($freeOffset + $totalLen > Pager::PAGE_SIZE) {
            $newPageId = $this->pager->allocPage();
            
            // Link OLD page to NEW page (Next Page ID at offset 0)
            $pData = substr_replace($pData, pack('V', $newPageId), 0, 4);
            $this->pager->writePage($currentPageId, $pData);

            $currentPageId = $newPageId;
            $pData = $this->pager->readPage($currentPageId);
            $freeOffset = unpack('v', substr($pData, 6, 2))[1];
            
            $this->updateTableLastPage($table, $currentPageId);
        }

        // Write Record Len
        $pData = substr_replace($pData, pack('v', $recordLen), $freeOffset, 2);
        // Write Data
        $pData = substr_replace($pData, $dataBuf, $freeOffset + 2, $recordLen);

        // Update Count
        $count = unpack('v', substr($pData, 4, 2))[1];
        $pData = substr_replace($pData, pack('v', $count + 1), 4, 2);
        
        // Update Free Offset
        $pData = substr_replace($pData, pack('v', $freeOffset + $totalLen), 6, 2);

        $this->pager->writePage($currentPageId, $pData);

        // Inject _rid (PageID:OffsetOfData)
        // Offset of data is $freeOffset + 2 (len prefix is 2 bytes)
        // Wait, $freeOffset was the start of the block.
        // Block: [Len:2][Data:N]
        // We want to identify the record. Let's use Block Start Offset ($freeOffset) for simplicity in calc,
        // but data starts at +2. Let's use Block Start.
        $data['_rid'] = "$currentPageId:$freeOffset";

        // Update Indexes
        $this->updateIndexes($table, $data);

        return "Bibit tertanam.";
    }

    private function checkMatch($obj, $criteria)
    {
        if (!$criteria) return true;

        if (isset($criteria['type']) && $criteria['type'] === 'compound') {
             $result = true;
             // Logic handling simplifying... assuming AND mostly or generic check
             // Doing simplified sequential check logic
             
             foreach ($criteria['conditions'] as $i => $cond) {
                 $match = $this->checkSingleCondition($obj, $cond);
                 if ($i === 0) {
                     $result = $match;
                 } else {
                     if ($cond['logic'] === 'OR') {
                         $result = $result || $match;
                     } else {
                         $result = $result && $match;
                     }
                 }
             }
             return $result;
        }

        return $this->checkSingleCondition($obj, $criteria);
    }

    private function checkSingleCondition($obj, $criteria)
    {
        $key = $criteria['key'];
        if (!array_key_exists($key, $obj)) return false; // Or strict?
        
        $val = $obj[$key];
        $target = $criteria['val'];

        switch ($criteria['op']) {
            case '=': return $val == $target;
            case '!=': return $val != $target;
            case '>': return $val > $target;
            case '<': return $val < $target;
            case '>=': return $val >= $target;
            case '<=': return $val <= $target;
            case 'IN': return in_array($val, $target);
            case 'NOT IN': return !in_array($val, $target);
            case 'LIKE':
                $pattern = '/^' . str_replace('%', '.*', $target) . '$/i';
                return preg_match($pattern, (string)$val);
            case 'BETWEEN':
                return $val >= $target[0] && $val <= $target[1];
            case 'IS NULL': return is_null($val);
            case 'IS NOT NULL': return !is_null($val);
            default: return false;
        }
    }

    private function select($table, $criteria, $sort, $limit, $offsetCount)
    {
        $entry = $this->findTableEntry($table);
        if (!$entry) throw new Exception("Kebun '$table' tidak ditemukan.");

        $results = [];

        // Index Opt
        if ($criteria && !isset($criteria['type']) && $criteria['op'] === '=' && !$sort) {
            $indexKey = "$table." . $criteria['key'];
            if (isset($this->indexes[$indexKey])) {
                $results = $this->indexes[$indexKey]->search($criteria['val']);
            } else {
                $results = $this->scanTable($entry, $criteria);
            }
        } else {
            $results = $this->scanTable($entry, $criteria);
        }

        // Sort
        if ($sort) {
            usort($results, function($a, $b) use ($sort) {
                $valA = $a[$sort['key']] ?? null;
                $valB = $b[$sort['key']] ?? null;
                if ($valA == $valB) return 0;
                $res = ($valA < $valB) ? -1 : 1;
                return ($sort['dir'] === 'desc') ? -$res : $res;
            });
        }

        // Limit/Offset
        $start = $offsetCount ?? 0;
        $len = $limit;
        
        if ($limit === null) return array_slice($results, $start);
        return array_slice($results, $start, $len);
    }

    private function scanTable($entry, $criteria)
    {
        $currentPageId = $entry['startPage'];
        $results = [];

        while ($currentPageId !== 0) {
            $pData = $this->pager->readPage($currentPageId);
            $count = unpack('v', substr($pData, 4, 2))[1];
            $offset = 8;

            for ($i = 0; $i < $count; $i++) {
                $len = unpack('v', substr($pData, $offset, 2))[1];
                $jsonStr = substr($pData, $offset + 2, $len);
                $obj = json_decode($jsonStr, true);
                
                if ($obj) {
                    $obj['_rid'] = "$currentPageId:$offset"; // Inject RowID
                    if ($this->checkMatch($obj, $criteria)) {
                        $results[] = $obj;
                    }
                }
                $offset += 2 + $len;
            }
            $currentPageId = unpack('V', substr($pData, 0, 4))[1];
        }
        return $results;
    }

    public function delete($table, $criteria)
    {
        // Optimization: If criteria contains _rid, we can target specific pages
        $targetInfos = [];
        if (isset($criteria['_rid'])) {
             // Single deletion or IN array
             $rids = is_array($criteria['_rid']) ? $criteria['_rid'] : [$criteria['_rid']];
             foreach ($rids as $rid) {
                 [$pid, $off] = explode(':', $rid);
                 $targetInfos[$pid][] = (int)$off;
             }
        } elseif (isset($criteria['op']) && $criteria['op'] === 'IN' && $criteria['key'] === '_rid') {
             // Handle IN operator for _rid
             foreach ($criteria['val'] as $rid) {
                 [$pid, $off] = explode(':', $rid);
                 $targetInfos[$pid][] = (int)$off;
             }
        } else {
             // Try to use Index via Select to find RIDs
             try {
                // Peek if select uses index
                // Note: We blindly trust select to be faster than scan if indexed.
                // Even scan-select to get RIDs might be slower than scan-delete directly due to double read?
                // Actually scan-delete READS and CHECKS. Select READS and CHECKS.
                // If Select uses Index, it's O(log N). Then Delete is O(1) per RID. Very fast.
                // If Select uses Scan, it's O(N). Then Delete is O(1) per RID.
                // Total O(N). Same as scan-delete.
                // BUT scan-delete rewrites pages. Targeted delete rewrites ONLY affected pages.
                // So resolving RIDs first is almost always better for scattered deletes.
                
                $candidates = $this->select($table, $criteria, null, null, null);
                if (!empty($candidates)) {
                    foreach ($candidates as $c) {
                        if (isset($c['_rid'])) {
                             [$pid, $off] = explode(':', $c['_rid']);
                             $targetInfos[$pid][] = (int)$off;
                        }
                    }
                }
             } catch (Exception $e) {
                 // Fallback to scan if select fails
             }
        }

        if (!empty($targetInfos)) {
            // Targeted Deletion (Fast Path)
            $deletedCount = 0;
            foreach ($targetInfos as $pid => $offsets) {
                $pData = $this->pager->readPage($pid);
                $count = unpack('v', substr($pData, 4, 2))[1];
                $readOffset = 8;
                $recordsToKeep = [];
                $modified = false;

                for ($i = 0; $i < $count; $i++) {
                    $len = unpack('v', substr($pData, $readOffset, 2))[1];
                    // Check if this record's offset is in our delete list
                    if (in_array($readOffset, $offsets)) {
                        $deletedCount++;
                        $modified = true;
                    } else {
                        $recordsToKeep[] = ['len' => $len, 'data' => substr($pData, $readOffset + 2, $len)];
                    }
                    $readOffset += 2 + $len;
                }

                if ($modified) {
                    $this->rewritePage($pid, $recordsToKeep);
                }
            }
            return "Berhasil menggusur $deletedCount bibit (Optimized).";
        }

        // Fallback: Full Scan
        $entry = $this->findTableEntry($table);
        if (!$entry) throw new Exception("Kebun '$table' tidak ditemukan.");
        $currentPageId = $entry['startPage'];
        $deletedCount = 0;

        while ($currentPageId !== 0) {
            $pData = $this->pager->readPage($currentPageId);
            $count = unpack('v', substr($pData, 4, 2))[1];
            $readOffset = 8;
            $recordsToKeep = [];
            $modified = false;

            for ($i = 0; $i < $count; $i++) {
                $len = unpack('v', substr($pData, $readOffset, 2))[1];
                $jsonStr = substr($pData, $readOffset + 2, $len);
                $obj = json_decode($jsonStr, true);
                
                // Inject RID for matching if needed, though usually criteria doesn't check RID in scan
                if ($obj) $obj['_rid'] = "$currentPageId:$readOffset";

                if ($obj && $this->checkMatch($obj, $criteria)) {
                    $deletedCount++;
                    $modified = true;
                } else {
                    $recordsToKeep[] = ['len' => $len, 'data' => substr($pData, $readOffset + 2, $len)];
                }
                $readOffset += 2 + $len;
            }

            if ($modified) {
                $this->rewritePage($currentPageId, $recordsToKeep);
            }
            $currentPageId = unpack('V', substr($pData, 0, 4))[1];
        }
        return "Berhasil menggusur $deletedCount bibit.";
    }

    private function rewritePage($pid, $recordsToKeep) {
        $writeOffset = 8;
        $pData = str_repeat("\0", Pager::PAGE_SIZE);
        
        // Restore NextPageID (we need to read it again or passed it? 
        // WowoEngine logic was reading it from old pData. 
        // Optimally we should just modify the buffer but keeping it clean:
        $oldPage = $this->pager->readPage($pid);
        $nextPageId = substr($oldPage, 0, 4);
        
        $pData = substr_replace($pData, $nextPageId, 0, 4);
        $pData = substr_replace($pData, pack('v', count($recordsToKeep)), 4, 2);

        foreach ($recordsToKeep as $rec) {
            $pData = substr_replace($pData, pack('v', $rec['len']), $writeOffset, 2);
            $pData = substr_replace($pData, $rec['data'], $writeOffset + 2, $rec['len']);
            $writeOffset += 2 + $rec['len'];
        }
        
        $pData = substr_replace($pData, pack('v', $writeOffset), 6, 2);
        $this->pager->writePage($pid, $pData);
    }

    public function update($table, $updates, $criteria)
    {
        // Optimized Update: Select (Get RIDs) -> Delete by RIDs -> Insert
        $records = $this->select($table, $criteria, null, null, null);
        if (empty($records)) return "Tidak ada bibit yang cocok untuk dipupuk.";

        $ridsToDelete = [];
        $newRecords = [];

        foreach ($records as $rec) {
            if (isset($rec['_rid'])) {
                $ridsToDelete[] = $rec['_rid'];
            }
            // Apply updates
            foreach ($updates as $k => $v) {
                $rec[$k] = $v;
            }
            unset($rec['_rid']); // Don't insert the old RID
            $newRecords[] = $rec;
        }

        if (!empty($ridsToDelete)) {
            // Bulk delete by RID (Fast)
            $this->delete($table, ['op' => 'IN', 'key' => '_rid', 'val' => $ridsToDelete]);
        }

        $count = 0;
        foreach ($newRecords as $rec) {
            $this->insert($table, $rec);
            $count++;
        }
        return "Berhasil memupuk $count bibit.";
    }

    // Index Methods

    public function createIndex($table, $field)
    {
        $entry = $this->findTableEntry($table);
        if (!$entry) throw new Exception("Kebun '$table' tidak ditemukan.");

        $indexKey = "$table.$field";
        if (isset($this->indexes[$indexKey])) return "Indeks sudah ada.";

        $index = new BTreeIndex();
        $index->name = $indexKey;
        $index->keyField = $field;

        $all = $this->select($table, null, null, null, null);
        foreach ($all as $rec) {
            if (isset($rec[$field])) {
                $index->insert($rec[$field], $rec);
            }
        }

        $this->indexes[$indexKey] = $index;
        return "Indeks dibuat pada '$indexKey'.";
    }

    public function showIndexes($table)
    {
        // Simple list return
        $list = [];
        foreach ($this->indexes as $k => $v) {
             if (!$table || str_starts_with($k, "$table.")) {
                 $list[] = $k;
             }
        }
        return empty($list) ? "Tidak ada indeks." : $list;
    }

    private function updateIndexes($table, $data)
    {
        foreach ($this->indexes as $key => $index) {
            list($tbl, $fld) = explode('.', $key);
            if ($tbl === $table && isset($data[$fld])) {
                $index->insert($data[$fld], $data);
            }
        }
    }

    // Aggregate
    public function aggregate($table, $func, $field, $criteria, $groupBy)
    {
        $records = $this->select($table, $criteria, null, null, null);
        
        if ($groupBy) {
            // Grouped
            $groups = [];
            foreach ($records as $r) {
                $k = $r[$groupBy] ?? 'NULL';
                if (!isset($groups[$k])) $groups[$k] = [];
                $groups[$k][] = $r;
            }
            
            $results = [];
            foreach ($groups as $k => $group) {
                $res = $this->calcAggregate($func, $field, $group);
                $res[$groupBy] = $k;
                $results[] = $res;
            }
            return $results;
        }

        return $this->calcAggregate($func, $field, $records);
    }

    private function calcAggregate($func, $field, $records)
    {
        $func = strtoupper($func);
        switch ($func) {
            case 'COUNT': return ['count' => count($records)];
            case 'SUM':
                $sum = array_reduce($records, fn($c, $i) => $c + ($i[$field] ?? 0), 0);
                return ['sum' => $sum, 'field' => $field];
            case 'AVG':
                if (count($records) === 0) return ['avg' => 0, 'field' => $field];
                $sum = array_reduce($records, fn($c, $i) => $c + ($i[$field] ?? 0), 0);
                return ['avg' => $sum / count($records), 'field' => $field];
            case 'MIN':
                 $vals = array_map(fn($r) => $r[$field] ?? PHP_INT_MAX, $records);
                 return ['min' => min($vals), 'field' => $field];
            case 'MAX':
                 $vals = array_map(fn($r) => $r[$field] ?? PHP_INT_MIN, $records);
                 return ['max' => max($vals), 'field' => $field];
            default: throw new Exception("Unknown agg func");
        }
    }
}
