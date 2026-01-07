<?php

namespace SawitDB\Engine;

use Exception;

class WowoEngine
{
    private $pager;
    private $indexes = []; // 'table.field' -> BTreeIndex instance
    private $parser;
    private $wal;
    
    // Query Cache
    private $queryCache = [];
    private $queryCacheLimit = 1000;

    public function __construct(string $filePath)
    {
        $this->pager = new Pager($filePath);
        $this->parser = new QueryParser();
        $this->wal = new WAL($filePath);
        
        // Persistence: Initialize System Tables
        $this->initSystem();
    }
    
    public function close()
    {
        if ($this->wal) $this->wal->close();
        if ($this->pager) $this->pager->close();
    }

    private function initSystem()
    {
        // Check if _indexes table exists
        $entry = $this->findTableEntry('_indexes');
        if (!$entry) {
            try {
                $this->createTable('_indexes');
            } catch (Exception $e) {
                // Ignore concurrency or existence issues
            }
        }
        
        // Load Indexes
        $this->loadIndexes();
    }

    private function loadIndexes()
    {
        if (!$this->findTableEntry('_indexes')) return;

        // Select all from _indexes
        // We can't use public select() easily because it might recurse or rely on state not ready?
        // But select() uses scanTable which is low level. Should be fine.
        $rows = $this->select('_indexes', null, null, null, null);
        
        foreach ($rows as $rec) {
            $table = $rec['table'] ?? null;
            $field = $rec['field'] ?? null;
            if (!$table || !$field) continue;

            $indexKey = "$table.$field";
            if (!isset($this->indexes[$indexKey])) {
                $index = new BTreeIndex();
                $index->name = $indexKey;
                $index->keyField = $field;

                // Populate Index
                try {
                    $entry = $this->findTableEntry($table);
                    if ($entry) {
                        $allRecords = $this->scanTable($entry, null);
                        foreach ($allRecords as $record) {
                            if (isset($record[$field])) {
                                $index->insert($record[$field], $record);
                            }
                        }
                        $this->indexes[$indexKey] = $index;
                    }
                } catch (Exception $e) {
                    // Log?
                }
            }
        }
    }

    public function query(string $query, array $params = [])
    {
        // Query Cache (Simple LRU)
        $cacheKey = $query;
        $cmd = null;
        if (isset($this->queryCache[$cacheKey])) {
            $cmd = $this->queryCache[$cacheKey]; // Copy array
        } else {
            // Parse without params to get template
            $cmd = $this->parser->parse($query, []);
            if ($cmd['type'] !== 'ERROR') {
                $this->queryCache[$cacheKey] = $cmd;
                if (count($this->queryCache) > $this->queryCacheLimit) {
                    array_shift($this->queryCache);
                }
            }
        }
        
        if ($cmd['type'] === 'EMPTY') return "";
        if ($cmd['type'] === 'ERROR') return "Error: " . $cmd['message'];

        if (!empty($params)) {
             $cmd = $this->parser->parse($query, $params);
        }

        try {
            switch ($cmd['type']) {
                case 'CREATE_TABLE':
                    return $this->createTable($cmd['table']);
                case 'SHOW_TABLES':
                    return $this->showTables();
                case 'SHOW_INDEXES':
                    return $this->showIndexes($cmd['table'] ?? null);
                case 'INSERT':
                    return $this->insert($cmd['table'], $cmd['data']);
                case 'SELECT':
                     // Handle Joins
                     $joins = $cmd['joins'] ?? [];
                     $rows = $this->select($cmd['table'], $cmd['criteria'], $cmd['sort'], $cmd['limit'], $cmd['offset'], $joins);
                     
                     // Strip system fields
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
                     }, $cleanRows);
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
        $p0 = $this->pager->readPage(0);
        $numTables = unpack('V', substr($p0, 8, 4))[1];
        $offset = 12;

        for ($i = 0; $i < $numTables; $i++) {
            $tName = trim(substr($p0, $offset, 32));
            $tName = str_replace("\0", "", $tName);

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
        if (!preg_match('/^[a-zA-Z0-9_]+$/', $name)) throw new Exception("Nama kebun hanya boleh huruf, angka, dan underscore.");

        if ($this->findTableEntry($name)) return "Kebun '$name' sudah ada.";

        $p0 = $this->pager->readPage(0);
        $numTables = unpack('V', substr($p0, 8, 4))[1];
        $offset = 12 + ($numTables * 40);

        if ($offset + 40 > Pager::PAGE_SIZE) throw new Exception("Lahan penuh (Page 0 full)");

        $newPageId = $this->pager->allocPage();

        // RELOAD Page 0 because allocPage modified it (totalPages incremented)
        $p0 = $this->pager->readPage(0);

        $p0 = substr_replace($p0, str_pad($name, 32, "\0"), $offset, 32);
        $p0 = substr_replace($p0, pack('V', $newPageId), $offset + 32, 4);
        $p0 = substr_replace($p0, pack('V', $newPageId), $offset + 36, 4);
        $p0 = substr_replace($p0, pack('V', $numTables + 1), 8, 4);

        $this->pager->writePage(0, $p0);
        
        $this->wal->logOperation('CREATE_TABLE', $name, $newPageId, null, null);
        return "Kebun '$name' telah dibuka.";
    }

    private function select($table, $criteria = null, $sort = null, $limit = null, $offsetCount = null, $joins = [])
    {
        $entry = $this->findTableEntry($table);
        if (!$entry) throw new Exception("Kebun '$table' tidak ditemukan.");

        $results = [];

        if (!empty($joins)) {
            // JOIN Logic
            $mainRows = $this->scanTable($entry, null);
            $currentRows = [];
            foreach ($mainRows as $row) {
                $newRow = $row; // Keep original for reference?
                foreach ($row as $k => $v) {
                    $newRow["$table.$k"] = $v;
                }
                $currentRows[] = $newRow;
            }

            foreach ($joins as $join) {
                $joinTable = $join['table'];
                $joinEntry = $this->findTableEntry($joinTable);
                if (!$joinEntry) throw new Exception("Kebun '$joinTable' tidak ditemukan.");

                $on = $join['on']; // { left, op, right }
                $nextRows = [];

                // Optimization: Hash Join if op is '='
                $useHashJoin = ($on['op'] === '=');
                //$useHashJoin = false; // DEBUG: Force Nested Loop to verify correctness first

                if ($useHashJoin) {
                    // Hash Phase
                    $joinMap = [];
                    $joinData = $this->scanTable($joinEntry, null);
                    
                    // Determine Right Key (strip prefix if exists)
                    $rightKey = $on['right'];
                    if (str_starts_with($rightKey, "$joinTable.")) {
                        $rightKey = substr($rightKey, strlen($joinTable) + 1);
                    }

                    foreach ($joinData as $row) {
                        $val = $row[$rightKey] ?? null;
                        if ($val === null) continue;
                        
                        $keyStr = (string)$val;
                        if (!isset($joinMap[$keyStr])) $joinMap[$keyStr] = [];
                        $joinMap[$keyStr][] = $row;
                    }

                    // Probe Phase
                    foreach ($currentRows as $leftRow) {
                        $leftVal = $leftRow[$on['left']] ?? null;
                        if ($leftVal === null) continue;
                        
                        $keyStr = (string)$leftVal;
                        if (isset($joinMap[$keyStr])) {
                            foreach ($joinMap[$keyStr] as $rightRow) {
                                // Merge
                                $merged = $leftRow;
                                foreach ($rightRow as $k => $v) {
                                    $merged["$joinTable.$k"] = $v;
                                }
                                $nextRows[] = $merged;
                            }
                        }
                    }

                } else {
                    // Nested Loop
                    $joinData = $this->scanTable($joinEntry, null);
                    foreach ($currentRows as $leftRow) {
                        foreach ($joinData as $rightRow) {
                            // Check Condition
                            $lVal = $leftRow[$on['left']] ?? null;
                            
                            $rightKey = $on['right'];
                             if (str_starts_with($rightKey, "$joinTable.")) {
                                $rightKey = substr($rightKey, strlen($joinTable) + 1);
                            }
                            $rVal = $rightRow[$rightKey] ?? null;

                            $match = false;
                            
                            switch ($on['op']) {
                                case '=': $match = ($lVal == $rVal); break;
                                case '!=': $match = ($lVal != $rVal); break;
                                case '>': $match = ($lVal > $rVal); break;
                                case '<': $match = ($lVal < $rVal); break;
                                // ... others
                            }

                            if ($match) {
                                $merged = $leftRow;
                                foreach ($rightRow as $k => $v) {
                                    $merged["$joinTable.$k"] = $v;
                                }
                                $nextRows[] = $merged;
                            }
                        }
                    }
                }
                $currentRows = $nextRows;
            }
            
            $results = $currentRows;
            
            // Apply Criteria on joined results
            if ($criteria) {
                $results = array_filter($results, function($bg) use ($criteria) {
                    return $this->checkMatch($bg, $criteria);
                });
            }

        } else {
            // Normal Selection
            if ($criteria && !isset($criteria['type']) && $criteria['op'] === '=' && !$sort) {
                // Index optimization for simple equality
                $indexKey = "$table." . $criteria['key'];
                if (isset($this->indexes[$indexKey])) {
                    $results = $this->indexes[$indexKey]->search($criteria['val']);
                } else {
                    $results = $this->scanTable($entry, $criteria);
                }
            } else {
                $results = $this->scanTable($entry, $criteria);
            }
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

        $hasSimpleCriteria = false;
        $cKey = null; $cOp = null; $cVal = null;
        
        if ($criteria && !isset($criteria['type'])) {
            $hasSimpleCriteria = true;
            $cKey = $criteria['key'];
            $cOp = $criteria['op'];
            $cVal = $criteria['val'];
        }

        while ($currentPageId !== 0) {
            $pageData = $this->pager->readPageObjects($currentPageId); // ['next'=>, 'items'=>]
            
            foreach ($pageData['items'] as $idx => $obj) {
                if ($hasSimpleCriteria) {
                    $val = $obj[$cKey] ?? null;
                    $matches = false;
                    switch ($cOp) {
                        case '=': $matches = ($val == $cVal); break;
                        case '>': $matches = ($val > $cVal); break;
                        case '<': $matches = ($val < $cVal); break;
                        default: $matches = $this->checkMatch($obj, $criteria);
                    }
                    if ($matches) {
                        $obj['_rid'] = "$currentPageId:$idx"; 
                        $results[] = $obj;
                    }
                } elseif (!$criteria || $this->checkMatch($obj, $criteria)) {
                    $obj['_rid'] = "$currentPageId:$idx"; 
                    $results[] = $obj;
                }
            }
            
            $currentPageId = $pageData['next'];
        }
        return $results;
    }

    private function checkMatch($obj, $criteria)
    {
        if (!$criteria) return true;

        if (isset($criteria['type']) && $criteria['type'] === 'compound') {
            $result = ($criteria['logic'] !== 'OR');
            
            foreach ($criteria['conditions'] as $cond) {
                $match = $this->checkMatch($obj, $cond);
                
                if ($criteria['logic'] === 'OR') {
                    $result = $result || $match;
                    if ($result) return true;
                } else {
                    $result = $result && $match;
                    if (!$result) return false;
                }
            }
            return $result;
        }

        return $this->checkSingleCondition($obj, $criteria);
    }

    private function checkSingleCondition($obj, $criteria)
    {
        $key = $criteria['key'];
        $val = $obj[$key] ?? null;
        $target = $criteria['val'];

        switch ($criteria['op']) {
            case '=': return $val == $target;
            case '!=': return $val != $target;
            case '>': return $val > $target;
            case '<': return $val < $target;
            case '>=': return $val >= $target;
            case '<=': return $val <= $target;
            case 'IN': return is_array($target) && in_array($val, $target);
            case 'NOT IN': return is_array($target) && !in_array($val, $target);
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

    public function createIndex($table, $field)
    {
        $entry = $this->findTableEntry($table);
        if (!$entry) throw new Exception("Kebun '$table' tidak ditemukan.");

        $indexKey = "$table.$field";
        if (isset($this->indexes[$indexKey])) return "Indeks sudah ada.";

        $index = new BTreeIndex();
        $index->name = $indexKey;
        $index->keyField = $field;

        $all = $this->scanTable($entry, null);
        foreach ($all as $rec) {
             if (isset($rec[$field])) {
                 $index->insert($rec[$field], $rec);
             }
        }

        $this->indexes[$indexKey] = $index;
        $this->insert('_indexes', ['table' => $table, 'field' => $field]);

        return "Indeks dibuat pada '$indexKey'.";
    }

    public function delete($table, $criteria)
    {
        $entry = $this->findTableEntry($table);
        if (!$entry) throw new Exception("Kebun '$table' tidak ditemukan.");
        
        $currentPageId = $entry['startPage'];
        $deletedCount = 0;
        
        while ($currentPageId !== 0) {
            $pData = $this->pager->readPage($currentPageId); // Raw buffer
            $count = unpack('v', substr($pData, 4, 2))[1];
            $readOffset = 8;
            $recordsToKeep = [];
            $modified = false;
            
            for ($i = 0; $i < $count; $i++) {
                $len = unpack('v', substr($pData, $readOffset, 2))[1];
                $jsonStr = substr($pData, $readOffset + 2, $len);
                $obj = json_decode($jsonStr, true);
                
                if ($obj && $this->checkMatch($obj, $criteria)) {
                    $deletedCount++;
                    $modified = true;
                    // WAL
                    $this->wal->logOperation('DELETE', $table, $currentPageId, $jsonStr, null);

                     if ($table !== '_indexes') {
                         $this->updateIndexes($table, null, $obj);
                     }
                } else {
                    $recordsToKeep[] = ['len' => $len, 'data' => substr($pData, $readOffset + 2, $len), 'obj' => $obj];
                }
                $readOffset += 2 + $len;
            }
            
            if ($modified) {
                $items = array_map(function($r) { return $r['obj']; }, $recordsToKeep);
                $next = unpack('V', substr($pData, 0, 4))[1];
                $this->pager->updatePageObjects($currentPageId, $next, $items);
            }
            
            $currentPageId = unpack('V', substr($pData, 0, 4))[1];
        }
        
        return "Berhasil menggusur $deletedCount bibit.";
    }
    
    public function insert($table, $data) {
        $entry = $this->findTableEntry($table);
        if (!$entry) throw new Exception("Kebun '$table' tidak ditemukan.");
        
        $json = json_encode($data);
        $dataBuf = $json;
        $recordLen = strlen($dataBuf);
        $totalLen = 2 + $recordLen;
        
        $currentPageId = $entry['lastPage'];
        $pData = $this->pager->readPage($currentPageId); // Raw
        $freeOffset = unpack('v', substr($pData, 6, 2))[1];
        
        if ($freeOffset + $totalLen > Pager::PAGE_SIZE) {
            $newPageId = $this->pager->allocPage();
            
            $pData = substr_replace($pData, pack('V', $newPageId), 0, 4);
            $this->pager->writePage($currentPageId, $pData);
            
            $currentPageId = $newPageId;
            $pData = $this->pager->readPage($currentPageId);
            $freeOffset = unpack('v', substr($pData, 6, 2))[1];
            
            $this->updateTableLastPage($table, $currentPageId);
        }
        
        $pData = substr_replace($pData, pack('v', $recordLen), $freeOffset, 2);
        $pData = substr_replace($pData, $dataBuf, $freeOffset + 2, $recordLen);
        
        $count = unpack('v', substr($pData, 4, 2))[1];
        $pData = substr_replace($pData, pack('v', $count + 1), 4, 2);
        $pData = substr_replace($pData, pack('v', $freeOffset + $totalLen), 6, 2);
        
        $this->pager->writePage($currentPageId, $pData);
        
        // WAL
        $this->wal->logOperation('INSERT', $table, $currentPageId, null, $json);

        if ($table !== '_indexes') {
            // Add _rid for index usage
            $data['_rid'] = "$currentPageId:$count";
            $this->updateIndexes($table, $data, null);
        }
        
        return "Bibit tertanam.";
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
            if (!str_starts_with($tName, '_')) {
                $tables[] = $tName;
            }
            $offset += 40;
        }
        return $tables;
    }
    
    public function showIndexes($table) 
    {
        $list = [];
        foreach ($this->indexes as $k => $v) {
            if (!$table || str_starts_with($k, "$table.")) {
                $list[] = $k;
            }
        }
        return empty($list) ? "Tidak ada indeks." : $list;
    }
    
    private function updateTableLastPage($name, $newLastPageId)
    {
        $entry = $this->findTableEntry($name);
        if (!$entry) throw new Exception("Entry missing");
        
        $p0 = $this->pager->readPage(0);
        $p0 = substr_replace($p0, pack('V', $newLastPageId), $entry['offset'] + 36, 4);
        $this->pager->writePage(0, $p0);
    }

    private function updateIndexes($table, $newObj, $oldObj)
    {
         foreach ($this->indexes as $key => $index) {
            list($tbl, $fld) = explode('.', $key);
            if ($tbl !== $table) continue;

             if ($oldObj && isset($oldObj[$fld])) {
                 if (!$newObj || $newObj[$fld] !== $oldObj[$fld]) {
                     $index->delete($oldObj[$fld]);
                 }
             }

             if ($newObj && isset($newObj[$fld])) {
                  if (!$oldObj || $newObj[$fld] !== $oldObj[$fld]) {
                      $index->insert($newObj[$fld], $newObj);
                  }
             }
        }
    }
    
    public function update($table, $updates, $criteria) {
        $records = $this->select($table, $criteria, null, null, null);
        if (empty($records)) return "Tidak ada bibit yang cocok.";
        
        $this->delete($table, $criteria);
        
        $count = 0;
        foreach ($records as $rec) {
             unset($rec['_rid']);
             foreach ($updates as $k => $v) $rec[$k] = $v;
             $this->insert($table, $rec);
             $count++;
        }
        return "Berhasil memupuk $count bibit.";
    }
    
    public function dropTable($name) {
         if ($name === '_indexes') return "Tidak boleh membakar catatan sistem.";
         $res = $this->_dropTableInternal($name);
         
         $toUnset = [];
         foreach ($this->indexes as $k => $i) {
             if (str_starts_with($k, "$name.")) $toUnset[] = $k;
         }
         foreach ($toUnset as $k) unset($this->indexes[$k]);
         
         $this->delete('_indexes', ['key' => 'table', 'op' => '=', 'val' => $name]);
         
         return $res;
    }
    
    private function _dropTableInternal($name) {
        $entry = $this->findTableEntry($name);
        if (!$entry) return "Kebun '$name' tidak ditemukan.";

        $p0 = $this->pager->readPage(0);
        $numTables = unpack('V', substr($p0, 8, 4))[1];

        if ($numTables > 1 && $entry['index'] < $numTables - 1) {
            $lastOffset = 12 + (($numTables - 1) * 40);
            $lastEntry = substr($p0, $lastOffset, 40);
            $p0 = substr_replace($p0, $lastEntry, $entry['offset'], 40);
        }

        $lastOffset = 12 + (($numTables - 1) * 40);
        $p0 = substr_replace($p0, str_repeat("\0", 40), $lastOffset, 40);
        $p0 = substr_replace($p0, pack('V', $numTables - 1), 8, 4);
        $this->pager->writePage(0, $p0);

        // WAL
        $this->wal->logOperation('DROP_TABLE', $name, 0, null, null);

        return "Kebun '$name' telah dibakar (Drop).";
    }

    public function aggregate($table, $func, $field, $criteria, $groupBy)
    {
        $records = $this->select($table, $criteria, null, null, null);
        
        if ($groupBy) {
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
