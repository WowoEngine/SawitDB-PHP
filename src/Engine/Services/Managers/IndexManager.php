<?php

namespace SawitDB\Engine\Services\Managers;

use Exception;
use SawitDB\Engine\BTreeIndex;
use SawitDB\Engine\WowoEngine;

class IndexManager
{
    private WowoEngine $db;

    public function __construct(WowoEngine $db)
    {
        $this->db = $db;
    }

    // Proxy to DB's index map
    private function &getIndexes() {
        return $this->db->indexes;
    }

    public function createIndex(string $table, string $field): string
    {
        $entry = $this->db->getTableManager()->findTableEntry($table);
        if (!$entry) throw new Exception("Kebun '$table' tidak ditemukan.");

        $indexKey = "$table.$field";
        $indexes = &$this->getIndexes();
        if (isset($indexes[$indexKey])) return "Indeks sudah ada.";

        $index = new BTreeIndex();
        $index->name = $indexKey;
        $index->keyField = $field;

        // Scan table to build index
        // Using scanTable from logic (WowoEngine usually exposes this for internal use)
        $all = $this->db->scanTable($entry, null);
        foreach ($all as $rec) {
             if (isset($rec[$field])) {
                 $index->insert($rec[$field], $rec);
             }
        }

        $indexes[$indexKey] = $index;
        
        // Persist to _indexes
        try {
            $this->db->insert('_indexes', ['table' => $table, 'field' => $field]);
        } catch (Exception $e) { /* ignore */ }

        return "Indeks dibuat pada '$indexKey'.";
    }

    public function showIndexes(?string $table = null) 
    {
        $list = [];
        foreach ($this->getIndexes() as $k => $v) {
            if (!$table || str_starts_with($k, "$table.")) {
                $list[] = $k;
            }
        }
        return empty($list) ? "Tidak ada indeks." : $list;
    }

    public function updateIndexes(string $table, ?array $newObj, ?array $oldObj): void
    {
         foreach ($this->getIndexes() as $key => $index) {
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
    
    public function removeIndexesForTable(string $table): void
    {
        $toUnset = [];
        $indexes = &$this->getIndexes();
        foreach ($indexes as $k => $i) {
             if (str_starts_with($k, "$table.")) $toUnset[] = $k;
        }
        foreach ($toUnset as $k) unset($indexes[$k]);
    }

    public function loadIndexes(): void
    {
        // Try to read _indexes table
        // This runs at startup
        // It relies on scanTable being available
        try {
            $entry = $this->db->getTableManager()->findTableEntry('_indexes');
            if (!$entry) return;

            $indexRecords = $this->db->scanTable($entry, null);
            $indexes = &$this->getIndexes();

            foreach ($indexRecords as $rec) {
                $table = $rec['table'];
                $field = $rec['field'];
                $indexKey = "$table.$field";

                if (!isset($indexes[$indexKey])) {
                    // Rebuild index
                    $tblEntry = $this->db->getTableManager()->findTableEntry($table);
                    if ($tblEntry) {
                        $index = new BTreeIndex();
                        $index->name = $indexKey;
                        $index->keyField = $field;

                        $all = $this->db->scanTable($tblEntry, null);
                        foreach ($all as $r) {
                            if (isset($r[$field])) {
                                $index->insert($r[$field], $r);
                            }
                        }
                        $indexes[$indexKey] = $index;
                    }
                }
            }
        } catch (Exception $e) {
            // Ignore startup errors if system table corrupt or missing
        }
    }
}
