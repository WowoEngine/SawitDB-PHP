<?php

namespace SawitDB\Engine\Services\Managers;

use Exception;
use SawitDB\Engine\Pager;
use SawitDB\Engine\WowoEngine;

class TableManager
{
    private WowoEngine $db;

    public function __construct(WowoEngine $db)
    {
        $this->db = $db;
    }

    public function findTableEntry(string $name): ?array
    {
        $pager = $this->db->getPager();
        $p0 = $pager->readPage(0);
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

    public function createTable(string $name): string
    {
        if (!$name) throw new Exception("Nama kebun tidak boleh kosong");
        if (strlen($name) > 32) throw new Exception("Nama kebun max 32 karakter");
        // Update regex to allow - probably? No, wowoengine usually strictly snake_case or alphanumeric
        if (!preg_match('/^[a-zA-Z0-9_]+$/', $name)) throw new Exception("Nama kebun hanya boleh huruf, angka, dan underscore.");

        if ($this->findTableEntry($name)) return "Kebun '$name' sudah ada.";

        $pager = $this->db->getPager();
        $p0 = $pager->readPage(0);
        $numTables = unpack('V', substr($p0, 8, 4))[1];
        $offset = 12 + ($numTables * 40);

        if ($offset + 40 > Pager::PAGE_SIZE) throw new Exception("Lahan penuh (Page 0 full)");

        $newPageId = $pager->allocPage();

        // RELOAD Page 0 because allocPage modified it (totalPages incremented)
        $p0 = $pager->readPage(0);

        $p0 = substr_replace($p0, str_pad($name, 32, "\0"), $offset, 32);
        $p0 = substr_replace($p0, pack('V', $newPageId), $offset + 32, 4);
        $p0 = substr_replace($p0, pack('V', $newPageId), $offset + 36, 4);
        $p0 = substr_replace($p0, pack('V', $numTables + 1), 8, 4);

        $pager->writePage(0, $p0);
        
        $this->db->getWAL()->logOperation('CREATE_TABLE', $name, $newPageId, null, null);
        $this->db->getEventHandler()->OnTableCreated($name, null, "CREATE TABLE $name");
        return "Kebun '$name' telah dibuka.";
    }

    public function dropTable(string $name): string
    {
        if ($name === '_indexes') return "Tidak boleh membakar catatan sistem.";
        $res = $this->_dropTableInternal($name);
        
        // Remove associated indexes
        $this->db->getIndexManager()->removeIndexesForTable($name);
        
        // Remove from _indexes table via query or direct delete?
        // Rely on Engine's delete to handle it or IndexManager's cleanup logic?
        // In JS it tries to delete from _indexes.
        try {
            $this->db->delete('_indexes', ['key' => 'table', 'op' => '=', 'val' => $name]);
        } catch (Exception $e) { /* ignore */ }
        
        return $res;
    }

    private function _dropTableInternal(string $name): string
    {
        $entry = $this->findTableEntry($name);
        if (!$entry) return "Kebun '$name' tidak ditemukan.";

        $pager = $this->db->getPager();
        $p0 = $pager->readPage(0);
        $numTables = unpack('V', substr($p0, 8, 4))[1];

        if ($numTables > 1 && $entry['index'] < $numTables - 1) {
            $lastOffset = 12 + (($numTables - 1) * 40);
            $lastEntry = substr($p0, $lastOffset, 40);
            $p0 = substr_replace($p0, $lastEntry, $entry['offset'], 40);
        }

        $lastOffset = 12 + (($numTables - 1) * 40);
        $p0 = substr_replace($p0, str_repeat("\0", 40), $lastOffset, 40);
        $p0 = substr_replace($p0, pack('V', $numTables - 1), 8, 4);
        $pager->writePage(0, $p0);

        // WAL - assuming getWAL is public on DB
        $this->db->getWAL()->logOperation('DROP_TABLE', $name, 0, null, null);
        $this->db->getEventHandler()->OnTableDropped($name, null, "DROP TABLE $name");

        return "Kebun '$name' telah dibakar (Drop).";
    }

    public function showTables(): array
    {
        $p0 = $this->db->getPager()->readPage(0);
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

    public function updateTableLastPage(string $name, int $newLastPageId): void
    {
        $entry = $this->findTableEntry($name);
        if (!$entry) throw new Exception("Entry missing");
        
        $pager = $this->db->getPager();
        $p0 = $pager->readPage(0);
        $p0 = substr_replace($p0, pack('V', $newLastPageId), $entry['offset'] + 36, 4);
        $pager->writePage(0, $p0);
    }
}
