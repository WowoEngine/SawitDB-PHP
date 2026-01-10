<?php

namespace SawitDB\Engine\Services\Executors;

use Exception;
use SawitDB\Engine\WowoEngine;

class DeleteExecutor
{
    private WowoEngine $db;

    public function __construct(WowoEngine $db)
    {
        $this->db = $db;
    }

    public function execute(string $table, $criteria): string
    {
        $entry = $this->db->getTableManager()->findTableEntry($table);
        if (!$entry) throw new Exception("Kebun '$table' tidak ditemukan.");
        
        $currentPageId = $entry['startPage'];
        $deletedCount = 0;
        $pager = $this->db->getPager();
        $conditionEvaluator = $this->db->getConditionEvaluator();
        
        while ($currentPageId !== 0) {
            $pData = $pager->readPage($currentPageId); // Raw buffer
            $count = unpack('v', substr($pData, 4, 2))[1];
            $readOffset = 8;
            $recordsToKeep = [];
            $modified = false;
            
            for ($i = 0; $i < $count; $i++) {
                $len = unpack('v', substr($pData, $readOffset, 2))[1];
                $jsonStr = substr($pData, $readOffset + 2, $len);
                $obj = json_decode($jsonStr, true);
                
                if ($obj && $conditionEvaluator->checkMatch($obj, $criteria)) {
                    $deletedCount++;
                    $modified = true;
                    // WAL
                    $this->db->getWAL()->logOperation('DELETE', $table, $currentPageId, $jsonStr, null);
                    $this->db->getEventHandler()->OnTableDeleted($table, [$obj], "DELETE FROM $table ...");

                     if ($table !== '_indexes') {
                         $this->db->getIndexManager()->updateIndexes($table, null, $obj);
                     }
                } else {
                    $recordsToKeep[] = ['len' => $len, 'data' => substr($pData, $readOffset + 2, $len), 'obj' => $obj];
                }
                $readOffset += 2 + $len;
            }
            
            if ($modified) {
                $items = array_map(function($r) { return $r['obj']; }, $recordsToKeep);
                $next = unpack('V', substr($pData, 0, 4))[1];
                $pager->updatePageObjects($currentPageId, $next, $items);
            }
            
            $currentPageId = unpack('V', substr($pData, 0, 4))[1];
        }
        
        return "Berhasil menggusur $deletedCount bibit.";
    }
}
