<?php

namespace SawitDB\Engine\Services\Executors;

use Exception;
use SawitDB\Engine\Pager;
use SawitDB\Engine\WowoEngine;

class InsertExecutor
{
    private WowoEngine $db;

    public function __construct(WowoEngine $db)
    {
        $this->db = $db;
    }

    public function execute(string $table, array $data): string
    {
        // Moved from WowoEngine::insert
        $entry = $this->db->getTableManager()->findTableEntry($table);
        if (!$entry) throw new Exception("Kebun '$table' tidak ditemukan.");
        
        $json = json_encode($data);
        $dataBuf = $json;
        $recordLen = strlen($dataBuf);
        $totalLen = 2 + $recordLen;
        
        $pager = $this->db->getPager();
        $currentPageId = $entry['lastPage'];
        $pData = $pager->readPage($currentPageId); // Raw
        $freeOffset = unpack('v', substr($pData, 6, 2))[1];
        
        if ($freeOffset + $totalLen > Pager::PAGE_SIZE) {
            $newPageId = $pager->allocPage();
            
            $pData = substr_replace($pData, pack('V', $newPageId), 0, 4);
            $pager->writePage($currentPageId, $pData);
            
            $currentPageId = $newPageId;
            $pData = $pager->readPage($currentPageId);
            $freeOffset = unpack('v', substr($pData, 6, 2))[1];
            
            $this->db->getTableManager()->updateTableLastPage($table, $currentPageId);
        }
        
        $pData = substr_replace($pData, pack('v', $recordLen), $freeOffset, 2);
        $pData = substr_replace($pData, $dataBuf, $freeOffset + 2, $recordLen);
        
        $count = unpack('v', substr($pData, 4, 2))[1];
        $pData = substr_replace($pData, pack('v', $count + 1), 4, 2);
        $pData = substr_replace($pData, pack('v', $freeOffset + $totalLen), 6, 2);
        
        $pager->writePage($currentPageId, $pData);
        
        // WAL
        $this->db->getWAL()->logOperation('INSERT', $table, $currentPageId, null, $json);

        $this->db->getEventHandler()->OnTableInserted($table, [$data], "INSERT INTO $table ...");

        if ($table !== '_indexes') {
            // Add _rid for index usage
            $data['_rid'] = "$currentPageId:$count";
            $this->db->getIndexManager()->updateIndexes($table, $data, null);
        }
        
        return "Bibit tertanam.";
    }
}
