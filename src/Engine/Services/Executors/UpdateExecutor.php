<?php

namespace SawitDB\Engine\Services\Executors;

use Exception;
use SawitDB\Engine\WowoEngine;

class UpdateExecutor
{
    private WowoEngine $db;

    public function __construct(WowoEngine $db)
    {
        $this->db = $db;
    }

    public function execute(string $table, array $updates, $criteria): string
    {
        // Re-use SelectExecutor to find records?
        // Or simply WowoEngine::select (delegating to SelectExecutor)?
        // Circular dependency if SelectExecutor needs DB and DB needs UpdateExecutor?
        // WowoEngine is the mediator.
        
        // $records = $this->db->select($table, $criteria, ...);
        // But $this->db->select logic is moved to SelectExecutor!
        // So we can use SelectExecutor directly or via DB wrapper
        
        // Let's use internal selectExecutor on DB if available, or instantiate one?
        // Better: DB exposes getSelectExecutor().
        
        // To avoid complexity, just call DB->select() which we will refactor to use executor.
        // Wait, if UpdateExecutor calls DB->select(), and DB->select() calls SelectExecutor->execute()...
        // It works.
        
        // HOWEVER, standard UPDATE logic in SawitDB is: Select matching, Delete old, Insert new.
        // This is inefficient but simple.
        
        // Refactored logic:
        $cmd = [
            'type' => 'SELECT',
            'table' => $table,
            'criteria' => $criteria,
            'col' => ['*'],
            'sort' => null,
            'limit' => null,
            'offset' => null,
            'joins' => []
        ];
        
        $records = $this->db->getSelectExecutor()->execute($cmd);
        
        if (empty($records)) return "Tidak ada bibit yang cocok.";
        
        $this->db->getDeleteExecutor()->execute($table, $criteria);
        
        $count = 0;
        $insertExec = $this->db->getInsertExecutor();
        
        foreach ($records as $rec) {
             unset($rec['_rid']);
             foreach ($updates as $k => $v) $rec[$k] = $v;
             $insertExec->execute($table, $rec);
             $count++;
        }
        $this->db->getEventHandler()->OnTableUpdated($table, $records, "UPDATE $table ...");
        return "Berhasil memupuk $count bibit.";
    }
}
