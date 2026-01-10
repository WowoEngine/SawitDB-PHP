<?php

namespace SawitDB\Engine\Services\Executors;

use Exception;
use SawitDB\Engine\WowoEngine;

class SelectExecutor
{
    private WowoEngine $db;

    public function __construct(WowoEngine $db)
    {
        $this->db = $db;
    }

    public function execute(array $cmd)
    {
        $table = $cmd['table'];
        $criteria = $cmd['criteria'];
        $sort = $cmd['sort'];
        $limit = $cmd['limit'];
        $offsetCount = $cmd['offset'];
        $joins = $cmd['joins'] ?? [];

        // Logic currently in WowoEngine::select, moved here or delegated?
        // Since WowoEngine has the select method, we can either:
        // 1. Fully move logic here (Preferred for true modularity)
        // 2. Wrap WowoEngine::select (Less risk, but less modular)
        
        // Let's implement logic here using Components
        
        $entry = $this->db->getTableManager()->findTableEntry($table);
        if (!$entry) throw new Exception("Kebun '$table' tidak ditemukan.");

        $results = [];

        if (!empty($joins)) {
            // Use JoinProcessor
            $mainRows = $this->db->scanTable($entry, null);
            $results = $this->db->getJoinProcessor()->process($table, $mainRows, $joins);
            
            if ($criteria) {
                // Use ConditionEvaluator
                $evaluator = $this->db->getConditionEvaluator();
                $results = array_filter($results, function($bg) use ($criteria, $evaluator) {
                    return $evaluator->checkMatch($bg, $criteria);
                });
            }
        } else {
            // Normal Selection
            if ($criteria && !isset($criteria['type']) && $criteria['op'] === '=' && !$sort) {
                // Index optimization
                $indexKey = "$table." . $criteria['key'];
                $indexes = $this->db->indexes; // Public access to indexes?
                if (isset($indexes[$indexKey])) {
                    $results = $indexes[$indexKey]->search($criteria['val']);
                } else {
                    $results = $this->db->scanTable($entry, $criteria);
                }
            } else {
                $results = $this->db->scanTable($entry, $criteria);
            }
        }

        // --- Post-Processing ---

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
        
        if ($limit !== null) {
            $results = array_slice($results, $start, $limit);
        } else if ($start > 0) {
            $results = array_slice($results, $start);
        }

        // Handle GROUP BY
        if (!empty($cmd['groupBy'])) {
            $g = $cmd['groupBy'];
            $grouped = [];
            foreach ($results as $r) {
                $k = $r[$g] ?? null;
                if (!isset($grouped[$k])) $grouped[$k] = $r;
            }
            $results = array_values($grouped);
        }
        
        // Handle HAVING
        if (!empty($cmd['having'])) {
            $evaluator = $this->db->getConditionEvaluator();
            $results = array_filter($results, function($r) use ($cmd, $evaluator) {
                return $evaluator->checkMatch($r, $cmd['having']);
            });
            $results = array_values($results);
        }

        $this->db->getEventHandler()->OnTableSelected($table, $results, "SELECT ...");

        return $results;
    }
}
