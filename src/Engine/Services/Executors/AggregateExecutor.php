<?php

namespace SawitDB\Engine\Services\Executors;

use Exception;
use SawitDB\Engine\WowoEngine;

class AggregateExecutor
{
    private WowoEngine $db;

    public function __construct(WowoEngine $db)
    {
        $this->db = $db;
    }

    public function execute(array $cmd)
    {
        $table = $cmd['table'];
        $func = $cmd['func'];
        $field = $cmd['field'];
        $criteria = $cmd['criteria'];
        $groupBy = $cmd['groupBy'];
        $having = $cmd['having'] ?? null;

        // Use SelectExecutor to get records
        $selectCmd = [
            'type' => 'SELECT',
            'table' => $table,
            'criteria' => $criteria,
            'col' => ['*'],
            'sort' => null,
            'limit' => null,
            'offset' => null,
            'joins' => []
        ];
        
        $records = $this->db->getSelectExecutor()->execute($selectCmd);

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
            
            if ($having) {
                $eval = $this->db->getConditionEvaluator();
                $results = array_filter($results, function($r) use ($having, $eval) {
                    return $eval->checkMatch($r, $having);
                });
                $results = array_values($results);
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
