<?php

namespace SawitDB\Engine\Services;

class ConditionEvaluator
{
    public function checkMatch($obj, $criteria)
    {
        if (!$criteria) return true;

        if (isset($criteria['type']) && $criteria['type'] === 'compound') {
            $result = ($criteria['logic'] !== 'OR');
            
            foreach ($criteria['conditions'] as $cond) {
                // Recursive check
                $matches = (isset($cond['type']) && $cond['type'] === 'compound')
                    ? $this->checkMatch($obj, $cond)
                    : $this->checkSingleCondition($obj, $cond);
                
                if ($criteria['logic'] === 'OR') {
                    $result = $result || $matches;
                    if ($result) return true; // Short circuit
                } else {
                    $result = $result && $matches;
                    if (!$result) return false; // Short circuit
                }
            }
            return $result;
        }

        return $this->checkSingleCondition($obj, $criteria);
    }

    public function checkSingleCondition($obj, $criteria)
    {
        $key = $criteria['key'];
        // Handle nested keys if necessary? Current JS doesn't explicitly, but PHP implementation of scanTable might flatten?
        // JS JoinProcessor prefixes keys.
        $val = $obj[$key] ?? null;
        $target = $criteria['val'];
        $op = strtoupper($criteria['op']);

        switch ($op) {
            case '=': 
                // Flexible comparison for numbers/strings like JS "5" == 5
                return $val == $target;
            case '!=': 
            case '<>':
                return $val != $target;
            case '>': return $val > $target;
            case '<': return $val < $target;
            case '>=': return $val >= $target;
            case '<=': return $val <= $target;
            case 'IN': 
                return is_array($target) && in_array($val, $target);
            case 'NOT IN': 
                return is_array($target) && !in_array($val, $target);
            case 'LIKE':
                // Escape regex characters
                $pattern = preg_quote($target, '/');
                // Replace SQL wildcards
                $pattern = str_replace('%', '.*', $pattern);
                $pattern = str_replace('_', '.', $pattern);
                return preg_match('/^' . $pattern . '$/i', (string)$val);
            case 'BETWEEN':
                return $val >= $target[0] && $val <= $target[1];
            case 'IS NULL': 
                return is_null($val);
            case 'IS NOT NULL': 
                return !is_null($val);
            default: return false;
        }
    }
}
