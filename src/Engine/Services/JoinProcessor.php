<?php

namespace SawitDB\Engine\Services;

use Exception;
use SawitDB\Engine\WowoEngine;

class JoinProcessor
{
    private $engine;

    public function __construct(WowoEngine $engine)
    {
        $this->engine = $engine;
    }

    public function process($mainTable, $initialRows, $joins)
    {
        if (empty($joins)) return $initialRows;

        // 1. Prefix columns of main table
        $currentRows = array_map(function($row) use ($mainTable) {
            $newRow = $row;
            foreach ($row as $k => $v) {
                if ($k !== '_rid') {
                    $newRow["$mainTable.$k"] = $v;
                }
            }
            return $newRow;
        }, $initialRows);

        // 2. Perform Joins
        foreach ($joins as $join) {
            $joinTable = $join['table'];
            
            // Access public methods of WowoEngine
            $joinEntry = $this->engine->getTableManager()->findTableEntry($joinTable);
            if (!$joinEntry) throw new Exception("Kebun '$joinTable' tidak ditemukan.");

            $joinType = strtoupper($join['type'] ?? 'INNER');
            
            // Get all rows from right table (scanTable with null criteria)
            $joinRows = $this->engine->scanTable($joinEntry, null);

            // Helper to prefix right row
            $prefixRightRow = function($row) use ($joinTable) {
                $prefixed = [];
                foreach ($row as $k => $v) {
                    if ($k !== '_rid') {
                        $prefixed[$k] = $v;
                        $prefixed["$joinTable.$k"] = $v;
                    }
                }
                return $prefixed;
            };

            // Helpers for NULL rows
            $createNullRightRow = function() use ($joinRows, $joinTable) {
                $nullRow = [];
                if (!empty($joinRows)) {
                    $first = reset($joinRows);
                    foreach ($first as $k => $v) {
                        if ($k !== '_rid') {
                            $nullRow[$k] = null;
                            $nullRow["$joinTable.$k"] = null;
                        }
                    }
                }
                return $nullRow;
            };

            $createNullLeftRow = function() use ($currentRows) {
                $nullRow = [];
                if (!empty($currentRows)) {
                    $first = reset($currentRows);
                    foreach ($first as $k => $v) {
                       $nullRow[$k] = null;
                    }
                }
                return $nullRow;
            };

            $nextRows = [];

            // CROSS JOIN
            if ($joinType === 'CROSS') {
                foreach ($currentRows as $leftRow) {
                    foreach ($joinRows as $rightRow) {
                        $nextRows[] = array_merge($leftRow, $prefixRightRow($rightRow));
                    }
                }
                $currentRows = $nextRows;
                continue;
            }

            // Normal Joins need ON condition
            // Determine keys
            $leftKeyName = $join['on']['left'];
            $rightKeyName = $join['on']['right'];
            if (str_starts_with($rightKeyName, "$joinTable.")) {
                $rightKeyName = substr($rightKeyName, strlen($joinTable) + 1);
            }
            $op = $join['on']['op'];

            // Match function
            $matchRows = function($leftRow, $rightRow) use ($leftKeyName, $rightKeyName, $op) {
                $lVal = $leftRow[$leftKeyName] ?? null;
                $rVal = $rightRow[$rightKeyName] ?? null;

                switch ($op) {
                    case '=': return $lVal == $rVal;
                    case '!=': case '<>': return $lVal != $rVal;
                    case '>': return $lVal > $rVal;
                    case '<': return $lVal < $rVal;
                    case '>=': return $lVal >= $rVal;
                    case '<=': return $lVal <= $rVal;
                    default: return false;
                }
            };

            // Hash Join Optimization
            $useHashJoin = ($op === '=');
            $joinMap = [];
            
            if ($useHashJoin) {
                foreach ($joinRows as $row) {
                    $val = $row[$rightKeyName] ?? null;
                    if ($val === null) continue;
                    $keyStr = (string)$val;
                    if (!isset($joinMap[$keyStr])) $joinMap[$keyStr] = [];
                    $joinMap[$keyStr][] = $row;
                }
            }

            // Track matched right rows (for FULL OUTER)
            // Using array key as index
            $matchedRightIndices = [];

            // Process LEFT / INNER / FULL
            if (in_array($joinType, ['INNER', 'LEFT', 'FULL'])) {
                foreach ($currentRows as $leftRow) {
                    $hasMatch = false;

                    if ($useHashJoin) {
                        $lVal = $leftRow[$leftKeyName] ?? null;
                        $keyStr = (string)$lVal;
                        if (isset($joinMap[$keyStr])) {
                            foreach ($joinMap[$keyStr] as $rightRow) {
                                $nextRows[] = array_merge($leftRow, $prefixRightRow($rightRow));
                                $hasMatch = true;
                                if ($joinType === 'FULL') {
                                    // Slow search for index, but acceptable for PHP port for now
                                    // Optimization: Store index in joinMap?
                                    $idx = array_search($rightRow, $joinRows, true); // true = strict? No, objects are arrays.
                                    if ($idx !== false) $matchedRightIndices[$idx] = true;
                                }
                            }
                        }
                    } else {
                        foreach ($joinRows as $idx => $rightRow) {
                            if ($matchRows($leftRow, $rightRow)) {
                                $nextRows[] = array_merge($leftRow, $prefixRightRow($rightRow));
                                $hasMatch = true;
                                if ($joinType === 'FULL') $matchedRightIndices[$idx] = true;
                            }
                        }
                    }

                    if (!$hasMatch && ($joinType === 'LEFT' || $joinType === 'FULL')) {
                        $nextRows[] = array_merge($leftRow, $createNullRightRow());
                    }
                }
            }

            // RIGHT JOIN
            if ($joinType === 'RIGHT') {
                // Build Left Map for Hash Join
                 $leftMap = [];
                 if ($useHashJoin) {
                     foreach ($currentRows as $row) {
                         $val = $row[$leftKeyName] ?? null;
                         if ($val !== null) {
                             $k = (string)$val;
                             if (!isset($leftMap[$k])) $leftMap[$k] = [];
                             $leftMap[$k][] = $row;
                         }
                     }
                 }

                 foreach ($joinRows as $rightRow) {
                     $hasMatch = false;
                     $prefixedRight = $prefixRightRow($rightRow);

                     if ($useHashJoin) {
                         $rVal = $rightRow[$rightKeyName] ?? null;
                         $k = (string)$rVal;
                         if (isset($leftMap[$k])) {
                             foreach ($leftMap[$k] as $leftRow) {
                                  $nextRows[] = array_merge($leftRow, $prefixedRight);
                                  $hasMatch = true;
                             }
                         }
                     } else {
                         foreach ($currentRows as $leftRow) {
                             if ($matchRows($leftRow, $rightRow)) {
                                 $nextRows[] = array_merge($leftRow, $prefixedRight);
                                 $hasMatch = true;
                             }
                         }
                     }

                     if (!$hasMatch) {
                          $nextRows[] = array_merge($createNullLeftRow(), $prefixedRight);
                     }
                 }
            }

            // FULL OUTER (Remaining Right Rows)
            if ($joinType === 'FULL') {
                foreach ($joinRows as $idx => $rightRow) {
                    if (!isset($matchedRightIndices[$idx])) {
                        $nextRows[] = array_merge($createNullLeftRow(), $prefixRightRow($rightRow));
                    }
                }
            }

            $currentRows = $nextRows;
        }

        return $currentRows;
    }
}
