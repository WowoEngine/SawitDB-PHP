<?php

namespace SawitDB\Engine;

use Exception;

class QueryParser
{
    public function tokenize(string $sql): array
    {
        // Regex to match tokens
        $tokenRegex = '/\s*(=>|!=|>=|<=|<>|[(),=*.<>?]|[a-zA-Z_]\w*|@\w+|\d+|\'[^\']*\'|"[^"]*")\s*/';
        preg_match_all($tokenRegex, $sql, $matches);
        return $matches[1] ?? [];
    }

    public function parse(string $queryString, array $params = []): array
    {
        $tokens = $this->tokenize($queryString);
        if (empty($tokens)) return ['type' => 'EMPTY'];

        $cmd = strtoupper($tokens[0]);
        $command = [];

        try {
            switch ($cmd) {
                case 'LAHAN':
                case 'CREATE':
                    if (isset($tokens[1]) && strtoupper($tokens[1]) === 'INDEX') {
                        $command = $this->parseCreateIndex($tokens);
                    } else {
                        $command = $this->parseCreate($tokens);
                    }
                    break;
                case 'LIHAT':
                case 'SHOW':
                    $command = $this->parseShow($tokens);
                    break;
                case 'TANAM':
                case 'INSERT':
                    $command = $this->parseInsert($tokens);
                    break;
                case 'PANEN':
                case 'SELECT':
                    $command = $this->parseSelect($tokens);
                    break;
                case 'GUSUR':
                case 'DELETE':
                    $command = $this->parseDelete($tokens);
                    break;
                case 'PUPUK':
                case 'UPDATE':
                    $command = $this->parseUpdate($tokens);
                    break;
                case 'BAKAR':
                case 'DROP':
                    $command = $this->parseDrop($tokens);
                    break;
                case 'INDEKS':
                    $command = $this->parseCreateIndex($tokens);
                    break;
                case 'HITUNG':
                    $command = $this->parseAggregate($tokens);
                    break;
                default:
                    throw new Exception("Perintah tidak dikenal: $cmd");
            }

            if (!empty($params)) {
                $this->bindParameters($command, $params);
            }
            return $command;
        } catch (Exception $e) {
            return ['type' => 'ERROR', 'message' => $e->getMessage()];
        }
    }

    private function parseCreate($tokens)
    {
        $name = '';
        if (strtoupper($tokens[0]) === 'CREATE') {
            if (strtoupper($tokens[1]) !== 'TABLE') throw new Exception("Syntax: CREATE TABLE [name]");
            $name = $tokens[2];
        } else {
            if (count($tokens) < 2) throw new Exception("Syntax: LAHAN [nama_kebun]");
            $name = $tokens[1];
        }
        return ['type' => 'CREATE_TABLE', 'table' => $name];
    }

    private function parseShow($tokens)
    {
        $cmd = strtoupper($tokens[0]);
        $sub = isset($tokens[1]) ? strtoupper($tokens[1]) : '';

        if ($cmd === 'LIHAT') {
            if ($sub === 'LAHAN') return ['type' => 'SHOW_TABLES'];
            if ($sub === 'INDEKS') return ['type' => 'SHOW_INDEXES', 'table' => $tokens[2] ?? null];
        } elseif ($cmd === 'SHOW') {
            if ($sub === 'TABLES') return ['type' => 'SHOW_TABLES'];
            if ($sub === 'INDEXES') return ['type' => 'SHOW_INDEXES', 'table' => $tokens[2] ?? null];
        }

        throw new Exception("Syntax: LIHAT LAHAN | SHOW TABLES | LIHAT INDEKS [table] | SHOW INDEXES");
    }

    private function parseDrop($tokens)
    {
        if (strtoupper($tokens[0]) === 'DROP') {
            if (isset($tokens[1]) && strtoupper($tokens[1]) === 'TABLE') {
                return ['type' => 'DROP_TABLE', 'table' => $tokens[2]];
            }
        } elseif (strtoupper($tokens[0]) === 'BAKAR') {
            if (isset($tokens[1]) && strtoupper($tokens[1]) === 'LAHAN') {
                return ['type' => 'DROP_TABLE', 'table' => $tokens[2]];
            }
        }
        throw new Exception("Syntax: BAKAR LAHAN [nama] | DROP TABLE [nama]");
    }

    private function parseInsert($tokens)
    {
        $i = 1;
        $table = '';

        if (strtoupper($tokens[0]) === 'INSERT') {
            if (strtoupper($tokens[1]) !== 'INTO') throw new Exception("Syntax: INSERT INTO [table] ...");
            $i = 2;
        } else {
            if (strtoupper($tokens[1]) !== 'KE') throw new Exception("Syntax: TANAM KE [kebun] ...");
            $i = 2;
        }

        $table = $tokens[$i];
        $i++;

        $cols = [];
        if ($tokens[$i] === '(') {
            $i++;
            while ($tokens[$i] !== ')') {
                if ($tokens[$i] !== ',') $cols[] = $tokens[$i];
                $i++;
                if (!isset($tokens[$i])) throw new Exception("Unclosed parenthesis in columns");
            }
            $i++;
        } else {
            throw new Exception("Syntax: ... [table] (col1, ...) ...");
        }

        $valueKeyword = strtoupper($tokens[$i]);
        if ($valueKeyword !== 'BIBIT' && $valueKeyword !== 'VALUES') throw new Exception("Expected BIBIT or VALUES");
        $i++;

        $vals = [];
        if ($tokens[$i] === '(') {
            $i++;
            while ($tokens[$i] !== ')') {
                if ($tokens[$i] !== ',') {
                    $val = $tokens[$i];
                    if (str_starts_with($val, "'") || str_starts_with($val, '"')) $val = substr($val, 1, -1);
                    else if (strtoupper($val) === 'NULL') $val = null;
                    else if (strtoupper($val) === 'TRUE') $val = true;
                    else if (strtoupper($val) === 'FALSE') $val = false;
                    else if (is_numeric($val)) $val = $val + 0; // Force num
                    $vals[] = $val;
                }
                $i++;
            }
        } else {
            throw new Exception("Syntax: ... VALUES (val1, ...)");
        }

        if (count($cols) !== count($vals)) throw new Exception("Columns and Values count mismatch");

        $data = array_combine($cols, $vals);

        return ['type' => 'INSERT', 'table' => $table, 'data' => $data];
    }

    private function parseSelect($tokens)
    {
        $i = 1;
        $cols = [];
        while ($i < count($tokens) && !in_array(strtoupper($tokens[$i]), ['DARI', 'FROM'])) {
            if ($tokens[$i] !== ',') $cols[] = $tokens[$i];
            $i++;
        }

        if ($i >= count($tokens)) throw new Exception("Expected DARI or FROM");
        $i++;

        $table = $tokens[$i];
        $i++;

        $criteria = null;
        if ($i < count($tokens) && in_array(strtoupper($tokens[$i]), ['DIMANA', 'WHERE'])) {
            $i++;
            $criteria = $this->parseWhere($tokens, $i);
            // Move i past WHERE clause
             while ($i < count($tokens) && !in_array(strtoupper($tokens[$i]), ['ORDER', 'LIMIT', 'OFFSET'])) {
                $i++;
            }
        }

        $sort = null;
        if ($i < count($tokens) && strtoupper($tokens[$i]) === 'ORDER') {
            $i++;
            if (strtoupper($tokens[$i]) === 'BY') $i++;
            $key = $tokens[$i];
            $i++;
            $dir = 'asc';
            if ($i < count($tokens) && in_array(strtoupper($tokens[$i]), ['ASC', 'DESC'])) {
                $dir = strtolower($tokens[$i]);
                $i++;
            }
            $sort = ['key' => $key, 'dir' => $dir];
        }

        $limit = null;
        $offset = null;

        if ($i < count($tokens) && strtoupper($tokens[$i]) === 'LIMIT') {
            $i++;
            $limit = (int)$tokens[$i];
            $i++;
        }

        if ($i < count($tokens) && strtoupper($tokens[$i]) === 'OFFSET') {
            $i++;
            $offset = (int)$tokens[$i];
            $i++;
        }

        return ['type' => 'SELECT', 'table' => $table, 'cols' => $cols, 'criteria' => $criteria, 'sort' => $sort, 'limit' => $limit, 'offset' => $offset];
    }

    private function parseWhere($tokens, $startIndex)
    {
        $conditions = [];
        $i = $startIndex;
        $currentLogic = 'AND';

        while ($i < count($tokens)) {
            $token = $tokens[$i];
            $upper = strtoupper($token);

            if ($upper === 'AND' || $upper === 'OR') {
                $currentLogic = $upper;
                $i++;
                continue;
            }

            if (in_array($upper, ['ORDER', 'LIMIT', 'OFFSET', 'GROUP', 'KELOMPOK'])) {
                break;
            }

            if ($i < count($tokens) - 1) {
                $key = $tokens[$i];
                $op = strtoupper($tokens[$i + 1]);
                $val = null;
                $consumed = 2; // key + op

                if ($op === 'BETWEEN') {
                     // Syntax: key BETWEEN v1 AND v2
                     $v1 = $tokens[$i+2];
                     $v2 = $tokens[$i+4];
                     
                     if ((str_starts_with($v1, "'") || str_starts_with($v1, '"'))) $v1 = substr($v1, 1, -1);
                     elseif (is_numeric($v1)) $v1 = $v1 + 0;

                     if ((str_starts_with($v2, "'") || str_starts_with($v2, '"'))) $v2 = substr($v2, 1, -1);
                     elseif (is_numeric($v2)) $v2 = $v2 + 0;

                     $conditions[] = ['key' => $key, 'op' => 'BETWEEN', 'val' => [$v1, $v2], 'logic' => $currentLogic];
                     $consumed = 5;
                } elseif ($op === 'IS') {
                    $next = strtoupper($tokens[$i+2]);
                    if ($next === 'NULL') {
                        $conditions[] = ['key' => $key, 'op' => 'IS NULL', 'val' => null, 'logic' => $currentLogic];
                        $consumed = 3;
                    } elseif ($next === 'NOT') {
                        $conditions[] = ['key' => $key, 'op' => 'IS NOT NULL', 'val' => null, 'logic' => $currentLogic];
                        $consumed = 4;
                    }
                } else {
                    $val = $tokens[$i + 2];
                    if (str_starts_with($val, "'") || str_starts_with($val, '"')) {
                        $val = substr($val, 1, -1);
                    } else if (is_numeric($val)) {
                        $val = $val + 0;
                    }
                    $conditions[] = ['key' => $key, 'op' => $op, 'val' => $val, 'logic' => $currentLogic];
                    $consumed = 3;
                }
                $i += $consumed;
            } else {
                break;
            }
        }

        if (count($conditions) === 1) return $conditions[0];
        return ['type' => 'compound', 'conditions' => $conditions];
    }

    private function parseDelete($tokens)
    {
        $i = 0;
        $table = '';

        if (strtoupper($tokens[0]) === 'DELETE') {
            if (strtoupper($tokens[1]) !== 'FROM') throw new Exception("Syntax: DELETE FROM [table] ...");
            $table = $tokens[2];
            $i = 3;
        } else {
            if (strtoupper($tokens[1]) !== 'DARI') throw new Exception("Syntax: GUSUR DARI [kebun] ...");
            $table = $tokens[2];
            $i = 3;
        }

        $criteria = null;
        if ($i < count($tokens) && in_array(strtoupper($tokens[$i]), ['DIMANA', 'WHERE'])) {
            $i++;
            $criteria = $this->parseWhere($tokens, $i);
        }

        return ['type' => 'DELETE', 'table' => $table, 'criteria' => $criteria];
    }

    private function parseUpdate($tokens)
    {
        $table = '';
        $i = 0;

        if (strtoupper($tokens[0]) === 'UPDATE') {
            $table = $tokens[1];
            if (strtoupper($tokens[2]) !== 'SET') throw new Exception("Expected SET");
            $i = 3;
        } else {
            $table = $tokens[1];
            if (strtoupper($tokens[2]) !== 'DENGAN') throw new Exception("Expected DENGAN");
            $i = 3;
        }

        $updates = [];
        while ($i < count($tokens) && !in_array(strtoupper($tokens[$i]), ['DIMANA', 'WHERE'])) {
            if ($tokens[$i] === ',') { $i++; continue; }
            $key = $tokens[$i];
            if ($tokens[$i+1] !== '=') throw new Exception("Syntax: key=value");
            $val = $tokens[$i+2];
            
            if (str_starts_with($val, "'") || str_starts_with($val, '"')) $val = substr($val, 1, -1);
            else if (is_numeric($val)) $val = $val + 0;

            $updates[$key] = $val;
            $i += 3;
        }

        $criteria = null;
        if ($i < count($tokens) && in_array(strtoupper($tokens[$i]), ['DIMANA', 'WHERE'])) {
            $i++;
            $criteria = $this->parseWhere($tokens, $i);
        }

        return ['type' => 'UPDATE', 'table' => $table, 'updates' => $updates, 'criteria' => $criteria];
    }

    private function parseCreateIndex($tokens)
    {
        if (strtoupper($tokens[0]) === 'CREATE') {
            $i = 2;
            if (strtoupper($tokens[$i]) !== 'ON' && isset($tokens[$i+1]) && strtoupper($tokens[$i+1]) === 'ON') {
                $i++;
            }
            if (strtoupper($tokens[$i]) !== 'ON') throw new Exception("Syntax: CREATE INDEX ... ON [table] ...");
            $i++;
            $table = $tokens[$i];
            $i++;
            if ($tokens[$i] !== '(') throw new Exception("Syntax: ... ( [field] )");
            $i++;
            $field = $tokens[$i];
            $i++;
            if ($tokens[$i] !== ')') throw new Exception("Unclosed paren");
            return ['type' => 'CREATE_INDEX', 'table' => $table, 'field' => $field];
        }

        if (count($tokens) < 4) throw new Exception("Syntax: INDEKS [table] PADA [field]");
        $table = $tokens[1];
        if (strtoupper($tokens[2]) !== 'PADA') throw new Exception("Expected PADA");
        $field = $tokens[3];
        return ['type' => 'CREATE_INDEX', 'table' => $table, 'field' => $field];
    }

    private function parseAggregate($tokens)
    {
        $i = 1;
        $aggFunc = strtoupper($tokens[$i]);
        $i++;
        if ($tokens[$i] !== '(') throw new Exception("Syntax: HITUNG FUNC(...)");
        $i++;
        $aggField = $tokens[$i] === '*' ? null : $tokens[$i];
        $i++;
        if ($tokens[$i] !== ')') throw new Exception("Expected closing paren");
        $i++;
        if (!in_array(strtoupper($tokens[$i]), ['DARI', 'FROM'])) throw new Exception("Expected DARI/FROM");
        $i++;
        $table = $tokens[$i];
        $i++;
        
        $criteria = null;
        if ($i < count($tokens) && in_array(strtoupper($tokens[$i]), ['DIMANA', 'WHERE'])) {
            $i++;
            $criteria = $this->parseWhere($tokens, $i);
             while ($i < count($tokens) && !in_array(strtoupper($tokens[$i]), ['KELOMPOK', 'GROUP'])) {
                $i++;
            }
        }

        $groupBy = null;
        if ($i < count($tokens) && in_array(strtoupper($tokens[$i]), ['KELOMPOK', 'GROUP'])) {
            if (strtoupper($tokens[$i]) === 'GROUP' && strtoupper($tokens[$i+1]) === 'BY') {
                $i += 2;
            } else {
                $i++;
            }
            $groupBy = $tokens[$i];
        }

        return ['type' => 'AGGREGATE', 'table' => $table, 'func' => $aggFunc, 'field' => $aggField, 'criteria' => $criteria, 'groupBy' => $groupBy];
    }

    private function bindParameters(&$command, $params)
    {
        $bindValue = function($val) use ($params) {
            if (is_string($val) && str_starts_with($val, '@')) {
                $pName = substr($val, 1);
                return $params[$pName] ?? $val;
            }
            return $val;
        };

        if (isset($command['criteria'])) {
            $this->bindCriteria($command['criteria'], $bindValue);
        }

        if (isset($command['data'])) {
            foreach ($command['data'] as $k => $v) {
                $command['data'][$k] = $bindValue($v);
            }
        }
    }

    private function bindCriteria(&$criteria, $bindFunc)
    {
        if (isset($criteria['type']) && $criteria['type'] === 'compound') {
            foreach ($criteria['conditions'] as &$cond) {
                $this->bindCriteria($cond, $bindFunc);
            }
        } else {
            if (isset($criteria['val'])) {
                if (is_array($criteria['val'])) {
                    $criteria['val'] = array_map($bindFunc, $criteria['val']);
                } else {
                    $criteria['val'] = $bindFunc($criteria['val']);
                }
            }
        }
    }
}
