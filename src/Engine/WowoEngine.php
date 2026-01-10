<?php

namespace SawitDB\Engine;

use Exception;
use SawitDB\Engine\Services\Events\DBEventHandler;
use SawitDB\Engine\Services\ConditionEvaluator;
use SawitDB\Engine\Services\JoinProcessor;
use SawitDB\Engine\Services\Managers\TableManager;
use SawitDB\Engine\Services\Managers\IndexManager;
use SawitDB\Engine\Services\Executors\SelectExecutor;
use SawitDB\Engine\Services\Executors\InsertExecutor;
use SawitDB\Engine\Services\Executors\UpdateExecutor;
use SawitDB\Engine\Services\Executors\DeleteExecutor;
use SawitDB\Engine\Services\Executors\AggregateExecutor;

class WowoEngine
{
    private Pager $pager;
    public array $indexes = []; // 'table.field' -> BTreeIndex instance. Public for IndexManager Access.
    private QueryParser $parser;
    private WAL $wal;
    
    // Query Cache
    private array $queryCache = [];
    private int $queryCacheLimit = 1000;

    // Services
    private DBEventHandler $eventHandler;
    private ConditionEvaluator $conditionEvaluator;
    private JoinProcessor $joinProcessor;
    private TableManager $tableManager;
    private IndexManager $indexManager;

    // Executors
    private SelectExecutor $selectExecutor;
    private InsertExecutor $insertExecutor;
    private UpdateExecutor $updateExecutor;
    private DeleteExecutor $deleteExecutor;
    private AggregateExecutor $aggregateExecutor;

    public function __construct(string $filePath)
    {
        $this->pager = new Pager($filePath);
        $this->parser = new QueryParser();
        $this->wal = new WAL($filePath);
        
        // Initialize Services
        $this->eventHandler = new DBEventHandler(); // Configurable via logic later
        $this->conditionEvaluator = new ConditionEvaluator();
        $this->joinProcessor = new JoinProcessor($this); // Pass DB if needed, logic currently static-ish but let's keep pattern
        $this->tableManager = new TableManager($this);
        $this->indexManager = new IndexManager($this);

        // Initialize Executors
        $this->selectExecutor = new SelectExecutor($this);
        $this->insertExecutor = new InsertExecutor($this);
        $this->updateExecutor = new UpdateExecutor($this);
        $this->deleteExecutor = new DeleteExecutor($this);
        $this->aggregateExecutor = new AggregateExecutor($this);

        // Persistence: Initialize System Tables via WowoEngine logic or Manager?
        $this->initSystem();
    }

    private function initSystem()
    {
        // Bootstrapping: Check _indexes existence
        if (!$this->tableManager->findTableEntry('_indexes')) {
            try {
                // Manually create system table or use manager if safe
                // The manager uses Pager directly, so it's safe.
                $this->tableManager->createTable('_indexes');
            } catch (Exception $e) { /* ignore */ }
        }
        $this->indexManager->loadIndexes();
    }
    
    // --- Accessors for Services ---
    public function getPager(): Pager { return $this->pager; }
    public function getWAL(): WAL { return $this->wal; }
    public function getEventHandler(): DBEventHandler { return $this->eventHandler; }
    public function getTableManager(): TableManager { return $this->tableManager; }
    public function getIndexManager(): IndexManager { return $this->indexManager; }
    public function getConditionEvaluator(): ConditionEvaluator { return $this->conditionEvaluator; }
    public function getJoinProcessor(): JoinProcessor { return $this->joinProcessor; }
    public function getSelectExecutor(): SelectExecutor { return $this->selectExecutor; }
    public function getInsertExecutor(): InsertExecutor { return $this->insertExecutor; }
    public function getUpdateExecutor(): UpdateExecutor { return $this->updateExecutor; }
    public function getDeleteExecutor(): DeleteExecutor { return $this->deleteExecutor; }
    public function getAggregateExecutor(): AggregateExecutor { return $this->aggregateExecutor; }

    public function close()
    {
        $this->pager->close();
    }

    public function query(string $query, array $params = [])
    {
        // Query Cache (Simple LRU)
        $cacheKey = $query;
        $cmd = null;
        if (isset($this->queryCache[$cacheKey])) {
            $cmd = $this->queryCache[$cacheKey]; // Copy array
        } else {
            // Parse without params to get template
            $cmd = $this->parser->parse($query, []);
            if ($cmd['type'] !== 'ERROR') {
                $this->queryCache[$cacheKey] = $cmd;
                if (count($this->queryCache) > $this->queryCacheLimit) {
                    array_shift($this->queryCache);
                }
            }
        }
        
        if ($cmd['type'] === 'EMPTY') return "";
        if ($cmd['type'] === 'ERROR') return "Error: " . $cmd['message'];

        if (!empty($params)) {
             $cmd = $this->parser->parse($query, $params);
        }

        try {
            switch ($cmd['type']) {
                case 'CREATE_TABLE':
                    return $this->tableManager->createTable($cmd['table']);
                case 'SHOW_TABLES':
                    return $this->tableManager->showTables();
                case 'SHOW_INDEXES':
                    return $this->indexManager->showIndexes($cmd['table'] ?? null);
                case 'INSERT':
                    return $this->insertExecutor->execute($cmd['table'], $cmd['data']);
                case 'SELECT':
                     // Executor returns raw arrays. Query method structures response if needed?
                     // SelectExecutor already returns array of rows.
                     $rows = $this->selectExecutor->execute($cmd);
                     
                     // Helper: Strip system fields if SELECT *
                     // Move this logic to Executor or keep here?
                     // Executor returns "clean" results or raw?
                     // Executor logic currently includes JoinProcessor which likely keeps _rid?
                     // Let's verify SelectExecutor logic.
                     
                     // In SelectExecutor, results might contain _rid.
                     // The JS format: SelectExecutor returns results. 
                     // We should process projection here or in Executor?
                     // JS SelectExecutor handles projection usually.
                     // Our SelectExecutor just returns filtered rows.
                     
                     $cleanRows = array_map(function($r) {
                         unset($r['_rid']);
                         return $r;
                     }, $rows);
                     
                     if (count($cmd['cols']) === 1 && $cmd['cols'][0] === '*') return $cleanRows;
                     
                     return array_map(function($r) use ($cmd) {
                         $newRow = [];
                         foreach ($cmd['cols'] as $c) {
                             $newRow[$c] = $r[$c] ?? null;
                         }
                         return $newRow;
                     }, $cleanRows);

                case 'DELETE':
                    return $this->deleteExecutor->execute($cmd['table'], $cmd['criteria']);
                case 'UPDATE':
                    return $this->updateExecutor->execute($cmd['table'], $cmd['updates'], $cmd['criteria']);
                case 'DROP_TABLE':
                    return $this->tableManager->dropTable($cmd['table']);
                case 'CREATE_INDEX':
                    return $this->indexManager->createIndex($cmd['table'], $cmd['field']);
                case 'AGGREGATE':
                    return $this->aggregateExecutor->execute($cmd);
                default:
                    return "Perintah tidak dikenal.";
            }
        } catch (Exception $e) {
            return "Error: " . $e->getMessage();
        }
    }

    // Proxy Methods for backward compatibility or ease of use for Executors
    // Executors can use $db->scanTable(...)
    public function scanTable(array $entry, $criteria)
    {
        $currentPageId = $entry['startPage'];
        $results = [];

        while ($currentPageId !== 0) {
            $pageData = $this->pager->readPageObjects($currentPageId); // ['next'=>, 'items'=>]
            
            foreach ($pageData['items'] as $idx => $obj) {
                if ($this->conditionEvaluator->checkMatch($obj, $criteria)) {
                    $obj['_rid'] = "$currentPageId:$idx"; 
                    $results[] = $obj;
                }
            }
            
            $currentPageId = $pageData['next'];
        }
        return $results;
    }
    
    // Convenience for Executors
    public function insert($table, $data) {
        return $this->insertExecutor->execute($table, $data);
    }
    
    public function delete($table, $criteria) {
        return $this->deleteExecutor->execute($table, $criteria);
    }
}
