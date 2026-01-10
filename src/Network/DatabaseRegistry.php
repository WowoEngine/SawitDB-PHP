<?php

namespace SawitDB\Network;

use SawitDB\Engine\WowoEngine;
use Exception;

class DatabaseRegistry
{
    private $dataDir;
    private $databases = []; // name -> WowoEngine

    public function __construct($dataDir)
    {
        $this->dataDir = $dataDir;
        if (!is_dir($this->dataDir)) {
            mkdir($this->dataDir, 0777, true);
        }
    }

    private function validateName($name)
    {
        if (!$name || !is_string($name)) {
            throw new Exception("Database name required");
        }
        if (!preg_match('/^[a-zA-Z0-9_-]+$/', $name)) {
            throw new Exception("Invalid database name. Only alphanumeric, hyphen, and underscore allowed.");
        }
        return true;
    }

    public function get($name)
    {
        $this->validateName($name);
        return $this->getOrCreate($name);
    }

    public function getOrCreate($name)
    {
        if (isset($this->databases[$name])) {
            return $this->databases[$name];
        }

        $dbPath = $this->dataDir . DIRECTORY_SEPARATOR . $name . '.sawit';
        $db = new WowoEngine($dbPath);
        $this->databases[$name] = $db;
        
        return $db;
    }

    public function exists($name)
    {
        $this->validateName($name);
        return file_exists($this->dataDir . DIRECTORY_SEPARATOR . $name . '.sawit');
    }

    public function create($name)
    {
        return $this->getOrCreate($name);
    }

    public function drop($name)
    {
        $this->validateName($name);
        $dbPath = $this->dataDir . DIRECTORY_SEPARATOR . $name . '.sawit';
        
        if (!file_exists($dbPath)) {
            throw new Exception("Database '$name' does not exist");
        }

        if (isset($this->databases[$name])) {
            $this->databases[$name]->close();
            unset($this->databases[$name]);
        }
        
        // Remove WAL if exists
        if (file_exists($dbPath . '.wal')) unlink($dbPath . '.wal');
        unlink($dbPath);
        
        return true;
    }

    public function listDatabases()
    {
        $files = scandir($this->dataDir);
        $dbs = [];
        foreach ($files as $file) {
            if (str_ends_with($file, '.sawit')) {
                $dbs[] = str_replace('.sawit', '', $file);
            }
        }
        return $dbs;
    }

    public function closeAll()
    {
        foreach ($this->databases as $name => $db) {
            try {
                $db->close();
            } catch (Exception $e) {
                // log error
            }
        }
        $this->databases = [];
    }
}
