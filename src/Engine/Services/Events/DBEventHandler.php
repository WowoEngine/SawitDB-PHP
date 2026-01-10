<?php

namespace SawitDB\Engine\Services\Events;

class DBEventHandler implements DBEvent
{
    private $adapter;
    private $logFile;

    public function __construct($adapter = 'default', $logFile = 'cdc.log')
    {
        $this->adapter = $adapter;
        $this->logFile = $logFile;
    }

    private function writeCDC($aql)
    {
        $log = $aql . PHP_EOL;
        // Basic append, non-blocking if possible but typically blocking in simple PHP
        file_put_contents($this->logFile, $log, FILE_APPEND);
    }

    public function OnTableSelected($table, $data, $aql)
    {
        if ($this->adapter === 'cpo') $this->writeCDC($aql);
    }

    public function OnTableUpdated($table, $data, $aql)
    {
        if ($this->adapter === 'cpo') $this->writeCDC($aql);
    }

    public function OnTableDeleted($table, $data, $aql)
    {
        if ($this->adapter === 'cpo') $this->writeCDC($aql);
    }

    public function OnTableInserted($table, $data, $aql)
    {
        if ($this->adapter === 'cpo') $this->writeCDC($aql);
    }

    public function OnTableCreated($table, $data, $aql)
    {
        if ($this->adapter === 'cpo') $this->writeCDC($aql);
    }

    public function OnTableDropped($table, $data, $aql)
    {
        if ($this->adapter === 'cpo') $this->writeCDC($aql);
    }
}
