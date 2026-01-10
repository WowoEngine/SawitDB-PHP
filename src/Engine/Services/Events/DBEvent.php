<?php

namespace SawitDB\Engine\Services\Events;

interface DBEvent
{
    public function OnTableSelected($table, $data, $aql);
    public function OnTableUpdated($table, $data, $aql);
    public function OnTableDeleted($table, $data, $aql);
    public function OnTableInserted($table, $data, $aql);
    public function OnTableCreated($table, $data, $aql);
    public function OnTableDropped($table, $data, $aql);
}
