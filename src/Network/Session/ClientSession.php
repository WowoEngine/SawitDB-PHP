<?php

namespace SawitDB\Network\Session;

class ClientSession
{
    public $clientId;
    public $authenticated = false;
    public $currentDatabase = null;
    public $connectedAt;

    public function __construct($clientId)
    {
        $this->clientId = $clientId;
        $this->connectedAt = time();
    }

    public function setAuth($isAuth)
    {
        $this->authenticated = $isAuth;
    }

    public function setDatabase($dbName)
    {
        $this->currentDatabase = $dbName;
    }
}
