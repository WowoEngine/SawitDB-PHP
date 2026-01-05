<?php

namespace SawitDB\Network;

use Exception;

class SawitClient
{
    private $socket;
    private $connected = false;
    private $host;
    private $port;
    private $requestId = 0;



    public $currentDatabase = null;
    private $user;
    private $pass;
    private $db;

    public function __construct(string $connStr)
    {
        $this->parseConnectionString($connStr);
    }

    private function parseConnectionString($connStr)
    {
        // sawitdb://user:pass@host:port/db
        $parts = parse_url($connStr);
        
        $this->host = $parts['host'] ?? 'localhost';
        $this->port = $parts['port'] ?? 7878;
        $this->user = $parts['user'] ?? null;
        $this->pass = $parts['pass'] ?? null;
        $this->db   = isset($parts['path']) ? ltrim($parts['path'], '/') : null;
    }

    public function connect()
    {
        $uri = "tcp://{$this->host}:{$this->port}";
        $this->socket = stream_socket_client($uri, $errno, $errstr, 10);
        
        if (!$this->socket) {
            throw new Exception("Connection failed: $errstr ($errno)");
        }
        
        $this->connected = true;
        
        // Read Welcome
        $welcome = $this->readResponse();
        if ($welcome['type'] !== 'welcome') {
            throw new Exception("Invalid handshake");
        }
        
        // Auth
        if ($this->user) {
            $this->send([
                'type' => 'auth', 
                'payload' => ['username' => $this->user, 'password' => $this->pass]
            ]);
            $res = $this->readResponse();
            if ($res['type'] === 'error') throw new Exception("Auth failed: " . $res['error']);
        }
        
        // Select DB
        if ($this->db) {
            $this->use($this->db);
        }
    }

    public function use($db)
    {
        $this->send(['type' => 'use', 'payload' => ['database' => $db]]);
        $res = $this->readResponse();
        if ($res['type'] === 'error') throw new Exception($res['error']);
        $this->currentDatabase = $db;
        return $res;
    }

    public function query($sql, $params = [])
    {
        $this->send([
            'type' => 'query',
            'payload' => ['query' => $sql, 'params' => $params]
        ]);
        
        $res = $this->readResponse();
        
        if ($res['type'] === 'query_result') {
            return $res['result'];
        } elseif ($res['type'] === 'error') {
            throw new Exception($res['error']);
        }
        return $res;
    }

    public function listDatabases()
    {
        $this->send(['type' => 'list_databases']);
        $res = $this->readResponse();
        if ($res['type'] === 'database_list') return $res['databases'];
        throw new Exception($res['error'] ?? 'Unknown error');
    }

    public function ping()
    {
        $start = microtime(true);
        $this->send(['type' => 'ping']);
        $res = $this->readResponse();
        $lat = (microtime(true) - $start) * 1000;
        return ['latency' => $lat, 'server_time' => $res['timestamp'] ?? 0];
    }
    
    public function stats()
    {
        $this->send(['type' => 'stats']);
        $res = $this->readResponse();
        if ($res['type'] === 'stats') return $res['stats'];
        throw new Exception($res['error'] ?? 'Unknown error');
    }

    private function send($req)
    {
        fwrite($this->socket, json_encode($req) . "\n");
    }

    private function readResponse()
    {
        $line = fgets($this->socket);
        if ($line === false) throw new Exception("Server disconnected");
        return json_decode($line, true);
    }
    
    public function disconnect()
    {
        if ($this->socket) fclose($this->socket);
        $this->connected = false;
    }
}
