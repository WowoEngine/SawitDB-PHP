<?php

namespace SawitDB\Network;

class SawitClient
{
    private $host;
    private $port;
    private $socket;
    
    // Parsed from connection string
    private $dbName;
    private $username;
    private $password;

    public function __construct($connectionString = 'sawitdb://127.0.0.1:8765')
    {
        $this->parseConnectionString($connectionString);
        $this->connect();
    }

    private function parseConnectionString($connStr) 
    {
        // Simple parser for sawitdb://[user:pass@]host:port/database
        $parsed = parse_url($connStr);
        
        if (!isset($parsed['scheme']) || $parsed['scheme'] !== 'sawitdb') {
            // Fallback for simple host if no scheme provided, or default
             throw new \Exception("Invalid protocol. Must start with sawitdb://");
        }

        $this->host = $parsed['host'] ?? '127.0.0.1';
        $this->port = $parsed['port'] ?? 8765;
        $this->dbName = isset($parsed['path']) ? ltrim($parsed['path'], '/') : null;
        $this->username = $parsed['user'] ?? null;
        $this->password = $parsed['pass'] ?? null;
    }

    // Ensure persistent connection logic or auto-reconnect
    private function connect()
    {
        $this->socket = stream_socket_client("tcp://{$this->host}:{$this->port}", $errno, $errstr, 30);
        if (!$this->socket) {
            throw new \Exception("Connect failed: $errstr ($errno)");
        }
        stream_set_timeout($this->socket, 30);
        
        // Auto-login and select DB if present in connection string
        if ($this->username && $this->password) {
            $this->login($this->username, $this->password);
        }
        
        if ($this->dbName) {
            $this->useDatabase($this->dbName);
        }
    }

    private function sendRequest($req) {
        if (!$this->socket || feof($this->socket)) {
            $this->connect();
        }

        $json = json_encode($req);
        $len = strlen($json);

        fwrite($this->socket, pack('N', $len));
        fwrite($this->socket, $json);

        // Read Response
        $header = fread($this->socket, 4);
        if ($header === false || strlen($header) < 4) {
            // Server might have closed it
            fclose($this->socket);
            throw new \Exception("Connection lost or invalid header");
        }

        $respLen = unpack('N', $header)[1];
        $respJson = "";
        $received = 0;
        
        while ($received < $respLen) {
            $chunk = fread($this->socket, $respLen - $received);
            if ($chunk === false || $chunk === '') break;
            $respJson .= $chunk;
            $received += strlen($chunk);
        }
        
        $response = json_decode($respJson, true);
        if (!$response) throw new \Exception("Invalid JSON response");

        if ($response['status'] === 'error') {
            throw new \Exception("Server Error: " . ($response['message'] ?? 'Unknown'));
        }

        return $response;
    }

    public function login($username, $password) {
        $req = [
            'command' => 'login',
            'username' => $username,
            'password' => $password
        ];
        return $this->sendRequest($req);
    }

    public function useDatabase($dbName) {
        $req = [
            'command' => 'use',
            'db' => $dbName
        ];
        return $this->sendRequest($req);
    }

    public function query(string $sql, array $params = [])
    {
        $req = [
            'command' => 'query',
            'sql' => $sql,
            'params' => $params
        ];
        $res = $this->sendRequest($req);
        return $res['data']; 
    }
    
    public function close() {
        if ($this->socket) {
            fclose($this->socket);
            $this->socket = null;
        }
    }
    
    public function __destruct() {
        $this->close();
    }
}
