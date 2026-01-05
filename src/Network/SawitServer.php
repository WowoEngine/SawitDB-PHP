<?php

namespace SawitDB\Network;

use SawitDB\Engine\WowoEngine;


use Exception;

class SawitServer
{
    private $host;
    private $port;
    private $dataDir;
    private $auth;
    private $socket;
    private $clients = [];
    private $databases = []; // Cache of WowoEngine instances
    private $running = false;
    private $clientData = []; // Store per-client state: auth, db, buffer

    public function __construct(array $config = [])
    {
        $this->host = $config['host'] ?? '0.0.0.0';
        $this->port = $config['port'] ?? 7878;
        $this->dataDir = $config['dataDir'] ?? __DIR__ . '/../data';
        $this->auth = $config['auth'] ?? null; // ['user' => 'pass']

        if (!is_dir($this->dataDir)) {
            mkdir($this->dataDir, 0777, true);
        }
    }

    public function start()
    {
        $uri = "tcp://{$this->host}:{$this->port}";
        $this->socket = stream_socket_server($uri, $errno, $errstr);

        if (!$this->socket) {
            die("Error: $errstr ($errno)\n");
        }

        echo "╔══════════════════════════════════════════════════╗\n";
        echo "║        🌴 SawitDB Server (PHP) - v1.0            ║\n";
        echo "╚══════════════════════════════════════════════════╝\n";
        echo "[Server] Listening on {$this->host}:{$this->port}\n";

        $this->running = true;
        
        // Non-blocking mode for main socket
        stream_set_blocking($this->socket, 0);

        while ($this->running) {
            $read = array_merge([$this->socket], $this->clients);
            $write = null;
            $except = null;

            // Wait for activity
            if (stream_select($read, $write, $except, 1) === false) {
                break;
            }

            foreach ($read as $sock) {
                if ($sock === $this->socket) {
                    // New connection
                    $client = stream_socket_accept($this->socket);
                    if ($client) {
                        stream_set_blocking($client, 0);
                        $id = (int)$client;
                        $this->clients[$id] = $client;
                        $this->clientData[$id] = [
                            'auth' => empty($this->auth),
                            'db' => null,
                            'buffer' => '',
                            'addr' => stream_socket_get_name($client, true)
                        ];
                        echo "[Server] New connection from {$this->clientData[$id]['addr']}\n";
                        
                        $this->send($client, [
                            'type' => 'welcome',
                            'message' => 'SawitDB Server (PHP)',
                            'version' => '1.0',
                            'protocol' => 'sawitdb'
                        ]);
                    }
                } else {
                    // Client data
                    $data = fread($sock, 8192);
                    $id = (int)$sock;

                    if ($data === false || $data === '') {
                        $this->disconnect($id);
                    } else {
                        $this->handleData($id, $data);
                    }
                }
            }
        }
    }

    private function disconnect($id)
    {
        if (isset($this->clients[$id])) {
            echo "[Server] Client disconnected: {$this->clientData[$id]['addr']}\n";
            fclose($this->clients[$id]);
            unset($this->clients[$id]);
            unset($this->clientData[$id]);
        }
    }

    private function handleData($id, $chunk)
    {
        $this->clientData[$id]['buffer'] .= $chunk;
        
        if (strlen($this->clientData[$id]['buffer']) > 1024 * 1024) {
            $this->disconnect($id); // DoS Protection: disconnect if buffer > 1MB
            return;
        }

        while (($pos = strpos($this->clientData[$id]['buffer'], "\n")) !== false) {
            $line = substr($this->clientData[$id]['buffer'], 0, $pos);
            $this->clientData[$id]['buffer'] = substr($this->clientData[$id]['buffer'], $pos + 1);
            
            if (trim($line) === '') continue;

            $req = json_decode($line, true);
            if (!$req || !isset($req['type'])) {
                $this->sendError($this->clients[$id], "Invalid JSON or missing type");
                continue;
            }

            $this->processRequest($id, $req);
        }
    }

    private function processRequest($id, $req)
    {
        $client = $this->clients[$id];
        $state = &$this->clientData[$id];
        $type = $req['type'];
        $payload = $req['payload'] ?? [];

        // Auth Check
        if (!$state['auth'] && $type !== 'auth') {
            $this->sendError($client, "Authentication required");
            return;
        }

        try {
            switch ($type) {
                case 'auth':
                    $u = $payload['username'] ?? '';
                    $p = $payload['password'] ?? '';
                    if (isset($this->auth[$u]) && hash_equals($this->auth[$u], (string)$p)) {
                        $state['auth'] = true;
                        $this->send($client, ['type' => 'auth_success', 'message' => 'Authenticated']);
                    } else {
                        $this->sendError($client, "Invalid credentials");
                    }
                    break;

                case 'use':
                    $db = $payload['database'] ?? '';
                    if (!preg_match('/^[a-zA-Z0-9_-]+$/', $db) || $db === '.' || $db === '..') {
                        $this->sendError($client, "Invalid database name");
                        break;
                    }
                    
                    // Canonical path check
                    $targetPath = realpath($this->dataDir) . DIRECTORY_SEPARATOR . $db . '.sawit';
                    // Note: realpath($targetPath) only works if file exists. SawitDB creates on fly.
                    // But since we strictly validate regex above, we are safe from traversal chars.
                    
                    // Ensure DB exists or init? SawitDB creates on open.
                    // Just set state, lazy load in query
                    $state['db'] = $db;
                    $this->send($client, ['type' => 'use_success', 'database' => $db, 'message' => "Switched to $db"]);
                    break;

                case 'list_databases':
                    $dbs = [];
                    foreach (glob($this->dataDir . '/*.sawit') as $f) {
                        $dbs[] = basename($f, '.sawit');
                    }
                    $this->send($client, ['type' => 'database_list', 'databases' => $dbs, 'count' => count($dbs)]);
                    break;

                case 'query':
                    if (!$state['db']) {
                        $this->sendError($client, "No database selected. Use 'MASUK WILAYAH' or .use");
                        break;
                    }
                    $this->handleQuery($id, $payload);
                    break;

                case 'ping':
                    $this->send($client, ['type' => 'pong', 'timestamp' => floor(microtime(true) * 1000)]);
                    break;

                case 'stats':
                    $this->send($client, ['type' => 'stats', 'stats' => [
                        'connections' => count($this->clients),
                        'memory_usage' => memory_get_usage(true)
                    ]]);
                    break;

                default:
                    $this->sendError($client, "Unknown command: $type");
            }
        } catch (Exception $e) {
            $this->sendError($client, "Server Error: " . $e->getMessage());
        }
    }

    private function handleQuery($id, $payload)
    {
        $client = $this->clients[$id];
        $state = $this->clientData[$id];
        $query = $payload['query'] ?? '';
        $params = $payload['params'] ?? [];

        $dbName = $state['db'];
        $dbPath = $this->dataDir . '/' . $dbName . '.sawit';

        if (!isset($this->databases[$dbName])) {
            $this->databases[$dbName] = new WowoEngine($dbPath);
        }

        $engine = $this->databases[$dbName];
        $start = microtime(true);
        $result = $engine->query($query, $params);
        $dur = (microtime(true) - $start) * 1000;

        $this->send($client, [
            'type' => 'query_result',
            'result' => $result,
            'query' => $query,
            'executionTime' => $dur
        ]);
    }

    private function send($client, $data)
    {
        @fwrite($client, json_encode($data) . "\n");
    }

    private function sendError($client, $msg)
    {
        $this->send($client, ['type' => 'error', 'error' => $msg]);
    }
}
