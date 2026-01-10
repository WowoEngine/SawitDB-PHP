<?php

namespace SawitDB\Network;

use SawitDB\Engine\WowoEngine;
use SawitDB\Network\DatabaseRegistry;
use SawitDB\Network\Auth\AuthManager;
use SawitDB\Network\Session\ClientSession;
use Exception;

class SawitServer
{
    private $host;
    private $port;
    private $socket;
    private $registry;
    private $authManager;
    private $running = true;
    
    private $clients = []; // Resource ID => Socket Resource
    private $sessions = []; // Resource ID => ClientSession

    public function __construct(DatabaseRegistry $registry, AuthManager $authManager, $host = '127.0.0.1', $port = 8765)
    {
        $this->registry = $registry;
        $this->authManager = $authManager;
        $this->host = $host;
        $this->port = $port;
    }

    public function start()
    {
        $address = "tcp://{$this->host}:{$this->port}";
        $context = stream_context_create(['socket' => ['so_reuseport' => true]]); 
        if (PHP_OS_FAMILY === 'Windows') {
             $context = stream_context_create([]);
        }

        $this->socket = stream_socket_server($address, $errno, $errstr, STREAM_SERVER_BIND|STREAM_SERVER_LISTEN, $context);

        if (!$this->socket) {
            die("Could not start server: $errstr ($errno)\n");
        }
        
        stream_set_blocking($this->socket, 0);

        echo "SawitDB Server running at $address\n";
        echo "Protocol: sawitdb://{$this->host}:{$this->port}/[database]\n";

        while ($this->running) {
            $read = $this->clients;
            $read[] = $this->socket; // Add listening socket
            $write = null;
            $except = null;
            
            // Wait for activity
            if (stream_select($read, $write, $except, 1) < 1) {
                continue;
            }
            
            // Check for new connection
            if (in_array($this->socket, $read)) {
                $client = @stream_socket_accept($this->socket);
                if ($client) {
                    stream_set_blocking($client, 0); // Non-blocking
                    $id = (int)$client;
                    $this->clients[$id] = $client;
                    $this->sessions[$id] = new ClientSession($id);
                    // echo "New connection: $id\n";
                }
                
                // Remove master socket from processing list
                $key = array_search($this->socket, $read);
                unset($read[$key]);
            }
            
            // Process Clients
            foreach ($read as $client) {
                $this->handleClientInput($client);
            }
        }
    }

    private function handleClientInput($client)
    {
        $id = (int)$client;
        if (!isset($this->sessions[$id])) return;
        
        $session = $this->sessions[$id];
        
        // Read Header (4 bytes)
        // In non-blocking mode, we must ensure we have enough data.
        // For simplicity in this demo, we assume packet arrives or we block slightly?
        // Correct way is buffering. Let's do a simple read attempt.
        
        $header = fread($client, 4);
        
        if ($header === false || strlen($header) === 0) {
            // Disconnected?
            if (feof($client)) {
                $this->disconnect($client);
            }
            return;
        }
        
        // If partial header, loop wait (naive) or buffer (better).
        // Naive blocking read for remainder
        while (strlen($header) < 4) {
            $chunk = fread($client, 4 - strlen($header));
            if ($chunk === false || strlen($chunk) === 0) {
                 if (feof($client)) { $this->disconnect($client); return; }
                 usleep(100);
            } else {
                $header .= $chunk;
            }
        }

        $len = unpack('N', $header)[1];
        if ($len > 1024 * 1024 * 10) { 
            $this->disconnect($client);
            return;
        }

        $jsonStr = "";
        $received = 0;
        
        // Blocking read for body (simple implementation)
        // Set blocking temporarily to ensure we get the command?
        // Or keep spinning? Spinning is CPU intensive. 
        // Let's set blocking for the body to ensure we don't partial read state complexly.
        stream_set_blocking($client, 1);
        
        while ($received < $len) {
            $chunk = fread($client, $len - $received);
            if ($chunk === false || $chunk === '') {
                 // Error
                 break; 
            }
            $jsonStr .= $chunk;
            $received += strlen($chunk);
        }
        
        stream_set_blocking($client, 0); // Reset

        if ($received !== $len) {
            $this->disconnect($client);
            return;
        }

        $request = json_decode($jsonStr, true);
        $response = ['status' => 'error', 'data' => null];

        if ($request && isset($request['command'])) {
             $cmd = strtolower($request['command']);
             
             if ($this->authManager->isEnabled() && !$session->authenticated && $cmd !== 'login') {
                 $response = ['status' => 'error', 'message' => 'Authentication required'];
             } else {
                 try {
                     switch ($cmd) {
                         case 'login':
                             $username = $request['username'] ?? '';
                             $password = $request['password'] ?? '';
                             if ($this->authManager->authenticate($username, $password)) {
                                 $session->setAuth(true);
                                 $response = ['status' => 'ok', 'message' => 'Logged in'];
                             } else {
                                 $response = ['status' => 'error', 'message' => 'Invalid credentials'];
                             }
                             break;
                             
                         case 'use':
                             $dbName = $request['db'] ?? '';
                             try {
                                 // Trigger create/load
                                 $this->registry->getOrCreate($dbName);
                                 $session->setDatabase($dbName);
                                 $response = ['status' => 'ok', 'message' => "Switched to database $dbName"];
                             } catch (Exception $e) {
                                  $response = ['status' => 'error', 'message' => $e->getMessage()];
                             }
                             break;
                             
                         case 'query':
                             $dbName = $session->currentDatabase;
                             if (!$dbName) {
                                 $response = ['status' => 'error', 'message' => 'No database selected. USE [db_name] first.'];
                             } else {
                                 $db = $this->registry->get($dbName);
                                 $sql = $request['sql'];
                                 $params = $request['params'] ?? [];
                                 $result = $db->query($sql, $params);
                                 $response = ['status' => 'ok', 'data' => $result];
                             }
                             break;
                             
                         default:
                             $response = ['status' => 'error', 'message' => 'Unknown command'];
                     }
                 } catch (Exception $e) {
                     $response = ['status' => 'error', 'message' => $e->getMessage()];
                 }
             }
        }

        $respJson = json_encode($response);
        $respLen = strlen($respJson);
        
        @fwrite($client, pack('N', $respLen));
        @fwrite($client, $respJson);
    }
    
    private function disconnect($client)
    {
        $id = (int)$client;
        if (isset($this->clients[$id])) {
            unset($this->clients[$id]);
            unset($this->sessions[$id]);
            @fclose($client);
        }
    }
}
