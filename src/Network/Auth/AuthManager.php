<?php

namespace SawitDB\Network\Auth;

class AuthManager
{
    private $users = [];
    private $enabled = false;

    public function __construct(array $config = [])
    {
        if (!empty($config['users'])) {
            $this->enabled = true;
            $this->users = $config['users'];
        }
    }

    public function isEnabled()
    {
        return $this->enabled;
    }

    /**
     * Hash password using SHA-256 with salt
     */
    public static function hashPassword($password, $salt = null)
    {
        if (!$salt) {
            $salt = bin2hex(random_bytes(16));
        }
        $hash = hash('sha256', $salt . $password);
        return "$salt:$hash";
    }

    public function verifyPassword($password, $storedHash)
    {
        if (strpos($storedHash, ':') !== false) {
            list($salt, $hash) = explode(':', $storedHash);
            $computedHash = hash('sha256', $salt . $password);
            
            return hash_equals($hash, $computedHash);
        } else {
            // Legacy plaintext fallback (not recommended but supported for parity if JS has it)
            return hash_equals($storedHash, $password);
        }
    }

    public function authenticate($username, $password)
    {
        if (!$this->enabled) return true;

        if (!isset($this->users[$username])) return false;
        
        $stored = $this->users[$username];
        return $this->verifyPassword($password, $stored);
    }
}
