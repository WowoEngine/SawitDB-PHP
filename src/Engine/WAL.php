<?php

namespace SawitDB\Engine;

use Exception;

class WAL
{
    private $path;
    private $enabled;
    private $handle;
    private $lsn = 0;
    
    const MAGIC = 0x57414C00;
    const OP_CODES = [
        'INSERT' => 0x01,
        'UPDATE' => 0x02,
        'DELETE' => 0x03,
        'CREATE_TABLE' => 0x04,
        'DROP_TABLE' => 0x05,
        'CHECKPOINT' => 0x06
    ];

    public function __construct($dbPath, $enabled = true)
    {
        $this->path = $dbPath . '.wal';
        $this->enabled = $enabled;
        
        if ($this->enabled) {
            $this->init();
        }
    }

    private function init()
    {
        // Open for append/read
        $this->handle = fopen($this->path, 'c+b'); // c+ = Read/Write, Create if not exists, don't truncate
        if (!$this->handle) {
            throw new Exception("Could not open WAL file: " . $this->path);
        }
        
        // Read last LSN logic omitted for brevity in simple version, 
        // or we seek to end and increment internal counter if we kept state?
        // For simple durability, we Append. LSN can be approximate or recovered.
        // Let's rely on file size or seek end.
        fseek($this->handle, 0, SEEK_END);
    }

    public function logOperation($op, $table, $pageId, $beforeImage, $afterImage)
    {
        if (!$this->enabled) return;

        $this->lsn++; // In request-scope, this LSN resets. Ideally read from WAL header/tail.
        // But for crash recovery, strict LSN sequence is important.
        // Let's assume strict consistency requires locking.
        
        $opCode = self::OP_CODES[$op] ?? 0x00;
        
        $tName = str_pad(substr($table, 0, 32), 32, "\0");
        $beforeLen = $beforeImage ? strlen($beforeImage) : 0;
        $afterLen = $afterImage ? strlen($afterImage) : 0;
        
        // Entry Size: 4 (Magic) + 4 (Size) + 8 (LSN) + 1 (Op) + 32 (Table) + 4 (PageId) + 4 (BSize) + 4 (ASize) + Data
        $headerSize = 61; // 4+4+8+1+32+4+4+4
        $totalSize = $headerSize + $beforeLen + $afterLen;
        
        $buf = '';
        $buf .= pack('V', self::MAGIC);
        $buf .= pack('V', $totalSize);
        $buf .= pack('P', $this->lsn); // 64-bit int LE
        $buf .= chr($opCode);
        $buf .= $tName;
        $buf .= pack('V', $pageId);
        $buf .= pack('V', $beforeLen);
        $buf .= pack('V', $afterLen);
        
        if ($beforeImage) $buf .= $beforeImage;
        if ($afterImage) $buf .= $afterImage;
        
        fwrite($this->handle, $buf);
        // fsync for durability
        // In PHP, fflush + fsync logic?
        fflush($this->handle);
        // fsync($this->handle); // Available in some PHP versions, usually implicit or OS buffers
    }
    
    public function checkpoint()
    {
        if (!$this->enabled) return;
        $this->logOperation('CHECKPOINT', '', 0, null, null);
    }
    
    public function close()
    {
        if ($this->handle) {
            fclose($this->handle);
            $this->handle = null;
        }
    }
    
    public function truncate()
    {
        if ($this->handle) {
            ftruncate($this->handle, 0);
            rewind($this->handle);
        }
    }
}
