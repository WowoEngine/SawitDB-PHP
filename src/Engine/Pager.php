<?php

namespace SawitDB\Engine;

use Exception;

class Pager
{
    const PAGE_SIZE = 4096;
    const MAGIC = 'WOWO';

    private $filePath;
    private $fp;

    public function __construct(string $filePath)
    {
        $this->filePath = $filePath;
        $this->open();
    }

    private function open()
    {
        if (!file_exists($this->filePath)) {
            $this->fp = fopen($this->filePath, 'w+b'); // w+ creates and opens for read/write, binary mode
            $this->initNewFile();
        } else {
            $this->fp = fopen($this->filePath, 'r+b'); // r+ opens existing for read/write, binary mode
        }

        if (!$this->fp) {
            throw new Exception("Could not open file: " . $this->filePath);
        }
    }

    private function initNewFile()
    {
        // Page 0 Init
        // 0-4: MAGIC
        // 4-8: Total Pages (starts at 1)
        // 8-12: Num Tables (starts at 0)
        
        $buf = str_repeat("\0", self::PAGE_SIZE);
        
        // Write MAGIC
        $buf = substr_replace($buf, self::MAGIC, 0, strlen(self::MAGIC));
        
        // Total Pages = 1 (UInt32LE)
        $totalPages = pack('V', 1);
        $buf = substr_replace($buf, $totalPages, 4, 4);

        // Num Tables = 0 (UInt32LE)
        $numTables = pack('V', 0);
        $buf = substr_replace($buf, $numTables, 8, 4);

        fwrite($this->fp, $buf);
    }

    public function readPage(int $pageId): string
    {
        $offset = $pageId * self::PAGE_SIZE;
        fseek($this->fp, $offset);
        $buf = fread($this->fp, self::PAGE_SIZE);
        
        if ($buf === false || strlen($buf) < self::PAGE_SIZE) {
            // If read is partial or fails (e.g. asking for page that doesn't exist yet but file pointer is there),
            // return zero-filled buffer or error. 
            // In SawitDB context, we assume allocPage handled expansion, so this should ideally rely on correct usage.
            // But for safety, return padded if short read.
            if ($buf === false) $buf = "";
            return str_pad($buf, self::PAGE_SIZE, "\0");
        }
        
        return $buf;
    }

    public function writePage(int $pageId, string $buf)
    {
        if (strlen($buf) !== self::PAGE_SIZE) {
            throw new Exception("Buffer must be 4KB");
        }
        
        $offset = $pageId * self::PAGE_SIZE;
        fseek($this->fp, $offset);
        fwrite($this->fp, $buf);
        
        // Force flush logic if needed, but simple fwrite is usually enough for this toy DB
        // fsync is not directly exposed for stream resources easily without custom extensions or fflush
        fflush($this->fp);
    }

    public function allocPage(): int
    {
        // Read Page 0 to get current total pages
        $page0 = $this->readPage(0);
        $totalPages = unpack('V', substr($page0, 4, 4))[1];

        $newPageId = $totalPages;
        $newTotal = $totalPages + 1;

        // Update Page 0
        $page0 = substr_replace($page0, pack('V', $newTotal), 4, 4);
        $this->writePage(0, $page0);

        // Init new page
        $newPage = str_repeat("\0", self::PAGE_SIZE);
        // Next Page = 0 (UInt32LE at 0) - implied 0
        // Count = 0 (UInt16LE at 4) - implied 0
        // Free Offset = 8 (UInt16LE at 6)
        $newPage = substr_replace($newPage, pack('v', 8), 6, 2);
        
        $this->writePage($newPageId, $newPage);

        return $newPageId;
    }

    public function __destruct()
    {
        if ($this->fp) {
            fclose($this->fp);
        }
    }
}
