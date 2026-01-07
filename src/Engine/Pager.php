<?php

namespace SawitDB\Engine;

use Exception;

class Pager
{
    const PAGE_SIZE = 4096;
    const MAGIC = 'WOWO';

    private $filePath;
    private $fp;

    // Caches
    private $cache = []; // PageID -> Binary String (LRU implicitly via array order)
    private $cacheLimit = 15000;
    
    // Object Cache
    private $objectCache = []; // PageID -> ['next' => int, 'items' => array]
    private $dirtyObjects = []; // PageID -> bool

    private $dirtyPages = []; // PageID -> bool
    private $lazyWrite = true;

    public function __construct(string $filePath)
    {
        $this->filePath = $filePath;
        $this->open();
    }

    private function open()
    {
        if (!file_exists($this->filePath)) {
            $this->fp = fopen($this->filePath, 'c+b'); // c+ ideal for read/write without truncating
            if (!$this->fp) $this->fp = fopen($this->filePath, 'w+b'); // Fallback
            $this->initNewFile();
        } else {
            $this->fp = fopen($this->filePath, 'r+b');
        }

        if (!$this->fp) {
            throw new Exception("Could not open file: " . $this->filePath);
        }
    }

    private function initNewFile()
    {
        $buf = str_repeat("\0", self::PAGE_SIZE);
        $buf = substr_replace($buf, self::MAGIC, 0, strlen(self::MAGIC));
        $totalPages = pack('V', 1);
        $buf = substr_replace($buf, $totalPages, 4, 4);
        $numTables = pack('V', 0);
        $buf = substr_replace($buf, $numTables, 8, 4);
        fwrite($this->fp, $buf);
    }

    /**
     * OPTIMIZATION: Read page as Objects (Zero-Copy-ish for PHP usersland?)
     * Returns: ['next' => int, 'items' => array]
     */
    public function readPageObjects(int $pageId): array
    {
        if (isset($this->objectCache[$pageId])) {
            // LRU Logic: Move to end
            $entry = $this->objectCache[$pageId];
            unset($this->objectCache[$pageId]);
            $this->objectCache[$pageId] = $entry;
            return $entry;
        }

        $buffer = $this->readPage($pageId);

        // Parse Header
        // 0-4: Next Page ID (UInt32LE)
        // 4-6: Count (UInt16LE)
        // 6-8: Free Offset (UInt16LE) - Not needed for read, but consistent
        $header = unpack('Vnext/vcount', $buffer);
        $next = $header['next'];
        $count = $header['count'];

        $items = [];
        $offset = 8;

        for ($i = 0; $i < $count; $i++) {
            $len = unpack('v', substr($buffer, $offset, 2))[1];
            $jsonStr = substr($buffer, $offset + 2, $len);
            
            // Optimization: Maybe don't decode if not needed? 
            // But caller expects objects.
            $obj = json_decode($jsonStr, true); // Associative arrays
            if ($obj !== null) {
                $items[] = $obj;
            }
            $offset += 2 + $len;
        }

        $entry = ['next' => $next, 'items' => $items];
        
        // Cache it
        $this->objectCache[$pageId] = $entry;
        
        // Manage Size? PHP arrays can grow huge. 
        // For now simple limit if needed or trust PHP GC.
        return $entry;
    }

    public function readPage(int $pageId): string
    {
        // Coherency: If dirty objects, serialize first
        if (isset($this->dirtyObjects[$pageId])) {
            $this->serializeObjectsToBuffer($pageId);
        }

        if (isset($this->cache[$pageId])) {
            // LRU Move to end
            $buf = $this->cache[$pageId];
            unset($this->cache[$pageId]);
            $this->cache[$pageId] = $buf;
            return $buf;
        }

        $offset = $pageId * self::PAGE_SIZE;
        fseek($this->fp, $offset);
        $buf = fread($this->fp, self::PAGE_SIZE);
        
        if ($buf === false || strlen($buf) < self::PAGE_SIZE) {
            if ($buf === false) $buf = "";
            $buf = str_pad($buf, self::PAGE_SIZE, "\0");
        }
        
        $this->cache[$pageId] = $buf;
        $this->enforceLimit();
        
        return $buf;
    }

    public function writePage(int $pageId, string $buf)
    {
        if (strlen($buf) !== self::PAGE_SIZE) {
            throw new Exception("Buffer must be 4KB");
        }
        
        $this->cache[$pageId] = $buf;
        
        // Invalidate Object Cache (it's stale now)
        unset($this->objectCache[$pageId]);
        unset($this->dirtyObjects[$pageId]);

        if ($this->lazyWrite) {
            $this->dirtyPages[$pageId] = true;
        } else {
            $this->flushPage($pageId);
        }
    }

    private function serializeObjectsToBuffer(int $pageId)
    {
        if (!isset($this->objectCache[$pageId])) return;

        $entry = $this->objectCache[$pageId];
        $buffer = str_repeat("\0", self::PAGE_SIZE);

        // Header
        $buffer = substr_replace($buffer, pack('V', $entry['next']), 0, 4);
        $buffer = substr_replace($buffer, pack('v', count($entry['items'])), 4, 2);

        $offset = 8;
        foreach ($entry['items'] as $obj) {
            $jsonStr = json_encode($obj);
            $len = strlen($jsonStr);

            if ($offset + 2 + $len > self::PAGE_SIZE) break; // Should not happen if managed correctly

            $buffer = substr_replace($buffer, pack('v', $len), $offset, 2);
            $buffer = substr_replace($buffer, $jsonStr, $offset + 2, $len);
            $offset += 2 + $len;
        }

        // Update Free Offset
        $buffer = substr_replace($buffer, pack('v', $offset), 6, 2);

        $this->cache[$pageId] = $buffer;
        unset($this->dirtyObjects[$pageId]);
        
        // Mark page as dirty so it gets written to disk
        $this->dirtyPages[$pageId] = true;
    }

    // Helper for Engine to mark objects modified
    public function markObjectsDirty(int $pageId)
    {
        $this->dirtyObjects[$pageId] = true;
    }

    // Helper to update objects directly (avoids full writePage from Engine)
    public function updatePageObjects(int $pageId, int $next, array $items)
    {
        $this->objectCache[$pageId] = ['next' => $next, 'items' => $items];
        $this->dirtyObjects[$pageId] = true;
        // Invalidate raw cache if we rely on objects source of truth
        // Actually serializeObjectsToBuffer will overwrite cache[$pageId]
    }

    private function flushPage(int $pageId)
    {
        if (isset($this->dirtyObjects[$pageId])) {
            $this->serializeObjectsToBuffer($pageId);
        }

        if (!isset($this->cache[$pageId])) return;

        $buf = $this->cache[$pageId];
        $offset = $pageId * self::PAGE_SIZE;
        
        fseek($this->fp, $offset);
        $written = fwrite($this->fp, $buf);
        
        if ($written !== self::PAGE_SIZE) {
            // Retry or throw
            // Try explicit flush and retry
            fflush($this->fp);
            $written = fwrite($this->fp, $buf);
             if ($written !== self::PAGE_SIZE) {
                 throw new Exception("Disk Write Failed at Page $pageId");
             }
        }
        // Force flush per page for safety during basic file mode? 
        // Or rely on batch flush.
        // Let's rely on batch flush() but ensure strict write check.
        
        unset($this->dirtyPages[$pageId]);
    }

    public function flush()
    {
        // Flush all dirty
        foreach ($this->dirtyObjects as $pid => $val) {
            $this->serializeObjectsToBuffer($pid);
        }
        
        // Sort pages for sequential write performance? (Optional)
        $pages = array_keys($this->dirtyPages);
        sort($pages);
        
        foreach ($pages as $pid) {
            $this->flushPage($pid);
        }
        
        // Also flush file buffer
        fflush($this->fp);
    }

    public function allocPage(): int
    {
        // Read Page 0 to get current total pages
        // Bypass cache check for atomic-like operation? No, cache is fine.
        $page0 = $this->readPage(0); // This might come from cache
        $totalPages = unpack('V', substr($page0, 4, 4))[1];

        $newPageId = $totalPages;
        $newTotal = $totalPages + 1;

        // Update Page 0
        $page0 = substr_replace($page0, pack('V', $newTotal), 4, 4);
        $this->writePage(0, $page0);

        // Init new page
        $newPage = str_repeat("\0", self::PAGE_SIZE);
        $newPage = substr_replace($newPage, pack('v', 8), 6, 2); // Free Offset = 8
        
        $this->writePage($newPageId, $newPage);

        return $newPageId;
    }

    private function enforceLimit()
    {
        while (count($this->cache) > $this->cacheLimit) {
            // Get first key (Oldest)
            $pageId = array_key_first($this->cache);
            
            if (isset($this->dirtyPages[$pageId]) || isset($this->dirtyObjects[$pageId])) {
                $this->flushPage($pageId);
            }
            
            unset($this->cache[$pageId]);
            unset($this->objectCache[$pageId]);
            unset($this->dirtyObjects[$pageId]);
        }
    }

    public function close()
    {
        if ($this->fp) {
            $this->flush();
            fclose($this->fp);
            $this->fp = null;
        }
    }

    public function __destruct()
    {
        if ($this->fp) {
            $this->flush();
            fclose($this->fp);
        }
    }
}
