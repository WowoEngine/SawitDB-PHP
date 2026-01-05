<?php

namespace SawitDB\Engine;

class BTreeNode
{
    public $isLeaf;
    public $keys = [];
    public $values = []; // For leaf nodes: array of record references
    public $children = []; // For internal nodes: array of BTreeNode

    public function __construct(bool $isLeaf = true)
    {
        $this->isLeaf = $isLeaf;
    }
}

class BTreeIndex
{
    private $order;
    private $root;
    public $name;
    public $keyField;

    public function __construct(int $order = 32)
    {
        $this->order = $order;
        $this->root = new BTreeNode(true);
        $this->name = null;
        $this->keyField = null;
    }

    public function insert($key, $value)
    {
        $root = $this->root;

        // If root is full, split it
        if (count($root->keys) >= $this->order) {
            $newRoot = new BTreeNode(false);
            $newRoot->children[] = $this->root;
            $this->splitChild($newRoot, 0);
            $this->root = $newRoot;
            $this->insertNonFull($this->root, $key, $value);
        } else {
            $this->insertNonFull($this->root, $key, $value);
        }
    }

    private function insertNonFull(BTreeNode $node, $key, $value)
    {
        $i = count($node->keys) - 1;

        if ($node->isLeaf) {
            // Insert key-value in sorted order
            // Expand arrays first
            $node->keys[] = null;
            $node->values[] = null;

            while ($i >= 0 && $key < $node->keys[$i]) {
                $node->keys[$i + 1] = $node->keys[$i];
                $node->values[$i + 1] = $node->values[$i];
                $i--;
            }

            $node->keys[$i + 1] = $key;
            $node->values[$i + 1] = $value;
        } else {
            // Find child to insert into
            while ($i >= 0 && $key < $node->keys[$i]) {
                $i--;
            }
            $i++;

            // Check if child is full
            if (count($node->children[$i]->keys) >= $this->order) {
                $this->splitChild($node, $i);
                if ($key > $node->keys[$i]) {
                    $i++;
                }
            }
            $this->insertNonFull($node->children[$i], $key, $value);
        }
    }

    private function splitChild(BTreeNode $parent, int $index)
    {
        $fullNode = $parent->children[$index];
        $newNode = new BTreeNode($fullNode->isLeaf);
        $mid = floor($this->order / 2);

        // Move half of keys to new node
        $newNode->keys = array_splice($fullNode->keys, $mid);
        
        if ($fullNode->isLeaf) {
            $newNode->values = array_splice($fullNode->values, $mid);
        } else {
            $newNode->children = array_splice($fullNode->children, $mid + 1);
        }

        // Pull middle key up
        // Note: array_splice reindexes keys numerically, so index 0 is correct
        $middleKey = array_shift($newNode->keys);
        
        // If leaf, values corresponding to the middle key must also directly move if we follow B+ tree or similar?
        // JS implementation:
        // const middleKey = newNode.keys.shift();
        // if (fullNode.isLeaf) newNode.values.shift(); 
        
        if ($fullNode->isLeaf) {
             array_shift($newNode->values);
             // In pure B-Tree, key goes up, but value separation is tricky.
             // But JS implementation drops the value associated with the shifted key from the NEW node?
             // Wait, if it's a leaf, (key, value) pairs are together.
             // If we promote middleKey, we just copy it for split?
             
             // Re-reading JS:
             /*
                const middleKey = newNode.keys.shift();
                if (fullNode.isLeaf) {
                    newNode.values.shift();
                }
             */
             // This implies the middle element is REMOVED from the leaf child arrays and promoted to parent.
             // But if it's a leaf, where does the value go? 
             // Actually, the JS implementation seems to rely on the fact that for simple indexing, 
             // we might not strictly need the value in the parent for lookups if we are just routing?
             // Or maybe it's just flawed? 
             // Let's stick to strict port of JS logic.
        }

        // Insert middleKey into parent
        array_splice($parent->keys, $index, 0, $middleKey);
        // Insert newNode into parent's children at index + 1
        // For array_splice with object/array insertion, ensure it's wrapped in array if strict typing expected, 
        // but PHP array_splice is flexible.
        // However, critical: array_splice reindexes.
        array_splice($parent->children, $index + 1, 0, [$newNode]);
    }

    public function search($key)
    {
        return $this->searchNode($this->root, $key);
    }

    private function searchNode(BTreeNode $node, $key)
    {
        $i = 0;
        while ($i < count($node->keys) && $key > $node->keys[$i]) {
            $i++;
        }

        if ($i < count($node->keys) && $key == $node->keys[$i]) {
            if ($node->isLeaf) {
                $val = $node->values[$i];
                // In PHP, arrays are both maps and lists. We stored a record (map) in values.
                // We want to return a list of records.
                // If we explicitly stored a list of duplicates, we might need checking, but standard insert here 
                // stores single record per key slot. So we wrap it.
                return [$val];
            } else {
                return $this->searchNode($node->children[$i + 1], $key);
            }
        }

        if ($node->isLeaf) {
            return [];
        }

        return $this->searchNode($node->children[$i], $key);
    }

    public function clear()
    {
        $this->root = new BTreeNode(true);
    }
    
    // Additional stats or range methods could be added here similar to JS
}
