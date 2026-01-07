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
            while ($i >= 0 && $key < $node->keys[$i]) {
                $i--;
            }
            $i++;

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

        $newNode->keys = array_splice($fullNode->keys, $mid);
        
        if ($fullNode->isLeaf) {
            $newNode->values = array_splice($fullNode->values, $mid);
        } else {
            $newNode->children = array_splice($fullNode->children, $mid + 1);
        }

        $middleKey = array_shift($newNode->keys);
        
        if ($fullNode->isLeaf) {
             array_shift($newNode->values);
        }

        array_splice($parent->keys, $index, 0, $middleKey);
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

    public function delete($key)
    {
        $this->deleteFromNode($this->root, $key);

        if (count($this->root->keys) === 0 && !$this->root->isLeaf) {
            $this->root = $this->root->children[0];
        }
    }

    private function deleteFromNode(BTreeNode $node, $key)
    {
        $i = 0;
        while ($i < count($node->keys) && $key > $node->keys[$i]) {
            $i++;
        }

        if ($i < count($node->keys) && $key == $node->keys[$i]) {
            if ($node->isLeaf) {
                array_splice($node->keys, $i, 1);
                array_splice($node->values, $i, 1);
                return true;
            } else {
                // Internal Match: Pass through to child right of key
                return $this->deleteFromNode($node->children[$i + 1], $key);
            }
        }

        if ($node->isLeaf) {
            return false;
        }

        return $this->deleteFromNode($node->children[$i], $key);
    }

    public function clear()
    {
        $this->root = new BTreeNode(true);
    }
}
