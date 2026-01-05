# SawitDB (PHP Version)

![SawitDB Banner](https://github.com/WowoEngine/SawitDB/raw/main/docs/sawitdb.jpg)

<div align="center">

[![Docs](https://img.shields.io/badge/Docs-Read%20Now-blue?style=for-the-badge&logo=googledocs)](https://wowoengine.github.io/SawitDB/)
[![Node.js Version](https://img.shields.io/badge/Node.js%20Version-Visit%20Repo-green?style=for-the-badge&logo=nodedotjs)](https://github.com/WowoEngine/SawitDB)
[![Go Version](https://img.shields.io/badge/Go%20Version-Visit%20Repo-cyan?style=for-the-badge&logo=go)](https://github.com/WowoEngine/SawitDB-Go)

</div>

**SawitDB** is a unique database solution stored in `.sawit` binary files.

The system features a custom **Paged Heap File** architecture similar to SQLite, using fixed-size 4KB pages to ensure efficient memory usage. What differentiates SawitDB is its unique **Agricultural Query Language (AQL)**, which replaces standard SQL keywords with Indonesian farming terminology.

**Now in PHP!** Connect via TCP using the `sawitdb://` protocol, similar to MongoDB.

**🚨 Emergency: Aceh Flood Relief**
Please support our brothers and sisters in Aceh.

[![Kitabisa](https://img.shields.io/badge/Kitabisa-Bantu%20Aceh-blue?style=flat&logo=heart)](https://kitabisa.com/campaign/donasipedulibanjiraceh)

*Organized by Human Initiative Aceh*

## Features

- **Paged Architecture**: Data is stored in 4096-byte binary pages.
- **Single File Storage**: All data, schema, and indexes are stored in a single `.sawit` file.
- **High Stability**: Uses 4KB atomic pages.
- **Data Integrity**: Uses `flock` and standard PHP stream locking.
- **Zero Bureaucracy**: Built with **standard PHP**. No massive vendor dependencies (only standard composer autoload).
- **Speed**: Optimized for PHP Opcache.
- **Network Support (NEW)**: Client-Server architecture using PHP Streams.

## Philosophy

### Filosofi (ID)
SawitDB dibangun dengan semangat "Kemandirian Data". Kami percaya database yang handal tidak butuh **Infrastruktur Langit** yang harganya triliunan tapi sering *down*. Berbeda dengan proyek negara yang mahal di *budget* tapi murah di kualitas, SawitDB menggunakan arsitektur **Single File** (`.sawit`) yang hemat biaya. Backup cukup *copy-paste*, tidak perlu sewa vendor konsultan asing. Fitur **`fsync`** kami menjamin data tertulis di *disk*, karena bagi kami, integritas data adalah harga mati, bukan sekadar bahan konferensi pers untuk minta maaf.

### Philosophy (EN)
SawitDB is built with the spirit of "Data Sovereignty". We believe a reliable database doesn't need **"Sky Infrastructure"** that costs trillions yet goes *down* often. Unlike state projects that are expensive in budget but cheap in quality, SawitDB uses a cost-effective **Single File** (`.sawit`) architecture. Backup is just *copy-paste*, no need to hire expensive foreign consultants. Our **`fsync`** feature guarantees data is written to *disk*, because for us, data integrity is non-negotiable, not just material for a press conference to apologize.

## Installation

Ensure you have PHP 8.0+ installed.

```bash
git clone https://github.com/WowoEngine/SawitDB-PHP.git
cd SawitDB-PHP
composer install
```

## Laravel Integration

SawitDB includes a ServiceProvider and Facade for seamless Laravel integration.

1.  **Install via Composer**: `composer require wowoengine/sawitdb-php`
2.  **Configuration**: Add the configuration to `config/database.php` (Optional, defaults to `database_path('sawit.db')`).

```php
'connections' => [
    'sawit' => [
        'database' => env('SAWIT_DB_DATABASE', database_path('plantation.sawit')),
    ],
],
```

3.  **Usage**:
```php
use SawitDB;

// In Controller
public function index() {
    SawitDB::query("LAHAN users");
    SawitDB::query("TANAM KE users (name) BIBIT ('Budi')");
    return SawitDB::query("PANEN * DARI users");
}
```

## Quick Start (Network Edition)

### 1. Start the Server
```bash
php bin/sawit-server.php
```
The server will start on `0.0.0.0:7878` by default.

### 2. Connect with Client (CLI)
```bash
php cli/remote.php sawitdb://localhost:7878/my_plantation
```

---

## Usage (Embedded)

You can use the `WowoEngine` directly in your PHP applications.

```php
use SawitDB\Engine\WowoEngine;

$db = new WowoEngine(__DIR__ . '/plantation.sawit');

// Create Table
$db->query("LAHAN trees");

// Insert (Plant)
$db->query("TANAM KE trees (id, type) BIBIT (1, 'Dura')");

// Select (Harvest)
$rows = $db->query("PANEN * DARI trees WHRE type='Dura'");
print_r($rows);
```

## Architecture Details

- **Modular Codebase**: Engine logic separated into `src/` components.
- **Page 0 (Master Page)**: Contains header and Table Directory.
- **Data & Indexes**: Stored in 4KB atomic pages.
- **WowoEngine**: Core engine orchestrating Pager, Index, and Query Parser.

## Benchmark Performance
Test Environment: PHP 8.0+, Single Thread, Windows (Local NVMe)

| Operation | Ops/Sec | Latency (avg) |
|-----------|---------|---------------|
| **INSERT** | ~22,780 | 0.044 ms |
| **SELECT (PK Index)** | ~55,555 | 0.018 ms |
| **UPDATE** | ~6,332 | 0.158 ms |
| **DELETE** | ~3,477 | 0.288 ms |

*Note: Results obtained on local development environment. High IOPS due to OS Page Cache & buffering.*

## Full Feature Comparison

| Feature | Tani Edition (AQL) | Generic SQL (Standard) | Notes |
|---------|-------------------|------------------------|-------|
| **Create DB** | `BUKA WILAYAH [db]` | `CREATE DATABASE [db]` | Creates `.sawit` in data/ |
| **Use DB** | `MASUK WILAYAH [db]` | `USE [db]` | Switch context |
| **Show DBs** | `LIHAT WILAYAH` | `SHOW DATABASES` | Lists available DBs |
| **Drop DB** | `BAKAR WILAYAH [db]` | `DROP DATABASE [db]` | **Irreversible!** |
| **Create Table** | `LAHAN [table]` | `CREATE TABLE [table]` | Schema-less creation |
| **Show Tables** | `LIHAT LAHAN` | `SHOW TABLES` | Lists tables in DB |
| **Drop Table** | `BAKAR LAHAN [table]` | `DROP TABLE [table]` | Deletes table & data |
| **Insert** | `TANAM KE [table] ... BIBIT (...)` | `INSERT INTO [table] (...) VALUES (...)` | Auto-ID if omitted |
| **Select** | `PANEN ... DARI [table] DIMANA ...` | `SELECT ... FROM [table] WHERE ...` | Supports Projection |
| **Update** | `PUPUK [table] DENGAN ... DIMANA ...` | `UPDATE [table] SET ... WHERE ...` | Atomic update |
| **Delete** | `GUSUR DARI [table] DIMANA ...` | `DELETE FROM [table] WHERE ...` | Row-level deletion |
| **Index** | `INDEKS [table] PADA [field]` | `CREATE INDEX ON [table] (field)` | B-Tree Indexing |
| **Count** | `HITUNG COUNT(*) DARI [table]` | `SELECT COUNT(*) FROM [table]` (via HITUNG) | Aggregation |
| **Sum** | `HITUNG SUM(col) DARI [table]` | `SELECT SUM(col) FROM [table]` (via HITUNG) | Aggregation |
| **Average** | `HITUNG AVG(col) DARI [table]` | `SELECT AVG(col) FROM [table]` (via HITUNG) | Aggregation |

### Supported Operators Table

| Operator | Syntax Example | Description |
|----------|----------------|-------------|
| **Comparison** | `=`, `!=`, `>`, `<`, `>=`, `<=` | Standard value comparison |
| **Logical** | `AND`, `OR` | Combine multiple conditions |
| **In List** | `IN ('coffee', 'tea')` | Matches any value in the list |
| **Not In** | `NOT IN ('water')` | Matches values NOT in list |
| **Pattern** | `LIKE 'Jwa%'` | Standard SQL wildcard matching |
| **Range** | `BETWEEN 1000 AND 5000` | Inclusive range check |
| **Null** | `IS NULL` | Check if field is empty/null |
| **Not Null** | `IS NOT NULL` | Check if field has value |
| **Limit** | `LIMIT 10` | Restrict number of rows |
| **Offset** | `OFFSET 5` | Skip first N rows (Pagination) |
| **Order** | `ORDER BY price DESC` | Sort by field (ASC/DESC) |

## Project Structure

- `src/Engine/`: Core Database Engine components (`WowoEngine`, `Pager`, `BTreeIndex`).
- `src/Network/`: Networking components (`SawitServer`, `SawitClient`).
- `src/Laravel/`: Laravel Integration (`ServiceProvider`, `Facade`).
- `bin/sawit-server.php`: Server Entry Point.
- `cli/`: Interactive CLI tools (`local.php`, `remote.php`).

## License

MIT License
