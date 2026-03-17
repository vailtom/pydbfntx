# pydbfntx

Pure-Python reader for Clipper/Harbour DBF files with NTX B-tree indexes.

No external dependencies — only Python's built-in `struct` module.

## Features

- Reads **DBF** files (dBASE III / Clipper format, cp850 encoding by default)
- Reads **NTX** B-tree index files (LargeFile and legacy modes)
- Full B-tree **seek** with optional softseek (`>=` semantics)
- Clipper-compatible **cursor navigation**: `go_top`, `go_bottom`, `goto`, `skip(n)`
- Cursor **state flags**: `bof()`, `eof()`, `found()`
- Table **metadata**: `lastrec()`, `lupdate()`, `indexkey()`, `indexord()`
- **Field accessors** by position: `field_count()`, `field_name(n)`, `field_get(rec, n)`
- **`SET DELETED`** flag to show or hide deleted records globally
- Multiple indexes with `set_order()`
- Efficient **`records_since()`** for date-range synchronisation

## Installation

```bash
pip install pydbfntx
```

Or directly from source:

```bash
git clone ...
pip install -e .
```

## Quick start

```python
from pydbfntx import DBFNTXTable
from datetime import date

table = DBFNTXTable('clientes.dbf', ['nome.ntx', 'codigo.ntx', 'dtmodif.ntx'])

# --- SET ORDER TO ---
table.set_order(1)           # by position (1-based)
table.set_order('codigo')    # by index filename (without extension)
table.set_order(0)           # no index (physical record order)

# --- DBGOTOP / DBGOBOTTOM / DBGOTO ---
record = table.go_top()      # DBGOTOP  — first record in active index order
record = table.go_bottom()   # DBGOBOTTOM — last record in active index order
record = table.goto(42)      # DBGOTO   — physical record #42

# --- DBSKIP ---
record = table.skip()        # DBSKIP    — advance 1 record
record = table.skip(3)       # DBSKIP 3  — advance 3 records
record = table.skip(-1)      # DBSKIP -1 — go back 1 record
# -> returns the new current record dict, or None when BOF/EOF is reached

# --- DBSEEK (exact match) ---
record = table.seek('ANA')               # Character index
record = table.seek(42.5)                # Numeric index
record = table.seek(date(2024, 1, 15))   # Date index
record = table.seek(True)                # Logical index
record = table.seek('01ANA')             # Composite key (always C)
# -> returns dict or None if not found

# --- DBSEEK with softseek (first record with key >= value) ---
record = table.seek(date(2024, 1, 1), softseek=True)

# --- State flags ---
table.bof()      # BOF()   — True if before the first record
table.eof()      # EOF()   — True if past the last record
table.found()    # FOUND() — True if the last seek found an exact match

# --- Table metadata ---
table.lastrec()   # LASTREC()  — total number of physical records
table.lupdate()   # LUPDATE()  — last update date (datetime.date or None)
table.indexkey()  # INDEXKEY() — key expression of the active index, e.g. 'NOME'
table.indexord()  # INDEXORD() — active index position (1-based), 0 if none

# --- Cursor position ---
table.recno()     # RECNO()   — current physical record number
table.deleted()   # DELETED() — True if current record is marked deleted

# --- Field accessors by position (1-based, like Clipper) ---
table.field_count()          # FCOUNT()      — number of fields
table.field_name(2)          # FIELDNAME(2)  — name of field 2
table.field_get(record, 2)   # FIELDGET(2)   — value of field 2 from a record dict

# --- SET DELETED ---
table.set_deleted(True)   # SET DELETED ON  — hide deleted records (default)
table.set_deleted(False)  # SET DELETED OFF — show deleted records

# --- Synchronise: records modified since a date ---
table.set_order('dtmodif')
for record in table.records_since(date(2024, 1, 1)):
    print(record['CODIGO'], record['DTMODIF'])

# --- Iterate all records in index order ---
for record in table:
    print(record)
```

## Typical navigation loop (Clipper style)

```python
table.set_order('nome')
table.go_top()

while not table.eof():
    rec = table.goto(table.recno())   # access current record
    print(table.field_get(rec, 1), table.field_get(rec, 2))
    table.skip()
```

Or more directly:

```python
table.set_order('nome')
rec = table.go_top()

while rec is not None and not table.eof():
    print(rec['CODIGO'], rec['NOME'].strip())
    rec = table.skip()
```

## Seek + Found pattern

```python
table.set_order('nome')
table.seek('CARLOS')

if table.found():
    print('found:', table.recno())
    # iterate forward from here
    while not table.eof():
        rec = table.goto(table.recno())
        if not rec['NOME'].startswith('CARLOS'):
            break
        print(rec)
        table.skip()
```

Softseek positions the cursor at the first record with key `>=` the search value.
`found()` is `True` only when the positioned key is an **exact** match:

```python
table.seek('CAR', softseek=True)   # lands on CARLOS
table.found()                      # False — 'CAR' != 'CARLOS...'

table.seek('CARLOS', softseek=True)
table.found()                      # True — exact match
```

## Record dict format

Each record is a plain `dict`:

```python
{
    '_recno':   42,             # physical record number (1-based)
    '_deleted': False,          # True if the record is marked deleted
    'CODIGO':   1,              # N field -> int or float
    'NOME':     'ANA         ', # C field -> str (right-padded)
    'DTMODIF':  date(2024,1,1), # D field -> datetime.date or None
    'ATIVO':    True,           # L field -> bool
}
```

## SET DELETED behaviour

| Method / situation | Deleted records |
|---|---|
| `__iter__()` | respects `set_deleted` flag |
| `records_since()` | respects `set_deleted` flag |
| `go_top()` / `go_bottom()` | respects `set_deleted` flag |
| `skip()` | respects `set_deleted` flag |
| `goto()` | **always** returns the record (same as Clipper `DBGOTO`) |
| `seek()` | returns the record regardless (index hit) |

Default is `set_deleted(True)` (deleted records are hidden), matching
Clipper's `SET DELETED ON` default.

## Key type encoding

| Clipper type | Python input accepted by `seek()` / `make_key()` |
|---|---|
| `C` (Character) | `str` or `bytes` — padded with spaces to `key_size` |
| `N` (Numeric)   | `int` or `float` — encoded via `ntx_num_to_str()` |
| `D` (Date)      | `datetime.date` or `datetime.datetime` |
| `L` (Logical)   | `bool` |
| Composite expr  | `str` or `bytes` (user pre-computes the combined key) |

## Multiple indexes

```python
table = DBFNTXTable('clientes.dbf', ['nome.ntx', 'codigo.ntx', 'dtmodif.ntx'])

# position 1 = nome.ntx      (active by default)
# position 2 = codigo.ntx
# position 3 = dtmodif.ntx

table.set_order('dtmodif')    # or table.set_order(3)
for rec in table.records_since(date(2024, 1, 1)):
    ...
```

## Low-level API

```python
from pydbfntx import NTXReader, DBFReader

# NTX header inspection
with NTXReader('dtmodif.ntx') as ntx:
    print(ntx.key_expr)   # 'DTMODIF'
    print(ntx.key_size)   # 8
    print(ntx.large_file) # True/False
    print(ntx.descend)    # False (ascending)

    # Iterate rec_num values (no DBF involved)
    for rec_num in ntx:
        print(rec_num)

    # Key at current stack position (after seek/go_top/go_bottom)
    key = ntx.current_key_bytes()

# DBF header inspection
with DBFReader('clientes.dbf') as dbf:
    print(dbf.num_records)
    print(dbf.last_update)    # datetime.date or None (Y2K-aware)
    print(dbf.field_names())
    rec = dbf.record(1)       # 1-based
```

## API reference — DBFNTXTable

### Constructor

```python
DBFNTXTable(dbf_path, ntx_paths=None, encoding='cp850')
```

### Index management

| Method | Clipper | Description |
|---|---|---|
| `set_order(n)` | `SET ORDER TO n` | Activate index by position (1-based) or name |
| `indexord()` | `INDEXORD()` | Active index position; 0 if none |
| `indexkey()` | `INDEXKEY()` | Key expression of active index; `''` if none |

### Navigation

| Method | Clipper | Description |
|---|---|---|
| `go_top()` | `DBGOTOP()` | First record in active order |
| `go_bottom()` | `DBGOBOTTOM()` | Last record in active order |
| `goto(n)` | `DBGOTO(n)` | Physical record n (1-based) |
| `skip(n=1)` | `DBSKIP(n)` | Advance n records; negative goes back |

### Seek

| Method | Clipper | Description |
|---|---|---|
| `seek(value)` | `DBSEEK(value)` | Exact key search |
| `seek(value, softseek=True)` | `SET SOFTSEEK ON` / `DBSEEK` | First record with key `>=` value |

### State flags

| Method | Clipper | Description |
|---|---|---|
| `bof()` | `BOF()` | Before first record |
| `eof()` | `EOF()` | Past last record |
| `found()` | `FOUND()` | Last seek was an exact match |
| `recno()` | `RECNO()` | Current physical record number |
| `deleted()` | `DELETED()` | Current record is marked deleted |

### Table metadata

| Method | Clipper | Description |
|---|---|---|
| `lastrec()` | `LASTREC()` | Total physical record count |
| `lupdate()` | `LUPDATE()` | Last update date from DBF header |

### Field accessors

| Method | Clipper | Description |
|---|---|---|
| `field_count()` | `FCOUNT()` | Number of fields |
| `field_name(n)` | `FIELDNAME(n)` | Name of field n (1-based) |
| `field_get(rec, n)` | `FIELDGET(n)` | Value of field n from a record dict |

### Deleted record filtering

| Method | Clipper | Description |
|---|---|---|
| `set_deleted(True)` | `SET DELETED ON` | Hide deleted records (default) |
| `set_deleted(False)` | `SET DELETED OFF` | Show deleted records |

### Iteration

| Method | Description |
|---|---|
| `for rec in table` | All records in active index / physical order |
| `records_since(value)` | Records with key `>=` value (softseek + forward scan) |

## Credits

This project was implemented with reference to the Harbour Project's open-source
implementation of the NTX format and DBF/NTX behaviour.

Relevant reference files from Harbour:
- `include/hbrddntx.h` — NTX header structure and constants
- `src/rdd/dbfntx/dbfntx1.c` — B-tree seek, key encoding, page layout

Reference project:
- https://github.com/harbour/core

This project is distributed under the GNU General Public License v3 or later
(GPL-3.0-or-later).

This project is not affiliated with or endorsed by the Harbour Project.

Harbour is a free and open-source compiler and runtime compatible with Clipper 5.x.
Thanks to the Harbour contributors for documenting and implementing these formats.

## Author

**Vailton Renato** — [github.com/vailtom](https://github.com/vailtom) — vailtom \<at\> gmail \<dot\> com

## License

This project is licensed under the **GNU General Public License v3 or later (GPL-3.0-or-later)**.
See [LICENSE](LICENSE) for the full text.
