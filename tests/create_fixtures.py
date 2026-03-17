"""
Creates minimal test DBF and NTX files for verifying pydbfntx.

Run this once to generate the fixtures used by test_read.py.
Requires the `dbf` package:  pip install dbf
If dbf is not available, falls back to writing raw binary.
"""

import struct
import os

OUT_DIR = os.path.join(os.path.dirname(__file__), 'fixtures')
os.makedirs(OUT_DIR, exist_ok=True)

DBF_PATH = os.path.join(OUT_DIR, 'clientes.dbf')
NTX_NOME_PATH = os.path.join(OUT_DIR, 'nome.ntx')
NTX_DT_PATH   = os.path.join(OUT_DIR, 'dtmodif.ntx')

# ---------------------------------------------------------------------------
# Minimal DBF writer (dBASE III compatible)
# ---------------------------------------------------------------------------

RECORDS = [
    # (CODIGO, NOME,         DTMODIF)
    (1,  'ANA',         '20240101'),
    (2,  'CARLOS',      '20230601'),
    (3,  'BEATRIZ',     '20240315'),
    (4,  'DANIEL',      '20221201'),
    (5,  'EVA',         '20240201'),
    (6,  'FERNANDO',    '20230901'),
    (7,  'GIOVANA',     '20240401'),
    (8,  'HUGO',        '20231015'),
    (9,  'IRIS',        '20240501'),
    (10, 'JOAO',        '20220801'),
]


def write_dbf():
    fields = [
        # (name, type, length, dec)
        ('CODIGO',  'N', 4, 0),
        ('NOME',    'C', 20, 0),
        ('DTMODIF', 'D', 8, 0),
    ]
    record_size = 1 + sum(f[2] for f in fields)  # 1 = deleted flag
    num_fields  = len(fields)
    header_size = 32 + num_fields * 32 + 1        # 32 header + fields + terminator

    with open(DBF_PATH, 'wb') as f:
        # DBF header (32 bytes)
        f.write(struct.pack('<B', 3))          # version dBASE III
        f.write(bytes([24, 3, 17]))            # last update (YY MM DD) - placeholder
        f.write(struct.pack('<I', len(RECORDS)))  # num records
        f.write(struct.pack('<H', header_size))
        f.write(struct.pack('<H', record_size))
        f.write(b'\x00' * 20)                  # reserved

        # Field descriptors (32 bytes each)
        for name, ftype, flen, fdec in fields:
            fld = bytearray(32)
            fld[:len(name)] = name.encode('ascii')
            fld[11] = ord(ftype)
            fld[16] = flen
            fld[17] = fdec
            f.write(bytes(fld))

        f.write(b'\x0D')  # header terminator

        # Records
        for codigo, nome, dtmodif in RECORDS:
            f.write(b'\x20')  # not deleted
            f.write(f'{codigo:4d}'.encode('ascii'))
            f.write(nome.encode('ascii').ljust(20))
            f.write(dtmodif.encode('ascii'))

    print(f'Written: {DBF_PATH}')


# ---------------------------------------------------------------------------
# Minimal NTX writer
# ---------------------------------------------------------------------------
# We build a flat (single-page) B-tree for simplicity — all keys in one leaf.
# This is valid NTX: the root is a leaf with no children.

NTXBLOCKSIZE = 1024
NTX_FLAG_LARGEFILE = 0x0200

def _ntx_num_to_str(value: int, key_size: int, key_dec: int) -> bytes:
    if key_dec > 0:
        s = format(value, f'{key_size}.{key_dec}f')
    else:
        s = format(value, f'{key_size}d')
    if len(s) > key_size:
        s = s[-key_size:]
    s = s.ljust(key_size)
    buf = list(s.encode('ascii'))
    i = 0
    while i < len(buf) and buf[i] == ord(' '):
        buf[i] = ord('0')
        i += 1
    if i < len(buf) and buf[i] == ord('-'):
        buf[i] = ord('0')
        buf = [
            (ord('0') - (b - ord('0')) - 4) if ord('0') <= b <= ord('9') else b
            for b in buf
        ]
    return bytes(buf)


def write_ntx(ntx_path, key_expr, key_size, key_dec, key_type, sorted_pairs):
    """
    Write a single-root-page NTX.

    sorted_pairs: list of (key_bytes, recno) already sorted by key_bytes
    """
    n = len(sorted_pairs)
    item_size = 8 + key_size  # child(4) + recno(4) + key(key_size)
    max_item   = (NTXBLOCKSIZE - 2 - 2) // (item_size + 2) - 1
    # offset array: (max_item+1) uint16 entries = 2*(max_item+2) bytes from offset 2
    # item data starts after offset array
    offset_array_size = (max_item + 2) * 2   # +2 for the extra "rightmost child" slot

    assert n <= max_item, f'Too many records ({n}) for a single-page NTX (max {max_item})'

    # Build root page (block 1)
    page = bytearray(NTXBLOCKSIZE)
    struct.pack_into('<H', page, 0, n)  # key_count

    data_start = 2 + (max_item + 2) * 2  # where item data begins
    offsets = []
    pos = data_start
    for i, (kb, recno) in enumerate(sorted_pairs):
        offsets.append(pos)
        struct.pack_into('<I', page, pos, 0)       # child_page = 0 (leaf)
        struct.pack_into('<I', page, pos + 4, recno)
        page[pos + 8: pos + 8 + key_size] = kb
        pos += item_size

    # Extra slot for rightmost child (child=0, rec=0)
    offsets.append(pos)
    struct.pack_into('<I', page, pos, 0)
    struct.pack_into('<I', page, pos + 4, 0)
    pos += item_size

    # Write offset array
    for i, off in enumerate(offsets):
        struct.pack_into('<H', page, 2 + i * 2, off)

    # NTX header (block 0)
    header = bytearray(NTXBLOCKSIZE)
    flags = NTX_FLAG_LARGEFILE | 0x0006
    struct.pack_into('<H', header, 0, flags)       # type/flags
    struct.pack_into('<H', header, 2, 1)            # version
    struct.pack_into('<I', header, 4, 1)            # root = block 1
    struct.pack_into('<I', header, 8, 2)            # next_page
    struct.pack_into('<H', header, 12, item_size)   # item_size
    struct.pack_into('<H', header, 14, key_size)    # key_size
    struct.pack_into('<H', header, 16, key_dec)     # key_dec
    struct.pack_into('<H', header, 18, max_item)    # max_item
    struct.pack_into('<H', header, 20, max_item // 2)  # half_page
    expr_bytes = key_expr.encode('latin-1')
    header[22: 22 + len(expr_bytes)] = expr_bytes
    header[280] = 0  # ascending

    with open(ntx_path, 'wb') as f:
        f.write(bytes(header))
        f.write(bytes(page))

    print(f'Written: {ntx_path}  ({n} keys)')


def write_nome_ntx():
    key_size = 20
    key_dec  = 0
    pairs = []
    for recno, (codigo, nome, dtmodif) in enumerate(RECORDS, 1):
        kb = nome.encode('cp850').ljust(key_size)[:key_size]
        pairs.append((kb, recno))
    pairs.sort(key=lambda x: x[0])
    write_ntx(NTX_NOME_PATH, 'NOME', key_size, key_dec, 'C', pairs)


def write_dtmodif_ntx():
    key_size = 8
    key_dec  = 0
    pairs = []
    for recno, (codigo, nome, dtmodif) in enumerate(RECORDS, 1):
        kb = dtmodif.encode('ascii')
        pairs.append((kb, recno))
    pairs.sort(key=lambda x: x[0])
    write_ntx(NTX_DT_PATH, 'DTMODIF', key_size, key_dec, 'D', pairs)


if __name__ == '__main__':
    write_dbf()
    write_nome_ntx()
    write_dtmodif_ntx()
    print('Test data created in', OUT_DIR)
