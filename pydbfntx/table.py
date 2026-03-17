"""
DBFNTXTable — combines DBFReader + NTXReader into a Clipper-like table API.
"""
# pydbfntx — Pure-Python reader for Clipper/Harbour DBF+NTX files
# Copyright (C) 2026  Vailton Renato  vailtom <at> gmail <dot> com
# https://github.com/vailtom/pydbfntx
#
# Implemented with reference to the Harbour Project's open-source implementation
# of the NTX format and DBF/NTX behaviour. Harbour: https://github.com/harbour/core
#   include/hbrddntx.h  —  NTX header structure and constants
#   src/rdd/dbfntx/dbfntx1.c  —  B-tree seek, key encoding, page layout
# This is an independent Python implementation.
# Thanks to the Harbour contributors for documenting and implementing these formats.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

import os
from datetime import date, datetime
from typing import Iterator

from .dbf import DBFReader
from .ntx import NTXReader


# ---------------------------------------------------------------------------
# Numeric key encoding — mirrors hb_ntxNumToStr() from dbfntx1.c:293
# ---------------------------------------------------------------------------

def ntx_num_to_str(value: int | float, key_size: int, key_dec: int) -> bytes:
    """
    Encode a numeric value as an NTX key string.

    Positive: leading spaces → '0', sign implicit in first digit being '0'
    Negative: same padding, then all digits inverted via  d' = '0' - (d-'0') - 4
              which maps '0'->',' '1'->'+' ... '9'->'#' (all < '0' in ASCII)
    """
    # Format as fixed-width decimal string (same as hb_itemStrBuf)
    if key_dec > 0:
        fmt = f'{key_size}.{key_dec}f'
    else:
        fmt = f'{key_size}d' if isinstance(value, int) else f'{key_size}.0f'

    try:
        s = format(value, fmt)
    except (TypeError, ValueError):
        s = ' ' * key_size

    # Truncate or pad to exactly key_size
    if len(s) > key_size:
        s = s[-key_size:]  # keep rightmost (can happen with very large numbers)
    s = s.ljust(key_size)

    buf = list(s.encode('ascii'))

    # Replace leading spaces with '0' (Clipper/Harbour behaviour)
    i = 0
    while i < len(buf) and buf[i] == ord(' '):
        buf[i] = ord('0')
        i += 1

    if i < len(buf) and buf[i] == ord('-'):
        # Negative: replace '-' with '0', then invert all digits
        buf[i] = ord('0')
        buf = [
            (ord('0') - (b - ord('0')) - 4) if ord('0') <= b <= ord('9') else b
            for b in buf
        ]

    return bytes(buf)


# ---------------------------------------------------------------------------
# DBFNTXTable
# ---------------------------------------------------------------------------

class DBFNTXTable:
    """
    High-level Clipper-style table that combines a DBF file with one or more
    NTX index files.

    Usage::

        table = DBFNTXTable('clientes.dbf', ['nome.ntx', 'dtmodif.ntx'])
        table.set_order(2)                          # activate dtmodif.ntx
        for rec in table.records_since(date(2024,1,1)):
            print(rec['CODIGO'], rec['DTMODIF'])
    """

    def __init__(
        self,
        dbf_path: str,
        ntx_paths: str | list[str] | None = None,
        encoding: str = 'cp850',
    ):
        self.dbf = DBFReader(dbf_path, encoding=encoding)
        self._ntx_list: list[NTXReader] = []
        self._ntx_names: list[str] = []
        self._active_ntx: NTXReader | None = None
        self._active_idx: int = 0  # 0 = no index (physical order)

        if ntx_paths is not None:
            if isinstance(ntx_paths, str):
                ntx_paths = [ntx_paths]
            for p in ntx_paths:
                ntx = NTXReader(p)
                self._ntx_list.append(ntx)
                self._ntx_names.append(os.path.splitext(os.path.basename(p))[0].lower())

        # Activate first index by default (if any)
        if self._ntx_list:
            self._active_ntx = self._ntx_list[0]
            self._active_idx = 1

        # Cursor state (Clipper-compatible navigation)
        self._recno: int = 0          # current physical record; 0 = not positioned
        self._bof: bool = False
        self._eof: bool = True        # starts as EOF (table not yet navigated)
        self._found: bool = False
        self._ntx_ready: bool = False  # NTX stack is valid at _recno position
        self._current_deleted: bool = False
        self._filter_deleted: bool = True  # SET DELETED ON by default

    def close(self):
        self.dbf.close()
        for ntx in self._ntx_list:
            ntx.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    # ------------------------------------------------------------------
    # SET ORDER TO — activate an index
    # ------------------------------------------------------------------

    def set_order(self, order: int | str):
        """
        Activate an index.

        order=0 (int)  → no index (physical record order)
        order=n (int)  → n-th index (1-based)
        order='name'   → index whose filename (without extension) matches
        """
        if isinstance(order, int):
            if order == 0:
                self._active_ntx = None
                self._active_idx = 0
            elif 1 <= order <= len(self._ntx_list):
                self._active_ntx = self._ntx_list[order - 1]
                self._active_idx = order
            else:
                raise IndexError(f'No index at position {order}')
        else:
            name = order.lower()
            for i, n in enumerate(self._ntx_names):
                if n == name:
                    self._active_ntx = self._ntx_list[i]
                    self._active_idx = i + 1
                    self._ntx_ready = False
                    return
            raise KeyError(f'No index named {order!r}')
        self._ntx_ready = False

    # ------------------------------------------------------------------
    # SET DELETED / field accessors / cursor accessors
    # ------------------------------------------------------------------

    def set_deleted(self, flag: bool):
        """
        SET DELETED ON/OFF.

        flag=True  → deleted records are invisible (filtered) — default
        flag=False → deleted records are visible
        """
        self._filter_deleted = flag

    def _visible(self, rec: dict | None) -> bool:
        """True if rec should be seen given the current SET DELETED flag."""
        if not rec:
            return False
        return (not self._filter_deleted) or (not rec['_deleted'])

    def field_count(self) -> int:
        """FCOUNT() — number of fields in the table."""
        return len(self.dbf.fields)

    def field_name(self, n: int) -> str:
        """FIELDNAME(n) — name of field n (1-based)."""
        return self.dbf.fields[n - 1][0]

    def field_get(self, rec: dict, n: int):
        """FIELDGET(n) — value of field n (1-based) from a record dict."""
        return rec[self.dbf.fields[n - 1][0]]

    def recno(self) -> int:
        """RECNO() — current physical record number (0 if not positioned)."""
        return self._recno

    def deleted(self) -> bool:
        """DELETED() — True if the current record is marked deleted."""
        return self._current_deleted

    # ------------------------------------------------------------------
    # Key type detection & encoding
    # ------------------------------------------------------------------

    def _key_type(self) -> str:
        """
        Detect the key type for the active NTX index.
        Returns 'C', 'N', 'D', or 'L'.
        """
        if self._active_ntx is None:
            return 'C'
        expr = self._active_ntx.key_expr.strip()
        # Check if it's a simple field name
        ft = self.dbf.field_type(expr.upper())
        if ft is not None:
            return ft if ft in ('C', 'N', 'D', 'L') else 'C'
        return 'C'  # composite expression

    def make_key(self, value) -> bytes:
        """
        Convert a Python value to the raw key bytes used in the NTX file.

        str / bytes  → Character: encode to cp850, pad/truncate to key_size
        int / float  → Numeric:   ntx_num_to_str encoding
        date         → Date:      'YYYYMMDD' ASCII bytes
        bool         → Logical:   b'T' or b'F'
        """
        if self._active_ntx is None:
            raise RuntimeError('No active index')

        ntx = self._active_ntx
        ktype = self._key_type()

        if isinstance(value, bool):
            return b'T' if value else b'F'

        if isinstance(value, (date, datetime)):
            if isinstance(value, datetime):
                value = value.date()
            return value.strftime('%Y%m%d').encode('ascii')

        if isinstance(value, (int, float)):
            return ntx_num_to_str(value, ntx.key_size, ntx.key_dec)

        if isinstance(value, str):
            encoded = value.encode(self.dbf.encoding, errors='replace')
            if len(encoded) < ntx.key_size:
                encoded = encoded + b' ' * (ntx.key_size - len(encoded))
            return encoded[:ntx.key_size]

        if isinstance(value, bytes):
            if len(value) < ntx.key_size:
                value = value + b' ' * (ntx.key_size - len(value))
            return value[:ntx.key_size]

        raise TypeError(f'Cannot convert {type(value).__name__} to NTX key')

    # ------------------------------------------------------------------
    # Navigation
    # ------------------------------------------------------------------

    def goto(self, recno: int) -> dict | None:
        """
        DBGOTO(n) — go to physical record number n (1-based).
        Returns the record dict (including deleted records) or None.
        """
        self._found = False
        rec = self.dbf.record(recno)
        if rec is None:
            self._eof = True
            self._bof = False
            self._ntx_ready = False
            return None
        self._recno = recno
        self._current_deleted = rec['_deleted']
        self._bof = False
        self._eof = False
        self._ntx_ready = False  # NTX stack unknown after physical goto
        return rec

    def go_top(self) -> dict | None:
        """
        DBGOTOP() — first record in active index order.
        Without an active index: first non-deleted physical record.
        """
        self._found = False
        if self._active_ntx is not None:
            rec_num = self._active_ntx.go_top()
            while rec_num is not None:
                rec = self.dbf.record(rec_num)
                if self._visible(rec):
                    self._recno = rec_num
                    self._current_deleted = rec['_deleted']
                    self._bof = False
                    self._eof = False
                    self._ntx_ready = True
                    return rec
                rec_num = self._active_ntx.next_rec()
            self._eof = True
            self._bof = False
            self._ntx_ready = False
            return None
        else:
            for i in range(1, self.dbf.num_records + 1):
                rec = self.dbf.record(i)
                if self._visible(rec):
                    self._recno = i
                    self._current_deleted = rec['_deleted']
                    self._bof = False
                    self._eof = False
                    self._ntx_ready = False
                    return rec
            self._eof = True
            self._bof = False
            return None

    def go_bottom(self) -> dict | None:
        """
        DBGOBOTTOM() — last record in active index order.
        Without an active index: last non-deleted physical record.
        """
        self._found = False
        if self._active_ntx is not None:
            rec_num = self._active_ntx.go_bottom()
            if rec_num is not None:
                rec = self.dbf.record(rec_num)
                if self._visible(rec):
                    self._recno = rec_num
                    self._current_deleted = rec['_deleted']
                    self._bof = False
                    self._eof = False
                    self._ntx_ready = True  # at bottom; next_rec() returns None
                    return rec
            self._eof = True
            self._bof = False
            self._ntx_ready = False
            return None
        else:
            for i in range(self.dbf.num_records, 0, -1):
                rec = self.dbf.record(i)
                if self._visible(rec):
                    self._recno = i
                    self._current_deleted = rec['_deleted']
                    self._bof = False
                    self._eof = False
                    self._ntx_ready = False
                    return rec
            self._eof = True
            self._bof = False
            return None

    # ------------------------------------------------------------------
    # Seek
    # ------------------------------------------------------------------

    def seek(self, value, softseek: bool = False) -> dict | None:
        """
        DBSEEK — find a record by index key.

        softseek=False: exact match; returns record or None
        softseek=True:  first record with key >= value; returns record or None
        """
        if self._active_ntx is None:
            raise RuntimeError('No active index — call set_order() first')

        key_bytes = self.make_key(value)
        rec_num = self._active_ntx.seek(key_bytes, softseek=softseek)
        if rec_num is None:
            self._found = False
            self._eof = True
            self._bof = False
            self._ntx_ready = False
            return None

        rec = self.dbf.record(rec_num)
        if rec:
            self._recno = rec_num
            self._current_deleted = rec['_deleted']
            self._bof = False
            self._eof = False
            self._ntx_ready = True
            if softseek:
                # FOUND() = True only when the positioned key is an exact match
                found_key = self._active_ntx.current_key_bytes()
                self._found = found_key is not None and found_key == key_bytes
            else:
                self._found = True
        return rec

    # ------------------------------------------------------------------
    # Clipper-compatible state accessors
    # ------------------------------------------------------------------

    def bof(self) -> bool:
        """BOF() — True if positioned before the first record."""
        return self._bof

    def eof(self) -> bool:
        """EOF() — True if positioned past the last record."""
        return self._eof

    def found(self) -> bool:
        """FOUND() — True if the last SEEK found an exact match."""
        return self._found

    def lastrec(self) -> int:
        """LASTREC() — total number of physical records (including deleted)."""
        return self.dbf.num_records

    def lupdate(self) -> date | None:
        """LUPDATE() — date of last update as stored in the DBF header."""
        return self.dbf.last_update

    def indexkey(self) -> str:
        """INDEXKEY() — key expression of the active index, or '' if none."""
        return self._active_ntx.key_expr if self._active_ntx else ''

    def indexord(self) -> int:
        """INDEXORD() — active index order (1-based), or 0 if no index."""
        return self._active_idx

    # ------------------------------------------------------------------
    # DBSKIP
    # ------------------------------------------------------------------

    def skip(self, n: int = 1) -> dict | None:
        """
        DBSKIP(n) — advance n records in current order (default 1).

        Deleted records are skipped automatically.
        Sets bof() / eof() flags.
        Returns the new current record dict, or None when BOF/EOF is reached.
        """
        if n == 0:
            return self.dbf.record(self._recno) if self._recno else None
        if self._eof and n > 0:
            return None
        if self._bof and n < 0:
            return None

        if self._active_ntx is None:
            return self._skip_physical(n)
        if n > 0:
            return self._skip_indexed_forward(n)
        return self._skip_indexed_backward(-n)

    def _skip_physical(self, n: int) -> dict | None:
        """Skip n records in physical order (n may be negative)."""
        recno = self._recno if self._recno else (1 if n > 0 else self.dbf.num_records)
        step = 1 if n > 0 else -1
        remaining = abs(n)
        while remaining > 0:
            recno += step
            if recno < 1:
                self._recno = 1
                self._bof = True
                self._eof = False
                return None
            if recno > self.dbf.num_records:
                self._recno = self.dbf.num_records
                self._eof = True
                self._bof = False
                return None
            rec = self.dbf.record(recno)
            if self._visible(rec):
                remaining -= 1
                self._recno = recno
                self._current_deleted = rec['_deleted']
        self._bof = False
        self._eof = False
        return self.dbf.record(self._recno)

    def _skip_indexed_forward(self, n: int) -> dict | None:
        """Skip n>0 records forward in index order."""
        if not self._ntx_ready:
            self._ntx_position_at_recno()
        remaining = n
        while remaining > 0:
            rec_num = self._active_ntx.next_rec()
            if rec_num is None:
                self._eof = True
                self._bof = False
                self._ntx_ready = False
                return None
            rec = self.dbf.record(rec_num)
            if self._visible(rec):
                remaining -= 1
                self._recno = rec_num
                self._current_deleted = rec['_deleted']
        self._bof = False
        self._eof = False
        self._ntx_ready = True
        return self.dbf.record(self._recno)

    def _skip_indexed_backward(self, n: int) -> dict | None:
        """
        Skip n>0 records backward in index order.
        Re-scans from go_top() to find the current position, then backs up n.
        """
        ntx = self._active_ntx
        # Collect visible recnos in index order up to and including _recno
        all_recnos: list[int] = []
        r = ntx.go_top()
        while r is not None:
            rec = self.dbf.record(r)
            if self._visible(rec):
                all_recnos.append(r)
            if r == self._recno:
                break
            r = ntx.next_rec()

        try:
            pos = all_recnos.index(self._recno)
        except ValueError:
            pos = len(all_recnos)  # deleted current: treat as just past all collected

        new_pos = pos - n
        if new_pos < 0:
            self._bof = True
            self._eof = False
            if all_recnos:
                self._recno = all_recnos[0]
            self._ntx_ready = False
            return None

        self._recno = all_recnos[new_pos]
        self._ntx_position_at_recno()
        self._bof = False
        self._eof = False
        rec = self.dbf.record(self._recno)
        if rec:
            self._current_deleted = rec['_deleted']
        return rec

    def _ntx_position_at_recno(self):
        """
        Position the NTX stack AT _recno so that next_rec() moves past it.
        Uses seek on the current record's key when possible; falls back to
        a linear scan from go_top().
        """
        if self._recno == 0 or self._active_ntx is None:
            self._ntx_ready = False
            return
        rec = self.dbf.record(self._recno)
        if rec is None:
            self._ntx_ready = False
            return

        ntx = self._active_ntx
        expr = ntx.key_expr.strip().upper()
        ftype = self.dbf.field_type(expr)

        if ftype in ('C', 'N', 'D', 'L'):
            field_val = rec.get(expr)
            if field_val is not None:
                key_bytes = self.make_key(field_val)
                r = ntx.seek(key_bytes, softseek=True)
                while r is not None:
                    if r == self._recno:
                        self._ntx_ready = True
                        return
                    cur_key = ntx.current_key_bytes()
                    if cur_key is None or cur_key > key_bytes:
                        break
                    r = ntx.next_rec()

        # Fallback: linear scan from top
        r = ntx.go_top()
        while r is not None:
            if r == self._recno:
                self._ntx_ready = True
                return
            r = ntx.next_rec()
        self._ntx_ready = False

    # ------------------------------------------------------------------
    # Iteration
    # ------------------------------------------------------------------

    def records_since(self, value) -> Iterator[dict]:
        """
        Yield all non-deleted records whose index key >= value.

        Equivalent to:  seek(value, softseek=True) + forward iteration.
        """
        if self._active_ntx is None:
            raise RuntimeError('No active index — call set_order() first')

        key_bytes = self.make_key(value)
        for rec_num in self._active_ntx.iter_from(key_bytes, softseek=True):
            rec = self.dbf.record(rec_num)
            if self._visible(rec):
                yield rec

    def __iter__(self) -> Iterator[dict]:
        """
        Iterate records respecting the SET DELETED flag.

        With active index: in index key order.
        Without active index: in physical record order.
        """
        if self._active_ntx is not None:
            for rec_num in self._active_ntx:
                rec = self.dbf.record(rec_num)
                if self._visible(rec):
                    yield rec
        else:
            for rec in self.dbf:
                if self._visible(rec):
                    yield rec
