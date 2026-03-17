"""
DBFReader — pure-Python reader for dBASE III / Clipper DBF files.
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

import struct
from datetime import date


# DBF header offsets
_DBF_HDR_NUM_RECORDS  = (4,  '<I')   # number of records
_DBF_HDR_HEADER_SIZE  = (8,  '<H')   # bytes in header
_DBF_HDR_RECORD_SIZE  = (10, '<H')   # bytes per record

# Field descriptor (32 bytes each, starting at offset 32)
_FLD_NAME_LEN  = 11
_FLD_TYPE_OFF  = 11
_FLD_LEN_OFF   = 16   # field length in bytes
_FLD_DEC_OFF   = 17   # number of decimals


class DBFReader:
    """
    Read-only access to a DBF file.

    Records are returned as dicts {FIELD_NAME: python_value}.
    Deleted records are included but marked with key '_deleted': True.
    """

    def __init__(self, dbf_path: str, encoding: str = 'cp850'):
        self.path = dbf_path
        self.encoding = encoding
        self._fh = open(dbf_path, 'rb')
        self._read_header()
        self._read_fields()

    def close(self):
        self._fh.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    # ------------------------------------------------------------------
    # Header / field descriptors
    # ------------------------------------------------------------------

    def _read_header(self):
        self._fh.seek(0)
        buf = self._fh.read(32)
        self.num_records = struct.unpack_from('<I', buf, 4)[0]
        self.header_size = struct.unpack_from('<H', buf, 8)[0]
        self.record_size = struct.unpack_from('<H', buf, 10)[0]
        # Last update date: bytes 1-3 (YY, MM, DD).
        # Clipper/xBase Y2K pivot: YY < 80 → 2000+YY, YY >= 80 → 1900+YY
        yy, mm, dd = buf[1], buf[2], buf[3]
        try:
            century = 2000 if yy < 80 else 1900
            self.last_update: date | None = date(century + yy, mm, dd) if mm and dd else None
        except ValueError:
            self.last_update = None

    def _read_fields(self):
        self._fh.seek(32)
        raw = self._fh.read(self.header_size - 32)
        self.fields = []   # list of (name, type, length, dec, offset_in_record)
        offset = 1  # first byte of record is the deleted flag
        i = 0
        while i + 32 <= len(raw):
            if raw[i] == 0x0D:  # header terminator
                break
            fld_bytes = raw[i:i + 32]
            name = fld_bytes[:_FLD_NAME_LEN].split(b'\x00')[0].decode('latin-1').strip()
            ftype = chr(fld_bytes[_FLD_TYPE_OFF])
            flen = fld_bytes[_FLD_LEN_OFF]
            fdec = fld_bytes[_FLD_DEC_OFF]
            self.fields.append((name, ftype, flen, fdec, offset))
            offset += flen
            i += 32

        # Build lookup dict: name → (type, length, dec, offset)
        self._field_map = {
            name: (ftype, flen, fdec, off)
            for name, ftype, flen, fdec, off in self.fields
        }

    # ------------------------------------------------------------------
    # Record I/O
    # ------------------------------------------------------------------

    def _record_offset(self, recno: int) -> int:
        """Convert 1-based record number to file byte offset."""
        return self.header_size + (recno - 1) * self.record_size

    def record(self, recno: int) -> dict | None:
        """
        Read record by 1-based physical record number.
        Returns a dict or None if recno is out of range.
        '_deleted' key is True if the record is marked deleted.
        """
        if recno < 1 or recno > self.num_records:
            return None
        self._fh.seek(self._record_offset(recno))
        raw = self._fh.read(self.record_size)
        deleted = raw[0] == 0x2A  # '*'
        result = {'_recno': recno, '_deleted': deleted}
        for name, ftype, flen, fdec, off in self.fields:
            raw_val = raw[off:off + flen]
            result[name] = self._decode_field(raw_val, ftype, flen, fdec)
        return result

    def _decode_field(self, raw: bytes, ftype: str, flen: int, fdec: int):
        """Decode a raw field value to a Python object."""
        text = raw.decode(self.encoding, errors='replace')
        if ftype == 'C':
            return text.rstrip('\x00')  # some DBFs null-pad instead of space-pad
        elif ftype == 'N':
            s = text.strip()
            if not s:
                return None
            try:
                return int(s) if fdec == 0 else float(s)
            except ValueError:
                return None
        elif ftype == 'D':
            s = text.strip()
            if len(s) == 8 and s.isdigit():
                try:
                    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
                except ValueError:
                    pass
            return None
        elif ftype == 'L':
            return raw[0:1] in (b'T', b't', b'Y', b'y')
        elif ftype == 'M':
            return text.strip()  # memo block number or text
        else:
            return text.rstrip()

    # ------------------------------------------------------------------
    # Field metadata
    # ------------------------------------------------------------------

    def field_type(self, field_name: str) -> str | None:
        """Return the type character ('C', 'N', 'D', 'L', 'M') or None."""
        entry = self._field_map.get(field_name.upper())
        return entry[0] if entry else None

    def field_names(self) -> list[str]:
        return [f[0] for f in self.fields]

    def __iter__(self):
        """Iterate all records in physical order."""
        for i in range(1, self.num_records + 1):
            yield self.record(i)
