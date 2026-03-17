"""
NTXReader — pure-Python reader for Clipper/Harbour NTX index files.

Format references:
  C:/harbour-core/include/hbrddntx.h
  C:/harbour-core/src/rdd/dbfntx/dbfntx1.c
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

NTXBLOCKSIZE = 1024
NTX_FLAG_LARGEFILE = 0x0200
NTX_DUMMYNODE = 0xFFFFFFFF

# NTXHEADER byte offsets (little-endian fields)
#   type[2]       @ 0
#   version[2]    @ 2
#   root[4]       @ 4
#   next_page[4]  @ 8
#   item_size[2]  @ 12
#   key_size[2]   @ 14
#   key_dec[2]    @ 16
#   max_item[2]   @ 18
#   half_page[2]  @ 20
#   key_expr[256] @ 22
#   unique[1]     @ 278
#   unknown1[1]   @ 279
#   descend[1]    @ 280
#   unknown2[1]   @ 281
#   for_expr[256] @ 282


def _cstring(buf: bytes, offset: int, length: int) -> str:
    raw = buf[offset: offset + length]
    null = raw.find(b'\x00')
    if null >= 0:
        raw = raw[:null]
    return raw.decode('latin-1').strip()


class NTXReader:
    """
    Read-only access to an NTX B-tree index file.

    Stack semantics
    ---------------
    _stack is a list of (block_number, key_index) pairs.  An entry (blk, i)
    means "key i of page blk is the NEXT key to be returned".  After returning
    it, next_rec() pops the entry and arranges the next one.

    The "extra slot" at index key_count on any page has rec_num == 0.
    _current_rec() skips entries with rec_num == 0 by popping them (this
    naturally handles bubbling up to ancestors and signalling EOF).
    """

    def __init__(self, ntx_path: str):
        self.path = ntx_path
        self._fh = open(ntx_path, 'rb')
        self._stack: list[tuple[int, int]] = []
        self._eof = False
        self._read_header()

    def close(self):
        self._fh.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    # ------------------------------------------------------------------
    # Header
    # ------------------------------------------------------------------

    def _read_header(self):
        # Block 0 is always at byte offset 0 (0 * 1024 == 0).
        # Read it directly so we can set large_file before any _page_offset call.
        self._fh.seek(0)
        buf = self._fh.read(NTXBLOCKSIZE)
        self.flags      = struct.unpack_from('<H', buf, 0)[0]
        self.version    = struct.unpack_from('<H', buf, 2)[0]
        self.root       = struct.unpack_from('<I', buf, 4)[0]
        self.next_page  = struct.unpack_from('<I', buf, 8)[0]
        self.item_size  = struct.unpack_from('<H', buf, 12)[0]
        self.key_size   = struct.unpack_from('<H', buf, 14)[0]
        self.key_dec    = struct.unpack_from('<H', buf, 16)[0]
        self.max_item   = struct.unpack_from('<H', buf, 18)[0]
        self.half_page  = struct.unpack_from('<H', buf, 20)[0]
        self.key_expr   = _cstring(buf, 22, 256)
        self.descend    = buf[280] != 0
        self.for_expr   = _cstring(buf, 282, 256)
        self.large_file = bool(self.flags & NTX_FLAG_LARGEFILE)

    # ------------------------------------------------------------------
    # Low-level page I/O
    # ------------------------------------------------------------------

    def _page_offset(self, block: int) -> int:
        """
        LargeFile=True  → offset = block * 1024   (block is a page index)
        LargeFile=False → offset = block           (block IS the byte offset)
        """
        return block * NTXBLOCKSIZE if self.large_file else block

    def _read_block(self, block: int) -> bytes:
        self._fh.seek(self._page_offset(block))
        return self._fh.read(NTXBLOCKSIZE)

    # ------------------------------------------------------------------
    # Page accessors
    # ------------------------------------------------------------------

    def _key_count(self, buf: bytes) -> int:
        return struct.unpack_from('<H', buf, 0)[0]

    def _key_entry(self, buf: bytes, i: int):
        """
        Return (child_page, rec_num, key_bytes) for item i.

        item[i].child_page  = left subtree for key i (keys < key[i])
        item[n].child_page  = rightmost subtree (keys > key[n-1])
        item[n].rec_num     = 0  (sentinel / extra slot)
        """
        off = struct.unpack_from('<H', buf, 2 + i * 2)[0]
        child_page = struct.unpack_from('<I', buf, off)[0]
        rec_num    = struct.unpack_from('<I', buf, off + 4)[0]
        key_bytes  = buf[off + 8: off + 8 + self.key_size]
        return child_page, rec_num, key_bytes

    # ------------------------------------------------------------------
    # Key comparison (memcmp — all NTX types sort as byte strings)
    # ------------------------------------------------------------------

    @staticmethod
    def _cmp(a: bytes, b: bytes) -> int:
        if a < b:
            return -1
        if a > b:
            return 1
        return 0

    # ------------------------------------------------------------------
    # Internal: descend to leftmost key
    # ------------------------------------------------------------------

    def _descend_left(self, block: int):
        """
        Follow the leftmost path from block until a leaf, pushing each node.

        After this call, self._stack[-1] is (leaf_block, 0) — the first key
        in the subtree rooted at block.
        """
        while block != 0 and block != NTX_DUMMYNODE:
            buf = self._read_block(block)
            n = self._key_count(buf)
            if n == 0:
                break
            self._stack.append((block, 0))
            child, _, _ = self._key_entry(buf, 0)
            block = child

    # ------------------------------------------------------------------
    # Internal: return current key rec_num (auto-advance past extra slots)
    # ------------------------------------------------------------------

    def _current_rec(self) -> int | None:
        """
        Return the rec_num at the current stack position.

        Entries where idx >= key_count are past the end of the page (they
        are the "extra slot" used only for the rightmost child pointer; its
        rec_num field contains stale data and must be ignored).
        Such entries are popped automatically until a valid (idx < n) entry
        is found, or the stack is empty (EOF).
        """
        while self._stack:
            blk, idx = self._stack[-1]
            buf = self._read_block(blk)
            n = self._key_count(buf)
            if idx < n:
                _, rec_num, _ = self._key_entry(buf, idx)
                if rec_num != 0:
                    return rec_num
            self._stack.pop()
        self._eof = True
        return None

    def current_key_bytes(self) -> bytes | None:
        """Return the key bytes at the current stack position, or None."""
        if not self._stack:
            return None
        blk, idx = self._stack[-1]
        buf = self._read_block(blk)
        n = self._key_count(buf)
        if idx < n:
            _, _, key_bytes = self._key_entry(buf, idx)
            return key_bytes
        return None

    # ------------------------------------------------------------------
    # Navigation: go_top, go_bottom
    # ------------------------------------------------------------------

    def go_top(self) -> int | None:
        """First key in index order. Returns rec_num or None."""
        self._stack = []
        self._eof = False
        self._descend_left(self.root)
        return self._current_rec()

    def go_bottom(self) -> int | None:
        """Last key in index order. Returns rec_num or None."""
        self._stack = []
        self._eof = False
        block = self.root
        while block != 0 and block != NTX_DUMMYNODE:
            buf = self._read_block(block)
            n = self._key_count(buf)
            if n == 0:
                break
            idx = n - 1  # last real key
            self._stack.append((block, idx))
            # Follow rightmost child = item[n].child_page
            child, _, _ = self._key_entry(buf, n)
            block = child
        return self._current_rec()

    # ------------------------------------------------------------------
    # Navigation: next_rec
    # ------------------------------------------------------------------

    def next_rec(self) -> int | None:
        """
        Advance to the next key in index order.
        Must be called after seek(), go_top(), go_bottom(), or a prior next_rec().
        Returns rec_num or None (EOF).
        """
        if self._eof or not self._stack:
            return None

        blk, idx = self._stack.pop()
        buf = self._read_block(blk)
        n = self._key_count(buf)

        # Right subtree of key[idx] = item[idx+1].child_page
        if idx + 1 <= n:
            right_child, _, _ = self._key_entry(buf, idx + 1)
            if right_child != 0 and right_child != NTX_DUMMYNODE:
                # Push parent comeback marker (key[idx+1] visited after subtree)
                self._stack.append((blk, idx + 1))
                self._descend_left(right_child)
            else:
                # Advance to next key on same page (or extra slot → auto-bubble)
                self._stack.append((blk, idx + 1))
        # If idx+1 > n: this shouldn't happen for valid keys

        return self._current_rec()

    # ------------------------------------------------------------------
    # Seek
    # ------------------------------------------------------------------

    def _pad_key(self, key_bytes: bytes) -> bytes:
        if len(key_bytes) < self.key_size:
            return key_bytes + b' ' * (self.key_size - len(key_bytes))
        return key_bytes[:self.key_size]

    def seek(self, key_bytes: bytes, softseek: bool = False) -> int | None:
        """
        B-tree seek.

        softseek=False → exact match; returns rec_num or None (EOF)
        softseek=True  → first key >= key_bytes; returns rec_num or None (EOF)

        Leaves the stack in a state suitable for next_rec() calls.
        """
        key_bytes = self._pad_key(key_bytes)
        self._stack = []
        self._eof = False

        block = self.root
        found_exact = False

        while block != 0 and block != NTX_DUMMYNODE:
            buf = self._read_block(block)
            n = self._key_count(buf)
            if n == 0:
                break

            # Binary search: find the LEFTMOST pos where key[pos] >= key_bytes.
            # On exact match (c==0) we do NOT stop — we continue searching left
            # (hi = mid - 1) so that we always descend into the left child of the
            # leftmost equal key.  This mirrors hb_ntxPageKeyFind() behaviour:
            # exact match sets iLast=i and iEnd=i-1, continuing the loop.
            lo, hi = 0, n - 1
            pos = n  # default: all keys < key_bytes → rightmost child
            while lo <= hi:
                mid = (lo + hi) >> 1
                _, _, k = self._key_entry(buf, mid)
                c = self._cmp(key_bytes, k)
                if c <= 0:          # key_bytes <= key[mid]: candidate
                    pos = mid
                    hi = mid - 1    # keep searching left for earlier match
                else:               # key_bytes > key[mid]
                    lo = mid + 1

            self._stack.append((block, pos))

            # Always descend into child[pos] (left subtree of key[pos]).
            # This ensures we reach the FIRST (leftmost) occurrence of the key,
            # even when duplicates exist across multiple pages.
            child, _, _ = self._key_entry(buf, pos)
            block = child

        # Leaf reached.  _current_rec() auto-bubbles past out-of-range slots.
        rec = self._current_rec()

        if softseek:
            return rec  # first record with key >= key_bytes (or None if EOF)

        # Exact seek: verify the found key matches.
        if rec is not None and self._stack:
            blk, idx = self._stack[-1]
            buf = self._read_block(blk)
            n = self._key_count(buf)
            if idx < n:
                _, _, found_key = self._key_entry(buf, idx)
                if self._cmp(key_bytes, found_key) == 0:
                    return rec

        self._eof = True
        self._stack = []
        return None

    # ------------------------------------------------------------------
    # Iteration
    # ------------------------------------------------------------------

    def __iter__(self):
        """Iterate all records in index order, yielding rec_num values."""
        rec = self.go_top()
        while rec is not None:
            yield rec
            rec = self.next_rec()

    def iter_from(self, key_bytes: bytes, softseek: bool = True):
        """
        Iterate records from key_bytes (inclusive), yielding rec_num values.
        """
        rec = self.seek(key_bytes, softseek=softseek)
        while rec is not None:
            yield rec
            rec = self.next_rec()
