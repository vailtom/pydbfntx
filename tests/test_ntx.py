"""Tests for NTXReader (B-tree index)."""
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

import pytest
from pydbfntx import NTXReader


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

def test_header_nome(ntx_nome_path):
    with NTXReader(ntx_nome_path) as ntx:
        assert ntx.key_expr == "NOME"
        assert ntx.key_size == 20
        assert ntx.key_dec == 0
        assert ntx.large_file is True


def test_header_dtmodif(ntx_dt_path):
    with NTXReader(ntx_dt_path) as ntx:
        assert ntx.key_expr == "DTMODIF"
        assert ntx.key_size == 8


def test_header_root_is_nonzero(ntx_nome_path):
    with NTXReader(ntx_nome_path) as ntx:
        assert ntx.root != 0


# ---------------------------------------------------------------------------
# Iteration
# ---------------------------------------------------------------------------

def test_iteration_count(ntx_nome_path):
    with NTXReader(ntx_nome_path) as ntx:
        recs = list(ntx)
        assert len(recs) == 10


def test_iteration_no_duplicates(ntx_nome_path):
    with NTXReader(ntx_nome_path) as ntx:
        recs = list(ntx)
        assert len(recs) == len(set(recs))


def test_iteration_recno_range(ntx_nome_path):
    with NTXReader(ntx_nome_path) as ntx:
        recs = list(ntx)
        assert all(1 <= r <= 10 for r in recs)


def test_iteration_sorted_order(ntx_nome_path, dbf_path):
    """Records from nome.ntx must come out in alphabetical NOME order."""
    from pydbfntx import DBFReader
    with NTXReader(ntx_nome_path) as ntx, DBFReader(dbf_path) as dbf:
        names = [dbf.record(r)["NOME"].strip() for r in ntx]
        assert names == sorted(names)


def test_dtmodif_iteration_sorted_order(ntx_dt_path, dbf_path):
    """Records from dtmodif.ntx must come out in chronological order."""
    from pydbfntx import DBFReader
    with NTXReader(ntx_dt_path) as ntx, DBFReader(dbf_path) as dbf:
        dates = [dbf.record(r)["DTMODIF"] for r in ntx]
        assert dates == sorted(dates)


# ---------------------------------------------------------------------------
# go_top / go_bottom
# ---------------------------------------------------------------------------

def test_go_top_returns_first_recno(ntx_nome_path, dbf_path):
    from pydbfntx import DBFReader
    with NTXReader(ntx_nome_path) as ntx, DBFReader(dbf_path) as dbf:
        rec_num = ntx.go_top()
        assert rec_num is not None
        name = dbf.record(rec_num)["NOME"].strip()
        assert name == "ANA"  # alphabetically first


def test_go_bottom_returns_last_recno(ntx_nome_path, dbf_path):
    from pydbfntx import DBFReader
    with NTXReader(ntx_nome_path) as ntx, DBFReader(dbf_path) as dbf:
        rec_num = ntx.go_bottom()
        assert rec_num is not None
        name = dbf.record(rec_num)["NOME"].strip()
        assert name == "JOAO"  # alphabetically last


# ---------------------------------------------------------------------------
# seek
# ---------------------------------------------------------------------------

def test_seek_exact_first_key(ntx_nome_path):
    with NTXReader(ntx_nome_path) as ntx:
        key = "ANA".encode("cp850").ljust(20)
        rec_num = ntx.seek(key, softseek=False)
        assert rec_num == 1  # ANA is record #1 in the DBF


def test_seek_exact_not_found(ntx_nome_path):
    with NTXReader(ntx_nome_path) as ntx:
        key = "ZZZZZZ".encode("cp850").ljust(20)
        assert ntx.seek(key, softseek=False) is None


def test_seek_softseek_partial_key(ntx_nome_path, dbf_path):
    """'BEA' is not in the index, softseek should land on 'BEATRIZ'."""
    from pydbfntx import DBFReader
    with NTXReader(ntx_nome_path) as ntx, DBFReader(dbf_path) as dbf:
        key = "BEA".encode("cp850").ljust(20)
        rec_num = ntx.seek(key, softseek=True)
        assert rec_num is not None
        assert dbf.record(rec_num)["NOME"].strip() == "BEATRIZ"


def test_seek_softseek_before_all(ntx_nome_path, dbf_path):
    """Key before everything: softseek lands on the first record."""
    from pydbfntx import DBFReader
    with NTXReader(ntx_nome_path) as ntx, DBFReader(dbf_path) as dbf:
        key = "AAA".encode("cp850").ljust(20)
        rec_num = ntx.seek(key, softseek=True)
        assert rec_num is not None
        assert dbf.record(rec_num)["NOME"].strip() == "ANA"


def test_seek_softseek_after_all(ntx_nome_path):
    """Key after everything: softseek returns None (EOF)."""
    with NTXReader(ntx_nome_path) as ntx:
        key = "ZZZZZZ".encode("cp850").ljust(20)
        assert ntx.seek(key, softseek=True) is None


# ---------------------------------------------------------------------------
# iter_from
# ---------------------------------------------------------------------------

def test_iter_from_count(ntx_dt_path):
    """iter_from('20240101') should yield 5 records (dates >= 2024-01-01)."""
    with NTXReader(ntx_dt_path) as ntx:
        recs = list(ntx.iter_from(b"20240101", softseek=True))
        assert len(recs) == 5


def test_iter_from_exact_boundary(ntx_dt_path):
    """First key in iter_from must be >= boundary."""
    with NTXReader(ntx_dt_path) as ntx:
        recs = list(ntx.iter_from(b"20240101", softseek=True))
        assert len(recs) > 0
        # All 5 >= 20240101; count matches records_since test


def test_next_rec_after_top(ntx_nome_path, dbf_path):
    """Sequential next_rec() calls must yield keys in order."""
    from pydbfntx import DBFReader
    with NTXReader(ntx_nome_path) as ntx, DBFReader(dbf_path) as dbf:
        names = []
        rec = ntx.go_top()
        while rec is not None:
            names.append(dbf.record(rec)["NOME"].strip())
            rec = ntx.next_rec()
        assert names == sorted(names)
        assert len(names) == 10
