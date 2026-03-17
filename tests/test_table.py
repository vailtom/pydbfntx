"""Tests for DBFNTXTable (high-level API)."""
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

from datetime import date
import pytest
from pydbfntx import DBFNTXTable


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_table(dbf_path, ntx_nome_path, ntx_dt_path):
    return DBFNTXTable(str(dbf_path), [str(ntx_nome_path), str(ntx_dt_path)])


# ---------------------------------------------------------------------------
# set_order
# ---------------------------------------------------------------------------

def test_set_order_by_number(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order(2)
        assert t._active_ntx.key_expr == "DTMODIF"


def test_set_order_by_name(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order("dtmodif")
        assert t._active_ntx.key_expr == "DTMODIF"


def test_set_order_zero(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order(0)
        assert t._active_ntx is None


def test_set_order_invalid_number(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        with pytest.raises(IndexError):
            t.set_order(99)


def test_set_order_invalid_name(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        with pytest.raises(KeyError):
            t.set_order("nonexistent")


# ---------------------------------------------------------------------------
# goto (physical)
# ---------------------------------------------------------------------------

def test_goto_record_3(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        rec = t.goto(3)
        assert rec is not None
        assert rec["_recno"] == 3
        assert rec["NOME"].strip() == "BEATRIZ"


def test_goto_out_of_range(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        assert t.goto(0) is None
        assert t.goto(999) is None


# ---------------------------------------------------------------------------
# go_top / go_bottom
# ---------------------------------------------------------------------------

def test_go_top_with_index(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order("nome")
        rec = t.go_top()
        assert rec["NOME"].strip() == "ANA"


def test_go_bottom_with_index(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order("nome")
        rec = t.go_bottom()
        assert rec["NOME"].strip() == "JOAO"


def test_go_top_physical_order(dbf_path, ntx_nome_path, ntx_dt_path):
    """Without index, go_top returns first physical non-deleted record."""
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order(0)
        rec = t.go_top()
        assert rec["_recno"] == 1


def test_go_bottom_physical_order(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order(0)
        rec = t.go_bottom()
        assert rec["_recno"] == 10


# ---------------------------------------------------------------------------
# seek — Character index
# ---------------------------------------------------------------------------

def test_seek_char_exact(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order("nome")
        rec = t.seek("ANA")
        assert rec is not None
        assert rec["NOME"].strip() == "ANA"


def test_seek_char_exact_not_found(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order("nome")
        assert t.seek("ZZZZZZ") is None


def test_seek_char_softseek(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order("nome")
        rec = t.seek("BEA", softseek=True)
        assert rec is not None
        assert rec["NOME"].strip() == "BEATRIZ"


def test_seek_char_softseek_before_all(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order("nome")
        rec = t.seek("AAA", softseek=True)
        assert rec is not None
        assert rec["NOME"].strip() == "ANA"


def test_seek_char_softseek_after_all(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order("nome")
        assert t.seek("ZZZZZZ", softseek=True) is None


# ---------------------------------------------------------------------------
# seek — Date index
# ---------------------------------------------------------------------------

def test_seek_date_exact(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order("dtmodif")
        rec = t.seek(date(2024, 1, 1))
        assert rec is not None
        assert rec["DTMODIF"] == date(2024, 1, 1)


def test_seek_date_softseek(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order("dtmodif")
        # 2024-01-05 not in index; softseek lands on 2024-02-01 (EVA)
        rec = t.seek(date(2024, 1, 5), softseek=True)
        assert rec is not None
        assert rec["DTMODIF"] >= date(2024, 1, 5)


# ---------------------------------------------------------------------------
# make_key
# ---------------------------------------------------------------------------

def test_make_key_str(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order("nome")
        key = t.make_key("ANA")
        assert len(key) == 20
        assert key.startswith(b"ANA")
        assert key == b"ANA" + b" " * 17


def test_make_key_date(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order("dtmodif")
        key = t.make_key(date(2024, 1, 1))
        assert key == b"20240101"


# ---------------------------------------------------------------------------
# Iteration
# ---------------------------------------------------------------------------

def test_iteration_with_index(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order("nome")
        recs = list(t)
        assert len(recs) == 10
        names = [r["NOME"].strip() for r in recs]
        assert names == sorted(names)


def test_iteration_physical_order(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order(0)
        recs = list(t)
        assert len(recs) == 10
        assert recs[0]["_recno"] == 1


def test_no_deleted_records_in_iteration(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order("nome")
        for rec in t:
            assert rec["_deleted"] is False


# ---------------------------------------------------------------------------
# records_since
# ---------------------------------------------------------------------------

def test_records_since_count(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order("dtmodif")
        recs = list(t.records_since(date(2024, 1, 1)))
        # ANA(2024-01-01), EVA(2024-02-01), BEATRIZ(2024-03-15),
        # GIOVANA(2024-04-01), IRIS(2024-05-01)
        assert len(recs) == 5


def test_records_since_all_gte(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order("dtmodif")
        boundary = date(2024, 1, 1)
        for rec in t.records_since(boundary):
            assert rec["DTMODIF"] >= boundary


def test_records_since_no_deleted(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order("dtmodif")
        for rec in t.records_since(date(2024, 1, 1)):
            assert rec["_deleted"] is False


def test_records_since_chronological_order(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order("dtmodif")
        dates = [r["DTMODIF"] for r in t.records_since(date(2020, 1, 1))]
        assert dates == sorted(dates)


def test_records_since_no_index_raises(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order(0)
        with pytest.raises(RuntimeError):
            list(t.records_since(date(2024, 1, 1)))
