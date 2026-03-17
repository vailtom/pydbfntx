"""Tests for DBFReader."""
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
from pydbfntx import DBFReader


def test_num_records(dbf_path):
    with DBFReader(dbf_path) as dbf:
        assert dbf.num_records == 10


def test_field_names(dbf_path):
    with DBFReader(dbf_path) as dbf:
        names = dbf.field_names()
        assert "CODIGO" in names
        assert "NOME" in names
        assert "DTMODIF" in names


def test_field_types(dbf_path):
    with DBFReader(dbf_path) as dbf:
        assert dbf.field_type("CODIGO") == "N"
        assert dbf.field_type("NOME") == "C"
        assert dbf.field_type("DTMODIF") == "D"


def test_field_type_unknown(dbf_path):
    with DBFReader(dbf_path) as dbf:
        assert dbf.field_type("NONEXISTENT") is None


def test_record_1(dbf_path):
    with DBFReader(dbf_path) as dbf:
        rec = dbf.record(1)
        assert rec is not None
        assert rec["_recno"] == 1
        assert rec["_deleted"] is False
        assert rec["NOME"].strip() == "ANA"
        assert rec["CODIGO"] == 1
        assert rec["DTMODIF"] == date(2024, 1, 1)


def test_record_numeric(dbf_path):
    with DBFReader(dbf_path) as dbf:
        for i in range(1, 11):
            rec = dbf.record(i)
            assert isinstance(rec["CODIGO"], int)
            assert rec["CODIGO"] == i


def test_record_date(dbf_path):
    with DBFReader(dbf_path) as dbf:
        rec = dbf.record(1)
        assert isinstance(rec["DTMODIF"], date)


def test_record_not_deleted(dbf_path):
    with DBFReader(dbf_path) as dbf:
        for i in range(1, 11):
            assert dbf.record(i)["_deleted"] is False


def test_record_out_of_range_low(dbf_path):
    with DBFReader(dbf_path) as dbf:
        assert dbf.record(0) is None


def test_record_out_of_range_high(dbf_path):
    with DBFReader(dbf_path) as dbf:
        assert dbf.record(11) is None


def test_iteration_count(dbf_path):
    with DBFReader(dbf_path) as dbf:
        records = list(dbf)
        assert len(records) == 10


def test_iteration_physical_order(dbf_path):
    with DBFReader(dbf_path) as dbf:
        records = list(dbf)
        recnos = [r["_recno"] for r in records]
        assert recnos == list(range(1, 11))


def test_context_manager(dbf_path):
    with DBFReader(dbf_path) as dbf:
        assert dbf.record(1) is not None
