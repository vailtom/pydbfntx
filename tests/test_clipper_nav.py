"""
Tests for Clipper-compatible navigation API:
  DBSKIP, BOF, EOF, FOUND, LASTREC, LUPDATE, INDEXKEY, INDEXORD,
  FCOUNT, FIELDNAME, FIELDGET, RECNO, DELETED, SET DELETED.
"""
# pydbfntx — Pure-Python reader for Clipper/Harbour DBF+NTX files
# Copyright (C) 2026  Vailton Renato  vailtom <at> gmail <dot> com
# https://github.com/vailtom/pydbfntx
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

from datetime import date
import pytest
from pydbfntx import DBFNTXTable


# ---------------------------------------------------------------------------
# Fixture helper
# ---------------------------------------------------------------------------

def make_table(dbf_path, ntx_nome_path, ntx_dt_path):
    return DBFNTXTable(str(dbf_path), [str(ntx_nome_path), str(ntx_dt_path)])


# ---------------------------------------------------------------------------
# FCOUNT / FIELDNAME / FIELDGET
# ---------------------------------------------------------------------------

def test_field_count(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        assert t.field_count() == 3


def test_field_name_all(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        assert t.field_name(1) == "CODIGO"
        assert t.field_name(2) == "NOME"
        assert t.field_name(3) == "DTMODIF"


def test_field_name_matches_field_names(dbf_path, ntx_nome_path, ntx_dt_path):
    """field_name(n) deve ser consistente com dbf.field_names()."""
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        expected = t.dbf.field_names()
        for n, name in enumerate(expected, start=1):
            assert t.field_name(n) == name


def test_field_get_by_position(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        rec = t.go_top()
        # field_get(rec, n) deve ser equivalente a rec[field_name(n)]
        for n in range(1, t.field_count() + 1):
            assert t.field_get(rec, n) == rec[t.field_name(n)]


def test_field_get_nome(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order("nome")
        rec = t.go_top()                    # primeiro em ordem alfabetica = ANA
        assert t.field_get(rec, 2).strip() == "ANA"


# ---------------------------------------------------------------------------
# RECNO / DELETED
# ---------------------------------------------------------------------------

def test_recno_after_go_top(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order("nome")
        rec = t.go_top()
        assert t.recno() == rec["_recno"]


def test_recno_after_goto(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.goto(5)
        assert t.recno() == 5


def test_recno_after_skip(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order(0)
        t.go_top()
        rec = t.skip(2)
        assert t.recno() == rec["_recno"]


def test_deleted_is_false_for_all_records(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order(0)
        rec = t.go_top()
        while not t.eof():
            assert t.deleted() is False
            assert t.deleted() == rec["_deleted"]
            rec = t.skip()


# ---------------------------------------------------------------------------
# Estado inicial
# ---------------------------------------------------------------------------

def test_initial_eof_is_true(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        assert t.eof() is True


def test_initial_bof_is_false(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        assert t.bof() is False


def test_initial_found_is_false(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        assert t.found() is False


# ---------------------------------------------------------------------------
# BOF / EOF apos navegacao
# ---------------------------------------------------------------------------

def test_eof_false_after_go_top(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.go_top()
        assert t.eof() is False
        assert t.bof() is False


def test_eof_false_after_go_bottom(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.go_bottom()
        assert t.eof() is False
        assert t.bof() is False


def test_eof_true_after_skip_past_end(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order("nome")
        t.go_bottom()
        result = t.skip(1)
        assert result is None
        assert t.eof() is True
        assert t.bof() is False


def test_bof_true_after_skip_before_start(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order("nome")
        t.go_top()
        result = t.skip(-1)
        assert result is None
        assert t.bof() is True
        assert t.eof() is False


def test_eof_true_after_failed_seek(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order("nome")
        t.seek("ZZZZZZ")
        assert t.eof() is True


# ---------------------------------------------------------------------------
# FOUND
# ---------------------------------------------------------------------------

def test_found_true_after_exact_seek(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order("nome")
        t.seek("ANA")
        assert t.found() is True


def test_found_false_after_exact_seek_miss(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order("nome")
        t.seek("ZZZZZZ")
        assert t.found() is False


def test_found_true_after_softseek_exact_match(dbf_path, ntx_nome_path, ntx_dt_path):
    """Softseek que pousa exatamente na chave deve retornar found=True."""
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order("nome")
        t.seek("ANA", softseek=True)
        assert t.found() is True


def test_found_false_after_softseek_approximate(dbf_path, ntx_nome_path, ntx_dt_path):
    """Softseek que pousa em chave diferente (>=) deve retornar found=False."""
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order("nome")
        rec = t.seek("BEA", softseek=True)   # pousa em BEATRIZ, nao em BEA
        assert rec is not None
        assert rec["NOME"].strip() == "BEATRIZ"
        assert t.found() is False


def test_found_reset_after_navigation(dbf_path, ntx_nome_path, ntx_dt_path):
    """go_top/go_bottom devem zerar found."""
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order("nome")
        t.seek("ANA")
        assert t.found() is True
        t.go_top()
        assert t.found() is False


# ---------------------------------------------------------------------------
# LASTREC / LUPDATE
# ---------------------------------------------------------------------------

def test_lastrec(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        assert t.lastrec() == 10


def test_lastrec_matches_num_records(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        assert t.lastrec() == t.dbf.num_records


def test_lupdate_is_date(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        lu = t.lupdate()
        assert lu is None or isinstance(lu, date)


def test_lupdate_y2k_pivot(dbf_path, ntx_nome_path, ntx_dt_path):
    """Ano < 80 no cabecalho DBF deve ser interpretado como seculo 21."""
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        lu = t.lupdate()
        if lu is not None:
            assert lu.year >= 2000


# ---------------------------------------------------------------------------
# INDEXKEY / INDEXORD
# ---------------------------------------------------------------------------

def test_indexord_default(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        assert t.indexord() == 1


def test_indexord_after_set_order(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order(2)
        assert t.indexord() == 2


def test_indexord_zero_when_no_index(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order(0)
        assert t.indexord() == 0


def test_indexkey_nome(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order("nome")
        assert t.indexkey() == "NOME"


def test_indexkey_dtmodif(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order("dtmodif")
        assert t.indexkey() == "DTMODIF"


def test_indexkey_empty_when_no_index(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order(0)
        assert t.indexkey() == ""


# ---------------------------------------------------------------------------
# DBSKIP — ordem por indice
# ---------------------------------------------------------------------------

def test_skip_forward_one(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order("nome")
        t.go_top()                          # ANA
        rec = t.skip(1)
        assert rec["NOME"].strip() == "BEATRIZ"


def test_skip_forward_multiple(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order("nome")
        t.go_top()                          # ANA
        rec = t.skip(3)
        assert rec["NOME"].strip() == "DANIEL"


def test_skip_backward_one(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order("nome")
        t.go_top()
        t.skip(3)                           # DANIEL
        rec = t.skip(-1)
        assert rec["NOME"].strip() == "CARLOS"


def test_skip_zero_returns_current(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order("nome")
        rec1 = t.go_top()
        rec2 = t.skip(0)
        assert rec1["_recno"] == rec2["_recno"]


def test_skip_index_order_is_alphabetical(dbf_path, ntx_nome_path, ntx_dt_path):
    """Percorrer via skip deve respeitar a ordem do indice."""
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order("nome")
        names = []
        rec = t.go_top()
        while rec is not None and not t.eof():
            names.append(rec["NOME"].strip())
            rec = t.skip()
        names.append(t.skip(0)["NOME"].strip() if not t.eof() else names[-1])
        # os nomes coletados antes do EOF devem estar em ordem
        assert names == sorted(names)


def test_skip_traversal_count(dbf_path, ntx_nome_path, ntx_dt_path):
    """Percorrer todos os registros via skip deve bater com lastrec."""
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order("nome")
        count = 1
        t.go_top()
        while t.skip() is not None:
            count += 1
        assert count == t.lastrec()


def test_skip_after_seek_continues_from_found(dbf_path, ntx_nome_path, ntx_dt_path):
    """skip(1) apos seek deve ir para o proximo registro na ordem do indice."""
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order("nome")
        t.seek("CARLOS")
        rec = t.skip(1)
        assert rec["NOME"].strip() == "DANIEL"


def test_skip_after_goto_with_index(dbf_path, ntx_nome_path, ntx_dt_path):
    """skip(1) apos goto (sem NTX posicionado) deve funcionar via reposicionamento."""
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order("nome")
        ana_rec = t.seek("ANA")
        ana_recno = ana_rec["_recno"]
        t.goto(ana_recno)              # _ntx_ready = False
        rec = t.skip(1)               # deve reposicionar e avancar
        assert rec is not None
        assert rec["NOME"].strip() == "BEATRIZ"


# ---------------------------------------------------------------------------
# DBSKIP — ordem fisica (sem indice)
# ---------------------------------------------------------------------------

def test_skip_physical_forward(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order(0)
        t.go_top()                     # recno 1
        rec = t.skip(1)
        assert rec["_recno"] == 2


def test_skip_physical_backward(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order(0)
        t.goto(5)
        rec = t.skip(-2)
        assert rec["_recno"] == 3


def test_skip_physical_eof(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order(0)
        t.go_bottom()
        assert t.skip(1) is None
        assert t.eof() is True


def test_skip_physical_bof(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order(0)
        t.go_top()
        assert t.skip(-1) is None
        assert t.bof() is True


# ---------------------------------------------------------------------------
# SET DELETED
# ---------------------------------------------------------------------------

def test_set_deleted_default_is_on(dbf_path, ntx_nome_path, ntx_dt_path):
    """Por padrao o filtro de deletados esta ativo."""
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        assert t._filter_deleted is True


def test_set_deleted_off_does_not_break_iteration(dbf_path, ntx_nome_path, ntx_dt_path):
    """Com SET DELETED OFF a iteracao continua funcionando."""
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_deleted(False)
        t.set_order("nome")
        recs = list(t)
        assert len(recs) == 10


def test_set_deleted_off_go_top_works(dbf_path, ntx_nome_path, ntx_dt_path):
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_deleted(False)
        t.set_order("nome")
        rec = t.go_top()
        assert rec is not None
        assert rec["NOME"].strip() == "ANA"


def test_set_deleted_toggle(dbf_path, ntx_nome_path, ntx_dt_path):
    """Alternar SET DELETED mantem contagens corretas (fixture sem deletados)."""
    with make_table(dbf_path, ntx_nome_path, ntx_dt_path) as t:
        t.set_order("nome")
        count_on = sum(1 for _ in t)

        t.set_deleted(False)
        count_off = sum(1 for _ in t)

        t.set_deleted(True)
        count_on2 = sum(1 for _ in t)

        # fixture nao tem registros deletados; contagens devem ser iguais
        assert count_on == count_off == count_on2 == 10
