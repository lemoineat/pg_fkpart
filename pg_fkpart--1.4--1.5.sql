/* contrib/pg_fkpart/pg_fkpart--1.4--1.5.sql */

--
-- PostgreSQL Partitioning by Foreign Key Utility
--
-- Copyright(C) 2012 Uptime Technologies, LLC.
-- Copyright(C) 2013 Lemoine Automation Technologies
--
-- This program is free software; you can redistribute it and/or modify
-- it under the terms of the GNU General Public License as published by
-- the Free Software Foundation; version 2 of the License.
--
-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.
--
-- You should have received a copy of the GNU General Public License along
-- with this program; if not, write to the Free Software Foundation, Inc.,
-- 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
--

--
-- pgfkpart._get_parent_index_def()
--
-- Get index definition string(s) for the parent table
--
CREATE OR REPLACE FUNCTION pgfkpart._get_parent_index_def (
  NAME,
  NAME
) RETURNS SETOF TEXT
AS $BODY$
DECLARE
  _nspname ALIAS FOR $1;
  _relname ALIAS FOR $2;
  _r RECORD;
  _indexdef TEXT;
BEGIN
  FOR _r IN SELECT index_name, index_def, index_isunique, index_immediate, index_isexclusion, constraint_def
FROM pgfkpart.parentindex
WHERE table_schema=_nspname AND table_name=_relname
  LOOP
    IF _r.index_isunique THEN
      _indexdef = 'ALTER TABLE ' || _nspname || '.' || _relname || '
      ADD CONSTRAINT ' || _r.index_name || ' UNIQUE' ||
      substring (_r.index_def from '\(.*\)');
      IF NOT _r.index_immediate THEN
        _indexdef = _indexdef || ' DEFERRABLE INITIALLY DEFERRED';
      END IF;
    ELSIF _r.index_isexclusion THEN
      _indexdef = 'ALTER TABLE ' || _nspname || '.' || _relname || '
      ADD CONSTRAINT ' || _r.index_name || ' ' || _r.constraint_def;
    ELSE
      _indexdef = _r.index_def;
    END IF;
    RETURN NEXT _indexdef;
  END LOOP;

  RETURN;
END
$BODY$ LANGUAGE 'plpgsql';

