/* contrib/pg_fkpart/pg_fkpart--1.6--1.7.sql */

--
-- PostgreSQL Partitioning by Foreign Key Utility
--
-- Copyright(C) 2012 Uptime Technologies, LLC.
-- Copyright(C) 2018 Lemoine Automation Technologies
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
-- pgfkpart._get_index_def()
--
-- Get index definition string(s) for new partition
--
-- Fix the index definition change between PostgreSQL 9.5 and PostgreSQL 9.6
--
CREATE OR REPLACE FUNCTION pgfkpart._get_index_def (
  NAME,
  NAME,
  NAME,
  NAME,
  TEXT,
  BOOL,
  BOOL,
  BOOL,
  TEXT
) RETURNS TEXT
AS $BODY$
DECLARE
  _nspname ALIAS FOR $1;
  _relname ALIAS FOR $2;
  _partname ALIAS FOR $3;
  _index_name ALIAS FOR $4;
  _index_def ALIAS FOR $5;
  _index_isunique ALIAS FOR $6;
  _index_immediate ALIAS FOR $7;
  _index_isexclusion ALIAS FOR $8;
  _constraint_def ALIAS FOR $9;
  _partindexname NAME;
  _partindexdef TEXT;
  _def TEXT;
BEGIN
  _partindexname = pgfkpart._get_index_name (_nspname, _relname, _partname, _index_name);
  IF _index_isunique THEN
    _partindexdef = 'ALTER TABLE pgfkpart.' || _partname || '
    ADD CONSTRAINT ' || _partindexname || ' UNIQUE' ||
    substring (_index_def from '\(.*\)');
    IF NOT _index_immediate THEN
      _partindexdef = _partindexdef || ' DEFERRABLE INITIALLY DEFERRED';
    END IF;
  ELSIF _index_isexclusion THEN
    _partindexdef = 'ALTER TABLE pgfkpart.' || _partname || '
    ADD CONSTRAINT ' || _partindexname || ' ' || _constraint_def;
  ELSE
    _partindexdef = regexp_replace(_index_def, 'INDEX .* ON ', 'INDEX ' || _partindexname || ' ON ');
    _partindexdef = replace(_partindexdef, ' ON ' || _relname, ' ON pgfkpart.' || _partname);
    _partindexdef = replace(_partindexdef, ' ON ' || _nspname || '.' || _relname, ' ON pgfkpart.' || _partname);      
  END IF;
  RETURN _partindexdef;
END
$BODY$ LANGUAGE 'plpgsql';
