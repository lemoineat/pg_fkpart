/* contrib/pg_fkpart/pg_fkpart--1.5--1.6.sql */

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
-- pgfkpart.unpartition_with_fk()
--
-- Unpartition a table
--
CREATE OR REPLACE FUNCTION pgfkpart.unpartition_with_fk (
  NAME,
  NAME,
  TEXT
) RETURNS void
AS $BODY$
DECLARE
  _nspname ALIAS FOR $1;
  _relname ALIAS FOR $2;
  _tmpfilepath ALIAS FOR $3;
  _foreignnspname NAME;
  _foreignrelname NAME;
  _foreign_column_name NAME;
  _r RECORD;
  _request TEXT;
BEGIN
  -- Complete _tmpfilepath if unknown
  IF _tmpfilepath IS NULL
  THEN _tmpfilepath := '/tmp/pgfkpart_' || _relname;
  END IF;
  -- Remove the triggers
  EXECUTE 'DROP FUNCTION IF EXISTS ' || _nspname || '.' || _relname || '_child_insert() CASCADE';
  EXECUTE 'DROP FUNCTION IF EXISTS ' || _nspname || '.' || _relname || '_parent_remove() CASCADE';
  -- Merge all the data
  EXECUTE 'SELECT pgfkpart._exec(
    $A$SELECT pgfkpart._merge_partition($$' || _nspname || '$$,
    $$' || _relname || '$$,
    $$$A$ || t.relname || $A$$$, NULL, 
    $$' || _tmpfilepath || '$$)$A$
  )
  FROM pg_class t, pg_namespace s 
  WHERE t.relname ~* $REGEX$^' || _relname || '_p\d*$$REGEX$ 
    AND t.relnamespace=s.oid
    AND s.nspname=$$pgfkpart$$';
  -- Update pgfkpart.partition
  DELETE FROM pgfkpart.partition
  WHERE table_schema=_nspname AND table_name=_relname;
  DELETE FROM pgfkpart.partforeignkey
  WHERE table_schema=_nspname AND table_name=_relname;
  -- Restore any old foreign key to a partitioned table
  FOR _r IN SELECT * FROM pgfkpart.partforeignkey
  WHERE foreign_table_schema=_nspname AND foreign_table_name=_relname LOOP
    _request := 'ALTER TABLE ' || _r.table_schema || '.' || _r.table_name ||' 
     ADD CONSTRAINT ' || _r.constraint_name || ' FOREIGN KEY (' || _r.column_name || ') 
          REFERENCES ' || _r.foreign_table_schema || '.' || _r.foreign_table_name || ' (' || _r.foreign_column_name || ') ';
    IF _r.match_option <> 'NONE' THEN
      _request := _request || _r.match_option;
    END IF;
    _request := _request || '
         ON UPDATE ' || _r.update_rule || ' ON DELETE ' || _r.delete_rule;
    EXECUTE _request;
  END LOOP;
  DELETE FROM pgfkpart.partforeignkey
  WHERE foreign_table_schema=_nspname AND foreign_table_name=_relname;
  -- Restore the indexes
  FOR _r IN SELECT pgfkpart._get_parent_index_def(_nspname, _relname) LOOP
    _request = _r._get_parent_index_def || ';';
    RAISE NOTICE 'unpartition_with_fk: %', _request;
    EXECUTE _request;
  END LOOP;
  DELETE FROM pgfkpart.parentindex WHERE table_schema=_nspname AND table_name=_relname;
END
$BODY$ LANGUAGE 'plpgsql';

-- 
-- pgfkpart.complete_partition()
--
-- Complete manually the partition of the already partitioned tables (from the new rows of the foreign table)
--
CREATE OR REPLACE FUNCTION pgfkpart.complete_partition () RETURNS void
AS $BODY$
BEGIN
  EXECUTE 'SELECT pgfkpart.complete_partition (table_schema, table_name, NULL)
  FROM pgfkpart.partition';
END
$BODY$
  LANGUAGE 'plpgsql';

-- 
-- pgfkpart.complete_partition()
--
-- Complete manually the partition of the specified table (from the new rows of the foreign table)
--
CREATE OR REPLACE FUNCTION pgfkpart.complete_partition (
  NAME,
  NAME
) RETURNS void
AS $BODY$
BEGIN
  EXECUTE pgfkpart.complete_partition ($1, $2, NULL);
END
$BODY$
  LANGUAGE 'plpgsql';

-- 
-- pgfkpart.complete_partition()
--
-- Complete manually the partition of the specified table (from the new rows of the foreign table)
--
CREATE OR REPLACE FUNCTION pgfkpart.complete_partition(NAME, NAME, TEXT)
  RETURNS void AS
$BODY$
DECLARE
  _nspname ALIAS FOR $1;
  _relname ALIAS FOR $2;
  _tmpfilepath ALIAS FOR $3;
  _column_name NAME;
  _foreign_column_name NAME;
  _foreignnspname NAME;
  _foreignrelname NAME;
  _r RECORD;
BEGIN
  -- Check if the table has already been partitioned
  SELECT table_schema, table_name, column_name, foreign_table_schema, foreign_table_name, foreign_column_name
  INTO _r
  FROM pgfkpart.partition
  WHERE table_schema=_nspname AND table_name=_relname;
  IF NOT FOUND
  THEN 
    RAISE EXCEPTION 'The table %.% is not partitioned', _nspname, _relname;
  END IF;
  -- Get _column_name and _foreign_column_name
  _column_name := _r.column_name;
  _foreignnspname := _r.foreign_table_schema;
  _foreignrelname := _r.foreign_table_name;
  _foreign_column_name := _r.foreign_column_name;
  -- Complete _tmpfilepath if unknown
  IF _tmpfilepath IS NULL
  THEN _tmpfilepath := '/tmp/pgfkpart_' || _relname;
  END IF;
  -- Execute _add_partition on all the rows of _foreignrelname
  RAISE INFO 'Complete partition of %.%...', _nspname, _relname;
  EXECUTE 'SELECT pgfkpart._exec(
    $A$SELECT pgfkpart._add_partition_with_fk($$' || _nspname || '$$,
    $$' || _relname || '$$,
    $$$A$ || ' || _foreign_column_name || ' || $A$$$,
    $$' || _column_name || '=$A$ || ' ||  _foreign_column_name || ' || $A$$$,
    $$' || _tmpfilepath || '$$)$A$
  )
  FROM ' || _foreignnspname || '.' || _foreignrelname || '
  WHERE NOT EXISTS (SELECT * FROM pg_class t, pg_namespace s
                    WHERE t.relname=(SELECT pgfkpart._get_partition_name($$' || _relname || '$$, ' || _foreignrelname || '.' || _foreign_column_name || '::text))
                    AND t.relnamespace=s.oid AND s.nspname=$$pgfkpart$$)';
  RAISE INFO 'complete_partition of %.% done', _nspname, _relname;
END
$BODY$
LANGUAGE plpgsql VOLATILE
COST 100;
