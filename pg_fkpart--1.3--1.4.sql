/* contrib/pg_fkpart/pg_fkpart--1.3--1.4.sql */

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
-- pgfkpart.partition_with_fk()
--
-- Partition a table following a specified foreign key
--
CREATE OR REPLACE FUNCTION pgfkpart.partition_with_fk (
  NAME,
  NAME,
  NAME,
  NAME,
  BOOLEAN,
  TEXT
) RETURNS void
AS $BODY$
DECLARE
  _nspname ALIAS FOR $1;
  _relname ALIAS FOR $2;
  _foreignnspname ALIAS FOR $3;
  _foreignrelname ALIAS FOR $4;
  _returning ALIAS FOR $5;
  _tmpfilepath ALIAS FOR $6;
  _column_name NAME;
  _foreign_column_name NAME;
  _r RECORD;
  _returning_text TEXT;
BEGIN
  -- Check if the table has already been partitioned
  SELECT table_schema, table_name, column_name, foreign_table_schema, foreign_table_name, foreign_column_name
  INTO _r
  FROM pgfkpart.partition
  WHERE table_schema=_nspname AND table_name=_relname;
  IF FOUND
  THEN 
    IF _r.foreign_table_schema=_foreignnspname AND _r.foreign_table_name=_foreignrelname
    THEN RAISE INFO 'The table %.% is already partitioned', _nspname, _relname; RETURN;
    ELSE RAISE EXCEPTION 'The table %.% is already partitioned but with the foreign key %.%', _nspname, _relname, _r.foreign_table_schema, _r.foreign_table_name;
    END IF;
  END IF;
  -- Get _column_name and _foreign_column_name
  SELECT column_name, foreign_column_name
  INTO _column_name, _foreign_column_name
  FROM pgfkpart._foreign_key_definitions
  WHERE table_name=_relname AND table_schema=_nspname
    AND foreign_table_name=_foreignrelname AND foreign_table_schema=_foreignnspname;
  IF NOT FOUND
  THEN RAISE EXCEPTION 'No foreign key is defined between %.% and %.%', _nspname, _relname, _foreignnspname, _foreignrelname;
  END IF;
  -- If one of the foreign key is on a partitioned table, move the foreign key to the partitioned tables
  -- It must be done before add_partition
  FOR _r IN SELECT d.* FROM pgfkpart._foreign_key_definitions d
INNER JOIN pgfkpart.partition p ON (d.foreign_table_name=p.table_name AND d.foreign_table_schema=p.table_schema)
WHERE d.table_schema=_nspname AND d.table_name=_relname LOOP
    -- Store this foreign key in table partforeignkey
    INSERT INTO pgfkpart.partforeignkey(constraint_name, table_schema, table_name, column_name, foreign_table_schema, foreign_table_name, foreign_column_name, match_option, update_rule, delete_rule)
    VALUES (_r.constraint_name, _r.table_schema, _r.table_name, _r.column_name, _r.foreign_table_schema, _r.foreign_table_name, _r.foreign_column_name, _r.match_option, _r.update_rule, _r.delete_rule);
    -- Remove the old foreign key
    EXECUTE 'ALTER TABLE ' || _nspname || '.' || _relname || ' DROP CONSTRAINT ' || _r.constraint_name;
  END LOOP;
  -- Complete _tmpfilepath if unknown
  IF _tmpfilepath IS NULL
  THEN _tmpfilepath := '/tmp/pgfkpart_' || _relname;
  END IF;
  -- Set _returning_text
  IF _returning
  THEN _returning_text := '_r';
  ELSE _returning_text := 'NULL';
  END IF;
  -- Store the indexes in pgfkpart.parentindex and remove them
  INSERT INTO pgfkpart.parentindex (table_schema, table_name, index_name, index_def, index_isunique, index_immediate, index_isprimary, index_isexclusion, constraint_def)
  SELECT _nspname, _relname, idxs.indexname, idxs.indexdef, idx.indisunique, idx.indimmediate, idx.indisprimary, idx.indisexclusion, pg_get_constraintdef(con.oid, true) AS constraint_def
  FROM pg_indexes idxs
  INNER JOIN pg_class cls2 ON (idxs.indexname=cls2.relname)
  INNER JOIN pg_index idx ON (idx.indexrelid=cls2.oid)
  INNER JOIN pg_class cls ON (cls.oid=idx.indrelid)
  INNER JOIN pg_namespace nsp ON (nsp.oid=cls.relnamespace)
  LEFT JOIN pg_constraint con ON (idxs.indexname=con.conname)
  WHERE nsp.nspname=_nspname
    AND cls.relname=_relname
    AND idx.indisprimary <> true;
  FOR _r IN SELECT index_name, index_isunique, index_isexclusion FROM pgfkpart.parentindex WHERE table_schema=_nspname AND table_name=_relname LOOP
    RAISE NOTICE 'partition_with_fk: about to remove index %', _r.index_name;
    IF _r.index_isunique OR _r.index_isexclusion THEN
      EXECUTE 'ALTER TABLE ' || _nspname || '.' || _relname || ' DROP CONSTRAINT IF EXISTS ' || _r.index_name || ' CASCADE';  
    END IF;
    EXECUTE 'DROP INDEX IF EXISTS ' || _r.index_name || ' CASCADE';
  END LOOP;
  -- Execute _add_partition on all the rows of _foreignrelname
  RAISE INFO 'Partitioning %.%...', _nspname, _relname;
  EXECUTE 'SELECT pgfkpart._exec(
    $A$SELECT pgfkpart._add_partition_with_fk($$' || _nspname || '$$,
    $$' || _relname || '$$,
    $$$A$ || ' || _foreign_column_name || ' || $A$$$,
    $$' || _column_name || '=$A$ || ' ||  _foreign_column_name || ' || $A$$$,
    $$' || _tmpfilepath || '$$)$A$
  )
  FROM ' || _foreignnspname || '.' || _foreignrelname;
  -- Store the table was partitioned
  INSERT INTO pgfkpart.partition (table_schema, table_name, column_name, foreign_table_schema, foreign_table_name, foreign_column_name)
  VALUES (_nspname, _relname, _column_name, _foreignnspname, _foreignrelname, _foreign_column_name);
  -- Add a trigger to the main table
  EXECUTE 'CREATE OR REPLACE FUNCTION ' || _nspname || '.' || _relname || '_child_insert ()
  RETURNS trigger AS
  $A$
  DECLARE
    _partition NAME;
    _column_name NAME;
    _column_value TEXT;
    _r ' || _nspname || '.' || _relname || '%ROWTYPE;
  BEGIN
    -- Get the column name
    SELECT column_name
    INTO _column_name
    FROM pgfkpart.partition
    WHERE table_schema=$$' || _nspname || '$$ AND table_name=$$' || _relname || '$$;
    -- Get the column value
    EXECUTE $$SELECT $1.$$ || _column_name
    INTO _column_value
    USING NEW;
    -- Get the partition name
    SELECT pgfkpart._get_partition_name($$' || _relname || '$$, _column_value)
    INTO _partition;
    -- Check if the partition name has already been created. If not, create it
    IF NOT EXISTS (SELECT * FROM pg_class t, pg_namespace s
WHERE t.relname=_partition
  AND t.relnamespace=s.oid
  AND s.nspname=$$pgfkpart$$)
    THEN EXECUTE $EXEC$SELECT pgfkpart._add_partition_with_fk($$' || _nspname || '$$,
    $$' || _relname || '$$,
    $$$EXEC$ || NEW.' || _column_name || ' || $EXEC$$$,
    $$' || _column_name || '= $EXEC$ || NEW.' || _column_name || ' || $EXEC$$$,
    $$' || _tmpfilepath || '$$)$EXEC$;
    END IF;
    -- Insert in the partition table instead
    EXECUTE $EXEC$INSERT INTO pgfkpart.$EXEC$ || _partition || $EXEC$ VALUES ($1.*) RETURNING *$EXEC$
      INTO _r
      USING NEW;
    RETURN ' || _returning_text || ';
  END
  $A$ LANGUAGE plpgsql';
  EXECUTE 'CREATE TRIGGER ' || _relname || '_before_insert
  BEFORE INSERT
  ON ' || _nspname || '.' || _relname || '
  FOR EACH ROW
  EXECUTE PROCEDURE ' || _nspname || '.' || _relname || '_child_insert();';
  IF _returning THEN
    EXECUTE 'CREATE OR REPLACE FUNCTION ' || _nspname || '.' || _relname || '_parent_remove ()
    RETURNS trigger AS
    $A$
    DECLARE
      _r ' || _relname || '%ROWTYPE;
    BEGIN
      DELETE FROM ONLY ' || _relname || ' WHERE ' || _relname || 'id = NEW.' || _relname || 'id 
      RETURNING * INTO _r;
      RETURN _r;
    END
    $A$ LANGUAGE plpgsql';
    EXECUTE 'CREATE TRIGGER ' || _relname || '_after_insert
    AFTER INSERT
    ON ' || _nspname || '.' || _relname || '
    FOR EACH ROW
    EXECUTE PROCEDURE ' || _nspname || '.' || _relname || '_parent_remove();';
  END IF;
  RAISE INFO 'Partitioning done';
END
$BODY$ LANGUAGE 'plpgsql';

--
-- Fix the buggy triggers
-- 
CREATE OR REPLACE FUNCTION pgfkpart._fix_triggers () RETURNS void
AS $BODY$
DECLARE
  _returning BOOLEAN;
  _tmpfilepath TEXT;
  _r RECORD;
  _returning_text TEXT;
BEGIN
  FOR _r IN SELECT table_schema, table_name, column_name, foreign_table_schema, foreign_table_name, foreign_column_name
      FROM pgfkpart.partition
      WHERE column_name <> foreign_column_name LOOP
    -- _returning: consider to simplify it is always false
    _returning := FALSE;
    -- Set _tmpfilepath
    _tmpfilepath := '/tmp/pgfkpart_' || _r.table_name;
    -- Set _returning_text
    IF _returning
    THEN _returning_text := '_r';
    ELSE _returning_text := 'NULL';
    END IF;
    -- Fix the trigger function
    EXECUTE 'CREATE OR REPLACE FUNCTION ' || _r.table_schema || '.' || _r.table_name || '_child_insert ()
    RETURNS trigger AS
    $A$
    DECLARE
      _partition NAME;
      _column_name NAME;
      _column_value TEXT;
      _r ' || _r.table_schema || '.' || _r.table_name || '%ROWTYPE;
    BEGIN
      -- Get the column name
      SELECT column_name
      INTO _column_name
      FROM pgfkpart.partition
      WHERE table_schema=$$' || _r.table_schema || '$$ AND table_name=$$' || _r.table_name || '$$;
      -- Get the column value
      EXECUTE $$SELECT $1.$$ || _column_name
      INTO _column_value
      USING NEW;
      -- Get the partition name
      SELECT pgfkpart._get_partition_name($$' || _r.table_name || '$$, _column_value)
      INTO _partition;
      -- Check if the partition name has already been created. If not, create it
      IF NOT EXISTS (SELECT * FROM pg_class t, pg_namespace s
  WHERE t.relname=_partition
    AND t.relnamespace=s.oid
    AND s.nspname=$$pgfkpart$$)
      THEN EXECUTE $EXEC$SELECT pgfkpart._add_partition_with_fk($$' || _r.table_schema || '$$,
      $$' || _r.table_name || '$$,
      $$$EXEC$ || NEW.' || _r.column_name || ' || $EXEC$$$,
      $$' || _r.column_name || '= $EXEC$ || NEW.' || _r.column_name || ' || $EXEC$$$,
      $$' || _tmpfilepath || '$$)$EXEC$;
      END IF;
      -- Insert in the partition table instead
      EXECUTE $EXEC$INSERT INTO pgfkpart.$EXEC$ || _partition || $EXEC$ VALUES ($1.*) RETURNING *$EXEC$
        INTO _r
        USING NEW;
      RETURN ' || _returning_text || ';
    END
    $A$ LANGUAGE plpgsql';
  END LOOP;
END
$BODY$ LANGUAGE 'plpgsql';

-- Execute _fix_triggers
SELECT pgfkpart._fix_triggers ();

-- Drop it once it is done
DROP FUNCTION pgfkpart._fix_triggers();

