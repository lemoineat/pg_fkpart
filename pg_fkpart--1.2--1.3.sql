/* contrib/pg_fkpart/pg_fkpart--1.1--1.2.sql */

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
  _constraintdef TEXT;
BEGIN
  FOR _r IN SELECT index_name, index_def, index_isunique, index_immediate, index_isexclusion, _constraintdef
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
      ADD CONSTRAINT ' || _r.index_name || ' ' || _constraintdef;
    ELSE
      _indexdef = _r.index_def;
    END IF;
    RETURN NEXT _indexdef;
  END LOOP;

  RETURN;
END
$BODY$ LANGUAGE 'plpgsql';


--
-- pgfkpart._get_index_def()
--
-- Get index definition string(s) for new partition
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
  END IF;
  RETURN _partindexdef;
END
$BODY$ LANGUAGE 'plpgsql';


--
-- pgfkpart._get_index_def()
--
-- Get index definition string(s) for new partition
--
CREATE OR REPLACE FUNCTION pgfkpart._get_index_def (
  NAME,
  NAME,
  NAME
) RETURNS SETOF TEXT
AS $BODY$
DECLARE
  _nspname ALIAS FOR $1;
  _relname ALIAS FOR $2;
  _partname ALIAS FOR $3;
  _r RECORD;
BEGIN
  FOR _r IN SELECT index_name, index_def, index_isunique, index_immediate, index_isexclusion, constraint_def
FROM pgfkpart.parentindex
WHERE table_schema=_nspname AND table_name=_relname
  LOOP
    RETURN NEXT pgfkpart._get_index_def (_nspname, _relname, _partname, _r.index_name, _r.index_def, _r.index_isunique, _r.index_immediate, _r.index_isexclusion, _r.constraint_def);
  END LOOP;

  RETURN;
END
$BODY$ LANGUAGE 'plpgsql';


--
-- pgfkpart._get_index_def()
--
-- Get index definition string(s) for new partition
--
CREATE OR REPLACE FUNCTION pgfkpart._get_index_def (
  NAME,
  NAME,
  NAME,
  NAME
) RETURNS SETOF TEXT
AS $BODY$
DECLARE
  _nspname ALIAS FOR $1;
  _relname ALIAS FOR $2;
  _partname ALIAS FOR $3;
  _indexname ALIAS FOR $4;
  _r RECORD;
BEGIN
  FOR _r IN SELECT index_name, index_def, index_isunique, index_immediate, index_isexclusion, constraint_def
FROM pgfkpart.parentindex
WHERE table_schema=_nspname AND table_name=_relname AND index_name=_indexname
  LOOP
    RETURN NEXT pgfkpart._get_index_def (_nspname, _relname, _partname, _r.index_name, _r.index_def, _r.index_isunique, _r.index_immediate, _r.index_isexclusion, _r.constraint_def);
  END LOOP;

  RETURN;
END
$BODY$ LANGUAGE 'plpgsql';


DO $$
BEGIN
  BEGIN
    ALTER TABLE pgfkpart.parentindex ADD COLUMN index_isexclusion boolean;
  EXCEPTION
    WHEN duplicate_column THEN RAISE NOTICE 'column index_isexclusion already exists in parentindex';
  END;
END;
$$;
UPDATE pgfkpart.parentindex SET index_isexclusion=FALSE WHERE index_isexclusion IS NULL;
ALTER TABLE pgfkpart.parentindex ALTER COLUMN index_isexclusion SET NOT NULL;
DO $$
BEGIN
  BEGIN
    ALTER TABLE pgfkpart.parentindex ADD COLUMN constraint_def text;
  EXCEPTION
    WHEN duplicate_column THEN RAISE NOTICE 'column constraint_def already exists in parentindex';
  END;
END;
$$;


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
    $$$EXEC$ || NEW.' || _foreign_column_name || ' || $EXEC$$$,
    $$' || _column_name || '= $EXEC$ || NEW.' || _foreign_column_name || ' || $EXEC$$$,
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
-- pgfkpart.dispatch_index()
--
-- Dispatch any nex index in the parent table into the children tables
--
CREATE OR REPLACE FUNCTION pgfkpart.dispatch_index (
  NAME,
  NAME
) RETURNS void
AS $BODY$
DECLARE
  _nspname ALIAS FOR $1;
  _relname ALIAS FOR $2;
  _r RECORD;
  _p RECORD;
  _partindexdef TEXT;
BEGIN
  -- Loop on all the new indexes
  FOR _r IN SELECT _nspname AS table_schema, _relname AS table_name, idxs.indexname AS index_name, idxs.indexdef AS index_def, idx.indisunique AS index_isunique, idx.indisexclusion AS index_isexclusion, idx.indimmediate AS index_immediate, idx.indisprimary AS index_isprimary, pg_get_constraintdef(con.oid, true) AS constraint_def
  FROM pg_indexes idxs
  INNER JOIN pg_class cls2 ON (idxs.indexname=cls2.relname)
  INNER JOIN pg_index idx ON (idx.indexrelid=cls2.oid)
  INNER JOIN pg_class cls ON (cls.oid=idx.indrelid)
  INNER JOIN pg_namespace nsp ON (nsp.oid=cls.relnamespace)
  LEFT JOIN pg_constraint con ON (idxs.indexname=con.conname)
  WHERE nsp.nspname=_nspname
    AND cls.relname=_relname
    AND idx.indisprimary <> true 
    AND EXISTS (SELECT 1 FROM pgfkpart.partition WHERE table_schema=_nspname AND table_name=_relname) LOOP
    -- Store the index in pgfkpart.parentindex
    INSERT INTO pgfkpart.parentindex (table_schema, table_name, index_name, index_def, index_isunique, index_immediate, index_isprimary, index_isexclusion, constraint_def)
    VALUES (_r.table_schema, _r.table_name, _r.index_name, _r.index_def, _r.index_isunique, _r.index_immediate, _r.index_isprimary, _r.index_isexclusion, _r.constraint_def);
    -- Remove the index in the parent table
    IF _r.index_isunique OR _r.index_isexclusion THEN
      EXECUTE 'ALTER TABLE ' || _nspname || '.' || _relname || ' DROP CONSTRAINT IF EXISTS ' || _r.index_name || ' CASCADE';  
    END IF;
    EXECUTE 'DROP INDEX IF EXISTS ' || _r.index_name || ' CASCADE';
    -- Add the index in the children tables
    FOR _p IN SELECT show_partition AS partition_name FROM pgfkpart.show_partition (_nspname, _relname) LOOP
      _partindexdef = pgfkpart._get_index_def (_nspname, _relname, _p.partition_name, _r.index_name, _r.index_def, _r.index_isunique, _r.index_immediate, _r.index_isexclusion, _r.constraint_def) || ';';
      RAISE NOTICE 'dispatch_index: %', _partindexdef;
      EXECUTE _partindexdef;
    END LOOP;
  END LOOP;

  RETURN;
END
$BODY$ LANGUAGE 'plpgsql';
