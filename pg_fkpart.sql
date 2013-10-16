﻿--
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
CREATE SCHEMA pgfkpart;

BEGIN;


--
-- pgfkpart._foreign_key_definitions view
--
CREATE OR REPLACE VIEW pgfkpart._foreign_key_definitions AS
SELECT
    tc.constraint_name, tc.table_schema, tc.table_name, kcu.column_name,
    ccu.table_schema AS foreign_table_schema,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name 
FROM 
    information_schema.table_constraints AS tc 
    JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name
    JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name
WHERE constraint_type = 'FOREIGN KEY';


--
-- pgfkpart._get_attname_by_attnum()
--
-- Get an attribute name by nspname, relname and attribute number
--
CREATE OR REPLACE FUNCTION pgfkpart._get_attname_by_attnum (
  NAME,
  NAME,
  SMALLINT
) RETURNS NAME
AS $BODY$
DECLARE
  _nspname ALIAS FOR $1;
  _relname ALIAS FOR $2;
  _attnum ALIAS  FOR $3;
  _attname NAME;
BEGIN
  SELECT a.attname INTO _attname
    FROM pg_namespace n, pg_class c, pg_attribute a
   WHERE n.nspname = _nspname
     AND c.relname = _relname
     AND n.oid = c.relnamespace
     AND c.oid = a.attrelid
     AND a.attnum = _attnum;

  RETURN _attname;
END;
$BODY$ LANGUAGE 'plpgsql';

--
-- pgfkpart._get_primary_key_def()
--
-- Get a primary key definition string for new partition.
--
CREATE OR REPLACE FUNCTION pgfkpart._get_primary_key_def (
  NAME,
  NAME,
  NAME
) RETURNS TEXT
AS $BODY$
DECLARE
  _nspname ALIAS FOR $1;
  _relname ALIAS FOR $2;
  _partname ALIAS FOR $3;
  _conname NAME;
  _conkey SMALLINT[];
  _size SMALLINT;
  _keyidx SMALLINT;
  _keyname NAME;
  _keys TEXT;
BEGIN
  SELECT a.conname, a.conkey, array_length(a.conkey, 1)
    INTO _conname, _conkey, _size
    FROM pg_namespace n, pg_class c, pg_constraint a
   WHERE n.nspname = _nspname
     AND c.relname = _relname
     AND n.oid = c.relnamespace
     AND c.oid = a.conrelid
     AND a.contype = 'p';

  IF NOT FOUND THEN
    RETURN '';
  END IF;

  _keys = '';
  
  FOR _keyidx IN 1.._size LOOP
    SELECT pgfkpart._get_attname_by_attnum(_nspname::name, _relname::name, _keyidx::smallint)
      INTO _keyname;

    _keys = _keys || ',' || _keyname;
  END LOOP;

  RETURN 'ALTER TABLE pgfkpart.' || _partname || ' ADD PRIMARY KEY (' || substring(_keys,2) || ')';
END;
$BODY$ LANGUAGE 'plpgsql';

--
-- pgfkpart._get_index_def()
--
-- Get index definition string(s) for new partition, excepting primary key.
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
  _indexname NAME;
  _indexdef TEXT;
BEGIN
  FOR _r IN SELECT idxs.indexname, idxs.indexdef, idx.indisunique, idx.indimmediate
FROM pg_indexes idxs
INNER JOIN pg_class cls2 ON (idxs.indexname=cls2.relname)
INNER JOIN pg_index idx ON (idx.indexrelid=cls2.oid)
INNER JOIN pg_class cls ON (cls.oid=idx.indrelid)
INNER JOIN pg_namespace nsp ON (nsp.oid=cls.relnamespace)
WHERE nsp.nspname=_nspname
  AND cls.relname=_relname
  AND idx.indisprimary <> true
  LOOP
    _indexname = regexp_replace (_r.indexname, '^' || _relname, _partname);
    IF _r.indisunique THEN
      _indexdef = 'ALTER TABLE pgfkpart.' || _partname || '
      ADD CONSTRAINT ' || _indexname || ' UNIQUE' ||
      substring (_r.indexdef from '\(.*\)');
      IF NOT _r.indimmediate THEN
        _indexdef = _indexdef || ' DEFERRABLE INITIALLY DEFERRED';
      END IF;
    ELSE
      _indexdef = regexp_replace(_r.indexdef, 'INDEX .* ON ', 'INDEX ' || _indexname || ' ON ');
      _indexdef = replace(_indexdef, ' ON ' || _relname, ' ON pgfkpart.' || _partname);      
    END IF;
    RETURN NEXT _indexdef;
  END LOOP;

  RETURN;
END
$BODY$ LANGUAGE 'plpgsql';

--
-- pgfkpart._get_partition_def()
--
-- Get a partiton definition string for new partition
--
CREATE OR REPLACE FUNCTION pgfkpart._get_partition_def (
  NAME,
  NAME,
  NAME,
  TEXT
) RETURNS TEXT
AS $BODY$
DECLARE
  _nspname ALIAS FOR $1;
  _relname ALIAS FOR $2;
  _partname ALIAS FOR $3;
  _cond ALIAS FOR $4;
  _partition_def TEXT;
  _const_def TEXT;
BEGIN
  _const_def = pgfkpart._get_constraint_def(_partname, _cond);

  _partition_def = 'CREATE TABLE pgfkpart.' || _partname || '( ';
  _partition_def = _partition_def || 'CONSTRAINT ' || _const_def;
  _partition_def = _partition_def || ') INHERITS (' || _nspname || '.' || _relname || ')';

  RETURN _partition_def;
END;
$BODY$ LANGUAGE 'plpgsql';

--
-- pgfkpart._get_export_query()
--
-- Get a query to export records with specified condition from parent table.
--
CREATE OR REPLACE FUNCTION pgfkpart._get_export_query (
  NAME,
  NAME,
  TEXT,
  TEXT
) RETURNS TEXT
AS $BODY$
DECLARE
  _nspname ALIAS FOR $1;
  _relname ALIAS FOR $2;
  _cond ALIAS FOR $3;
  _temp_file ALIAS FOR $4;
  _query TEXT;
BEGIN
  _query = 'COPY ( ' || 'SELECT * FROM ' || _nspname || '.' || _relname || ' WHERE ' || _cond || ' ) to ''' || _temp_file || '''';

  RETURN _query;
END;
$BODY$ LANGUAGE 'plpgsql';

--
-- pgfkpart._get_import_query()
--
-- Get a query to import records into specified partition
--
CREATE OR REPLACE FUNCTION pgfkpart._get_import_query (
  NAME,
  NAME,
  TEXT
) RETURNS TEXT
AS $BODY$
DECLARE
  _nspname ALIAS FOR $1;
  _relname ALIAS FOR $2;
  _temp_file ALIAS FOR $3;
  _query TEXT;
BEGIN
  _query = 'COPY ' || _nspname ||  '.' || _relname || ' FROM ''' || _temp_file || '''';

  RETURN _query;
END;
$BODY$ LANGUAGE 'plpgsql';

--
-- pgfkpart._add_partition()
--
-- Add a new partition with a specified condition
--
CREATE OR REPLACE FUNCTION pgfkpart._add_partition (
  NAME,
  NAME,
  NAME,
  TEXT,
  TEXT
) RETURNS BOOLEAN
AS $BODY$
DECLARE
  _nspname ALIAS FOR $1;
  _relname ALIAS FOR $2;
  _partname ALIAS FOR $3;
  _cond ALIAS FOR $4;
  _temp_file ALIAS FOR $5;
  _r RECORD;
  _def TEXT;
BEGIN
  FOR _r IN SELECT pgfkpart._get_partition_def(_nspname, _relname, _partname, _cond) LOOP
    _def = _r._get_partition_def || ';';
    RAISE NOTICE 'add_partition: %', _def;
    EXECUTE _def;
  END LOOP;
  
  SELECT pgfkpart._get_export_query(_nspname, _relname, _cond, _temp_file)
    INTO _def;
  _def = _def || ';';
  RAISE NOTICE 'add_partition: %', _def;
  EXECUTE _def;

  _def = 'DELETE FROM ' || _nspname || '.' || _relname || ' WHERE ' || _cond;
  _def = _def || ';';
  RAISE NOTICE 'add_partition: %', _def;
  EXECUTE _def;

  SELECT pgfkpart._get_import_query('pgfkpart', _partname, _temp_file)
    INTO _def;
  _def = _def || ';';
  RAISE NOTICE 'add_partition: %', _def;
  EXECUTE _def;

  SELECT pgfkpart._get_primary_key_def(_nspname, _relname, _partname)
    INTO _def;
  _def = _def || ';';
  RAISE NOTICE 'add_partition: %', _def;
  EXECUTE _def;

  FOR _r IN SELECT pgfkpart._get_index_def(_nspname, _relname, _partname) LOOP
    _def = _r._get_index_def || ';';
    RAISE NOTICE 'add_partition: %', _def;
    EXECUTE _def;
  END LOOP;

  RETURN true;
END
$BODY$ LANGUAGE 'plpgsql';

--
-- pgfkpart.merge_partition()
--
-- Merge a partition into the parent table.
--
CREATE OR REPLACE FUNCTION pgfkpart._merge_partition (
  NAME,
  NAME,
  NAME,
  TEXT,
  TEXT
) RETURNS BOOLEAN
AS $BODY$
DECLARE
  _nspname ALIAS FOR $1;
  _relname ALIAS FOR $2;
  _partname ALIAS FOR $3;
  _cond ALIAS FOR $4;
  _temp_file ALIAS FOR $5;
  _r RECORD;
  _def TEXT;
BEGIN
  SELECT pgfkpart._get_export_query('pgfkpart', _partname, '1 = 1', _temp_file)
    INTO _def;
  _def = _def || ';';
  RAISE NOTICE 'merge_partition: %', _def;
  EXECUTE _def;

  SELECT pgfkpart._get_import_query(_nspname, _relname, _temp_file)
    INTO _def;
  _def = _def || ';';
  RAISE NOTICE 'merge_partition: %', _def;
  EXECUTE _def;

  SELECT pgfkpart._get_detach_partition_def(_nspname, _relname, _partname)
    INTO _def;
  _def = _def || ';';
  RAISE NOTICE 'merge_partition: %', _def;
  EXECUTE _def;

  _def = 'DROP TABLE pgfkpart.' || _partname;
  _def = _def || ';';
  RAISE NOTICE 'merge_partition: %', _def;
  EXECUTE _def;

  RETURN true;
END
$BODY$ LANGUAGE 'plpgsql';

--
-- pgfkpart._get_constraint_name()
--
CREATE OR REPLACE FUNCTION pgfkpart._get_constraint_name (
  NAME
) RETURNS TEXT
AS $BODY$
DECLARE
  _partname ALIAS FOR $1;
BEGIN
  RETURN '__' || _partname || '_check';
END;
$BODY$ LANGUAGE 'plpgsql';

--
-- pgfkpart._get_constraint_def()
--
CREATE OR REPLACE FUNCTION pgfkpart._get_constraint_def (
  NAME,
  TEXT
) RETURNS TEXT
AS $BODY$
DECLARE
  _partname ALIAS FOR $1;
  _cond ALIAS FOR $2;
BEGIN
  RETURN pgfkpart._get_constraint_name(_partname) || ' CHECK(' || _cond || ')';
END;
$BODY$ LANGUAGE 'plpgsql';

--
-- pgfkpart._get_attach_partition_def()
--
-- Get a definition string for attaching a partition
--
CREATE OR REPLACE FUNCTION pgfkpart._get_attach_partition_def (
  NAME,
  NAME,
  NAME,
  TEXT
) RETURNS TEXT
AS $BODY$
DECLARE
  _nspname ALIAS FOR $1;
  _relname ALIAS FOR $2;
  _partname ALIAS FOR $3;
  _cond ALIAS FOR $4;
  _partition_def TEXT;
  _const_def TEXT;
BEGIN
  _const_def = pgfkpart._get_constraint_def(_partname, _cond);

  _partition_def = 'ALTER TABLE pgfkpart.' || _partname;
  _partition_def = _partition_def || ' INHERIT ' || _relname || ',';
  _partition_def = _partition_def || ' ADD CONSTRAINT ' || _const_def;

  RETURN _partition_def;
END;
$BODY$ LANGUAGE 'plpgsql';

--
-- pgfkpart.attach_partition()
--
-- Attach a new partition to the parent table with a specified condition
--
CREATE OR REPLACE FUNCTION pgfkpart.attach_partition (
  NAME,
  NAME,
  NAME,
  TEXT
) RETURNS BOOLEAN
AS $BODY$
DECLARE
  _nspname ALIAS FOR $1;
  _relname ALIAS FOR $2;
  _partname ALIAS FOR $3;
  _cond ALIAS FOR $4;
  _r RECORD;
  _def TEXT;
BEGIN
  --
  -- Check whether constraint is valid for this partition.
  --
  _def = 'SELECT count(*) FROM pgfkpart.' || _partname || ' WHERE NOT (' || _cond || ')';
  RAISE NOTICE 'attach_partition: %', _def;
  FOR _r IN EXECUTE _def LOOP
    IF _r.count > 0 THEN
      RAISE EXCEPTION 'attach_partition: % record(s) in this partition does not satisfy specified constraint.', _r.count;
    END IF;
  END LOOP;

  FOR _r IN SELECT pgfkpart._get_attach_partition_def(_nspname, _relname, _partname, _cond) LOOP
    _def = _r._get_attach_partition_def || ';';
    RAISE NOTICE 'attach_partition: %', _def;
    EXECUTE _def;
  END LOOP;

--  SELECT pgfkpart._get_primary_key_def(_nspname, _relname, _partname)
--    INTO _def;
--  _def = _def || ';';
--  RAISE NOTICE 'attach_partition: %', _def;
--  EXECUTE _def;
--
--  FOR _r IN SELECT pgfkpart._get_index_def(_nspname, _relname, _partname) LOOP
--    _def = _r._get_index_def || ';';
--    RAISE NOTICE 'attach_partition: %', _def;
--    EXECUTE _def;
--  END LOOP;

  RETURN true;
END
$BODY$ LANGUAGE 'plpgsql';

--
-- pgfkpart._get_detach_partition_def()
--
-- Get a definition string for detaching a partition
--
CREATE OR REPLACE FUNCTION pgfkpart._get_detach_partition_def (
  NAME,
  NAME,
  NAME
) RETURNS TEXT
AS $BODY$
DECLARE
  _nspname ALIAS FOR $1;
  _relname ALIAS FOR $2;
  _partname ALIAS FOR $3;
  _partition_def TEXT;
  _const_name TEXT;
BEGIN
  _const_name = pgfkpart._get_constraint_name(_partname);

  _partition_def = 'ALTER TABLE pgfkpart.' || _partname;
  _partition_def = _partition_def || ' NO INHERIT ' || _relname || ',';
  _partition_def = _partition_def || ' DROP CONSTRAINT ' || _const_name;

  RETURN _partition_def;
END;
$BODY$ LANGUAGE 'plpgsql';

--
-- pgfkpart.detach_partition()
--
-- Detach a partition from the parent table.
--
CREATE OR REPLACE FUNCTION pgfkpart.detach_partition (
  NAME,
  NAME,
  NAME
) RETURNS BOOLEAN
AS $BODY$
DECLARE
  _nspname ALIAS FOR $1;
  _relname ALIAS FOR $2;
  _partname ALIAS FOR $3;
  _r RECORD;
  _def TEXT;
BEGIN
  FOR _r IN SELECT pgfkpart._get_detach_partition_def(_nspname, _relname, _partname) LOOP
    _def = _r._get_detach_partition_def || ';';
    RAISE NOTICE 'detach_partition: %', _def;
    EXECUTE _def;
  END LOOP;

  RETURN true;
END
$BODY$ LANGUAGE 'plpgsql';


CREATE OR REPLACE FUNCTION pgfkpart.show_partition (
  NAME,
  NAME
) RETURNS SETOF NAME
AS $BODY$
DECLARE
  _nspname ALIAS FOR $1;
  _relname ALIAS FOR $2;
  _partname NAME;
BEGIN
  FOR _partname IN SELECT c.relname 
                     FROM pg_namespace n, pg_class p, pg_inherits i, pg_class c
                    WHERE n.nspname=_nspname
                      AND n.oid=p.relnamespace
                      AND p.relname=_relname
                      AND p.oid=i.inhparent
                      AND i.inhrelid=c.oid
                    ORDER BY c.relname LOOP
    RETURN NEXT _partname;
  END LOOP;
END
$BODY$ LANGUAGE 'plpgsql';


CREATE OR REPLACE FUNCTION pgfkpart._exec(
  TEXT
) RETURNS void
AS
$BODY$
BEGIN
 EXECUTE $1;
 END;
$BODY$
  LANGUAGE 'plpgsql';

-- DROP TABLE IF EXISTS pgfkpart.partition;
CREATE TABLE pgfkpart.partition
(
  partitionid SERIAL NOT NULL,
  table_schema NAME NOT NULL,
  table_name NAME NOT NULL,
  column_name NAME NOT NULL,
  foreign_table_schema NAME NOT NULL,
  foreign_table_name NAME NOT NULL,
  foreign_column_name NAME NOT NULL,
  CONSTRAINT partitions_pkey PRIMARY KEY (partitionid),
  CONSTRAINT partitions_key UNIQUE (table_schema, table_name)
)
WITH (
  OIDS=FALSE
);


CREATE OR REPLACE FUNCTION pgfkpart._get_partition_name (
  NAME,
  TEXT
) RETURNS NAME
AS $BODY$
DECLARE
  _relname ALIAS FOR $1;
  _column_value ALIAS FOR $2;
BEGIN
  RETURN _relname || '_p' || _column_value;
END
$BODY$
  LANGUAGE 'plpgsql';


CREATE OR REPLACE FUNCTION pgfkpart.partition_with_fk (
  NAME,
  NAME,
  NAME,
  NAME
) RETURNS void
AS $BODY$
BEGIN
  EXECUTE pgfkpart.partition_with_fk ($1, $2, $3, $4, NULL);
END
$BODY$
  LANGUAGE 'plpgsql';


CREATE OR REPLACE FUNCTION pgfkpart.partition_with_fk (
  NAME,
  NAME,
  NAME,
  NAME,
  TEXT
) RETURNS void
AS $BODY$
DECLARE
  _nspname ALIAS FOR $1;
  _relname ALIAS FOR $2;
  _foreignnspname ALIAS FOR $3;
  _foreignrelname ALIAS FOR $4;
  _tmpfilepath ALIAS FOR $5;
  _column_name NAME;
  _foreign_column_name NAME;
BEGIN
  -- Complete _tmpfilepath if unknown
  IF _tmpfilepath IS NULL
  THEN _tmpfilepath := '/tmp/pgfkpart_' || _relname;
  END IF;
  -- Get _column_name and _foreign_column_name
  SELECT column_name, foreign_column_name
  INTO _column_name, _foreign_column_name
  FROM pgfkpart._foreign_key_definitions
  WHERE table_name=_relname AND table_schema=_nspname
    AND foreign_table_name=_foreignrelname AND foreign_table_schema=_foreignnspname;
  -- Execute _add_partition on all the rows of _foreignrelname
  EXECUTE 'SELECT pgfkpart._exec(
    $A$SELECT pgfkpart._add_partition($$' || _nspname || '$$,
    $$' || _relname || '$$,
    $$' || _relname || '_p$A$ || ' || _foreign_column_name || ' || $A$$$,
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
    _r ' || _relname || '%ROWTYPE;
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
    THEN EXECUTE $EXEC$SELECT pgfkpart._add_partition($$' || _nspname || '$$,
    $$' || _relname || '$$,
    $$$EXEC$ || _partition || $EXEC$$$,
    $$' || _column_name || '= $EXEC$ || NEW.' || _foreign_column_name || ' || $EXEC$$$,
    $$' || _tmpfilepath || '$$)$EXEC$;
    END IF;
    -- Insert in the partition table instead
    EXECUTE $EXEC$INSERT INTO pgfkpart.$EXEC$ || _partition || $EXEC$ VALUES ($1.*) RETURNING *$EXEC$
      INTO _r
      USING NEW;
    RETURN _r;
  END
  $A$ LANGUAGE plpgsql';
  EXECUTE 'CREATE TRIGGER ' || _relname || '_before_insert
  BEFORE INSERT
  ON ' || _nspname || '.' || _relname || '
  FOR EACH ROW
  EXECUTE PROCEDURE ' || _nspname || '.' || _relname || '_child_insert();';
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
END
$BODY$ LANGUAGE 'plpgsql';


CREATE OR REPLACE FUNCTION pgfkpart.unpartition_with_fk (
  NAME,
  NAME
) RETURNS void
AS $BODY$
BEGIN
  EXECUTE pgfkpart.unpartition_with_fk ($1, $2, NULL);
END
$BODY$
  LANGUAGE 'plpgsql';


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
  _column_name NAME;
  _foreign_column_name NAME;
  _r RECORD;
BEGIN
  -- Complete _tmpfilepath if unknown
  IF _tmpfilepath IS NULL
  THEN _tmpfilepath := '/tmp/pgfkpart_' || _relname;
  END IF;
  -- Get the column name
  SELECT column_name, foreign_table_schema, foreign_table_name, foreign_column_name
  INTO _column_name, _foreignnspname, _foreignrelname, _foreign_column_name
  FROM pgfkpart.partition
  WHERE table_schema=_nspname AND table_name=_relname;
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
END
$BODY$ LANGUAGE 'plpgsql';

COMMIT;
