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

-- pg_fkpart unpackaged (1.3.0)

-- Comment the next line for an upgrade
CREATE SCHEMA pgfkpart;

-- TODO:
-- manage the tables with a primary key which is not _table_name || 'id'


BEGIN;

--
-- pgfkpart._foreign_key_definitions view
--
DROP VIEW IF EXISTS pgfkpart._foreign_key_definitions;
CREATE OR REPLACE VIEW pgfkpart._foreign_key_definitions AS
SELECT
    tc.constraint_name, tc.table_schema, tc.table_name, kcu.column_name,
    ccu.table_schema AS foreign_table_schema,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name,
    rc.match_option,
    rc.update_rule,
    rc.delete_rule
FROM 
    information_schema.table_constraints AS tc 
    JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name
    JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name
    LEFT OUTER JOIN information_schema.referential_constraints AS rc ON tc.constraint_name = rc.constraint_name
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
-- pgfkpart._get_index_name()
--
-- Get the index name for a partition
--
CREATE OR REPLACE FUNCTION pgfkpart._get_index_name (
  NAME,
  NAME,
  NAME,
  NAME
) RETURNS NAME
AS $BODY$
DECLARE
  _nspname ALIAS FOR $1;
  _relname ALIAS FOR $2;
  _partname ALIAS FOR $3;
  _index_name ALIAS FOR $4;
BEGIN
  RETURN regexp_replace (_index_name, '^' || _relname, _partname);
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
-- pgfkpart._merge_partition()
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
  _partition_def = _partition_def || ' NO INHERIT ' || _nspname || '.' ||  _relname || ',';
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
CREATE TABLE IF NOT EXISTS pgfkpart.partition
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

-- Reference to the foreign keys that were 'partitioned'
-- DROP TABLE IF EXISTS pgfkpart.partforeignkey
CREATE TABLE IF NOT EXISTS pgfkpart.partforeignkey
(
  partforeignkeyid SERIAL NOT NULL,
  constraint_name NAME NOT NULL,
  table_schema NAME NOT NULL,
  table_name NAME NOT NULL,
  column_name NAME NOT NULL,
  foreign_table_schema NAME NOT NULL,
  foreign_table_name NAME NOT NULL,
  foreign_column_name NAME NOT NULL,
  match_option TEXT NOT NULL,
  update_rule TEXT NOT NULL,
  delete_rule TEXT NOT NULL
)
WITH (
  OIDS=FALSE
);


DO $$
BEGIN
  BEGIN
    ALTER TABLE IF EXISTS pgfkpart.parentindex ADD COLUMN index_isexclusion boolean;
  EXCEPTION
    WHEN duplicate_column THEN RAISE NOTICE 'column index_isexclusion already exists in parentindex';
  END;
END;
$$;
DO $$
BEGIN
  BEGIN
    ALTER TABLE IF EXISTS pgfkpart.parentindex ADD COLUMN constraint_def text;
  EXCEPTION
    WHEN duplicate_column THEN RAISE NOTICE 'column constraint_def already exists in parentindex';
  END;
END;
$$;

-- Table to store the initial parent indexes
-- DROP TABLE IF EXISTS pgfkpart.parentindex
CREATE TABLE IF NOT EXISTS pgfkpart.parentindex
(
  parentindexid SERIAL NOT NULL,
  table_schema NAME NOT NULL,
  table_name NAME NOT NULL,
  index_name NAME NOT NULL,
  index_def TEXT NOT NULL,
  index_isunique BOOLEAN NOT NULL,
  index_immediate BOOLEAN NOT NULL,
  index_isprimary BOOLEAN NOT NULL,
  index_isexclusion BOOLEAN NOT NULL,
  constraint_def TEXT,
  CONSTRAINT parentindex_pkey PRIMARY KEY (parentindexid),
  CONSTRAINT parentindex_key UNIQUE (table_schema, table_name, index_name)
)
WITH (
  OIDS=FALSE
);

UPDATE pgfkpart.parentindex SET index_isexclusion=FALSE WHERE index_isexclusion IS NULL;
ALTER TABLE pgfkpart.parentindex ALTER COLUMN index_isexclusion SET NOT NULL;


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


CREATE OR REPLACE FUNCTION pgfkpart._add_partition_with_fk (
  NAME,
  NAME,
  TEXT,
  TEXT,
  TEXT
) RETURNS BOOLEAN
AS $BODY$
DECLARE
  _nspname ALIAS FOR $1;
  _relname ALIAS FOR $2;
  _column_value ALIAS FOR $3;
  _cond ALIAS FOR $4;
  _temp_file ALIAS FOR $5;
  _partname NAME;
  _r RECORD;
  _request TEXT;
  _result BOOLEAN;
BEGIN
  _partname := pgfkpart._get_partition_name (_relname, _column_value);
  -- Execute _add_partition
  SELECT pgfkpart._add_partition (_nspname, _relname, _partname, _cond, _temp_file) INTO _result;
  -- Restore the foreign key if needed
  FOR _r IN SELECT * FROM pgfkpart.partforeignkey WHERE table_schema=_nspname AND table_name=_relname LOOP
    -- Check the foreign key table exists. If not, create it
    EXECUTE 'SELECT * FROM pg_class t, pg_namespace s
WHERE t.relname=$$' || pgfkpart._get_partition_name (_r.foreign_table_name, _column_value) || '$$
  AND t.relnamespace=s.oid
  AND s.nspname=$$pgfkpart$$';
    IF NOT FOUND THEN
      EXECUTE 'SELECT pgfkpart._add_partition_with_fk(
      $$' || _r.foreign_table_schema || '$$,
      $$' || _r.foreign_table_name || '$$,
      $$' || _column_value || '$$,
      $$' || _r.foreign_column_name || '=' || _column_value || '$$,
      $$' || _temp_file || _r.foreign_table_name || '$$)';
    END IF;
    -- Add the foreign key once the foreign key table exists
     _request := 'ALTER TABLE pgfkpart.' || _partname ||' 
      ADD CONSTRAINT ' || _r.constraint_name || ' FOREIGN KEY (' || _r.column_name || ') 
          REFERENCES pgfkpart.' || pgfkpart._get_partition_name (_r.foreign_table_name, _column_value) || ' (' || _r.foreign_column_name || ') ';
     IF _r.match_option <> 'NONE' THEN
       _request := _request || _r.match_option;
     END IF;
     _request := _request || '
          ON UPDATE ' || _r.update_rule || ' ON DELETE ' || _r.delete_rule;
     EXECUTE _request;
   END LOOP;
   RETURN _result;
END
$BODY$
  LANGUAGE 'plpgsql';


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
  BOOLEAN
) RETURNS void
AS $BODY$
BEGIN
  EXECUTE pgfkpart.partition_with_fk ($1, $2, $3, $4, $5, NULL);
END
$BODY$
  LANGUAGE 'plpgsql';


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
-- pgfkpart.unpartition_with_fk()
--
-- Unpartition a table
--
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

--
-- pgfkpart.drop_index()
--
-- Remove an index in all the children tables
--
CREATE OR REPLACE FUNCTION pgfkpart.drop_index (
  NAME,
  NAME,
  NAME
) RETURNS void
AS $BODY$
DECLARE
  _nspname ALIAS FOR $1;
  _relname ALIAS FOR $2;
  _indexname ALIAS FOR $3;
  _p RECORD;
  _partindexname NAME;
BEGIN
  -- Remove the index in pgfkpart.parentindex
  DELETE FROM pgfkpart.parentindex
  WHERE table_schema=_nspname AND table_name=_relname AND index_name=_indexname;
  -- Remove the index in the parent table if any
  DROP INDEX IF EXISTS _indexname;
  -- Remove the index in all the children table
  FOR _p IN SELECT show_partition AS partition_name FROM pgfkpart.show_partition (_nspname, _relname) LOOP
    _partindexname = pgfkpart._get_index_name (_nspname, _relname, _p.partition_name, _indexname);
    RAISE NOTICE 'drop_index: %', _partindexname;
    EXECUTE 'DROP INDEX IF EXISTS pgfkpart.' || _partindexname;
  END LOOP;

  RETURN;
END
$BODY$ LANGUAGE 'plpgsql';

--
-- pgfkpart.drop_unique_constraint()
--
-- Remove a unique constraint in all the children tables
--
CREATE OR REPLACE FUNCTION pgfkpart.drop_unique_constraint (
  NAME,
  NAME,
  NAME
) RETURNS void
AS $BODY$
DECLARE
  _nspname ALIAS FOR $1;
  _relname ALIAS FOR $2;
  _constraintname ALIAS FOR $3;
  _p RECORD;
  _partconstraintname NAME;
BEGIN
  -- Remove the associated index in pgfkpart.parentindex
  DELETE FROM pgfkpart.parentindex
  WHERE table_schema=_nspname AND table_name=_relname AND index_name=_constraintname;
  -- Remove the constraint in the parent table if any
  EXECUTE 'ALTER TABLE ' || _nspname || '.' || _relname || ' DROP CONSTRAINT IF EXISTS ' || _constraintname || ' CASCADE';  
  -- Remove the constraint in all the children table
  FOR _p IN SELECT show_partition AS partition_name FROM pgfkpart.show_partition (_nspname, _relname) LOOP
    _partconstraintname = pgfkpart._get_index_name (_nspname, _relname, _p.partition_name, _constraintname);
    RAISE NOTICE 'drop_unique_constraint: %', _partconstraintname;
    EXECUTE 'ALTER TABLE pgfkpart.' || _p.partition_name || ' DROP CONSTRAINT IF EXISTS ' || _partconstraintname || ' CASCADE';
  END LOOP;

  RETURN;
END
$BODY$ LANGUAGE 'plpgsql';

COMMIT;
