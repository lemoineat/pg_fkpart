/* contrib/pg_fkpart/pg_fkpart--unpackaged--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_fkpart" to load this file. \quit

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

--ALTER EXTENSION pg_fkpart ADD schema pgfkpart; -- Because the schema contains the extension
ALTER EXTENSION pg_fkpart ADD VIEW pgfkpart._foreign_key_definitions;
ALTER EXTENSION pg_fkpart ADD function pgfkpart._get_attname_by_attnum(NAME,NAME,SMALLINT);
ALTER EXTENSION pg_fkpart ADD function pgfkpart._get_primary_key_def(NAME,NAME,NAME);
ALTER EXTENSION pg_fkpart ADD function pgfkpart._get_parent_index_def(NAME,NAME);
ALTER EXTENSION pg_fkpart ADD function pgfkpart._get_index_name(NAME,NAME,NAME,NAME);
ALTER EXTENSION pg_fkpart ADD function pgfkpart._get_index_def(NAME,NAME,NAME,NAME,TEXT,BOOL,BOOL);
ALTER EXTENSION pg_fkpart ADD function pgfkpart._get_index_def(NAME,NAME,NAME);
ALTER EXTENSION pg_fkpart ADD function pgfkpart._get_index_def(NAME,NAME,NAME,NAME);
ALTER EXTENSION pg_fkpart ADD function pgfkpart._get_partition_def(NAME,NAME,NAME,TEXT);
ALTER EXTENSION pg_fkpart ADD function pgfkpart._get_export_query(NAME,NAME,TEXT,TEXT);
ALTER EXTENSION pg_fkpart ADD function pgfkpart._get_import_query(NAME,NAME,TEXT);
ALTER EXTENSION pg_fkpart ADD function pgfkpart._add_partition(NAME,NAME,NAME,TEXT,TEXT);
ALTER EXTENSION pg_fkpart ADD function pgfkpart._merge_partition(NAME,NAME,NAME,TEXT,TEXT);
ALTER EXTENSION pg_fkpart ADD function pgfkpart._get_constraint_name(NAME);
ALTER EXTENSION pg_fkpart ADD function pgfkpart._get_constraint_def(NAME,TEXT);
ALTER EXTENSION pg_fkpart ADD function pgfkpart._get_attach_partition_def(NAME,NAME,NAME,TEXT);
ALTER EXTENSION pg_fkpart ADD function pgfkpart.attach_partition(NAME,NAME,NAME,TEXT);
ALTER EXTENSION pg_fkpart ADD function pgfkpart._get_detach_partition_def(NAME,NAME,NAME);
ALTER EXTENSION pg_fkpart ADD function pgfkpart.detach_partition(NAME,NAME,NAME);
ALTER EXTENSION pg_fkpart ADD function pgfkpart.show_partition(NAME,NAME);
ALTER EXTENSION pg_fkpart ADD function pgfkpart._exec(TEXT);
ALTER EXTENSION pg_fkpart ADD TABLE pgfkpart.partition;
SELECT pg_catalog.pg_extension_config_dump('pgfkpart.partition', '');
ALTER EXTENSION pg_fkpart ADD TABLE pgfkpart.partforeignkey;
SELECT pg_catalog.pg_extension_config_dump('pgfkpart.partforeignkey', '');
ALTER EXTENSION pg_fkpart ADD TABLE pgfkpart.parentindex;
SELECT pg_catalog.pg_extension_config_dump('pgfkpart.parentindex', '');
ALTER EXTENSION pg_fkpart ADD function pgfkpart._get_partition_name(NAME,TEXT);
ALTER EXTENSION pg_fkpart ADD function pgfkpart._add_partition_with_fk(NAME,NAME,TEXT,TEXT,TEXT);
ALTER EXTENSION pg_fkpart ADD function pgfkpart.partition_with_fk(NAME,NAME,NAME,NAME,BOOLEAN);
ALTER EXTENSION pg_fkpart ADD function pgfkpart.partition_with_fk(NAME,NAME,NAME,NAME,BOOLEAN,TEXT);
ALTER EXTENSION pg_fkpart ADD function pgfkpart.unpartition_with_fk(NAME,NAME);
ALTER EXTENSION pg_fkpart ADD function pgfkpart.unpartition_with_fk(NAME,NAME,TEXT);
ALTER EXTENSION pg_fkpart ADD function pgfkpart.dispatch_index(NAME,NAME);
ALTER EXTENSION pg_fkpart ADD function pgfkpart.drop_index(NAME,NAME,NAME);
ALTER EXTENSION pg_fkpart ADD function pgfkpart.drop_unique_constraint(NAME,NAME,NAME);
