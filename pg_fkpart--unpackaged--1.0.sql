/* contrib/pg_fkpart/pg_fkpart--unpackaged--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pgfkpart" to load this file. \quit

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

ALTER EXTENSION pgfkpart ADD schema pgfkpart;

ALTER EXTENSION pgfkpart ADD function pgpart._get_attname_by_attnum(NAME,NAME,SMALLINT);
ALTER EXTENSION pgfkpart ADD function pgpart._get_primary_key_def(NAME,NAME,NAME);
ALTER EXTENSION pgfkpart ADD function pgpart._get_index_def(NAME,NAME,NAME);
ALTER EXTENSION pgfkpart ADD function pgpart._get_partition_def(NAME,NAME,NAME,TEXT);
ALTER EXTENSION pgfkpart ADD function pgpart._get_export_query(NAME,NAME,TEXT,TEXT);
ALTER EXTENSION pgfkpart ADD function pgpart._get_import_query(NAME,NAME,TEXT);
ALTER EXTENSION pgfkpart ADD function pgpart.add_partition(NAME,NAME,NAME,TEXT,TEXT);
ALTER EXTENSION pgfkpart ADD function pgpart.merge_partition(NAME,NAME,NAME,TEXT,TEXT);
ALTER EXTENSION pgfkpart ADD function pgpart._get_constraint_name(NAME);
ALTER EXTENSION pgfkpart ADD function pgpart._get_constraint_def(NAME,TEXT);
ALTER EXTENSION pgfkpart ADD function pgpart._get_attach_partition_def(NAME,NAME,NAME,TEXT);
ALTER EXTENSION pgfkpart ADD function pgpart.attach_partition(NAME,NAME,NAME,TEXT);
ALTER EXTENSION pgfkpart ADD function pgpart._get_detach_partition_def(NAME,NAME,NAME);
ALTER EXTENSION pgfkpart ADD function pgpart.detach_partition(NAME,NAME,NAME);
ALTER EXTENSION pgfkpart ADD function pgpart.show_partition(NAME,NAME);
