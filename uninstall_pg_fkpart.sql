/* contrib/pg_fkpart/uninstall_pg_fkpart.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pgfkpart" to load this file. \quit

--
-- PostgreSQL Partitioning Utility
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

-- unpartition all the existing tables first
SELECT pgfkpart._exec ('select pgfkpart.unpartition_with_fk ($$' || table_schema || '$$, $$' || table_name || '$$)')
FROM pgfkpart.partition;

DROP SCHEMA pgfkpart CASCADE;
