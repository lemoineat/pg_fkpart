/* contrib/pg_fkpart/pg_fkpart--1.0--1.1.sql */

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

SELECT pg_catalog.pg_extension_config_dump('pgfkpart.partition_partitionid_seq', '');
SELECT pg_catalog.pg_extension_config_dump('pgfkpart.partforeignkey_partforeignkeyid_seq', '');
SELECT pg_catalog.pg_extension_config_dump('pgfkpart.parentindex_parentindexid_seq', '');
