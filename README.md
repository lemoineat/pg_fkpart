
pg_fkpart extension


About pg_fkpart
=============

pg_fkpart is a PostgreSQL extension to partition tables following a
foreign key of a table.

It is initially based of pg_part from Satoshi Nagayasu <snaga@uptime.jp>.


SQL Functions
=============

pg_fkpart extension provides SQL functions and tables in `pgfkpart` schema, and these functions are *NOT* relocatable so far.


pgfkpart.partition
------------------

pgfkpart.partition is a table that contains the list of partitioned tables.

Columns:

- partitionid : ID
- table_schema : schema of the partitioned table
- table_name : name of the partitioned table
- column_name : foreign key column on which the table was partitioned
- foreign_table_schema : schema of the foreign key table
- foreign_table_name : name of the foreign key table
- foreign_column_name : column name in the foreign key table


pgfkpart.partition_with_fk
--------------------------

pgfkpart.partition_with_fk() function partitions a table following a specified foreign key

    pgfkpart.partition_with_fk(table_schema, table_name, foreign_table_schema, foreign_table_name, returning)
    pgfkpart.partition_with_fk(table_schema, table_name, foreign_table_schema, foreign_table_name, returning, tmp_file_path)

Parameters:

- table_schema : schema of the table to partition
- table_name : name of the table to partition
- foreign_table_schema : schema of the foreign key table
- foreign_table_name : name of the foreign key table
- returning : should the partitioned table support the SQL RETURNING command or not ?
- tmp_file_path : optional parameter to specify the temporary file path. By default: /tmp/pgfkpart_ followed by table_name


pgfkpart.unpartition_with_fk
----------------------------

pgfkpart.unpartition_with_fk() function unpartitions a table.

    pgfkpart.unpartition_with_fk(table_schema, table_name)
    pgfkpart.unpartition_with_fk(table_schema, table_name, tmp_file_path)

Parameters:

- table_schema : schema of the partitioned table
- table_name : name of the partitioned table
- tmp_file_path : optional parameter to specify the temporary file path. By default: /tmp/pgfkpart_ followed by table_name


pgfkpart.dispatch_index
-----------------------

pgfkpart.dispatch_index() function dispatches any new index (or a unique/exclusion constraint) in the parent tables to the children tables.

    pgfkpart.dispatch_index(table_schema, table_name)

Parameters:

- table_schema : schema of the partitioned table
- table_name : name of the partitioned table


pgfkpart.drop_index
-------------------

pgfkpart.drop_index() function removes an index in all the children tables.

    pgfkpart.drop_index(table_schema, table_name, index_name)

Parameters:

- table_schema : schema of the partitioned table
- table_name : name of the partitioned table
- index_name: name of the index


pgfkpart.drop_unique_constraint
-------------------------------

pgfkpart.drop_unique_constraint() function removes a unique or exclusion constraint in all the children tables.

    pgfkpart.drop_unique_constraint(table_schema, table_name, constraint_name)

Parameters:

- table_schema : schema of the partitioned table
- table_name : name of the partitioned table
- constraint_name: name of the unique or exclusion constraint


Authors
=======

    Nicolas Relange <nrelange@lemoinetechnologies.com>

For the intitial pg_part:
    Satoshi Nagayasu <snaga@uptime.jp>
