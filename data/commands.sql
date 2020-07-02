-- noinspection SqlResolveForFile

--! ListTables:
SELECT
	tablename
FROM
	pg_catalog.pg_tables
WHERE
	schemaname != 'pg_catalog'
AND schemaname != 'information_schema';
--! TableColumns:
SELECT
	column_name, udt_name, is_nullable, character_maximum_length
FROM
	information_schema.COLUMNS
WHERE
	TABLE_NAME = '${table}' ORDER BY ordinal_position;
--! TableKeys:
SELECT key_column FROM (SELECT kcu.table_schema,
       kcu.table_name,
       tco.constraint_name,
       tco.constraint_type,
       kcu.ordinal_position AS position,
       kcu.column_name AS key_column
FROM information_schema.table_constraints tco
JOIN information_schema.key_column_usage kcu
     ON kcu.constraint_name = tco.constraint_name
     AND kcu.constraint_schema = tco.constraint_schema
     AND kcu.constraint_name = tco.constraint_name
WHERE tco.constraint_type = 'PRIMARY KEY'
ORDER BY kcu.table_schema,
         kcu.table_name,
         position) AS X WHERE table_name = '${table}';
--! ForeignKeyRelations:
SELECT DISTINCT
    tc.constraint_name,
    kcu.column_name,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name
FROM
    information_schema.table_constraints AS tc
    JOIN information_schema.key_column_usage AS kcu
      ON tc.constraint_name = kcu.constraint_name
      AND tc.table_schema = kcu.table_schema
    JOIN information_schema.constraint_column_usage AS ccu
      ON ccu.constraint_name = tc.constraint_name
      AND ccu.table_schema = tc.table_schema
WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name = '${table}';
--! AllTablesForeignKeyRelations:
SELECT DISTINCT
        tc.table_name,
        ccu.table_name AS foreign_table_name
    FROM
        information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
          ON tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
          ON ccu.constraint_name = tc.constraint_name
          AND ccu.table_schema = tc.table_schema
    WHERE tc.constraint_type = 'FOREIGN KEY';
--! Generic Insert:
INSERT INTO ${table} (${id}) VALUES (${id_value});