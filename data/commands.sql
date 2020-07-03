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
--! Table Item Count:
SELECT COUNT(*) FROM ${table};
--! Generic Select:
SELECT * FROM ${table} OFFSET ${offset} LIMIT ${limit};
--! Generic Select Cond:
SELECT * FROM ${table} WHERE ${conditions};
--! Generic Delete Cond:
DELETE FROM ${table} WHERE ${conditions};
--! Generic Update Cond:
UPDATE ${table} SET ${update_conditions} WHERE ${conditions};
--! Gist Extension:
CREATE EXTENSION IF NOT EXISTS btree_gist;
--! TimeMachine Func:
CREATE FUNCTION time_machine_versioning_${table}() RETURNS TRIGGER AS
$$
BEGIN
    IF TG_OP = 'UPDATE'
    THEN
        IF ${confliction_check} THEN
            RAISE EXCEPTION 'ID Cols is considered to be constant and not changed during time!';
        END IF;

        UPDATE  ${table} SET __lifetime = tstzrange(lower(__lifetime), current_timestamp)
        WHERE   ${id_search} AND current_timestamp <@ __lifetime;

        IF NOT FOUND THEN
            RETURN NULL;
        END IF;
    END IF;

    IF TG_OP IN ('INSERT', 'UPDATE')
    THEN
        INSERT INTO ${table} (${id_cols}, __lifetime ${cols}) VALUES
            (${id_cols_new}, tstzrange(current_timestamp, TIMESTAMPTZ 'infinity') ${col_datas});
        RETURN NEW;
    END IF;

    IF TG_OP = 'DELETE'
    THEN
        UPDATE ${table} SET __lifetime = tstzrange(lower(__lifetime), current_timestamp)
        WHERE ${id_search_old} AND current_timestamp <@ __lifetime;
        IF FOUND THEN
            RETURN OLD;
        ELSE
            RETURN NULL;
        END IF;
    END IF;
END;
$$ LANGUAGE plpgsql;
--! TimeMachine View:
CREATE VIEW ${table}_t AS
    SELECT  ${cols}
    FROM    ${table}
    WHERE   current_setting('time_machine.time')::tstzrange <@ __lifetime;
--! TimeMachine View Now:
CREATE VIEW ${table}_now AS
    SELECT  ${cols}
    FROM    ${table}
    WHERE   current_timestamp <@ __lifetime;
--! TimeMachine Trigger:
CREATE TRIGGER ${table}_trigger
    INSTEAD OF INSERT OR UPDATE OR DELETE
    ON ${table}_now
    FOR EACH ROW
    EXECUTE PROCEDURE time_machine_versioning_${table}();
--! ID Cols All:
SELECT ${id} FROM ${table};
--! Set TimeMachine Time:
SET time_machine.time = '2020-01-01 00:00:00';