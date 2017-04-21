/* ------------------------------------------------------------------------
 *
 * init.sql
 *		Creates config table and provides common utility functions
 *
 * Copyright (c) 2017, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

DO language plpgsql
$$
	DECLARE
		pathman_schema TEXT;

	BEGIN
		SELECT extnamespace::regnamespace
		FROM pg_catalog.pg_extension
		WHERE extname = 'pg_pathman'
		INTO pathman_schema;

		IF '@extschema@' != pathman_schema THEN
			RAISE EXCEPTION 'pathman_sharding should be installed into pg_pathman''s schema';
		END IF;
	END
$$;


CREATE OR REPLACE VIEW @extschema@.pathman_foreign_partition_list AS
SELECT ppl.*,
	   fdw.fdwname AS fdw_name,
	   fs.srvname AS server,
	   fs.srvoptions AS server_options
FROM @extschema@.pathman_partition_list AS ppl
JOIN pg_catalog.pg_foreign_table AS ft ON ppl.partition = ft.ftrelid
JOIN pg_catalog.pg_foreign_server AS fs ON ft.ftserver = fs.oid
JOIN pg_catalog.pg_foreign_data_wrapper AS fdw ON fs.srvfdw = fdw.oid;

GRANT SELECT ON @extschema@.pathman_foreign_partition_list TO PUBLIC;


/*
 * Distribute partitions among foreign servers.
 */
CREATE OR REPLACE FUNCTION @extschema@.distribute_partitions_among_servers(
	IN partitions		TEXT[],
	IN foreign_servers	TEXT[],
	OUT partition		TEXT,
	OUT foreign_server	TEXT)
RETURNS SETOF RECORD AS $$
DECLARE
	current_parts		TEXT[];
	current_parts_len	INT4;
	partitions_len		INT4 := array_length(partitions, 1);
	foreign_servers_len	INT4 := array_length(foreign_servers, 1);
	j					INT4;

BEGIN
	IF array_ndims(partitions) IS NULL OR
	   array_ndims(partitions) != 1 THEN
		RAISE EXCEPTION '"partitions" should have exactly 1 dimension';
	END IF;

	IF array_ndims(foreign_servers) IS NULL OR
	   array_ndims(foreign_servers) != 1 THEN
		RAISE EXCEPTION '"foreign_servers" should have exactly 1 dimension';
	END IF;

	FOR current_parts IN (SELECT partitions[i: i + foreign_servers_len - 1]
						  FROM generate_series(1, partitions_len, foreign_servers_len) i)
	LOOP
		current_parts_len := array_length(current_parts, 1);

		FOR j IN 1..current_parts_len
		LOOP
			partition := current_parts[j];
			foreign_server := foreign_servers[j];

			RETURN NEXT;
		END LOOP;
	END LOOP;

	RETURN;
END
$$ language plpgsql STRICT;
