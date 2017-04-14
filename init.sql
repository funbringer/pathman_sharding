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
