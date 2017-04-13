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
