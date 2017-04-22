/* ------------------------------------------------------------------------
 *
 * hash.sql
 *		HASH sharding functions
 *
 * Copyright (c) 2017, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

/*
 * Creates foreign hash partitions for specified relation
 */
CREATE OR REPLACE FUNCTION @extschema@.create_foreign_hash_partitions(
	parent_relid		REGCLASS,
	attribute			TEXT,
	partitions_count	INT4,
	foreign_servers		TEXT[],
	partition_data		BOOLEAN DEFAULT TRUE,
	partition_names		TEXT[] DEFAULT NULL,
	distibution_proc	REGPROC DEFAULT '@extschema@.distribute_partitions_among_servers')
RETURNS INTEGER AS
$$
DECLARE
	v_partition_relid	REGCLASS;
	v_part_name			TEXT;
	v_foreign_server	TEXT;
	v_check_name		TEXT;
	v_cond				TEXT;
	v_atttype			REGTYPE;
	cur_partition_idx	INT4 := 0;

BEGIN
	PERFORM @extschema@.validate_relname(parent_relid);

	IF partition_data = true THEN
		/* Acquire data modification lock */
		PERFORM @extschema@.prevent_relation_modification(parent_relid);
	ELSE
		/* Acquire lock on parent */
		PERFORM @extschema@.lock_partitioned_relation(parent_relid);
	END IF;

	attribute := lower(attribute);
	PERFORM @extschema@.common_relation_checks(parent_relid, attribute);


	/* Check if foreign servers array is null */
	IF foreign_servers IS NULL THEN
		RAISE EXCEPTION '"foreign_servers" should not be NULL';
	END IF;

	/* Check dimensions of foreign servers array */
	IF array_ndims(foreign_servers) IS NULL OR
	   array_ndims(foreign_servers) != 1 THEN
		RAISE EXCEPTION '"foreign_servers" should have exactly 1 dimension';
	END IF;

	/* Check if foreign servers array is empty */
	IF array_length(foreign_servers, 1) = 0 THEN
		RAISE EXCEPTION '"foreign_servers" should not be empty';
	END IF;


	/* Build partition names if needed */
	IF partition_names IS NULL THEN
		SELECT array_agg(quote_ident(schema) || '.' ||
						 quote_ident(relname || '_' ||
						 partition_idx))
		FROM @extschema@.get_plain_schema_and_relname(parent_relid),
			 generate_series(1, partitions_count) AS partition_idx
		INTO partition_names;
	ELSE
		IF array_ndims(partition_names) IS NULL OR
		   array_ndims(partition_names) != 1 THEN
			RAISE EXCEPTION '"partition_names" should have exactly 1 dimension';
		END IF;

		IF array_length(partition_names, 1) != partitions_count THEN
			RAISE EXCEPTION '"partition_names" should have exactly "partitions_count" members';
		END IF;
	END IF;


	/* Insert new entry to pathman config */
	INSERT INTO @extschema@.pathman_config (partrel, attname, parttype)
	VALUES (parent_relid, attribute, 1);

	/* Create partitions */
	FOR v_part_name, v_foreign_server IN
		EXECUTE format('SELECT * FROM %s($1, $2)', distibution_proc)
		USING partition_names, foreign_servers
	LOOP
		EXECUTE format('CREATE FOREIGN TABLE %s() INHERITS (%s) SERVER %s',
					   v_part_name,
					   parent_relid::TEXT,
					   v_foreign_server);

		v_partition_relid := v_part_name::REGCLASS;

		SELECT @extschema@.get_base_type(atttypid) FROM pg_catalog.pg_attribute
		WHERE attname = attribute AND attrelid = parent_relid
		INTO v_atttype;

		v_cond := @extschema@.build_hash_condition(v_atttype,
												   attribute,
												   partitions_count,
												   cur_partition_idx);

		v_check_name := @extschema@.build_check_constraint_name(v_partition_relid,
																attribute);

		EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s)',
					   v_partition_relid,
					   v_check_name,
					   v_cond);

		cur_partition_idx := cur_partition_idx + 1;
	END LOOP;

	/* Copy data */
	IF partition_data = true THEN
		PERFORM @extschema@.set_enable_parent(parent_relid, false);
		PERFORM @extschema@.partition_data(parent_relid);
	ELSE
		PERFORM @extschema@.set_enable_parent(parent_relid, true);
	END IF;

	RETURN partitions_count;
END
$$ LANGUAGE plpgsql
SET client_min_messages = WARNING;
