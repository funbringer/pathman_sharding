/* ------------------------------------------------------------------------
 *
 * range.sql
 *		RANGE sharding functions
 *
 * Copyright (c) 2017, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

/*
 * Add new shard
 */
CREATE OR REPLACE FUNCTION @extschema@.add_foreign_range_partition(
	parent_relid	REGCLASS,
	start_value		ANYELEMENT,
	end_value		ANYELEMENT,
	partition_name	TEXT,
	foreign_server	TEXT)
RETURNS TEXT AS
$$
DECLARE
	v_part_name			TEXT;
	v_attname			TEXT;
	v_check_name		TEXT;
	v_cond				TEXT;
	v_partition_relid	REGCLASS;

BEGIN
	PERFORM @extschema@.validate_relname(parent_relid);

	/* Acquire lock on parent */
	PERFORM @extschema@.lock_partitioned_relation(parent_relid);

	IF start_value >= end_value THEN
		RAISE EXCEPTION 'failed to create shard: start_value is greater than end_value';
	END IF;

	/* check range overlap */
	IF @extschema@.get_number_of_partitions(parent_relid) > 0 THEN
		PERFORM @extschema@.check_range_available(parent_relid,
												  start_value,
												  end_value);
	END IF;

	SELECT attname
	FROM @extschema@.pathman_config
	WHERE partrel = parent_relid
	INTO v_attname;

	SELECT quote_ident(schema) || '.' || quote_ident(partition_name)
	FROM @extschema@.get_plain_schema_and_relname(parent_relid)
	INTO v_part_name;

	EXECUTE format('CREATE FOREIGN TABLE %s() INHERITS (%s) SERVER %s',
				   v_part_name,
				   parent_relid::TEXT,
				   foreign_server);

	v_partition_relid := v_part_name::REGCLASS;

	v_cond := @extschema@.build_range_condition(v_partition_relid,
												v_attname,
												start_value,
												end_value);

	v_check_name := @extschema@.build_check_constraint_name(v_partition_relid,
															v_attname);

	EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s)',
				   v_partition_relid,
				   v_check_name,
				   v_cond);

	PERFORM @extschema@.on_update_partitions(parent_relid);

	RETURN v_part_name;
END
$$
LANGUAGE plpgsql;
