#!/usr/bin/env python3

import testgres

import json
from functools import reduce
from time import time


preload_libs = "shared_preload_libraries = 'pg_tsdtm, pathman_sharding, pg_pathman'"
server_sql = "create server {0} foreign data wrapper {1} options(dbname '{2}', port '{3}', host 'localhost')"
user_mapping_sql = "create user mapping for public server {0}"

DB_NAME = 'postgres'


def append_config(node, config):
	config_name = config['name']

	for config_entry in config['settings']:
		config_key = config_entry['key']
		config_value = config_entry['value']

		config_string = '{0} = {1}'.format(config_key, config_value)
		node.append_conf(config_name, config_string)
		print('\t' + config_string)

def create_extension(node, ext_name):
	print('\t\t{}'.format(ext_name))
	node.execute(
		DB_NAME,
		'create extension {} cascade'.format(ext_name),
		commit=True
	)

def register_shards(master, shards, shard_fdw):
	with master.connect(dbname=DB_NAME) as master_con:
		master_con.begin()

		for shard in shards:
			master_con.execute(
				server_sql.format(
					shard.name,
					shard_fdw,
					DB_NAME,
					shard.port
				)
			)

			master_con.execute(
				user_mapping_sql.format(
					shard.name
				)
			)

		master_con.commit()

def create_tables(master, shards, tables):
	for table in tables:
		create_one_table(master, shards, table)

def create_one_table(master, shards, table):
	print()

	# extract table's definition
	table_name = table['name']
	table_columns = table['columns']

	with master.connect(dbname=DB_NAME) as master_con:
		master_con.begin()

		print('creating table {}'.format(table_name))
		for column in table_columns:
			print('\t{}'.format(column))

		master_con.execute(
			"create table {0}({1})".format(
				table_name,
				','.join([column for column in table['columns']])
			)
		)

		master_con.commit()

	table_partitioning = table['partitioning']
	if table_partitioning is not None:
		create_partitions(master, shards, table_name, table_partitioning)

	table_data_source = table['data_source']
	if table_data_source is not None:
		insert_data(master, table_name, table_data_source)

	table_extra_command = table['extra_command']
	if table_extra_command is not None:
		execute_extra_command(master, table_extra_command)

def create_partitions(master, shards, table_name, table_partitioning):
	partitioning_type = table_partitioning['type']
	partitioning_expression = table_partitioning['expression']
	partitioning_num_partitions = table_partitioning['partitions']

	if partitioning_type != 'HASH':
		raise Exception('only HASH sharding is supported')

	with master.connect(dbname=DB_NAME) as master_con:
		master_con.begin()

		table_sql = (
			"select create_foreign_hash_partitions("
			"'{0}'::regclass," 		# table name
			"'{1}'::text,"			# expression
			"'{2}'::int,"			# number of partitions
			"'{{ {3} }}'::text[]"	# shards
			")"
		)

		print(
			'creating {0} foreign hash partitions ({1})'.format(
				partitioning_num_partitions,
				partitioning_expression
			)
		)
		master_con.execute(
			table_sql.format(
				table_name,
				partitioning_expression,
				partitioning_num_partitions,
				','.join([shard.name for shard in shards])
			)
		)

		master_con.commit()

def insert_data(master, table_name, table_data_source):
	with master.connect(dbname=DB_NAME) as master_con:
		master_con.begin()

		print('inserting data: {}'.format(table_data_source), end=' ... ', flush=True)
		t1 = time()
		master_con.execute("insert into test {}".format(table_data_source))
		t2 = time()
		print('finished ({} ms)'.format(round((t2 - t1) * 1000, 2)))

		master_con.commit()

def execute_extra_command(master, table_extra_command):
	print ('extra command: {}'.format(table_extra_command))
	master.psql(DB_NAME, table_extra_command)

def report_stats(master):
	print()
	with master.connect(dbname=DB_NAME) as master_con:
		part_info = master_con.execute(
			"select server, count(*)"
			" from pathman_foreign_partition_list"
			" group by server"
			" order by server asc"
		)

		for shard_name, part_count in part_info:
			print('{0}: {1} foreign partitions'.format(shard_name, part_count))

		print(
			'total: {0} foreign partitions'.format(
				sum([tup[1] for tup in part_info])
			)
		)

def main():
	# read json config describing cluster
	config = None
	with open('pathman_sharding.json') as config_file:
		config = json.loads(config_file.read())

	# settings for master
	master_configs = config['master']['configs']
	master_extensions = config['master']['extensions']
	master_tables = config['master']['tables']

	# settings for shards
	shard_count = config['master']['shards']
	shard_configs = config['shard']['configs']
	shard_extensions = config['shard']['extensions']
	shard_fdw = config['shard']['fdw']

	master = None
	shards = []
	try:
		print('initializing master')
		master = testgres.get_new_node('master').init()
		print('\tport = {}'.format(master.port))
		for config in master_configs:
			append_config(master, config)
		master = master.start()

		print('\tcreating extensions')
		for extension in master_extensions:
			create_extension(master, extension)

		for i in range(0, shard_count):
			print()
			shard_name = 'shard_{}'.format(i)

			print('initializing {}'.format(shard_name))
			shard = testgres.get_new_node(shard_name).init()
			print('\tport = {}'.format(shard.port))
			for config in shard_configs:
				append_config(shard, config)
			shard = shard.start()

			print('\tcreating extensions')
			for extension in shard_extensions:
				create_extension(shard, extension)

			shards.append(shard)

		# setup foreign servers and user mappings
		register_shards(master, shards, shard_fdw)

		# create partitioned tables
		create_tables(master, shards, master_tables)

		# show information
		report_stats(master)

	except Exception as e:
		raise(e)

	finally:
		if master is not None:
			master.cleanup()

		for shard in shards:
			shard.cleanup()


if __name__ == "__main__":
	main()
