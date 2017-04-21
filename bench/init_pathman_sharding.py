#!/usr/bin/env python3

import testgres
from functools import reduce
from time import time


preload_libs = "shared_preload_libraries = 'pg_tsdtm, pathman_sharding, pg_pathman'"
server_sql = "create server {0} foreign data wrapper {1} options(dbname '{2}', port '{3}', host 'localhost')"
user_mapping_sql = "create user mapping for public server {0}"


NUM_ROWS = 100000
NUM_PARTITIONS = 100
NUM_SHARDS = 3

DB_NAME = 'postgres'
FDW_NAME = 'postgres_fdw'


def create_extension(node, ext_name):
	node.execute(
		DB_NAME,
		'create extension {} cascade'.format(ext_name),
		commit=True
	)

def register_shards(master, shards):
	with master.connect(dbname=DB_NAME) as master_con:
		master_con.begin()

		for shard in shards:
			master_con.execute(
				server_sql.format(
					shard.name,
					FDW_NAME,
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

def create_partitions(master, shards):
	print()
	with master.connect(dbname=DB_NAME) as master_con:
		master_con.begin()

		print('creating table test(val int not null)')
		master_con.execute("create table test(val int not null)")

		table_sql = (
			"select create_foreign_hash_partitions("
			"'test'::regclass," 	# table name
			"'val'::text,"			# column name
			"{0}::int,"				# number of partitions
			"'{{ {1} }}'::text[]"	# shards
			")"
		)

		print('creating foreign hash partitions ({})'.format(NUM_PARTITIONS))
		master_con.execute(
			table_sql.format(
				NUM_PARTITIONS,
				','.join([shard.name for shard in shards])
			)
		)

		print('inserting {} rows'.format(NUM_ROWS), end='... ', flush=True)
		t1 = time()
		master_con.execute(
			"insert into test select generate_series(1, $1)",
			NUM_ROWS
		)
		t2 = time()
		print('finished ({} ms)'.format(round((t2 - t1) * 1000, 2)))

		master_con.commit()

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
			print('{0}: {1} partitions'.format(shard_name, part_count))

		print(
			'total: {0} partitions'.format(
				sum([tup[1] for tup in part_info])
			)
		)

def main():
	master = None
	shards = []
	try:
		print('initializing master')
		master = (
			testgres
				.get_new_node('master')
				.init()
				.append_conf('postgresql.conf', preload_libs)
				.start()
		)
		print('\tcreating extensions')
		create_extension(master, FDW_NAME)
		create_extension(master, 'pg_pathman')
		create_extension(master, 'pathman_sharding')
		create_extension(master, 'pg_tsdtm')

		print()
		for i in range(0, NUM_SHARDS):
			shard_name = 'shard_{}'.format(i)

			print('initializing {}'.format(shard_name))
			shard = (
				testgres
					.get_new_node(shard_name)
					.init()
					.start()
			)
			print('\tcreating extensions')
			create_extension(shard, 'pg_tsdtm')

			shards.append(shard)

		# setup foreign servers and user mappings
		register_shards(master, shards)

		# create partitioned table
		create_partitions(master, shards)

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

