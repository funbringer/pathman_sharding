/* ------------------------------------------------------------------------
 *
 * pathman_sharding.h
 *		Main logic (foreign table creation etc)
 *
 * Copyright (c) 2017, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#ifndef PATHMAN_SHARDING_H
#define PATHMAN_SHARDING_H

#include "postgres.h"
#include "nodes/parsenodes.h"
#include "utils/rel.h"


#define PG_PATHMAN		"pg_pathman"
#define POSTGRES_FDW	"postgres_fdw"

#define PG_PATHMAN_CONFIG_TABLE			"pathman_config"

#define POSTGRES_FDW_COMMAND_PROC		"postgres_fdw_execute_custom_command"


bool is_pathman_sharding_related_ftable_creation(const Node *parsetree,
												 Oid *parent_tbl,
												 Oid *foreign_tbl,
												 Oid *foreign_server);

bool is_pathman_sharding_related_ftable_drop(const Node *parsetree,
											 Oid *foreign_tbl,
											 Oid *foreign_server);


void pathman_sharding_inflate_foreign_table(Oid parent_rel,
											Oid foreign_rel,
											Oid foreign_server);

void pathman_sharding_deflate_foreign_table(Oid foreign_tbl,
											Oid foreign_server);


#endif /* PATHMAN_SHARDING_H */
