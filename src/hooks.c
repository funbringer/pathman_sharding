/* ------------------------------------------------------------------------
 *
 * hooks.c
 *		Utility statement hook machinery
 *
 * Copyright (c) 2017, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "hooks.h"
#include "pathman_sharding.h"

#include "postgres.h"
#include "tcop/utility.h"


static ProcessUtility_hook_type	pathman_sharding_utility_hook_next;


/*
 * Utility function invoker hook.
 * NOTE: 'first_arg' is (PlannedStmt *) in PG 10, or (Node *) in PG <= 9.6.
 */
static void
#if PG_VERSION_NUM >= 100000
pathman_sharding_utility_hook(PlannedStmt *first_arg,
							  const char *queryString,
							  ProcessUtilityContext context,
							  ParamListInfo params,
							  QueryEnvironment *queryEnv,
							  DestReceiver *dest, char *completionTag)
{
#define QUERY_ENV queryEnv,

	Node   *parsetree		= first_arg->utilityStmt;
#else
pathman_sharding_utility_hook(Node *first_arg,
							  const char *queryString,
							  ProcessUtilityContext context,
							  ParamListInfo params,
							  DestReceiver *dest,
							  char *completionTag)
{
#define QUERY_ENV

	Node   *parsetree		= first_arg;
#endif
	Oid		parent_rel,
			foreign_rel,
			foreign_server;

	/* Are we going to drop a foreign table? */
	if (is_pathman_sharding_related_ftable_drop(parsetree,
												&foreign_rel,
												&foreign_server))
		pathman_sharding_deflate_foreign_table(foreign_rel, foreign_server);

	if (pathman_sharding_utility_hook_next)
		pathman_sharding_utility_hook_next(first_arg, queryString,
										   context, params,
										   QUERY_ENV
										   dest, completionTag);
	else
		standard_ProcessUtility(first_arg, queryString,
								context, params,
								QUERY_ENV
								dest, completionTag);

	/* Did we create a foreign table using postgres_fdw? */
	if (is_pathman_sharding_related_ftable_creation(parsetree,
													&parent_rel,
													&foreign_rel,
													&foreign_server))
		pathman_sharding_inflate_foreign_table(parent_rel,
											   foreign_rel,
											   foreign_server);
}

/* Setup our callbacks */
void
pathman_sharding_init_static_hook_data(void)
{
	pathman_sharding_utility_hook_next = ProcessUtility_hook;
	ProcessUtility_hook = pathman_sharding_utility_hook;
}
