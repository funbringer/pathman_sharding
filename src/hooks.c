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


static ProcessUtility_hook_type	pathman_sharding_process_utility_hook_next;


static void pathman_sharding_process_utility_hook(Node *parsetree,
												  const char *queryString,
												  ProcessUtilityContext context,
												  ParamListInfo params,
												  DestReceiver *dest,
												  char *completionTag);


/* Setup our callbacks */
void
pathman_sharding_init_static_hook_data(void)
{
	pathman_sharding_process_utility_hook_next = ProcessUtility_hook;
	ProcessUtility_hook = pathman_sharding_process_utility_hook;
}


static void
pathman_sharding_process_utility_hook(Node *parsetree,
									  const char *queryString,
									  ProcessUtilityContext context,
									  ParamListInfo params,
									  DestReceiver *dest,
									  char *completionTag)
{
	Oid		parent_rel,
			foreign_rel,
			foreign_server;

	/* Are we going to drop a foreign table? */
	if (is_pathman_sharding_related_ftable_drop(parsetree,
												&foreign_rel,
												&foreign_server))
		pathman_sharding_deflate_foreign_table(foreign_rel, foreign_server);

	if (pathman_sharding_process_utility_hook_next)
		pathman_sharding_process_utility_hook_next(parsetree, queryString,
												   context, params,
												   dest, completionTag);
	else
		standard_ProcessUtility(parsetree, queryString, context,
								params, dest, completionTag);

	/* Did we create a foreign table using postgres_fdw? */
	if (is_pathman_sharding_related_ftable_creation(parsetree,
													&parent_rel,
													&foreign_rel,
													&foreign_server))
		pathman_sharding_inflate_foreign_table(parent_rel,
											   foreign_rel,
											   foreign_server);
}
