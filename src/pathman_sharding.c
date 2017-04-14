/* ------------------------------------------------------------------------
 *
 * pathman_sharding.c
 *		Main logic (foreign table creation etc)
 *
 * Copyright (c) 2017, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "hooks.h"
#include "parse_utils.h"
#include "pathman_sharding.h"

#include "postgres.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/sysattr.h"
#include "access/htup_details.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_extension.h"
#include "commands/extension.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "foreign/foreign.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"


PG_MODULE_MAGIC;


/* Convenience macro */
#define get_extension_schema_name(extname) \
	get_namespace_name(get_extension_schema(extname))


void _PG_init(void);

static char *rebuild_indexdef_for_partition(char *index_def,
											Oid index_relid,
											Relation parent_rel,
											Relation foreign_rel);

static const char *get_rel_name_quoted(Oid relid);
static const char *get_rel_name_qualified(Oid relid);

static Oid pg_pathman_get_parent(Oid partition_relid);
static void pgfdw_execute_command(const char *command, Oid foreign_server);
static Oid dispatch_function(const char *funcname);
static Oid get_extension_schema(const char *extname);


/* Main initialization function */
void
_PG_init(void)
{
	pathman_sharding_init_static_hook_data();
}


/*
 * ----------------------
 *  Utility stmt filters
 * ----------------------
 */

bool
is_pathman_sharding_related_ftable_creation(const Node *parsetree,
											Oid *parent_tbl,		/* ret val #1 */
											Oid *foreign_tbl,		/* ret val #2 */
											Oid *foreign_server)	/* ret val #3 */
{
	const CreateForeignTableStmt   *stmt;
	ForeignServer				   *fs;
	ForeignDataWrapper			   *fdw;
	Oid								foreign_relid,
									parent_relid;

	if (!IsA(parsetree, CreateForeignTableStmt))
		return false;

	stmt = (const CreateForeignTableStmt *) parsetree;

	fs = GetForeignServerByName(stmt->servername, false);

	/* Return 'foreign_server' */
	if (foreign_server)
		*foreign_server = fs->serverid;

	fdw = GetForeignDataWrapper(fs->fdwid);
	if (strcmp(fdw->fdwname, POSTGRES_FDW) != 0)
		return false;

	foreign_relid = RangeVarGetRelid(stmt->base.relation, NoLock, false);
	parent_relid = pg_pathman_get_parent(foreign_relid);
	if (!OidIsValid(parent_relid))
		return false;

	/* Return 'parent_tbl' */
	if (parent_tbl)
		*parent_tbl = parent_relid;

	/* Return 'foreign_tbl' */
	if (foreign_tbl)
		*foreign_tbl = foreign_relid;

	return true;
}

bool
is_pathman_sharding_related_ftable_drop(const Node *parsetree,
										Oid *foreign_tbl,			/* ret val #1 */
										Oid *foreign_server)		/* ret val #2 */
{
	const DropStmt *stmt;
	ListCell	   *lc;

	if (!IsA(parsetree, DropStmt))
		return false;

	stmt = (const DropStmt *) parsetree;

	if (stmt->removeType != OBJECT_FOREIGN_TABLE)
		return false;

	foreach (lc, stmt->objects)
	{
		List			   *namelist = (List *) lfirst(lc);
		RangeVar		   *foreign_rv;
		Oid					foreign_relid;
		ForeignTable	   *ft;
		ForeignServer	   *fs;
		ForeignDataWrapper *fdw;

		foreign_rv = makeRangeVarFromNameList(namelist);
		foreign_relid = RangeVarGetRelid(foreign_rv, AccessExclusiveLock, false);

		ft = GetForeignTable(foreign_relid);
		fs = GetForeignServer(ft->serverid);
		fdw = GetForeignDataWrapper(fs->fdwid);
		if (strcmp(fdw->fdwname, POSTGRES_FDW) != 0)
			return false;

		if (!OidIsValid(pg_pathman_get_parent(foreign_relid)))
			return false;

		/* Return 'foreign_tbl' */
		if (foreign_tbl)
			*foreign_tbl = foreign_relid;

		/* Return 'foreign_server' */
		if (foreign_server)
			*foreign_server = fs->serverid;

		return true;
	}

	return false;
}


/*
 * --------------------------------------
 *  Create\drop tables on foreign server
 * --------------------------------------
 */

void
pathman_sharding_inflate_foreign_table(Oid parent_tbl,
									   Oid foreign_tbl,
									   Oid foreign_server)
{
	Relation		parent_rel,
					foreign_rel;
	TupleDesc		foreign_descr;
	List		   *index_list;
	StringInfoData	query;
	ListCell	   *lc;
	int				i;

	/* Should be locked already */
	foreign_rel = heap_open(foreign_tbl, NoLock);
	foreign_descr = RelationGetDescr(foreign_rel);

	initStringInfo(&query);
	appendStringInfo(&query, "CREATE TABLE %s.%s (",
					 quote_identifier(get_namespace_name(
											RelationGetNamespace(foreign_rel))),
					 quote_identifier(RelationGetRelationName(foreign_rel)));

	for (i = 0; i < foreign_descr->natts; i++)
	{
		Form_pg_attribute attr = foreign_descr->attrs[i];

		if (i != 0)
			appendStringInfoString(&query, ", ");

		/* NAME TYPE[(typmod)] [NOT NULL] [COLLATE "collation"] */
		appendStringInfo(&query, "%s %s%s%s",
						 quote_identifier(NameStr(attr->attname)),
						 format_type_with_typemod_qualified(attr->atttypid,
															attr->atttypmod),
						 (attr->attnotnull ? " NOT NULL" : ""),
						 (attr->attcollation ?
									psprintf(" COLLATE \"%s\"",
											 get_collation_name(attr->attcollation)) :
									""));
	}

	appendStringInfoChar(&query, ')');

	/* Create table on foreign server */
	pgfdw_execute_command(query.data, foreign_server);


	/* Open parent in order to copy its indexes */
	parent_rel = heap_open(parent_tbl, AccessShareLock);

	index_list = RelationGetIndexList(parent_rel);
	foreach (lc, index_list)
	{
		PG_TRY();
		{
			Oid		indexid = lfirst_oid(lc);
			Datum	indexdef;
			char   *indexdef_ctsr;

			/* Protect index from concurrent drop */
			LockRelationOid(indexid, AccessShareLock);

			/* Fetch definition of this index */
			indexdef = DirectFunctionCall1(pg_get_indexdef,
										   ObjectIdGetDatum(indexid));
			indexdef_ctsr = TextDatumGetCString(indexdef);

			resetStringInfo(&query);
			appendStringInfoString(&query,
								   rebuild_indexdef_for_partition(indexdef_ctsr,
																  indexid,
																  parent_rel,
																  foreign_rel));

			UnlockRelationOid(indexid, AccessShareLock);

			/* Create index on foreign server */
			pgfdw_execute_command(query.data, foreign_server);
		}
		PG_CATCH();
		{
			/* DO NOTHING */
		}
		PG_END_TRY();
	}

	heap_close(parent_rel, AccessShareLock);
	heap_close(foreign_rel, NoLock);
}

void
pathman_sharding_deflate_foreign_table(Oid foreign_tbl,
									   Oid foreign_server)
{
	char *query;

	query = psprintf("DROP TABLE %s.%s CASCADE",
					 quote_identifier(get_namespace_name(
											get_rel_namespace(foreign_tbl))),
					 quote_identifier(get_rel_name(foreign_tbl)));

	/* Drop table on foreign server */
	pgfdw_execute_command(query, foreign_server);
}


/*
 * ------------------
 *  Helper functions
 * ------------------
 */

static char *
rebuild_indexdef_for_partition(char *index_def,
							   Oid index_relid,
							   Relation parent_rel,
							   Relation foreign_rel)
{
	char		   *cur = index_def,
				   *prev;

	const char	   *idxname = get_rel_name_quoted(index_relid);
	const char	   *tblname = get_rel_name_qualified(RelationGetRelid(parent_rel));

	StringInfoData	buffer;

	elog(DEBUG1, "pathman_sharding: OLD indexdef is %s", index_def);

	initStringInfo(&buffer);

	/* CREATE [UNIQUE] INDEX */
	prev = cur;
	cur = skip_consts(cur, 2, "CREATE INDEX", "CREATE UNIQUE INDEX");
	appendStringInfo(&buffer, "%s ", prev);

	/* index name */
	cur = skip_consts(cur, 1, idxname);

	/* ON */
	cur = skip_consts(cur, 1, "ON");
	appendStringInfoString(&buffer, "ON ");

	/* table name */
	cur = skip_consts(cur, 1, tblname);
	appendStringInfo(&buffer, "%s.%s ",
					 quote_identifier(get_namespace_name(RelationGetNamespace(foreign_rel))),
					 quote_identifier(RelationGetRelationName(foreign_rel)));

	/* USING */
	cur = skip_consts(cur, 1, "USING");
	appendStringInfoString(&buffer, "USING ");

	/* type */
	prev = cur;
	cur = skip_ident(cur);
	appendStringInfo(&buffer, "%s ", prev);

	/* (columns) */
	cur = strchr(cur, '(');
	if (!cur) elog(ERROR, "could not parse index columns");
	appendStringInfoChar(&buffer, '(');
	cur++;

	prev = cur;
	cur = skip_until(cur, ')');
	if (!cur) elog(ERROR, "could not parse index columns");
	appendStringInfo(&buffer, "%s)", prev);

	elog(DEBUG1, "pathman_sharding: NEW indexdef is %s", buffer.data);

	return buffer.data;
}

static const char *
get_rel_name_quoted(Oid relid)
{
	const char *relname = get_rel_name(relid);

	return relname ? quote_identifier(relname) : NULL;
}

static const char *
get_rel_name_qualified(Oid relid)
{
	Oid		nspid = get_rel_namespace(relid);
	char   *nspname;

	/* Qualify the name if it's not visible in search path */
	nspname = RelationIsVisible(relid) ? NULL : get_namespace_name(nspid);

	return quote_qualified_identifier(nspname, get_rel_name(relid));
}

static Oid
pg_pathman_get_parent(Oid partition_relid)
{
	char   *query;
	Oid		parent_relid = InvalidOid;
	int		res;

	query = psprintf("SELECT partrel FROM %s.%s "
					 " JOIN pg_catalog.pg_inherits "
					 " ON partrel = inhparent"
					 " WHERE inhrelid = %u"
					 " LIMIT 1",
					 quote_identifier(get_extension_schema_name(PG_PATHMAN)),
					 quote_identifier(PG_PATHMAN_CONFIG_TABLE),
					 partition_relid);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "could not connect using SPI");

	res = SPI_execute(query, false, 0);
	if (res == SPI_OK_SELECT && SPI_processed > 0)
	{
		SPITupleTable  *spi_tuptable = SPI_tuptable;
		TupleDesc		spi_tupdesc = spi_tuptable->tupdesc;
		Datum			value;
		bool			isnull;

		value = heap_getattr(spi_tuptable->vals[0], 1,
							 spi_tupdesc, &isnull);

		if (!isnull)
			parent_relid = DatumGetObjectId(value);
	}

	SPI_finish();

	elog(DEBUG1, "pathman_sharding: parent is %u", parent_relid);

	pfree(query);

	return parent_relid;
}

static void
pgfdw_execute_command(const char *command, Oid foreign_server)
{
	ForeignServer  *server = GetForeignServer(foreign_server);
	Oid				pgfdw_exec_procid;
	const char	   *proc_name;

	elog(DEBUG1, "pathman_sharding: query: %s, server: %s",
		 command, server->servername);

	/* Build schema-qualified name of POSTGRES_FDW_COMMAND_PROC */
	proc_name = psprintf("%s.%s",
						 quote_identifier(get_extension_schema_name(POSTGRES_FDW)),
						 quote_identifier(POSTGRES_FDW_COMMAND_PROC));

	pgfdw_exec_procid = dispatch_function(proc_name);
	if (!OidIsValid(pgfdw_exec_procid))
		elog(ERROR, "cannot find function %s", proc_name);

	/* Call POSTGRES_FDW_COMMAND_PROC */
	OidFunctionCall2(pgfdw_exec_procid,
					 CStringGetTextDatum(command),
					 CStringGetTextDatum(server->servername));
}

static Oid
dispatch_function(const char *funcname)
{
	Oid procid = InvalidOid;

	PG_TRY();
	{
		Datum proc;

		/* Fetch Oid of POSTGRES_FDW_COMMAND_PROC */
		proc = DirectFunctionCall1(to_regproc, CStringGetTextDatum(funcname));
		procid = DatumGetObjectId(proc);
	}
	PG_CATCH();
	{
		/* DO NOTHING */
	}
	PG_END_TRY();

	return procid;
}

static Oid
get_extension_schema(const char *extname)
{
	Oid				result;
	Relation		rel;
	SysScanDesc		scandesc;
	HeapTuple		tuple;
	ScanKeyData		entry[1];
	Oid				ext_schema;

	ext_schema = get_extension_oid(extname, true);
	if (ext_schema == InvalidOid)
		return InvalidOid; /* exit if postgres_fdw does not exist */

	ScanKeyInit(&entry[0],
				ObjectIdAttributeNumber,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(ext_schema));

	rel = heap_open(ExtensionRelationId, AccessShareLock);
	scandesc = systable_beginscan(rel, ExtensionOidIndexId,
								  true, NULL, 1, entry);

	tuple = systable_getnext(scandesc);

	/* We assume that there can be at most one matching tuple */
	if (HeapTupleIsValid(tuple))
		result = ((Form_pg_extension) GETSTRUCT(tuple))->extnamespace;
	else
		result = InvalidOid;

	systable_endscan(scandesc);

	heap_close(rel, AccessShareLock);

	return result;
}
