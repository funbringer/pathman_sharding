/* ------------------------------------------------------------------------
 *
 * parse_utils.h
 *		Functions for indexdef parsing.
 *
 * Copyright (c) 2017, Postgres Professional
 * Portions Copyright (c) 2008-2011, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2011, Itagaki Takahiro
 * Portions Copyright (c) 2012-2015, The Reorg Development Team
 *
 * ------------------------------------------------------------------------
 */

#ifndef PARSE_UTILS_H
#define PARSE_UTILS_H

char *skip_consts(char *str, int n, ...);
char *skip_until_const(char *str, const char *arg);
char *skip_ident(char *str);
char *skip_until(char *str, char arg);

#endif /* PARSE_UTILS_H */
