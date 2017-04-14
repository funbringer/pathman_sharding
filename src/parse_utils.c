/* ------------------------------------------------------------------------
 *
 * parse_utils.c
 *		Functions for indexdef parsing.
 *
 * Copyright (c) 2017, Postgres Professional
 * Portions Copyright (c) 2008-2011, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2011, Itagaki Takahiro
 * Portions Copyright (c) 2012-2015, The Reorg Development Team
 *
 * ------------------------------------------------------------------------
 */

#include "parse_utils.h"

#include "postgres.h"
#include <stdarg.h>


#define IsToken(c) \
	(IS_HIGHBIT_SET((c)) || isalnum((unsigned char) (c)) || (c) == '_')


static void parse_error(const char *str) pg_attribute_noreturn();

static void
parse_error(const char *str)
{
	elog(ERROR, "parse error near: ... %s", str);
}


char *
skip_consts(char *str, int n, ...)
{
	va_list		args;
	char	   *result = NULL;
	int			i;

	AssertArg(str);
	AssertArg(n > 0);

	va_start(args, n);

	for (i = 0; i < n; i++)
	{
		const char *arg;
		Size		len;

		arg = va_arg(args, const char *);
		AssertArg(arg);
		len = strlen(arg);

		if (strncmp(str, arg, len) == 0)
		{
			str[len] = '\0';
			result = str + len + 1;
			break;
		}
	}

	va_end(args);

	if (!result)
		parse_error(str);

	return result; /* keep compiler happy */
}

char *
skip_ident(char *str)
{
	AssertArg(str);

	while (*str && isspace((unsigned char) *str))
		str++;

	if (*str == '"')
	{
		str++;

		for (;;)
		{
			char *next_quote = strchr(str, '"');

			if (!next_quote)
			{
				parse_error(str);
				return NULL;
			}

			else if (next_quote[1] != '"')
			{
				next_quote[1] = '\0';
				return next_quote + 2;
			}

			else /* escaped quote ("") */
			{
				str = next_quote + 2;
			}
		}
	}
	else
	{
		while (*str && IsToken(*str))
			str++;

		str[0] = '\0';
		return str + 1;
	}

	parse_error(str);
	return NULL;
}

char *
skip_until(char *str, char arg)
{
	char	instr = 0;
	int		nopen = 0;

	for (; *str && (nopen > 0 || instr != 0 || *str != arg); str++)
	{
		if (instr)
		{
			if (str[0] == instr)
			{
				if (str[1] == instr)
					str++;
				else
					instr = 0;
			}
			else if (str[0] == '\\')
				str++;	/* next char is always string */
		}
		else
		{
			switch (str[0])
			{
				case '(':
					nopen++;
					break;

				case ')':
					nopen--;
					break;

				case '\'':
				case '"':
					instr = str[0];
					break;
			}
		}
	}

	if (nopen == 0 && instr == 0)
	{
		if (*str)
		{
			*str = '\0';
			return str + 1;
		}
		else return NULL;
	}

	parse_error(str);
	return NULL;
}
