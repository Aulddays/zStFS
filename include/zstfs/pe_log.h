#pragma once

#include <stdlib.h>

enum
{
	PLV_MINLEVEL = -1,
	PLV_DEBUG,
	PLV_VERBOSE,
	PLV_TRACE,
	PLV_INFO,
	PLV_WARNING,
	PLV_ERROR,
	PLV_MAXLEVEL
};

#define PELOG_LOG(a) pelog_printf a
#define PELOG_ERROR_RETURN(X, Y) do { PELOG_LOG(X); return Y; } while(0)
#define PELOG_ERROR_RETURNVOID(X) do { PELOG_LOG(X); return; } while(0)

// MSVC uses "%Iu" for size_t while gcc uses "%zu"
#ifdef _MSC_VER
#	define PL_SIZET "%Iu"
#else
#	define PL_SIZET "%zu"
#endif

int pelog_printf(int level, const char *format, ...);
int pelog_setlevel(int level, int *old_level = NULL);
int pelog_setlevel(const char *level, int *old_level = NULL);
int pelog_setfile(const char *fileName, bool linebuf = false);
int pelog_setfile_rotate(size_t filesize_kb, size_t maxkeep, const char *fileName, bool linebuf = false);
