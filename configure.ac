AC_PREREQ([2.69])
AC_INIT([zstfs], [0.1.0], [none])
AM_INIT_AUTOMAKE([foreign subdir-objects])
AC_CONFIG_SRCDIR([libzstfs/market.cpp])
AC_CONFIG_HEADERS([config.h])

AC_PROG_CXX
AC_PROG_RANLIB

AC_LANG([C++])
AC_MSG_CHECKING([whether $CXX supports C++11])
zstfs_saved_CXXFLAGS="$CXXFLAGS"
CXXFLAGS="$CXXFLAGS -std=c++11"
AC_COMPILE_IFELSE([
	AC_LANG_PROGRAM([[#include <memory>]], [[static_assert(__cplusplus >= 201103L, "C++11 is required");]])
], [AC_MSG_RESULT([yes])], [
	AC_MSG_RESULT([no])
	CXXFLAGS="$zstfs_saved_CXXFLAGS"
	AC_MSG_ERROR([a C++11 compiler is required])
])

AC_CONFIG_FILES([
	Makefile
	libzstfs/Makefile
	test/Makefile
	makevault/Makefile
	daemon/Makefile
])
AC_OUTPUT
