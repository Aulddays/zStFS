# libconfig - lightweight configuration file parser.
# Built as a static convenience library linked directly into the daemon and
# tools that need configuration parsing. Pre-generated grammar.c and scanner.c
# are used so bison/flex are not required at build time.

noinst_LIBRARIES = libconfig.a

libconfig_a_SOURCES = \
	libconfig.c \
	libconfig.h \
	grammar.c \
	grammar.h \
	scanctx.c \
	scanctx.h \
	scanner.c \
	scanner.h \
	strbuf.c \
	strbuf.h \
	strvec.c \
	strvec.h \
	util.c \
	util.h \
	parsectx.h \
	wincompat.c \
	wincompat.h

libconfig_a_CFLAGS = $(AM_CFLAGS)
