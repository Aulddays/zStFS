bin_PROGRAMS = makevault
makevault_SOURCES = makevault.cpp
makevault_CPPFLAGS = -I$(top_srcdir)/libconfig -I$(top_srcdir)/include
makevault_LDFLAGS = -pthread
makevault_LDADD = $(top_builddir)/libzstfs/libzstfs.a $(top_builddir)/libconfig/libconfig.a
