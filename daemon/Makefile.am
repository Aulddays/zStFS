bin_PROGRAMS = zstfsd

zstfsd_SOURCES = \
	main.cpp \
	server.cpp \
	server.h

zstfsd_CPPFLAGS = -I$(top_srcdir) -I$(top_srcdir)/asio -I$(top_srcdir)/include
zstfsd_CXXFLAGS = -pthread
zstfsd_LDFLAGS = -pthread
zstfsd_LDADD = $(top_builddir)/libzstfs/libzstfs.a
