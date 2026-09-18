check_PROGRAMS = test_api test_calendar test_codec test_active
TESTS = test_api test_calendar test_codec test_active
AM_CXXFLAGS = -pthread
AM_LDFLAGS = -pthread

test_api_SOURCES = test_api.cpp
test_api_CPPFLAGS = -I$(top_srcdir)/include
test_api_LDADD = $(top_builddir)/libzstfs/libzstfs.a $(top_builddir)/libconfig/libconfig.a

test_calendar_SOURCES = test_calendar.cpp
test_calendar_CPPFLAGS = -I$(top_srcdir)/include -I$(top_srcdir)/libzstfs
test_calendar_LDADD = $(top_builddir)/libzstfs/libzstfs.a $(top_builddir)/libconfig/libconfig.a

test_codec_SOURCES = test_codec.cpp
test_codec_CPPFLAGS = -I$(top_srcdir)/include -I$(top_srcdir)/libzstfs
test_codec_LDADD = $(top_builddir)/libzstfs/libzstfs.a $(top_builddir)/libconfig/libconfig.a

test_active_SOURCES = test_active.cpp
test_active_CPPFLAGS = -I$(top_srcdir)/include -I$(top_srcdir)/libzstfs
test_active_LDFLAGS = -pthread
test_active_LDADD = $(top_builddir)/libzstfs/libzstfs.a $(top_builddir)/libconfig/libconfig.a
