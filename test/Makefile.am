check_PROGRAMS = test_api test_calendar
TESTS = test_api test_calendar

test_api_SOURCES = test_api.cpp
test_api_CPPFLAGS = -I$(top_srcdir)/include
test_api_LDADD = $(top_builddir)/libzstfs/libzstfs.a

test_calendar_SOURCES = test_calendar.cpp
test_calendar_CPPFLAGS = -I$(top_srcdir)/include -I$(top_srcdir)/libzstfs
test_calendar_LDADD = $(top_builddir)/libzstfs/libzstfs.a
