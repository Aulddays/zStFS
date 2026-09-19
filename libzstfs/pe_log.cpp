#include "stdarg.h"
#include "stdio.h"
#include <time.h>
#include <string>
#include <sstream>
#include <deque>
//#include <boost/thread.hpp>
#include <string.h>
#include <vector>
#include <atomic>
#include <dirent.h>
#include <algorithm>
#include <libgen.h>

#include <zstfs/pe_log.h>
#define DIR_SEP '/'

#if defined(_DEBUG) && defined(_MSC_VER)
#	ifndef DBG_NEW
#		define DBG_NEW new ( _NORMAL_BLOCK , __FILE__ , __LINE__ )
#		define new DBG_NEW
#	endif
#endif  // _DEBUG

//FILE *pelog_out_stream = stderr;
//std::string pelog_out_file = "";
//boost::mutex pelog_mutex;
int pelog_logLevel = PLV_DEBUG;
#ifdef _WIN32
bool pelog_linebuf = false;
#endif

// A simple global spin-lock
class LockGuard {
	static std::atomic_flag locked;
public:
	LockGuard() {
		while (locked.test_and_set(std::memory_order_acquire)) { ; }
	}
	~LockGuard() {
		locked.clear(std::memory_order_release);
	}
};
std::atomic_flag LockGuard::locked = ATOMIC_FLAG_INIT;

// helper for output stream
static struct PelogOutStream
{
	FILE *stream;
	size_t fmaxsize = -1;	// max file size before rotate
	size_t fsize = -1;	// current file size
	size_t nkeep = -1;	// number of history files to keep
	std::deque<std::string> kept;	// history files currently be
	std::string filename;
	bool linebuf = false;
	PelogOutStream() : stream(stderr){}
	~PelogOutStream()
	{
		close();
	}
	void close()
	{
		if (stream != stdout && stream != stderr)
		{
			fclose(stream);
			stream = stderr;
		}
	}
	int set(const char *filename, bool linebuf = false)
	{
		int ret = _openf(filename, linebuf);
		if (!ret)
		{
			fmaxsize = nkeep = -1;
			this->filename = filename ? filename : "";
			this->linebuf = linebuf;
		}
		return ret;
	}
	int _openf(const char *filename, bool linebuf)
	{
		if (!filename || !*filename)
		{
			if (stream && stream != stderr)
				fclose(stream);
			stream = stderr;
			return 0;
		}
		FILE *newstr = fopen(filename, "a");
		if (newstr)
		{
			FILE *oldstr = stream;
#ifdef _WIN32
			// _IOLBF is the same as _IOFBF on Win32
			pelog_linebuf = linebuf;
#endif
			if (linebuf)
			{
#ifndef _WIN32
				setvbuf(newstr, NULL, _IOLBF, 4096);
#endif
			}
			else
				setvbuf(newstr, NULL, _IOFBF, 512);
			stream = newstr;
			if (oldstr != stderr)
				fclose(oldstr);
			return 0;
		}
		return 1;
	}
	FILE *get()
	{
		// check whether need rotation
		if (filename.length() && fmaxsize != (size_t)-1 && fsize >= fmaxsize && stream != stderr)
		{
			fclose(stream);
			stream = stderr;
			while (kept.size() + 2 > nkeep)
			{
				remove(kept.front().c_str());
				kept.pop_front();
			}
			std::vector<char> bufin(std::max((size_t)260, filename.length() + 40));
			char *buf = &bufin[0];
			sprintf(buf, "%s.%ld", filename.c_str(), (long)time(NULL));
			for (int i = 0; true; ++i)
			{
				FILE *fp = fopen(buf, "rb");	// rename() on Linux will overwrite target, so check existance first
				if (fp)
					fclose(fp);
				if (!fp && rename(filename.c_str(), buf) == 0)
				{
					kept.emplace_back(buf);
					break;
				}
				if (i > 100)	// too many failures, give up
				{
					remove(filename.c_str());
					break;
				}
				sprintf(buf, "%s.%ld.%d", filename.c_str(), (long)time(NULL), i);
			}
			fsize = 0;
			_openf(filename.c_str(), linebuf);
		}
		return stream;
	}
	int setrot(size_t size, size_t maxkeep, const char *filename, bool linebuf = false)
	{
		// get current log file size and history log files
		std::vector<char> bufin(std::max((size_t)260, strlen(filename) * 2 + 10));
		char *buf = &bufin[0];
		strcpy(buf, filename);
		std::string dirn = dirname(buf);
		strcpy(buf, filename);
		std::string filen = basename(buf);
		std::vector<std::string> files;
		fsize = 0;
		DIR *dir = opendir(dirn.c_str());
		if (dir)
		{
			for (dirent *ent = readdir(dir); ent; ent = readdir(dir))
			{
				if (ent->d_type != DT_REG)
					continue;
				strcpy(buf, ent->d_name);
				char *entname = basename(buf);
				if (0 == strncmp(entname, filen.c_str(), filen.length()))
				{
					if (entname[filen.length()] == 0)	// target file, get its current size
					{
						FILE *fp = fopen(filename, "rb");
						if (fp)
						{
							fseek(fp, 0, SEEK_END);
							fsize = ftell(fp);
							fclose(fp);
						}
					}
					else if (entname[filen.length()] == '.' && isdigit(entname[filen.length() + 1]))
						files.emplace_back(dirn + DIR_SEP + entname);
				}
			}
			closedir(dir);
		}
		std::sort(files.begin(), files.end());
		kept.clear();
		kept.insert(kept.end(), std::make_move_iterator(files.begin()), std::make_move_iterator(files.end()));
		while (kept.size() + 1 > maxkeep)
		{
			remove(kept.front().c_str());
			kept.pop_front();
		}
		// set file
		int ret = set(filename, linebuf);
		// store rotate parameters
		fmaxsize = size;
		nkeep = maxkeep;
		return ret;
	}
	void recordsize(size_t size)
	{
		if (fmaxsize != (size_t)-1)
			fsize += size;
	}

} pelog_out_stream;

static const char *pelog_levelDesc[] =
{
	"DBG",
	"VRB",
	"TRC",
	"INF",
	"WRN",
	"ERR",
};
static_assert(sizeof(pelog_levelDesc) / sizeof(pelog_levelDesc[0]) == PLV_MAXLEVEL, \
	"pelog_levelDesc size error");

int pelog_printf(int level, const char *format, ...)
{
	va_list v;
	if(level <= PLV_MINLEVEL || level >= PLV_MAXLEVEL)
	{
		pelog_printf(PLV_ERROR, "PELOG: Error log level %d\n", level);
		return -1;
	}
	if(level < pelog_logLevel)
		return 0;
	int ret = 0;
	{	// for lock_guard
		LockGuard lock;
		FILE *out_stream = pelog_out_stream.get();
		time_t curTime = time(NULL);
		tm *curTm = localtime(&curTime);
		char buf[32];
		strftime(buf, 32, "%Y%m%d %H:%M:%S", curTm);
		//std::ostringstream stm;
		//stm << std::this_thread::get_id();
		int printed = fprintf(out_stream, "[%s %s] ", pelog_levelDesc[level], buf);
		va_start(v, format);
		printed += vfprintf(out_stream, format, v);
		va_end(v);
#ifdef _WIN32
		// Win32 does not support _IOLBF line buffer, flush buf to simulate
		if (pelog_linebuf)
			fflush(out_stream);
#endif
		pelog_out_stream.recordsize(printed);
	}
	return ret;
}

int pelog_setlevel(int level, int *old_level)
{
	if(old_level)
		*old_level = pelog_logLevel;
	if(level <= PLV_MINLEVEL || level >= PLV_MAXLEVEL)
	{
		pelog_printf(PLV_ERROR, "PELOG: Error log level %d\n", level);
		return -1;
	}
	pelog_printf(PLV_TRACE, "PELOG: Setting log lovel to %s\n", pelog_levelDesc[level]);
	pelog_logLevel = level;
	return 0;
}

int pelog_setlevel(const char *level, int *old_level)
{
	for(int i = 0; i < PLV_MAXLEVEL; ++i)
		if(0 == strcmp(level, pelog_levelDesc[i]))
			return pelog_setlevel(i, old_level);
	pelog_printf(PLV_ERROR, "PELOG: Unsupported log level \"%s\". Current level is %s\n", level, pelog_levelDesc[pelog_logLevel]);
	return -1;
}

int pelog_setfile(const char *fileName, bool linebuf)
{
	return pelog_out_stream.set(fileName, linebuf);
}

int pelog_setfile_rotate(size_t filesize_kb, size_t maxkeep, const char *fileName, bool linebuf)
{
	if (fileName && *fileName)
	{
		PELOG_LOG((PLV_INFO, "Set log file %s\n", fileName));
		if (filesize_kb != (size_t)-1)
			PELOG_LOG((PLV_INFO, "Log rotate size %llu KB, history num %llu\n",
				(unsigned long long)filesize_kb, (unsigned long long)maxkeep));
	}
	return pelog_out_stream.setrot(filesize_kb * 1024, maxkeep, fileName, linebuf);
}
