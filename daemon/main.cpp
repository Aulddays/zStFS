// daemon/main.cpp
//
// Command-line entry point for zstfsd. Loads the configuration file, starts
// the HTTP service, and runs until SIGINT or SIGTERM is received.
//
// Usage: zstfsd <config-file>

#include <iostream>
#include <string>

#include "config.h"
#include "server.h"
#include <zstfs/pe_log.h>

int main(int argc, char** argv)
{
	if (argc != 2)
		PELOG_ERROR_RETURN((PLV_ERROR, "Usage %s <config-file>\n", argv[0]), 2);

	zstfsd::DaemonConfig config;
	zstfs::Status status = zstfsd::LoadDaemonConfig(argv[1], &config);
	if (!status.ok())
		PELOG_ERROR_RETURN((PLV_ERROR, "Config error: %s\n", status.message().c_str()), -1);

	PELOG_LOG((PLV_INFO, "zstfsd Starting\n"));
	zstfsd::HttpServer server(config);
	int ret = server.run();
	PELOG_LOG((PLV_INFO, "zstfsd Finish\n"));
	return ret;
}
