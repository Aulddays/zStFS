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

namespace {

void Usage(const char* program) {
	std::cerr << "usage: " << program << " <config-file>\n";
}

}  // namespace

int main(int argc, char** argv) {
	if (argc != 2) {
		Usage(argv[0]);
		return 2;
	}

	zstfsd::DaemonConfig config;
	zstfs::Status status = zstfsd::LoadDaemonConfig(argv[1], &config);
	if (!status.ok()) {
		std::cerr << "config error: " << status.message() << "\n";
		return 1;
	}

	zstfsd::HttpServer server(config);
	std::cerr << "zstfsd listening on " << config.listen_addr
	          << ":" << config.listen_port << "\n";
	return server.run();
}
