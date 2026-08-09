// daemon/main.cpp
//
// Command-line entry point for zstfsd. The daemon owns one configured zStFS
// root and exposes the HTTP service until SIGINT or SIGTERM is received.

#include <cstdlib>
#include <iostream>
#include <string>

#include "server.h"

namespace {

bool ParsePort(const char* text, unsigned short* port) {
	char* end = NULL;
	long value = std::strtol(text, &end, 10);
	if (end == text || *end != '\0' || value < 1 || value > 65535) return false;
	*port = static_cast<unsigned short>(value);
	return true;
}

void Usage(const char* program) {
	std::cerr << "usage: " << program << " <root-path> [port]\n";
}

}  // namespace

int main(int argc, char** argv) {
	if (argc < 2 || argc > 3) {
		Usage(argv[0]);
		return 2;
	}
	unsigned short port = 8080;
	if (argc == 3 && !ParsePort(argv[2], &port)) {
		std::cerr << "invalid port\n";
		return 2;
	}
	zstfsd::HttpServer server(argv[1], port);
	std::cerr << "zstfsd listening on port " << port << "\n";
	return server.run();
}
