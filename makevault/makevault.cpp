// makevault.cpp
//
// Offline command-line wrapper for the library-owned Vault compactor. The
// daemon must be stopped while this command publishes replacement files.

#include <cstdlib>
#include <iostream>
#include <string>

#include "zstfs/market.h"

namespace {

void Usage(const char* program) {
	std::cerr << "usage: " << program
		<< " <market-path> <market-type> <daily|hourly> <cutoff-local-time>\n";
}

bool ParseFrequency(const std::string& value, zstfs::Frequency* frequency) {
	if (value == "daily") {
		*frequency = zstfs::Frequency::Daily;
		return true;
	}
	if (value == "hourly") {
		*frequency = zstfs::Frequency::Hourly;
		return true;
	}
	return false;
}

}  // namespace

int main(int argc, char** argv) {
	if (argc != 5) {
		Usage(argv[0]);
		return EXIT_FAILURE;
	}
	zstfs::Frequency frequency;
	if (!ParseFrequency(argv[3], &frequency)) {
		Usage(argv[0]);
		std::cerr << "error: frequency must be daily or hourly\n";
		return EXIT_FAILURE;
	}
	zstfs::VaultCompactionStats stats = {};
	const zstfs::Status status = zstfs::CompactVault(argv[1], argv[2], frequency, argv[4], &stats);
	if (!status.ok()) {
		std::cerr << "makevault: " << status.message() << "\n";
		return EXIT_FAILURE;
	}
	std::cout << "input blocks: " << stats.input_blocks << "\n"
		<< "output blocks: " << stats.output_blocks << "\n"
		<< "temporary bytes: " << stats.temporary_bytes << "\n"
		<< "I/O bytes: " << stats.io_bytes << "\n"
		<< "elapsed milliseconds: " << stats.elapsed_milliseconds << "\n";
	return EXIT_SUCCESS;
}
