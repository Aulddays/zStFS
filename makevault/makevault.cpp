// makevault.cpp
//
// Offline command-line wrapper for the library-owned Vault compactor. Runs
// compaction for every market and frequency defined in the config file.
// The daemon must be stopped while this command publishes replacement files.
//
// Usage: makevault <config-file> <cutoff-local-time>

#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include "libconfig.h"
#include "zstfs/market.h"

namespace {

void Usage(const char* program) {
	std::cerr << "usage: " << program
		<< " <config-file> <cutoff-local-time>\n";
}

// Reads root_path from the config file. Markets are loaded through the
// library's LoadMarketsConfig; here we only need the data root directory.
std::string ReadRootPath(const std::string& config_path) {
	struct ConfigHandle : public config_t {
		ConfigHandle() { config_init(this); }
		~ConfigHandle() { config_destroy(this); }
	};
	ConfigHandle cfg;
	if (config_read_file(&cfg, config_path.c_str()) != CONFIG_TRUE) {
		return "";
	}
	const char* root_path = NULL;
	if (!config_lookup_string(&cfg, "root_path", &root_path) ||
		root_path == NULL || *root_path == '\0') {
		return "";
	}
	return std::string(root_path);
}

}  // namespace

int main(int argc, char** argv) {
	if (argc != 3) {
		Usage(argv[0]);
		return EXIT_FAILURE;
	}
	const std::string config_path = argv[1];
	const std::string cutoff_time = argv[2];

	const std::string root_path = ReadRootPath(config_path);
	if (root_path.empty()) {
		std::cerr << "makevault: failed to read root_path from config file\n";
		return EXIT_FAILURE;
	}

	std::vector<zstfs::MarketDef> markets;
	zstfs::Status status = zstfs::LoadMarketsConfig(config_path, &markets);
	if (!status.ok()) {
		std::cerr << "makevault: " << status.message() << "\n";
		return EXIT_FAILURE;
	}

	// Run compaction for every market and frequency defined in the config.
	// Results are printed per market+frequency so failures are easy to locate.
	int exit_code = EXIT_SUCCESS;
	for (size_t i = 0; i < markets.size(); ++i) {
		const zstfs::MarketDef& def = markets[i];
		for (int f = 0; f < 2; ++f) {
			const zstfs::Frequency freq =
				(f == 0) ? zstfs::Frequency::Daily : zstfs::Frequency::Hourly;
			const char* freq_name = (f == 0) ? "daily" : "hourly";
			zstfs::VaultCompactionStats stats = {};
			status = zstfs::CompactVault(config_path, def.name, freq, cutoff_time, &stats);
			if (!status.ok()) {
				std::cerr << "makevault: " << def.name << "/" << freq_name
					<< ": " << status.message() << "\n";
				exit_code = EXIT_FAILURE;
				continue;
			}
			std::cout << def.name << "/" << freq_name
				<< ": input_blocks=" << stats.input_blocks
				<< " output_blocks=" << stats.output_blocks
				<< " io_bytes=" << stats.io_bytes
				<< " elapsed_ms=" << stats.elapsed_milliseconds
				<< "\n";
		}
	}
	return exit_code;
}
