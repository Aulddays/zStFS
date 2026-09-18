// daemon/config.h
//
// Parses the zstfsd configuration file using libconfig. The config file
// controls the listen address/port, cache sizes, and the list of markets.
// Config is parsed once at startup; the resulting DaemonConfig is passed to
// the server and worker for initialization.

#pragma once

#include <cstddef>
#include <string>
#include <vector>

#include "zstfs/market.h"
#include "zstfs/status.h"

namespace zstfsd {

struct DaemonConfig {
	// HTTP listen settings.
	std::string listen_addr;
	unsigned short listen_port;

	// Cache size limits in bytes. When 0, defaults from libzstfs are used.
	size_t compressed_cache_bytes;
	size_t decoded_cache_bytes;

	// zStFS root directory (where market data lives).
	std::string root_path;

	// Configured markets.
	std::vector<zstfs::MarketDef> markets;
};

// Loads and parses the daemon configuration file at config_path. Returns Ok
// and fills *out on success, or an error status with a descriptive message
// on parse failure or missing required fields.
zstfs::Status LoadDaemonConfig(const std::string& config_path, DaemonConfig* out);

}  // namespace zstfsd
