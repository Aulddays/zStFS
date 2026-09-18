// daemon/config.cpp
//
// Implements the zstfsd configuration file parser using libconfig.
// The daemon reads its own fields (listen address, cache sizes, root_path)
// directly; the markets list is loaded through the library's
// LoadMarketsConfig() function so both layers share one config format.
//
// All settings live at the top level to keep the config file flat:
//
//   listen_addr             - bind address (default: "0.0.0.0")
//   listen_port             - bind port (default: 8080)
//   root_path               - zStFS data root directory (required)
//   compressed_cache_mb     - compressed page cache size in MB (0 = default)
//   decoded_cache_mb        - decoded record cache size in MB (0 = default)
//   markets                 - named market definitions (see libzstfs)
//
// Sane defaults are applied for optional fields; root_path and markets are
// required.

#include "config.h"

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <sys/stat.h>

#include "libconfig.h"
#include "zstfs/market.h"

namespace zstfsd {

// Forward declarations of internal helpers defined at the bottom of this file.
static std::string format_parse_error(const config_t* cfg);

// =============================================================================
// Public API

zstfs::Status LoadDaemonConfig(const std::string& config_path, DaemonConfig* out) {
	if (out == NULL) {
		return zstfs::Status::Error(zstfs::ErrorCode::InvalidArgument, "config output is required");
	}
	if (config_path.empty()) {
		return zstfs::Status::Error(zstfs::ErrorCode::InvalidArgument, "config path is required");
	}

	// RAII wrapper around the C config_t struct so we never leak on error paths.
	struct ConfigHandle : public config_t {
		ConfigHandle() { config_init(this); }
		~ConfigHandle() { config_destroy(this); }
	};
	ConfigHandle cfg;
	if (config_read_file(&cfg, config_path.c_str()) != CONFIG_TRUE) {
		return zstfs::Status::Error(zstfs::ErrorCode::IoError, format_parse_error(&cfg));
	}

	DaemonConfig result = {};

	// --- listen address and port ---
	result.listen_addr = config_get_string(&cfg, "listen_addr", "0.0.0.0");
	int port = config_get_int(&cfg, "listen_port", 8080);
	if (port < 1 || port > 65535) {
		return zstfs::Status::Error(zstfs::ErrorCode::InvalidArgument,
			"config: listen_port out of range (1-65535)");
	}
	result.listen_port = static_cast<unsigned short>(port);

	// --- root_path (required) ---
	const char* root_path = NULL;
	if (!config_lookup_string(&cfg, "root_path", &root_path) ||
		root_path == NULL || *root_path == '\0') {
		return zstfs::Status::Error(zstfs::ErrorCode::InvalidArgument,
			"config: root_path is required");
	}
	result.root_path = root_path;

	// --- cache sizes in MB (optional; 0 means use library defaults) ---
	int compressed_mb = config_get_int(&cfg, "compressed_cache_mb", 0);
	int decoded_mb = config_get_int(&cfg, "decoded_cache_mb", 0);
	if (compressed_mb < 0) compressed_mb = 0;
	if (decoded_mb < 0) decoded_mb = 0;
	result.compressed_cache_bytes = static_cast<size_t>(compressed_mb) * 1024 * 1024;
	result.decoded_cache_bytes = static_cast<size_t>(decoded_mb) * 1024 * 1024;

	// --- markets (loaded through the library's config parser) ---
	zstfs::Status status = zstfs::LoadMarketsConfig(config_path, &result.markets);
	if (!status.ok()) return status;

	*out = result;
	return zstfs::Status::Ok();
}

// =============================================================================
// Internal helpers

// Builds a formatted parse error message including file, line, and text.
static std::string format_parse_error(const config_t* cfg) {
	std::string msg = "config parse error";
	const char* file = config_error_file(cfg);
	if (file != NULL && *file != '\0') {
		msg += " in ";
		msg += file;
	}
	int line = config_error_line(cfg);
	if (line > 0) {
		msg += " line " + std::to_string(line);
	}
	const char* text = config_error_text(cfg);
	if (text != NULL && *text != '\0') {
		msg += ": ";
		msg += text;
	}
	return msg;
}

}  // namespace zstfsd
