// markets.cpp
//
// Owns the root-level configuration boundary for zStFS. The caller supplies a
// root directory and its fixed markets.conf file; this module creates every
// market below that root and never lets caller-provided paths select storage.

#include "zstfs/market.h"

#include "calendar.h"

#include <cerrno>
#include <cctype>
#include <fstream>
#include <sys/stat.h>
#include <sys/types.h>

namespace zstfs {

// =============================================================================
// Root Configuration Parsing
// markets.conf is intentionally a small immutable startup contract: name,type.

static std::string Trim(const std::string& value) {
	std::string::size_type begin = value.find_first_not_of(" \t\r\n");
	if (begin == std::string::npos) {
		return "";
	}
	std::string::size_type end = value.find_last_not_of(" \t\r\n");
	return value.substr(begin, end - begin + 1);
}

static bool ValidMarketName(const std::string& name) {
	if (name.empty() || name == "." || name == "..") {
		return false;
	}
	for (std::string::const_iterator it = name.begin(); it != name.end(); ++it) {
		const unsigned char value = static_cast<unsigned char>(*it);
		if (!std::isalnum(value) && *it != '_' && *it != '-') {
			return false;
		}
	}
	return true;
}

static bool ParseMarketLine(const std::string& line, std::string* name,
	std::string* type) {
	const std::string::size_type separator = line.find(',');
	if (separator == std::string::npos || line.find(',', separator + 1) != std::string::npos) {
		return false;
	}
	*name = Trim(line.substr(0, separator));
	*type = Trim(line.substr(separator + 1));
	return ValidMarketName(*name) && !type->empty();
}

// =============================================================================
// Root Lifecycle
// The root and markets.conf belong to the application; market subdirectories
// are owned exclusively by this library after configuration has been accepted.

Markets::Markets(const std::string& root_path)
	: root_path_(root_path), by_name_(), status_(Status::Ok()) {
	status_ = load_configuration();
}

const std::string& Markets::root_path() const {
	return root_path_;
}

Status Markets::status() const {
	return status_;
}

Status Markets::load_configuration() {
	if (root_path_.empty()) {
		return Status::Error(ErrorCode::InvalidArgument, "zstfs root path is required");
	}
	struct stat metadata = {};
	if (stat(root_path_.c_str(), &metadata) != 0 || !S_ISDIR(metadata.st_mode)) {
		return Status::Error(ErrorCode::IoError, "zstfs root path is not a directory");
	}
	const std::string markets_path = root_path_ + "/markets";
	if (mkdir(markets_path.c_str(), 0755) != 0 && errno != EEXIST) {
		return Status::Error(ErrorCode::IoError, "cannot create markets directory");
	}
	if (stat(markets_path.c_str(), &metadata) != 0 || !S_ISDIR(metadata.st_mode)) {
		return Status::Error(ErrorCode::IoError, "markets path is not a directory");
	}

	const std::string config_path = root_path_ + "/markets.conf";
	std::ifstream input(config_path.c_str());
	if (!input) {
		return Status::Error(ErrorCode::IoError, "cannot open root markets.conf");
	}

	std::map<std::string, std::unique_ptr<Market> > loaded;
	std::string line;
	unsigned int line_number = 0;
	while (std::getline(input, line)) {
		++line_number;
		line = Trim(line);
		if (line.empty() || line[0] == '#') {
			continue;
		}
		std::string name;
		std::string type;
		if (!ParseMarketLine(line, &name, &type)) {
			return Status::Error(ErrorCode::InvalidArgument,
				"invalid markets.conf line " + std::to_string(line_number));
		}
		if (loaded.find(name) != loaded.end()) {
			return Status::Error(ErrorCode::AlreadyPresent,
				"duplicate market name in markets.conf");
		}
		const std::string market_path = root_path_ + "/markets/" + name;
		std::unique_ptr<Market> market(new Market(name, market_path, type));
		if (!market->status().ok()) {
			return market->status();
		}
		loaded[name] = std::move(market);
	}
	if (!input.eof()) {
		return Status::Error(ErrorCode::IoError, "cannot read root markets.conf");
	}

	by_name_.swap(loaded);
	return Status::Ok();
}

// =============================================================================
// Market Lookup

Status Markets::get(const std::string& name, Market** out) {
	if (out == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "market output is required");
	}
	if (!status_.ok()) {
		return status_;
	}
	std::map<std::string, std::unique_ptr<Market> >::iterator found = by_name_.find(name);
	if (found == by_name_.end()) {
		return Status::Error(ErrorCode::NotFound, "market was not found");
	}
	*out = found->second.get();
	return Status::Ok();
}

Status Markets::get(const std::string& name, const Market** out) const {
	if (out == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "market output is required");
	}
	if (!status_.ok()) {
		return status_;
	}
	std::map<std::string, std::unique_ptr<Market> >::const_iterator found = by_name_.find(name);
	if (found == by_name_.end()) {
		return Status::Error(ErrorCode::NotFound, "market was not found");
	}
	*out = found->second.get();
	return Status::Ok();
}

Status Markets::seal_all_before(const std::string& today_local, int trading_days_back) {
	if (!status_.ok()) {
		return status_;
	}
	Status first_error = Status::Ok();
	for (std::map<std::string, std::unique_ptr<Market> >::iterator it = by_name_.begin();
		 it != by_name_.end(); ++it) {
		// Compute the cutoff date using this market's own calendar so different
		// market types (different holidays / schedules) each get the right
		// number of trading days back. The cutoff date is the same for both
		// daily and hourly since they share the daily TimeId coordinate.
		zstfs::TimeId today_id = 0;
		Status status = it->second->calendar_.get() == NULL ? Status::Ok() :
			it->second->calendar_->time_id(today_local, &today_id);
		if (!status.ok() && first_error.ok()) {
			first_error = status;
			continue;
		}
		const int today_day = static_cast<int>(time_day(today_id));
		const int cutoff_day = today_day - trading_days_back;
		if (cutoff_day < 0 && first_error.ok()) {
			first_error = Status::Error(ErrorCode::InvalidArgument,
				"trading days back exceeds calendar range");
			continue;
		}
		std::string cutoff_local;
		status = it->second->calendar_->date(daily_bar_id(static_cast<TimeId>(cutoff_day)),
			&cutoff_local);
		if (!status.ok() && first_error.ok()) {
			first_error = status;
			continue;
		}
		Status daily_status = it->second->history(Frequency::Daily).seal_before(cutoff_local);
		if (!daily_status.ok() && first_error.ok()) {
			first_error = daily_status;
		}
		Status hourly_status = it->second->history(Frequency::Hourly).seal_before(cutoff_local);
		if (!hourly_status.ok() && first_error.ok()) {
			first_error = hourly_status;
		}
	}
	return first_error;
}

}  // namespace zstfs
