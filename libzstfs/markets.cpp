// markets.cpp
//
// Owns the root-level configuration boundary for zStFS. The unified config
// file uses libconfig format; this module reads only the "markets" group from
// it and constructs the corresponding Market objects under root_path/markets/.
// Higher-level layers (daemon, tools) parse their own fields from the same
// config file.

#include "zstfs/market.h"

#include "calendar.h"

#include <cerrno>
#include <cctype>
#include <sys/stat.h>
#include <sys/types.h>

#include "libconfig.h"

namespace zstfs {

// =============================================================================
// Public API — config loading

Status LoadMarketsConfig(const std::string& config_path,
                         std::vector<MarketDef>* out_markets) {
	if (out_markets == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "output is required");
	}
	if (config_path.empty()) {
		return Status::Error(ErrorCode::InvalidArgument, "config path is required");
	}

	// RAII wrapper around the C config_t struct so we never leak on error paths.
	struct ConfigHandle : public config_t {
		ConfigHandle() { config_init(this); }
		~ConfigHandle() { config_destroy(this); }
	};
	ConfigHandle cfg;
	if (config_read_file(&cfg, config_path.c_str()) != CONFIG_TRUE) {
		std::string msg = "config parse error";
		const char* file = config_error_file(&cfg);
		if (file != NULL && *file != '\0') {
			msg += " in ";
			msg += file;
		}
		int line = config_error_line(&cfg);
		if (line > 0) {
			msg += " line " + std::to_string(line);
		}
		const char* text = config_error_text(&cfg);
		if (text != NULL && *text != '\0') {
			msg += ": ";
			msg += text;
		}
		return Status::Error(ErrorCode::IoError, msg);
	}

	config_setting_t* markets_group = config_lookup(&cfg, "markets");
	if (markets_group == NULL) {
		return Status::Error(ErrorCode::InvalidArgument,
			"config: 'markets' group is required");
	}
	int count = config_setting_length(markets_group);
	if (count <= 0) {
		return Status::Error(ErrorCode::InvalidArgument,
			"config: no markets defined");
	}
	std::vector<MarketDef> defs;
	defs.reserve(static_cast<size_t>(count));
	for (int i = 0; i < count; ++i) {
		config_setting_t* market = config_setting_get_elem(markets_group, i);
		const char* name = config_setting_name(market);
		if (name == NULL || *name == '\0') {
			return Status::Error(ErrorCode::InvalidArgument,
				"config: market " + std::to_string(i) + " has no name");
		}
		const char* schedule = NULL;
		if (!config_setting_lookup_string(market, "schedule", &schedule) || schedule == NULL) {
			return Status::Error(ErrorCode::InvalidArgument,
				std::string("config: market '") + name + "' missing 'schedule'");
		}
		MarketDef def = {};
		def.name = name;
		def.schedule = schedule;
		def.fields = DataFields::OHLCV;
		const char* fields_str = NULL;
		if (config_setting_lookup_string(market, "fields", &fields_str) &&
			fields_str != NULL) {
			std::string f(fields_str);
			if (f == "cv" || f == "CV") {
				def.fields = DataFields::CV;
			} else if (f != "ohlcv" && f != "OHLCV") {
				return Status::Error(ErrorCode::InvalidArgument,
					std::string("config: market '") + name +
					"' has invalid fields (must be 'ohlcv' or 'cv')");
			}
		}
		defs.push_back(def);
	}
	out_markets->swap(defs);
	return Status::Ok();
}

// =============================================================================
// Markets construction

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

Markets::Markets(const std::string& root_path, const std::vector<MarketDef>& markets)
	: root_path_(root_path), by_name_(), status_(Status::Ok()) {
	status_ = initialize(markets);
}

Status Markets::initialize(const std::vector<MarketDef>& markets) {
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

	std::map<std::string, std::unique_ptr<Market> > loaded;
	for (size_t i = 0; i < markets.size(); ++i) {
		const MarketDef& def = markets[i];
		if (!ValidMarketName(def.name)) {
			return Status::Error(ErrorCode::InvalidArgument,
				"invalid market name: " + def.name);
		}
		if (def.schedule.empty()) {
			return Status::Error(ErrorCode::InvalidArgument,
				"market schedule is required for: " + def.name);
		}
		if (loaded.find(def.name) != loaded.end()) {
			return Status::Error(ErrorCode::AlreadyPresent,
				"duplicate market name: " + def.name);
		}
		const std::string market_path = root_path_ + "/markets/" + def.name;
		std::unique_ptr<Market> market(new Market(def.name, market_path, def.schedule, def.fields));
		if (!market->status().ok()) {
			return market->status();
		}
		loaded[def.name] = std::move(market);
	}
	by_name_.swap(loaded);
	return Status::Ok();
}

const std::string& Markets::root_path() const {
	return root_path_;
}

Status Markets::status() const {
	return status_;
}

// =============================================================================
// Market lookup

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
