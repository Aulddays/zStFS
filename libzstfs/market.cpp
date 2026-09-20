// market.cpp
//
// Opens one market generation. Each storage layer (Active, Staging, Vault)
// handles its own persistence and crash recovery independently. Multi-file
// operations like seal and compaction use ad-hoc transaction markers (.txn)
// for crash detection and forward/backward recovery. There is no permanent
// manifest file tracking the full file set; each store validates its own
// files at load time via magic numbers and record-level checksums.

#include "zstfs/market.h"

#include <algorithm>
#include <dirent.h>
#include <errno.h>
#include <fstream>
#include <functional>
#include <sstream>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

#include "calendar.h"
#include "serialization.h"

namespace zstfs {

namespace {

}  // namespace

Market::Market(const std::string& name, const std::string& path,
	const std::string& schedule, DataFields fields)
	: name_(name), path_(path), schedule_(schedule), fields_(fields),
	calendar_(new Calendar(schedule)),
	symbols_(new Symbols()), actions_(new Actions()), daily_history_(),
	hourly_history_(), status_(Status::Ok())
{
	bool symbols_created = false;
	status_ = initialize_storage();
	if (status_.ok())
		status_ = symbols_->configure_persistence(path_ + "/symbols.bin", &symbols_created);
	if (status_.ok())
		status_ = actions_->configure_persistence(path_ + "/actions.bin");
	daily_history_.reset(new History(Frequency::Daily, *calendar_, path_, *actions_,
		status_, fields_));
	hourly_history_.reset(new History(Frequency::Hourly, *calendar_, path_, *actions_,
		status_, fields_));
	if (status_.ok())
	{
		if (!daily_history_->status().ok())
			status_ = daily_history_->status();
		else if (!hourly_history_->status().ok())
			status_ = hourly_history_->status();
	}
}

Market::~Market() {
}

Status Market::initialize_storage() {
	if (mkdir(path_.c_str(), 0755) != 0 && errno != EEXIST) {
		return Status::Error(ErrorCode::IoError, "cannot create market directory");
	}
	struct stat metadata = {};
	if (stat(path_.c_str(), &metadata) != 0 || !S_ISDIR(metadata.st_mode)) {
		return Status::Error(ErrorCode::IoError, "market path is not a directory");
	}
	return Status::Ok();
}

const std::string& Market::name() const {
	return name_;
}

const std::string& Market::path() const {
	return path_;
}

const std::string& Market::schedule() const {
	return schedule_;
}

Status Market::status() const {
	return status_;
}

Symbols& Market::symbols() {
	return *symbols_;
}

const Symbols& Market::symbols() const {
	return *symbols_;
}

Actions& Market::actions() {
	return *actions_;
}

const Actions& Market::actions() const {
	return *actions_;
}

History& Market::history(Frequency frequency) {
	return frequency == Frequency::Daily ? *daily_history_ : *hourly_history_;
}

const History& Market::history(Frequency frequency) const {
	return frequency == Frequency::Daily ? *daily_history_ : *hourly_history_;
}

}  // namespace zstfs
