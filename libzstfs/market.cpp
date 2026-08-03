#include "zstfs/market.h"

namespace zstfs {

Market::Market(const std::string& name,
               const std::string& path,
               const std::string& local_time_zone)
	: name_(name),
	  path_(path),
	  local_time_zone_(local_time_zone),
	  symbols_(new Symbols()),
	  actions_(new Actions()),
	  daily_history_(new History(Frequency::Daily)),
	  hourly_history_(new History(Frequency::Hourly)) {
}

Market::~Market() {
}

const std::string& Market::name() const {
	return name_;
}

const std::string& Market::path() const {
	return path_;
}

const std::string& Market::local_time_zone() const {
	return local_time_zone_;
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
