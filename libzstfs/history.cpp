#include "zstfs/market.h"

#include "history.h"

namespace zstfs {

ActiveStore::ActiveStore(Frequency frequency)
	: frequency_(frequency) {
}

StagingStore::StagingStore(Frequency frequency)
	: frequency_(frequency) {
}

VaultStore::VaultStore(Frequency frequency)
	: frequency_(frequency) {
}

History::History(Frequency frequency)
	: frequency_(frequency),
	  active_(new ActiveStore(frequency)),
	  staging_(new StagingStore(frequency)),
	  vault_(new VaultStore(frequency)) {
}

History::~History() {
}

Frequency History::frequency() const {
	return frequency_;
}

Status History::put(const Bar& bar) {
	(void)bar;
	return Status::Error(ErrorCode::NotImplemented, "history put is not implemented");
}

Status History::put(const std::vector<Bar>& bars) {
	(void)bars;
	return Status::Error(ErrorCode::NotImplemented, "history batch put is not implemented");
}

Status History::get(SymbolId symbol_id,
                    const std::string& local_time,
                    Bar* out) const {
	(void)symbol_id;
	(void)local_time;
	if (out == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "out is required");
	}
	return Status::Error(ErrorCode::NotImplemented, "history get is not implemented");
}

Status History::get(SymbolId symbol_id,
                    const std::string& begin,
                    const std::string& end,
                    AdjustMode adjust_mode,
                    std::vector<Bar>* out) const {
	(void)symbol_id;
	(void)begin;
	(void)end;
	(void)adjust_mode;
	if (out == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "out is required");
	}
	out->clear();
	return Status::Error(ErrorCode::NotImplemented, "history range get is not implemented");
}

Status History::get(const std::vector<SymbolId>& symbol_ids,
                    const std::string& begin,
                    const std::string& end,
                    AdjustMode adjust_mode,
                    std::vector<Bar>* out) const {
	(void)symbol_ids;
	(void)begin;
	(void)end;
	(void)adjust_mode;
	if (out == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "out is required");
	}
	out->clear();
	return Status::Error(ErrorCode::NotImplemented, "history range get is not implemented");
}

Status History::flush() {
	return Status::Error(ErrorCode::NotImplemented, "history flush is not implemented");
}

Status History::seal_before(const std::string& local_time) {
	(void)local_time;
	return Status::Error(ErrorCode::NotImplemented, "history sealing is not implemented");
}

}  // namespace zstfs
