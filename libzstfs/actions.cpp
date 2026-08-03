#include "zstfs/market.h"

#include <algorithm>

namespace zstfs {

static bool ActionOrder(const Action& left, const Action& right) {
	if (left.effective_date != right.effective_date) {
		return left.effective_date < right.effective_date;
	}
	return left.id < right.id;
}

Actions::Actions()
	: next_id_(kInvalidActionId + 1) {
}

Status Actions::add(const Action& action, ActionId* out_id) {
	if (out_id == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "out_id is required");
	}
	if (action.symbol_id == kInvalidSymbolId || action.effective_date.empty()) {
		return Status::Error(ErrorCode::InvalidArgument,
		                     "symbol and effective date are required");
	}
	if (next_id_ == kInvalidActionId) {
		return Status::Error(ErrorCode::Conflict, "action identifier space is exhausted");
	}

	Action stored = action;
	stored.id = next_id_;
	by_id_[stored.id] = stored;
	*out_id = stored.id;
	++next_id_;
	return Status::Ok();
}

Status Actions::get(SymbolId symbol_id,
                    const std::string& begin,
                    const std::string& end,
                    std::vector<Action>* out) const {
	if (out == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "out is required");
	}
	if (begin > end) {
		return Status::Error(ErrorCode::InvalidArgument, "begin must not exceed end");
	}

	out->clear();
	for (std::map<ActionId, Action>::const_iterator it = by_id_.begin();
	     it != by_id_.end(); ++it) {
		const Action& action = it->second;
		if (action.symbol_id == symbol_id && action.effective_date >= begin &&
		    action.effective_date <= end) {
			out->push_back(action);
		}
	}
	std::sort(out->begin(), out->end(), ActionOrder);
	return Status::Ok();
}

Status Actions::update(ActionId id, const Action& action) {
	if (action.symbol_id == kInvalidSymbolId || action.effective_date.empty()) {
		return Status::Error(ErrorCode::InvalidArgument,
		                     "symbol and effective date are required");
	}
	std::map<ActionId, Action>::iterator found = by_id_.find(id);
	if (found == by_id_.end()) {
		return Status::Error(ErrorCode::NotFound, "action was not found");
	}

	Action stored = action;
	stored.id = id;
	found->second = stored;
	return Status::Ok();
}

Status Actions::remove(ActionId id) {
	std::map<ActionId, Action>::iterator found = by_id_.find(id);
	if (found == by_id_.end()) {
		return Status::Error(ErrorCode::NotFound, "action was not found");
	}
	by_id_.erase(found);
	return Status::Ok();
}

}  // namespace zstfs
