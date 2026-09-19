// actions.cpp
//
// Owns raw corporate actions and their durable ZAC8 representation. Stable
// source event keys make retries, corrections, and withdrawals idempotent;
// the library maps those keys to private Action IDs. Actions are written as one
// complete replacement file, then the Market publishes a manifest describing
// that accepted file. Adjustment anchors are derived only in memory so
// persisted data remains auditable and lossless.

#include "zstfs/market.h"

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <fstream>
#include <utility>
#include <vector>

#include <unistd.h>

#include "serialization.h"
#include <zstfs/pe_log.h>

namespace zstfs {

// =============================================================================
// Lifecycle and Persistence Configuration
// Market supplies persistence before actions accept mutations.

Actions::Actions()
	: next_id_(kInvalidActionId + 1), status_(Status::Ok()) {
}

Status Actions::configure_persistence(
	const std::string& file_path,
	const std::function<Status()>& publish_manifest) {
	path_ = file_path;
	publish_manifest_ = publish_manifest;
	return load();
}

Status Actions::status() const {
	return status_;
}

// =============================================================================
// ZAC8 Persistence
// ZAC8 retains raw events only; adjustment anchors remain in memory.

static const uint16_t kActionsVersion = 2;
static const std::string::size_type kMaxExternalEventKeyLength = 65535;
typedef std::pair<SymbolId, std::string> ExternalActionKey;

static ExternalActionKey ActionKey(const Action& action) {
	return ExternalActionKey(action.symbol_id, action.external_event_key);
}

static bool ValidActionType(ActionType type) {
	const int value = static_cast<int>(type);
	return value >= static_cast<int>(ActionType::Split) &&
		value <= static_cast<int>(ActionType::RightsIssue);
}

static bool ValidAction(const Action& action) {
	if (action.external_event_key.empty() ||
		action.external_event_key.size() > kMaxExternalEventKeyLength ||
		action.symbol_id == kInvalidSymbolId || action.effective_date.empty() ||
		!ValidActionType(action.type) || !std::isfinite(action.factor) ||
		!std::isfinite(action.cash_value)) {
		return false;
	}
	if ((action.type == ActionType::Split ||
	     action.type == ActionType::ReverseSplit ||
	     action.type == ActionType::StockDividend ||
	     action.type == ActionType::RightsIssue) && action.factor <= 0.0) {
		return false;
	}
	return true;
}

static void PutAction(std::vector<uint8_t>* bytes, const Action& action) {
	PutU32(bytes, action.id);
	PutU32(bytes, action.symbol_id);
	PutU8(bytes, static_cast<uint8_t>(action.type));
	PutU8(bytes, 0);
	PutU16(bytes, 0);
	PutDouble(bytes, action.factor);
	PutDouble(bytes, action.cash_value);
	PutString(bytes, action.effective_date);
	PutString(bytes, action.external_event_key);
}

static bool GetAction(const std::vector<uint8_t>& bytes, size_t* offset, Action* action) {
	uint32_t id = 0;
	uint32_t symbol_id = 0;
	uint8_t type = 0;
	uint8_t reserved8 = 0;
	uint16_t reserved16 = 0;
	if (!GetU32(bytes, offset, &id) || !GetU32(bytes, offset, &symbol_id) ||
		!GetU8(bytes, offset, &type) || !GetU8(bytes, offset, &reserved8) ||
		!GetU16(bytes, offset, &reserved16) || reserved8 != 0 || reserved16 != 0 ||
		!GetDouble(bytes, offset, &action->factor) ||
		!GetDouble(bytes, offset, &action->cash_value) ||
		!GetString(bytes, offset, &action->effective_date) ||
		!GetString(bytes, offset, &action->external_event_key)) {
		return false;
	}
	action->id = id;
	action->symbol_id = symbol_id;
	action->type = static_cast<ActionType>(type);
	return ValidAction(*action) && action->id != kInvalidActionId;
}

Status Actions::load()
{
	by_id_.clear();
	by_external_event_key_.clear();
	anchors_.clear();
	next_id_ = kInvalidActionId + 1;
	std::ifstream input(path_.c_str(), std::ios::binary);
	if (!input)
	{
		if (access(path_.c_str(), F_OK) == 0)
		{
			PELOG_LOG((PLV_ERROR, "actions load: cannot open %s (errno=%d)\n", path_.c_str(), errno));
			status_ = Status::Error(ErrorCode::IoError, "cannot open actions.bin");
			return status_;
		}
		status_ = save(by_id_, next_id_);
		return status_;
	}
	std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(input)),
		std::istreambuf_iterator<char>());
	size_t offset = 0;
	uint16_t version = 0;
	uint16_t reserved = 0;
	uint32_t count = 0;
	uint32_t persisted_next_id = 0;
	if (bytes.size() < 16 || bytes[0] != 'Z' || bytes[1] != 'A' ||
		bytes[2] != 'C' || bytes[3] != '8')
	{
		PELOG_LOG((PLV_ERROR, "actions load: bad magic in %s\n", path_.c_str()));
		status_ = Status::Error(ErrorCode::CorruptData, "invalid actions.bin magic");
		return status_;
	}
	offset = 4;
	if (!GetU16(bytes, &offset, &version) || !GetU16(bytes, &offset, &reserved) ||
		!GetU32(bytes, &offset, &count) || !GetU32(bytes, &offset, &persisted_next_id) ||
		reserved != 0 || persisted_next_id == 0 || count > bytes.size()) {
		status_ = Status::Error(ErrorCode::CorruptData, "invalid actions.bin header");
		return status_;
	}
	if (version != kActionsVersion) {
		status_ = Status::Error(ErrorCode::CorruptData,
			"unsupported actions.bin version; reimport corporate actions");
		return status_;
	}
	ActionId max_id = kInvalidActionId;
	for (uint32_t i = 0; i < count; ++i) {
		Action action = {};
		if (!GetAction(bytes, &offset, &action) || by_id_.find(action.id) != by_id_.end() ||
			by_external_event_key_.find(ActionKey(action)) != by_external_event_key_.end()) {
			status_ = Status::Error(ErrorCode::CorruptData, "invalid actions.bin record");
			return status_;
		}
		by_id_[action.id] = action;
		by_external_event_key_[ActionKey(action)] = action.id;
		max_id = std::max(max_id, action.id);
	}
	if (offset != bytes.size() || persisted_next_id <= max_id) {
		status_ = Status::Error(ErrorCode::CorruptData, "invalid actions.bin tail");
		return status_;
	}
	next_id_ = persisted_next_id;
	rebuild_anchors();
	status_ = Status::Ok();
	return status_;
}

Status Actions::save(const std::map<ActionId, Action>& actions,
	ActionId next_id) const {
	std::vector<uint8_t> bytes;
	bytes.push_back('Z');
	bytes.push_back('A');
	bytes.push_back('C');
	bytes.push_back('8');
	PutU16(&bytes, kActionsVersion);
	PutU16(&bytes, 0);
	PutU32(&bytes, static_cast<uint32_t>(actions.size()));
	PutU32(&bytes, next_id);
	for (std::map<ActionId, Action>::const_iterator it = actions.begin();
		 it != actions.end(); ++it) {
		PutAction(&bytes, it->second);
	}

	const std::string temporary = path_ + ".tmp-actions";
	std::ofstream output(temporary.c_str(), std::ios::binary | std::ios::trunc);
	if (!output)
	{
		PELOG_LOG((PLV_ERROR, "actions save: cannot create temp file %s\n", temporary.c_str()));
		return Status::Error(ErrorCode::IoError, "cannot create actions temporary file");
	}
	output.write(reinterpret_cast<const char*>(&bytes[0]), bytes.size());
	output.close();
	if (!output || rename(temporary.c_str(), path_.c_str()) != 0)
	{
		unlink(temporary.c_str());
		PELOG_LOG((PLV_ERROR, "actions save: cannot publish %s (errno=%d)\n", path_.c_str(), errno));
		return Status::Error(ErrorCode::IoError, "cannot publish actions.bin");
	}
	return Status::Ok();
}

Status Actions::persist(const std::map<ActionId, Action>& actions,
	ActionId next_id) {
	std::map<ExternalActionKey, ActionId> external_event_keys;
	for (std::map<ActionId, Action>::const_iterator it = actions.begin();
		 it != actions.end(); ++it) {
		if (!ValidAction(it->second) ||
			external_event_keys.find(ActionKey(it->second)) != external_event_keys.end()) {
			return Status::Error(ErrorCode::InvalidArgument, "invalid action set");
		}
		external_event_keys[ActionKey(it->second)] = it->first;
	}
	Status status = save(actions, next_id);
	if (!status.ok()) {
		return status;
	}
	if (publish_manifest_) {
		status = publish_manifest_();
		if (!status.ok()) {
			return status;
		}
	}
	by_id_ = actions;
	by_external_event_key_ = external_event_keys;
	next_id_ = next_id;
	rebuild_anchors();
	status_ = Status::Ok();
	return Status::Ok();
}

// =============================================================================
// Raw Action Mutation and Query
// Raw queries and mutations use stable effective-date then ActionId ordering.

static bool ActionOrder(const Action& left, const Action& right) {
	if (left.effective_date != right.effective_date) {
		return left.effective_date < right.effective_date;
	}
	return left.id < right.id;
}

static bool SameAction(const Action& left, const Action& right) {
	return left.id == right.id &&
		left.external_event_key == right.external_event_key &&
		left.symbol_id == right.symbol_id &&
		left.effective_date == right.effective_date &&
		left.type == right.type && left.factor == right.factor &&
		left.cash_value == right.cash_value;
}

Status Actions::upsert(const Action& action) {
	if (!status_.ok()) {
		return status_;
	}
	if (!ValidAction(action)) {
		return Status::Error(ErrorCode::InvalidArgument, "invalid action");
	}
	std::map<ExternalActionKey, ActionId>::const_iterator existing =
		by_external_event_key_.find(ActionKey(action));
	if (existing == by_external_event_key_.end()) {
		if (next_id_ == kInvalidActionId) {
			return Status::Error(ErrorCode::Conflict, "action identifier space is exhausted");
		}
		Action stored = action;
		stored.id = next_id_;
		std::map<ActionId, Action> candidate = by_id_;
		candidate[stored.id] = stored;
		return persist(candidate, next_id_ + 1);
	}

	Action stored = action;
	stored.id = existing->second;
	std::map<ActionId, Action>::const_iterator current = by_id_.find(stored.id);
	if (current == by_id_.end()) {
		return Status::Error(ErrorCode::CorruptData, "action key index is invalid");
	}
	if (SameAction(current->second, stored)) {
		return Status::Ok();
	}
	std::map<ActionId, Action> candidate = by_id_;
	candidate[stored.id] = stored;
	return persist(candidate, next_id_);
}

Status Actions::get(SymbolId symbol_id, const std::string& begin,
	const std::string& end, std::vector<Action>* out) const {
	if (out == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "out is required");
	}
	if (!status_.ok()) {
		return status_;
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

Status Actions::remove(SymbolId symbol_id, const std::string& external_event_key) {
	if (!status_.ok()) {
		return status_;
	}
	if (symbol_id == kInvalidSymbolId || external_event_key.empty() ||
		external_event_key.size() > kMaxExternalEventKeyLength) {
		return Status::Error(ErrorCode::InvalidArgument, "invalid external action key");
	}
	std::map<ExternalActionKey, ActionId>::const_iterator found =
		by_external_event_key_.find(ExternalActionKey(symbol_id, external_event_key));
	if (found == by_external_event_key_.end()) {
		return Status::Ok();
	}
	std::map<ActionId, Action> candidate = by_id_;
	candidate.erase(found->second);
	return persist(candidate, next_id_);
}

// =============================================================================
// Adjustment Anchors and Bar Transformation
// Anchors are derived from raw actions and never persisted. Forward applies
// inverse transforms through a bar; Backward applies later normal transforms.

void Actions::rebuild_anchors() {
	anchors_.clear();
	for (std::map<ActionId, Action>::const_iterator it = by_id_.begin();
		 it != by_id_.end(); ++it) {
		anchors_[it->second.symbol_id].push_back(it->second);
	}
	for (std::map<SymbolId, std::vector<Action> >::iterator it = anchors_.begin();
		 it != anchors_.end(); ++it) {
		std::sort(it->second.begin(), it->second.end(), ActionOrder);
	}
}

static void ApplyAction(const Action& action, bool inverse, Bar* bar) {
	if (action.type == ActionType::CashDividend) {
		if (inverse) {
			bar->open += action.cash_value;
			bar->high += action.cash_value;
			bar->low += action.cash_value;
			bar->close += action.cash_value;
		} else {
			bar->open -= action.cash_value;
			bar->high -= action.cash_value;
			bar->low -= action.cash_value;
			bar->close -= action.cash_value;
		}
		return;
	}

	const bool split = action.type == ActionType::Split ||
		action.type == ActionType::StockDividend;
	const double factor = action.factor;
	if (split) {
		if (inverse) {
			bar->open *= factor;
			bar->high *= factor;
			bar->low *= factor;
			bar->close *= factor;
			bar->volume /= factor;
		} else {
			bar->open /= factor;
			bar->high /= factor;
			bar->low /= factor;
			bar->close /= factor;
			bar->volume *= factor;
		}
		return;
	}

	if (action.type == ActionType::ReverseSplit) {
		if (inverse) {
			bar->open /= factor;
			bar->high /= factor;
			bar->low /= factor;
			bar->close /= factor;
			bar->volume *= factor;
		} else {
			bar->open *= factor;
			bar->high *= factor;
			bar->low *= factor;
			bar->close *= factor;
			bar->volume /= factor;
		}
		return;
	}

	// A rights issue is the factor transform followed by its cash transform.
	// Its inverse must undo cash first, then undo the factor transform.
	if (inverse) {
		bar->open += action.cash_value;
		bar->high += action.cash_value;
		bar->low += action.cash_value;
		bar->close += action.cash_value;
		bar->open *= factor;
		bar->high *= factor;
		bar->low *= factor;
		bar->close *= factor;
		bar->volume /= factor;
	} else {
		bar->open /= factor;
		bar->high /= factor;
		bar->low /= factor;
		bar->close /= factor;
		bar->volume *= factor;
		bar->open -= action.cash_value;
		bar->high -= action.cash_value;
		bar->low -= action.cash_value;
		bar->close -= action.cash_value;
	}
}

Status Actions::adjust(SymbolId symbol_id, const std::string& local_time,
	AdjustMode mode, Bar* bar) const {
	if (bar == NULL || bar->symbol_id != symbol_id) {
		return Status::Error(ErrorCode::InvalidArgument, "invalid adjustment bar");
	}
	if (mode == AdjustMode::Raw || bar->state != BarState::Normal) {
		return Status::Ok();
	}
	std::map<SymbolId, std::vector<Action> >::const_iterator found = anchors_.find(symbol_id);
	if (found == anchors_.end()) {
		return Status::Ok();
	}
	const std::vector<Action>& actions = found->second;
	if (mode == AdjustMode::Forward) {
		for (std::vector<Action>::const_reverse_iterator it = actions.rbegin();
			 it != actions.rend(); ++it) {
			if (it->effective_date <= local_time) {
				ApplyAction(*it, true, bar);
			}
		}
	} else if (mode == AdjustMode::Backward) {
		for (std::vector<Action>::const_iterator it = actions.begin();
			 it != actions.end(); ++it) {
			if (it->effective_date > local_time) {
				ApplyAction(*it, false, bar);
			}
		}
	} else {
		return Status::Error(ErrorCode::InvalidArgument, "unknown adjustment mode");
	}
	return Status::Ok();
}

}  // namespace zstfs
