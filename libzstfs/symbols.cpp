#include "zstfs/market.h"

namespace zstfs {

Symbols::Symbols()
	: next_id_(kInvalidSymbolId + 1) {
}

Status Symbols::add(const Symbol& symbol, SymbolId* out_id) {
	if (out_id == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "out_id is required");
	}
	if (symbol.code.empty()) {
		return Status::Error(ErrorCode::InvalidArgument, "symbol code is required");
	}
	if (by_code_.find(symbol.code) != by_code_.end()) {
		return Status::Error(ErrorCode::AlreadyPresent, "symbol code already exists");
	}
	if (next_id_ == kInvalidSymbolId) {
		return Status::Error(ErrorCode::Conflict, "symbol identifier space is exhausted");
	}

	Symbol stored = symbol;
	stored.id = next_id_;
	by_id_[stored.id] = stored;
	by_code_[stored.code] = stored.id;
	*out_id = stored.id;
	++next_id_;
	return Status::Ok();
}

Status Symbols::get(SymbolId id, Symbol* out) const {
	if (out == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "out is required");
	}
	std::map<SymbolId, Symbol>::const_iterator found = by_id_.find(id);
	if (found == by_id_.end()) {
		return Status::Error(ErrorCode::NotFound, "symbol was not found");
	}
	*out = found->second;
	return Status::Ok();
}

Status Symbols::find(const std::string& code, Symbol* out) const {
	if (out == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "out is required");
	}
	std::map<std::string, SymbolId>::const_iterator index = by_code_.find(code);
	if (index == by_code_.end()) {
		return Status::Error(ErrorCode::NotFound, "symbol code was not found");
	}
	return get(index->second, out);
}

Status Symbols::update(SymbolId id, const Symbol& symbol) {
	if (symbol.code.empty()) {
		return Status::Error(ErrorCode::InvalidArgument, "symbol code is required");
	}
	std::map<SymbolId, Symbol>::iterator found = by_id_.find(id);
	if (found == by_id_.end()) {
		return Status::Error(ErrorCode::NotFound, "symbol was not found");
	}

	std::map<std::string, SymbolId>::const_iterator code = by_code_.find(symbol.code);
	if (code != by_code_.end() && code->second != id) {
		return Status::Error(ErrorCode::AlreadyPresent, "symbol code already exists");
	}

	Symbol stored = symbol;
	stored.id = id;
	found->second = stored;
	by_code_[stored.code] = id;
	return Status::Ok();
}

Status Symbols::remove(SymbolId id) {
	std::map<SymbolId, Symbol>::iterator found = by_id_.find(id);
	if (found == by_id_.end()) {
		return Status::Error(ErrorCode::NotFound, "symbol was not found");
	}
	found->second.state = SymbolState::Retired;
	return Status::Ok();
}

Status Symbols::list(std::vector<Symbol>* out) const {
	if (out == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "out is required");
	}
	out->clear();
	for (std::map<SymbolId, Symbol>::const_iterator it = by_id_.begin();
	     it != by_id_.end(); ++it) {
		out->push_back(it->second);
	}
	return Status::Ok();
}

}  // namespace zstfs
