#include "zstfs/market.h"

#include "serialization.h"

#include <fstream>
#include <iterator>

namespace zstfs {

static const uint16_t kSymbolsVersion = 1;

static bool ValidDateOrEmpty(const std::string& value) {
	if (value.empty()) {
		return true;
	}
	if (value.size() != 8) {
		return false;
	}
	for (std::string::const_iterator it = value.begin(); it != value.end(); ++it) {
		if (*it < '0' || *it > '9') {
			return false;
		}
	}
	return true;
}

static bool AliasIsActive(const SymbolAlias& alias, const std::string& date) {
	return (alias.begin_date.empty() || alias.begin_date <= date) &&
	       (alias.end_date.empty() || date <= alias.end_date);
}

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
	std::map<std::string, bool> new_codes;
	new_codes[symbol.code] = true;
	for (std::vector<SymbolAlias>::const_iterator alias = symbol.aliases.begin();
	     alias != symbol.aliases.end(); ++alias) {
		if (alias->code.empty() || !ValidDateOrEmpty(alias->begin_date) ||
		    !ValidDateOrEmpty(alias->end_date) ||
		    (!alias->begin_date.empty() && !alias->end_date.empty() &&
		     alias->begin_date > alias->end_date) ||
		    by_code_.find(alias->code) != by_code_.end() ||
		    !new_codes.insert(std::make_pair(alias->code, true)).second) {
			return Status::Error(ErrorCode::InvalidArgument,
			                     "invalid or duplicate symbol alias");
		}
	}
	if (next_id_ == kInvalidSymbolId) {
		return Status::Error(ErrorCode::Conflict, "symbol identifier space is exhausted");
	}

	Symbol stored = symbol;
	stored.id = next_id_;
	by_id_[stored.id] = stored;
	by_code_[stored.code] = stored.id;
	for (std::vector<SymbolAlias>::const_iterator alias = stored.aliases.begin();
	     alias != stored.aliases.end(); ++alias) {
		by_code_[alias->code] = stored.id;
	}
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

Status Symbols::find(const std::string& code,
                     const std::string& date_value,
                     Symbol* out) const {
	if (out == NULL || date_value.empty()) {
		return Status::Error(ErrorCode::InvalidArgument,
		                     "date and output are required");
	}
	std::map<std::string, SymbolId>::const_iterator index = by_code_.find(code);
	if (index == by_code_.end()) {
		return Status::Error(ErrorCode::NotFound, "symbol code was not found");
	}
	std::map<SymbolId, Symbol>::const_iterator found = by_id_.find(index->second);
	if (found == by_id_.end()) {
		return Status::Error(ErrorCode::CorruptData, "symbol code index is invalid");
	}
	if (found->second.code == code) {
		*out = found->second;
		return Status::Ok();
	}
	for (std::vector<SymbolAlias>::const_iterator alias =
		     found->second.aliases.begin();
	     alias != found->second.aliases.end(); ++alias) {
		if (alias->code == code && AliasIsActive(*alias, date_value)) {
			*out = found->second;
			return Status::Ok();
		}
	}
	return Status::Error(ErrorCode::NotFound, "symbol alias was not active");
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
	if (stored.code != found->second.code) {
		bool already_alias = false;
		for (std::vector<SymbolAlias>::const_iterator alias = stored.aliases.begin();
		     alias != stored.aliases.end(); ++alias) {
			already_alias = already_alias || alias->code == found->second.code;
		}
		if (!already_alias) {
			SymbolAlias old_alias = {found->second.code, "", ""};
			stored.aliases.push_back(old_alias);
		}
	}
	std::map<std::string, bool> new_codes;
	new_codes[stored.code] = true;
	for (std::vector<SymbolAlias>::const_iterator alias = stored.aliases.begin();
	     alias != stored.aliases.end(); ++alias) {
		std::map<std::string, SymbolId>::const_iterator existing =
			by_code_.find(alias->code);
		if (alias->code.empty() || !ValidDateOrEmpty(alias->begin_date) ||
		    !ValidDateOrEmpty(alias->end_date) ||
		    (!alias->begin_date.empty() && !alias->end_date.empty() &&
		     alias->begin_date > alias->end_date) ||
		    !new_codes.insert(std::make_pair(alias->code, true)).second ||
		    (existing != by_code_.end() && existing->second != id)) {
			return Status::Error(ErrorCode::InvalidArgument,
			                     "invalid or duplicate symbol alias");
		}
	}
	for (std::map<std::string, SymbolId>::iterator it = by_code_.begin();
	     it != by_code_.end();) {
		if (it->second == id) {
			by_code_.erase(it++);
		} else {
			++it;
		}
	}
	found->second = stored;
	by_code_[stored.code] = id;
	for (std::vector<SymbolAlias>::const_iterator alias = stored.aliases.begin();
	     alias != stored.aliases.end(); ++alias) {
		by_code_[alias->code] = id;
	}
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

Status Symbols::save(const std::string& file_path) const {
	std::vector<uint8_t> data;
	data.push_back('Z');
	data.push_back('S');
	data.push_back('Y');
	data.push_back('M');
	PutU16(&data, kSymbolsVersion);
	PutU16(&data, 0);
	PutU32(&data, static_cast<uint32_t>(by_id_.size()));
	PutU32(&data, static_cast<uint32_t>(by_code_.size()));
	PutU32(&data, next_id_);

	for (std::map<SymbolId, Symbol>::const_iterator it = by_id_.begin();
	     it != by_id_.end(); ++it) {
		const Symbol& symbol = it->second;
		std::vector<uint8_t> record;
		PutU32(&record, symbol.id);
		PutU8(&record, static_cast<uint8_t>(symbol.state));
		if (!PutString(&record, symbol.code) ||
		    !PutString(&record, symbol.name) ||
		    !PutString(&record, symbol.security_type) ||
		    !PutString(&record, symbol.industry) ||
		    !PutString(&record, symbol.list_date) ||
		    !PutString(&record, symbol.delist_date)) {
			return Status::Error(ErrorCode::InvalidArgument,
			                     "symbol field is too long");
		}
		PutU64(&record, symbol.share_capital);
		PutU64(&record, symbol.tradable_share);
		PutU32(&record, symbol.volume_unit);
		if (symbol.aliases.size() > std::numeric_limits<uint16_t>::max()) {
			return Status::Error(ErrorCode::InvalidArgument,
			                     "too many symbol aliases");
		}
		PutU16(&record, static_cast<uint16_t>(symbol.aliases.size()));
		for (std::vector<SymbolAlias>::const_iterator alias = symbol.aliases.begin();
		     alias != symbol.aliases.end(); ++alias) {
			if (!PutString(&record, alias->code) ||
			    !PutString(&record, alias->begin_date) ||
			    !PutString(&record, alias->end_date)) {
				return Status::Error(ErrorCode::InvalidArgument,
				                     "symbol alias field is too long");
			}
		}
		PutU32(&data, static_cast<uint32_t>(record.size()));
		data.insert(data.end(), record.begin(), record.end());
	}

	for (std::map<std::string, SymbolId>::const_iterator it = by_code_.begin();
	     it != by_code_.end(); ++it) {
		if (!PutString(&data, it->first)) {
			return Status::Error(ErrorCode::InvalidArgument,
			                     "symbol code is too long");
		}
		PutU32(&data, it->second);
	}

	std::ofstream output(file_path.c_str(), std::ios::binary | std::ios::trunc);
	if (!output) {
		return Status::Error(ErrorCode::IoError, "failed to open symbols file");
	}
	output.write(reinterpret_cast<const char*>(&data[0]), data.size());
	if (!output) {
		return Status::Error(ErrorCode::IoError, "failed to write symbols file");
	}
	return Status::Ok();
}

Status Symbols::load(const std::string& file_path) {
	std::ifstream input(file_path.c_str(), std::ios::binary);
	if (!input) {
		return Status::Error(ErrorCode::IoError, "failed to open symbols file");
	}
	std::vector<uint8_t> data((std::istreambuf_iterator<char>(input)),
	                          std::istreambuf_iterator<char>());
	size_t offset = 0;
	if (data.size() < 20 || data[0] != 'Z' || data[1] != 'S' ||
	    data[2] != 'Y' || data[3] != 'M') {
		return Status::Error(ErrorCode::CorruptData, "invalid symbols file header");
	}
	offset = 4;
	uint16_t version = 0;
	uint16_t reserved = 0;
	uint32_t symbol_count = 0;
	uint32_t alias_count = 0;
	uint32_t next_id = 0;
	if (!GetU16(data, &offset, &version) || !GetU16(data, &offset, &reserved) ||
	    !GetU32(data, &offset, &symbol_count) ||
	    !GetU32(data, &offset, &alias_count) ||
	    !GetU32(data, &offset, &next_id) || version != kSymbolsVersion) {
		return Status::Error(ErrorCode::CorruptData, "unsupported symbols file");
	}

	std::map<SymbolId, Symbol> loaded_by_id;
	std::map<std::string, SymbolId> loaded_by_code;
	for (uint32_t index = 0; index < symbol_count; ++index) {
		uint32_t record_size = 0;
		if (!GetU32(data, &offset, &record_size) ||
		    offset + record_size > data.size()) {
			return Status::Error(ErrorCode::CorruptData, "truncated symbol record");
		}
		size_t record_end = offset + record_size;
		std::vector<uint8_t> record(data.begin() + offset,
		                            data.begin() + record_end);
		size_t record_offset = 0;
		Symbol symbol = {};
		uint8_t state = 0;
		if (!GetU32(record, &record_offset, &symbol.id) ||
		    !GetU8(record, &record_offset, &state) ||
		    !GetString(record, &record_offset, &symbol.code) ||
		    !GetString(record, &record_offset, &symbol.name) ||
		    !GetString(record, &record_offset, &symbol.security_type) ||
		    !GetString(record, &record_offset, &symbol.industry) ||
		    !GetString(record, &record_offset, &symbol.list_date) ||
		    !GetString(record, &record_offset, &symbol.delist_date) ||
		    !GetU64(record, &record_offset, &symbol.share_capital) ||
		    !GetU64(record, &record_offset, &symbol.tradable_share) ||
		    !GetU32(record, &record_offset, &symbol.volume_unit) ||
		    symbol.id == kInvalidSymbolId ||
		    state > static_cast<uint8_t>(SymbolState::Retired) ||
		    symbol.code.empty()) {
			return Status::Error(ErrorCode::CorruptData, "invalid symbol record");
		}
		symbol.state = static_cast<SymbolState>(state);
		if (record_offset < record.size()) {
			uint16_t alias_count = 0;
			std::map<std::string, bool> record_codes;
			record_codes[symbol.code] = true;
			if (!GetU16(record, &record_offset, &alias_count)) {
				return Status::Error(ErrorCode::CorruptData,
				                     "invalid symbol alias list");
			}
			for (uint16_t alias_index = 0; alias_index < alias_count; ++alias_index) {
				SymbolAlias alias;
				if (!GetString(record, &record_offset, &alias.code) ||
				    !GetString(record, &record_offset, &alias.begin_date) ||
				    !GetString(record, &record_offset, &alias.end_date) ||
				    alias.code.empty() || !ValidDateOrEmpty(alias.begin_date) ||
				    !ValidDateOrEmpty(alias.end_date) ||
				    (!alias.begin_date.empty() && !alias.end_date.empty() &&
				     alias.begin_date > alias.end_date) ||
				    !record_codes.insert(std::make_pair(alias.code, true)).second) {
					return Status::Error(ErrorCode::CorruptData,
					                     "invalid symbol alias");
				}
				symbol.aliases.push_back(alias);
			}
		}
		offset = record_end;
		if (!loaded_by_id.insert(std::make_pair(symbol.id, symbol)).second) {
			return Status::Error(ErrorCode::CorruptData, "duplicate symbol identifier");
		}
	}

	for (uint32_t index = 0; index < alias_count; ++index) {
		std::string code;
		uint32_t id = 0;
		if (!GetString(data, &offset, &code) || !GetU32(data, &offset, &id) ||
		    code.empty() || loaded_by_id.find(id) == loaded_by_id.end() ||
		    !loaded_by_code.insert(std::make_pair(code, id)).second) {
			return Status::Error(ErrorCode::CorruptData, "invalid symbol code index");
		}
	}
	if (offset != data.size() || next_id == kInvalidSymbolId) {
		return Status::Error(ErrorCode::CorruptData, "trailing symbols data");
	}

	by_id_.swap(loaded_by_id);
	by_code_.swap(loaded_by_code);
	next_id_ = next_id;
	return Status::Ok();
}

}  // namespace zstfs
