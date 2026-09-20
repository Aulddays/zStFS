// symbols.cpp
//
// Owns Symbol metadata and its ZSYM v1 persistence. Every accepted mutation
// replaces the complete file atomically before the Market publishes a manifest
// for that file, so the in-memory indexes only describe published metadata.

#include "zstfs/market.h"

#include "serialization.h"
#include <zstfs/pe_log.h>

#include <cerrno>
#include <cstdio>
#include <fstream>
#include <iterator>

#include <unistd.h>

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

// SymbolsEqual compares two symbol records field by field, excluding the
// library-assigned id. Used for diff detection in upsert and batch mode so
// unchanged records do not trigger a snapshot write.
static bool SymbolsEqual(const Symbol& a, const Symbol& b) {
	if (a.code != b.code ||
	    a.name != b.name ||
	    a.security_type != b.security_type ||
	    a.exchange != b.exchange ||
	    a.board != b.board ||
	    a.industry != b.industry ||
	    a.list_date != b.list_date ||
	    a.delist_date != b.delist_date ||
	    a.share_capital != b.share_capital ||
	    a.tradable_share != b.tradable_share ||
	    a.state != b.state ||
	    a.trade_state != b.trade_state ||
	    a.aliases.size() != b.aliases.size()) {
		return false;
	}
	for (size_t i = 0; i < a.aliases.size(); ++i) {
		if (a.aliases[i].code != b.aliases[i].code ||
		    a.aliases[i].begin_date != b.aliases[i].begin_date ||
		    a.aliases[i].end_date != b.aliases[i].end_date) {
			return false;
		}
	}
	return true;
}

// ValidateSymbolFields checks code uniqueness and alias validity against a
// candidate code index. Returns NotFound-style error messages via Status.
static Status ValidateSymbolFields(
	const Symbol& symbol,
	SymbolId self_id,
	const std::map<std::string, SymbolId>& existing_codes) {
	if (symbol.code.empty()) {
		return Status::Error(ErrorCode::InvalidArgument, "symbol code is required");
	}
	std::map<std::string, bool> seen_codes;
	seen_codes[symbol.code] = true;
	std::map<std::string, SymbolId>::const_iterator primary =
		existing_codes.find(symbol.code);
	if (primary != existing_codes.end() && primary->second != self_id) {
		return Status::Error(ErrorCode::AlreadyPresent, "symbol code already exists");
	}
	for (std::vector<SymbolAlias>::const_iterator alias = symbol.aliases.begin();
	     alias != symbol.aliases.end(); ++alias) {
		if (alias->code.empty() || !ValidDateOrEmpty(alias->begin_date) ||
		    !ValidDateOrEmpty(alias->end_date) ||
		    (!alias->begin_date.empty() && !alias->end_date.empty() &&
		     alias->begin_date > alias->end_date)) {
			return Status::Error(ErrorCode::InvalidArgument,
			                     "invalid or duplicate symbol alias");
		}
		if (!seen_codes.insert(std::make_pair(alias->code, true)).second) {
			return Status::Error(ErrorCode::InvalidArgument,
			                     "invalid or duplicate symbol alias");
		}
		std::map<std::string, SymbolId>::const_iterator existing =
			existing_codes.find(alias->code);
		if (existing != existing_codes.end() && existing->second != self_id) {
			return Status::Error(ErrorCode::InvalidArgument,
			                     "invalid or duplicate symbol alias");
		}
	}
	return Status::Ok();
}

Symbols::Symbols()
	: next_id_(kInvalidSymbolId + 1), status_(Status::Ok()),
	  in_batch_(false), batch_dirty_(false) {
}

// Persistence is configured before a Market exposes its Symbols collection.
// A new empty file is created immediately so Market can include it in its
// initial manifest publication.
Status Symbols::configure_persistence(
	const std::string& file_path,
	bool* created) {
	if (created == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "created is required");
	}
	if (file_path.empty()) {
		return Status::Error(ErrorCode::InvalidArgument,
		                     "symbols persistence configuration is invalid");
	}

	path_ = file_path;
	*created = false;
	Status loaded = load();
	if (loaded.ok()) {
		status_ = Status::Ok();
		return status_;
	}
	if (loaded.code() != ErrorCode::NotFound) {
		status_ = loaded;
		return status_;
	}

	Status saved = save(by_id_, by_code_, next_id_);
	if (!saved.ok()) {
		status_ = saved;
		return status_;
	}
	*created = true;
	status_ = Status::Ok();
	return status_;
}

// apply_upsert performs the in-memory insert-or-update comparison. It validates
// fields first, then either inserts a new symbol (assigning the next available
// id) or compares against the stored record. When the stored record already
// matches exactly, it sets *changed = false and leaves state untouched.
bool Symbols::apply_upsert(const Symbol& symbol, SymbolId* out_id) {
	std::map<std::string, SymbolId>::const_iterator existing_code =
		by_code_.find(symbol.code);
	if (existing_code != by_code_.end()) {
		// Update path: compare and replace only if fields differ.
		SymbolId id = existing_code->second;
		Symbol& stored = by_id_[id];
		if (SymbolsEqual(symbol, stored)) {
			if (out_id != NULL) {
				*out_id = id;
			}
			return false;
		}
		// If the caller changed the primary code, the old code becomes an alias
		// so lookups by historical code continue to resolve to the same symbol.
		Symbol updated = symbol;
		updated.id = id;
		if (updated.code != stored.code) {
			bool already_alias = false;
			for (std::vector<SymbolAlias>::const_iterator alias =
				     updated.aliases.begin();
			     alias != updated.aliases.end(); ++alias) {
				if (alias->code == stored.code) {
					already_alias = true;
					break;
				}
			}
			if (!already_alias) {
				SymbolAlias old_alias = {stored.code, "", ""};
				updated.aliases.push_back(old_alias);
			}
		}
		// Rebuild the code index entry for this symbol: remove every code that
		// resolved to it, then re-add primary code and all aliases.
		for (std::map<std::string, SymbolId>::iterator it = by_code_.begin();
		     it != by_code_.end();) {
			if (it->second == id) {
				by_code_.erase(it++);
			} else {
				++it;
			}
		}
		by_code_[updated.code] = id;
		for (std::vector<SymbolAlias>::const_iterator alias =
			     updated.aliases.begin();
		     alias != updated.aliases.end(); ++alias) {
			by_code_[alias->code] = id;
		}
		stored = updated;
		if (out_id != NULL) {
			*out_id = id;
		}
		return true;
	}

	// Insert path: assign a fresh id and insert into both indexes.
	Symbol stored = symbol;
	stored.id = next_id_;
	by_id_[stored.id] = stored;
	by_code_[stored.code] = stored.id;
	for (std::vector<SymbolAlias>::const_iterator alias = stored.aliases.begin();
	     alias != stored.aliases.end(); ++alias) {
		by_code_[alias->code] = stored.id;
	}
	++next_id_;
	if (out_id != NULL) {
		*out_id = stored.id;
	}
	return true;
}

// flush_if_dirty is the single persistence gate for all mutations. Outside a
// batch it immediately writes the snapshot and publishes the manifest; inside
// a batch it only marks the batch dirty so commit_batch can do one write.
Status Symbols::flush_if_dirty(const std::map<SymbolId, Symbol>& symbols,
                               const std::map<std::string, SymbolId>& codes,
                               SymbolId next_id) {
	if (!status_.ok()) {
		return status_;
	}
	if (path_.empty())
		return Status::Error(ErrorCode::Conflict, "symbols persistence is not configured");
	if (in_batch_) {
		batch_dirty_ = true;
		return Status::Ok();
	}
	return persist(symbols, codes, next_id);
}

Status Symbols::upsert(const Symbol& symbol, SymbolId* out_id, bool* updated) {
	if (updated != NULL) {
		*updated = false;
	}
	std::map<std::string, SymbolId>::const_iterator existing_code =
		by_code_.find(symbol.code);
	SymbolId self_id = kInvalidSymbolId;
	if (existing_code != by_code_.end()) {
		self_id = existing_code->second;
	}
	// For updates we also need to validate against the index with this symbol's
	// old codes removed, since the primary code may be changing. Build a
	// temporary candidate index for that check.
	Status validated = ValidateSymbolFields(symbol, self_id, by_code_);
	if (!validated.ok() && self_id == kInvalidSymbolId) {
		return validated;
	}
	if (self_id != kInvalidSymbolId) {
		std::map<std::string, SymbolId> candidate_codes = by_code_;
		for (std::map<std::string, SymbolId>::iterator it = candidate_codes.begin();
		     it != candidate_codes.end();) {
			if (it->second == self_id) {
				candidate_codes.erase(it++);
			} else {
				++it;
			}
		}
		Status revalidated = ValidateSymbolFields(symbol, self_id, candidate_codes);
		if (!revalidated.ok()) {
			return revalidated;
		}
	}
	if (self_id == kInvalidSymbolId && next_id_ == kInvalidSymbolId) {
		return Status::Error(ErrorCode::Conflict,
		                     "symbol identifier space is exhausted");
	}
	// apply_upsert mutates memory immediately so reads inside the batch see the
	// new state. flush_if_dirty decides whether to persist now or at commit.
	bool changed = apply_upsert(symbol, out_id);
	if (!changed) {
		return Status::Ok();
	}
	Status flushed = flush_if_dirty(by_id_, by_code_, next_id_);
	if (!flushed.ok() && !in_batch_) {
		// Outside a batch, a failed persist leaves memory mutated because
		// apply_upsert ran first. This matches the existing pre-batch behavior
		// where persist failure also leaves candidate state partially applied;
		// the caller should treat the object as unusable (status_ is set).
		return flushed;
	}
	if (updated != NULL) {
		*updated = true;
	}
	return flushed;
}

Status Symbols::add(const Symbol& symbol, SymbolId* out_id) {
	if (out_id == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "out_id is required");
	}
	if (by_code_.find(symbol.code) != by_code_.end()) {
		return Status::Error(ErrorCode::AlreadyPresent, "symbol code already exists");
	}
	bool updated = false;
	Status s = upsert(symbol, out_id, &updated);
	if (!s.ok()) {
		return s;
	}
	// upsert on a fresh record always reports updated; guard against future
	// changes to diff logic by asserting the invariant here.
	(void)updated;
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
	std::map<SymbolId, Symbol>::iterator found = by_id_.find(id);
	if (found == by_id_.end()) {
		return Status::Error(ErrorCode::NotFound, "symbol was not found");
	}
	// Update is keyed by id, not by code, so we validate against a candidate
	// index that has all of this symbol's current codes removed first.
	std::map<std::string, SymbolId> candidate_codes = by_code_;
	for (std::map<std::string, SymbolId>::iterator it = candidate_codes.begin();
	     it != candidate_codes.end();) {
		if (it->second == id) {
			candidate_codes.erase(it++);
		} else {
			++it;
		}
	}
	Status validated = ValidateSymbolFields(symbol, id, candidate_codes);
	if (!validated.ok()) {
		return validated;
	}
	// Diff check: skip persistence when nothing actually changed. We compare
	// against the stored record with id ignored (id is stable for updates).
	Symbol compare = symbol;
	compare.id = id;
	// Preserve the old-code-as-alias behavior: if the primary code changed, the
	// old code is auto-appended to aliases before we compare and apply.
	if (compare.code != found->second.code) {
		bool already_alias = false;
		for (std::vector<SymbolAlias>::const_iterator alias = compare.aliases.begin();
		     alias != compare.aliases.end(); ++alias) {
			if (alias->code == found->second.code) {
				already_alias = true;
				break;
			}
		}
		if (!already_alias) {
			SymbolAlias old_alias = {found->second.code, "", ""};
			compare.aliases.push_back(old_alias);
		}
	}
	if (SymbolsEqual(compare, found->second)) {
		return Status::Ok();
	}
	// Apply mutation to memory immediately.
	found->second = compare;
	// Rebuild the code index for this symbol.
	for (std::map<std::string, SymbolId>::iterator it = by_code_.begin();
	     it != by_code_.end();) {
		if (it->second == id) {
			by_code_.erase(it++);
		} else {
			++it;
		}
	}
	by_code_[compare.code] = id;
	for (std::vector<SymbolAlias>::const_iterator alias = compare.aliases.begin();
	     alias != compare.aliases.end(); ++alias) {
		by_code_[alias->code] = id;
	}
	return flush_if_dirty(by_id_, by_code_, next_id_);
}

Status Symbols::remove(SymbolId id) {
	std::map<SymbolId, Symbol>::iterator found = by_id_.find(id);
	if (found == by_id_.end()) {
		return Status::Error(ErrorCode::NotFound, "symbol was not found");
	}
	if (found->second.state == SymbolState::Retired) {
		return Status::Ok();
	}
	found->second.state = SymbolState::Retired;
	return flush_if_dirty(by_id_, by_code_, next_id_);
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

Status Symbols::begin_batch() {
	if (!status_.ok()) {
		return status_;
	}
	if (in_batch_) {
		return Status::Error(ErrorCode::Conflict, "symbols batch is already active");
	}
	if (path_.empty())
		return Status::Error(ErrorCode::Conflict, "symbols persistence is not configured");
	// Snapshot current state so abort_batch can roll back without touching disk.
	saved_by_id_ = by_id_;
	saved_by_code_ = by_code_;
	saved_next_id_ = next_id_;
	in_batch_ = true;
	batch_dirty_ = false;
	return Status::Ok();
}

Status Symbols::commit_batch(bool* changed) {
	if (!status_.ok()) {
		return status_;
	}
	if (!in_batch_) {
		return Status::Error(ErrorCode::Conflict, "no symbols batch is active");
	}
	bool did_change = batch_dirty_;
	Status result = Status::Ok();
	if (batch_dirty_) {
		result = persist(by_id_, by_code_, next_id_);
		if (!result.ok()) {
			// Leave the batch open on failure so the caller can abort_batch and
			// discard the failed mutations. The in-memory state is still the
			// mutated candidate; saved_by_id_ still holds the last good snapshot.
			return result;
		}
	}
	in_batch_ = false;
	batch_dirty_ = false;
	saved_by_id_.clear();
	saved_by_code_.clear();
	saved_next_id_ = kInvalidSymbolId;
	if (changed != NULL) {
		*changed = did_change;
	}
	return Status::Ok();
}

Status Symbols::abort_batch() {
	if (!in_batch_) {
		return Status::Ok();
	}
	// Roll back in-memory state to the pre-batch snapshot. Disk was never
	// touched during the batch, so no file-level cleanup is needed.
	by_id_.swap(saved_by_id_);
	by_code_.swap(saved_by_code_);
	next_id_ = saved_next_id_;
	saved_by_id_.clear();
	saved_by_code_.clear();
	saved_next_id_ = kInvalidSymbolId;
	in_batch_ = false;
	batch_dirty_ = false;
	return Status::Ok();
}

// save serializes supplied candidate indexes so a failed persistence attempt
// cannot alter the currently visible Symbols state.
Status Symbols::save(const std::map<SymbolId, Symbol>& symbols,
                     const std::map<std::string, SymbolId>& codes,
                     SymbolId next_id) const {
	std::vector<uint8_t> data;
	data.push_back('Z');
	data.push_back('S');
	data.push_back('Y');
	data.push_back('M');
	PutU16(&data, kSymbolsVersion);
	PutU16(&data, 0);
	PutU32(&data, static_cast<uint32_t>(symbols.size()));
	PutU32(&data, static_cast<uint32_t>(codes.size()));
	PutU32(&data, next_id);

	for (std::map<SymbolId, Symbol>::const_iterator it = symbols.begin();
	     it != symbols.end(); ++it) {
		const Symbol& symbol = it->second;
		std::vector<uint8_t> record;
		PutU32(&record, symbol.id);
		PutU8(&record, static_cast<uint8_t>(symbol.state));
		if (!PutString(&record, symbol.code) ||
		    !PutString(&record, symbol.name) ||
		    !PutString(&record, symbol.security_type) ||
		    !PutString(&record, symbol.exchange) ||
		    !PutString(&record, symbol.board) ||
		    !PutString(&record, symbol.industry) ||
		    !PutString(&record, symbol.list_date) ||
		    !PutString(&record, symbol.delist_date) ||
		    !PutString(&record, symbol.trade_state)) {
			return Status::Error(ErrorCode::InvalidArgument,
			                     "symbol field is too long");
		}
		PutU64(&record, symbol.share_capital);
		PutU64(&record, symbol.tradable_share);
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

	for (std::map<std::string, SymbolId>::const_iterator it = codes.begin();
	     it != codes.end(); ++it) {
		if (!PutString(&data, it->first)) {
			return Status::Error(ErrorCode::InvalidArgument,
			                     "symbol code is too long");
		}
		PutU32(&data, it->second);
	}

	// The temporary name shares the target directory so rename is an atomic
	// replacement operation on the file system holding the Market.
	const std::string temporary = path_ + ".tmp-symbols";
	std::ofstream output(temporary.c_str(), std::ios::binary | std::ios::trunc);
	if (!output)
	{
		PELOG_LOG((PLV_ERROR, "symbols save: cannot create temp file %s\n", temporary.c_str()));
		return Status::Error(ErrorCode::IoError,
		                     "failed to create symbols temporary file");
	}
	output.write(reinterpret_cast<const char*>(&data[0]), data.size());
	output.close();
	if (!output || rename(temporary.c_str(), path_.c_str()) != 0)
	{
		unlink(temporary.c_str());
		PELOG_LOG((PLV_ERROR, "symbols save: cannot publish %s (errno=%d)\n", path_.c_str(), errno));
		return Status::Error(ErrorCode::IoError, "failed to publish symbols file");
	}
	return Status::Ok();
}

// Writes the full snapshot via atomic tmp+rename, then swaps in-memory state.
// Single-file rename is atomic so no manifest coordination is needed.
Status Symbols::persist(const std::map<SymbolId, Symbol>& symbols,
                        const std::map<std::string, SymbolId>& codes,
                        SymbolId next_id) {
	if (path_.empty())
		return Status::Error(ErrorCode::Conflict, "symbols persistence is not configured");

	PELOG_LOG((PLV_INFO, "symbols persist: %zu symbols, next_id=%u\n",
		symbols.size(), next_id));
	Status saved = save(symbols, codes, next_id);
	if (!saved.ok()) {
		return saved;
	}
	by_id_ = symbols;
	by_code_ = codes;
	next_id_ = next_id;
	return Status::Ok();
}

Status Symbols::load()
{
	std::ifstream input(path_.c_str(), std::ios::binary);
	if (!input)
	{
		if (errno == ENOENT)
		{
			return Status::Error(ErrorCode::NotFound, "symbols file was not found");
		}
		PELOG_LOG((PLV_ERROR, "symbols load: cannot open %s (errno=%d)\n", path_.c_str(), errno));
		return Status::Error(ErrorCode::IoError, "failed to open symbols file");
	}
	std::vector<uint8_t> data((std::istreambuf_iterator<char>(input)),
	                          std::istreambuf_iterator<char>());
	size_t offset = 0;
	if (data.size() < 20 || data[0] != 'Z' || data[1] != 'S' ||
	    data[2] != 'Y' || data[3] != 'M')
	{
		PELOG_LOG((PLV_ERROR, "symbols load: bad magic in %s\n", path_.c_str()));
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
		    !GetString(record, &record_offset, &symbol.exchange) ||
		    !GetString(record, &record_offset, &symbol.board) ||
		    !GetString(record, &record_offset, &symbol.industry) ||
		    !GetString(record, &record_offset, &symbol.list_date) ||
		    !GetString(record, &record_offset, &symbol.delist_date) ||
		    !GetString(record, &record_offset, &symbol.trade_state) ||
		    !GetU64(record, &record_offset, &symbol.share_capital) ||
		    !GetU64(record, &record_offset, &symbol.tradable_share) ||
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
	if (offset != data.size() || next_id == kInvalidSymbolId)
	{
		PELOG_LOG((PLV_ERROR, "symbols load: corrupt tail in %s (offset=%zu size=%zu next_id=%u)\n",
			path_.c_str(), offset, data.size(), next_id));
		return Status::Error(ErrorCode::CorruptData, "trailing symbols data");
	}

	by_id_.swap(loaded_by_id);
	by_code_.swap(loaded_by_code);
	next_id_ = next_id;
	return Status::Ok();
}

}  // namespace zstfs
