// history.cpp
//
// Implements the History coordinator and cross-layer compaction workflow.
// Individual Active, Staging, and Vault persistence remains in store.cpp.
//
#include "history.h"

#include "codec.h"

#include "libconfig.h"
#include "zstfs/market.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <dirent.h>
#include <fstream>
#include <iterator>
#include <limits>
#include <queue>
#include <set>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>

#include "serialization.h"
#include "zstfs/market.h"
#include <zstfs/pe_log.h>

namespace zstfs {

static std::string FrequencyPath(const std::string& market_path, Frequency frequency) {
	const std::string path = market_path + (frequency == Frequency::Daily ? "/daily" : "/hourly");
	if (mkdir(path.c_str(), 0755) != 0 && errno != EEXIST) {
		return std::string();
	}
	return path;
}

// Runtime IDs isolate shared Vault cache entries across independently opened
// markets. They are process-local namespaces for the immutable store cache.
static std::mutex g_runtime_market_id_mutex;
static uint64_t g_next_runtime_market_id = 1;

static uint64_t NextRuntimeMarketId() {
	std::lock_guard<std::mutex> lock(g_runtime_market_id_mutex);
	return g_next_runtime_market_id++;
}

History::History(Frequency frequency,
			 const Calendar& calendar,
			 const std::string& market_path,
			 const Actions& actions,
			 const Status& initial_status,
			 DataFields fields)
	: frequency_(frequency),
	  calendar_(calendar),
	  actions_(actions),
	  fields_(fields),
	  status_(initial_status),
	  frequency_path_(),
	  active_(),
	  staging_(),
	  vault_() {
	// Manifest validation precedes store construction. A failed open retains a
	// History shell so the existing accessor remains source compatible, but all
	// operations return the original CorruptData or I/O status.
	if (!status_.ok()) {
		return;
	}
	const std::string frequency_path = FrequencyPath(market_path, frequency);
	if (frequency_path.empty()) {
		status_ = Status::Error(ErrorCode::IoError, "cannot create frequency directory");
		return;
	}
	const uint64_t runtime_market_id = NextRuntimeMarketId();
	active_.reset(new ActiveStore(frequency, calendar, frequency_path));
	staging_.reset(new StagingStore(frequency, calendar, frequency_path, runtime_market_id));
	vault_.reset(new VaultStore(frequency, calendar, frequency_path, runtime_market_id));
	frequency_path_ = frequency_path;
}

Status History::status() const {
	return status_;
}

History::~History() {
}

Frequency History::frequency() const {
	return frequency_;
}

Status History::put(const Bar& bar) {
	if (!status_.ok()) {
		return status_;
	}
	if (bar.frequency != frequency_ || bar.symbol_id == kInvalidSymbolId ||
		!ValidState(bar.state)) {
		PELOG_ERROR_RETURN((PLV_WARNING,
			"history put: invalid bar header: symbol_id=%u frequency=%d state=%d\n",
			bar.symbol_id, static_cast<int>(bar.frequency), static_cast<int>(bar.state)),
			Status::Error(ErrorCode::InvalidArgument, "invalid history bar"));
	}
	// In Cv data mode, open/high/low are all set equal to close. This allows
	// callers to provide close-only inputs without needing to populate the
	// other price fields, and keeps stored bars self-consistent.
	BlockBar block_bar = {};
	block_bar.state = bar.state;
	block_bar.close = bar.close;
	block_bar.volume = bar.volume;
	if (fields_ == DataFields::CV) {
		block_bar.open = bar.close;
		block_bar.high = bar.close;
		block_bar.low = bar.close;
	} else {
		block_bar.open = bar.open;
		block_bar.high = bar.high;
		block_bar.low = bar.low;
	}
	if (!ValidBar(block_bar)) {
		PELOG_ERROR_RETURN((PLV_WARNING,
			"history put: invalid bar values: symbol_id=%u time=%s "
			"open=%f high=%f low=%f close=%f volume=%f\n",
			bar.symbol_id, bar.time.c_str(),
			block_bar.open, block_bar.high, block_bar.low, block_bar.close, block_bar.volume),
			Status::Error(ErrorCode::InvalidArgument, "invalid history bar"));
	}
	TimeId time_id = 0;
	TimeId block_id = 0;
	BlockOff block_offset = 0;
	Status status = ResolveTime(calendar_, frequency_, bar.time,
						&time_id, &block_id, &block_offset);
	if (!status.ok()) {
		return status;
	}
	if (active_->contains(bar.symbol_id, time_id) ||
		staging_->contains(bar.symbol_id, time_id)) {
		PELOG_ERROR_RETURN((PLV_WARNING,
			"history put: bar already present: symbol_id=%u time=%s\n",
			bar.symbol_id, bar.time.c_str()),
			Status::Error(ErrorCode::AlreadyPresent, "history bar is already present"));
	}
	status = active_->put(bar.symbol_id, time_id, block_id, block_offset, block_bar, false);
	if (!status.ok())
	{
		return status;
	}
	return active_->flush_if_needed();
}

Status History::put(const std::vector<Bar>& bars) {
	if (!status_.ok()) {
		return status_;
	}
	std::vector<ResolvedBar> resolved;
	std::set<std::pair<SymbolId, TimeId> > seen;
	resolved.reserve(bars.size());
	for (size_t i = 0; i < bars.size(); ++i) {
		const Bar& bar = bars[i];
		if (bar.frequency != frequency_ || bar.symbol_id == kInvalidSymbolId ||
			!ValidState(bar.state)) {
			PELOG_ERROR_RETURN((PLV_WARNING,
				"history batch put: invalid bar[%zu] header: symbol_id=%u frequency=%d state=%d\n",
				i, bar.symbol_id, static_cast<int>(bar.frequency), static_cast<int>(bar.state)),
				Status::Error(ErrorCode::InvalidArgument, "invalid history batch bar"));
		}
		// Apply data-type normalization before validation so Cv-mode inputs
		// (which may omit open/high/low) pass the consistency check.
		BlockBar block_bar = {};
		block_bar.state = bar.state;
		block_bar.close = bar.close;
		block_bar.volume = bar.volume;
		if (fields_ == DataFields::CV) {
			block_bar.open = bar.close;
			block_bar.high = bar.close;
			block_bar.low = bar.close;
		} else {
			block_bar.open = bar.open;
			block_bar.high = bar.high;
			block_bar.low = bar.low;
		}
		if (!ValidBar(block_bar)) {
			PELOG_ERROR_RETURN((PLV_WARNING,
				"history batch put: invalid bar[%zu] values: symbol_id=%u time=%s "
				"open=%f high=%f low=%f close=%f volume=%f\n",
				i, bar.symbol_id, bar.time.c_str(),
				block_bar.open, block_bar.high, block_bar.low, block_bar.close, block_bar.volume),
				Status::Error(ErrorCode::InvalidArgument, "invalid history batch bar"));
		}
		ResolvedBar item = {};
		item.bar = bar;
		Status status = ResolveTime(calendar_, frequency_, bar.time,
								&item.time_id, &item.block_id, &item.block_offset);
		if (!status.ok()) {
			return status;
		}
		if (!seen.insert(std::make_pair(bar.symbol_id, item.time_id)).second ||
			active_->contains(bar.symbol_id, item.time_id) ||
			staging_->contains(bar.symbol_id, item.time_id)) {
			PELOG_ERROR_RETURN((PLV_WARNING,
				"history batch put: duplicate bar at index %zu: symbol_id=%u time=%s\n",
				i, bar.symbol_id, bar.time.c_str()),
				Status::Error(ErrorCode::AlreadyPresent, "history batch contains an existing bar"));
		}
		resolved.push_back(item);
	}
	for (size_t i = 0; i < resolved.size(); ++i) {
		const ResolvedBar& item = resolved[i];
		BlockBar block_bar = {};
		block_bar.state = item.bar.state;
		block_bar.close = item.bar.close;
		block_bar.volume = item.bar.volume;
		if (fields_ == DataFields::CV) {
			block_bar.open = item.bar.close;
			block_bar.high = item.bar.close;
			block_bar.low = item.bar.close;
		} else {
			block_bar.open = item.bar.open;
			block_bar.high = item.bar.high;
			block_bar.low = item.bar.low;
		}
		Status status = active_->put(item.bar.symbol_id, item.time_id, item.block_id,
							 item.block_offset, block_bar, false);
		if (!status.ok()) {
			return status;
		}
	}
	return active_->flush_if_needed();
}

Status History::get(SymbolId symbol_id,
			    const std::string& time,
			    Bar* out) const {
	if (!status_.ok()) {
		return status_;
	}
	if (out == NULL || symbol_id == kInvalidSymbolId) {
		return Status::Error(ErrorCode::InvalidArgument, "history read output and symbol are required");
	}
	// An empty time means "latest bar". We first find the latest TimeId
	// using each store's in-memory index, then read the single bar at that time.
	// This avoids scanning the full history range and only pays for one point
	// lookup across the three layers, the same as a time-specified get.
	if (time.empty())
	{
		TimeId latest = 0;
		bool found = false;
		TimeId active_latest = 0;
		Status active_status = active_->latest_time(symbol_id, &active_latest);
		if (active_status.ok())
		{
			latest = active_latest;
			found = true;
		}
		else if (active_status.code() != ErrorCode::NotFound)
		{
			return active_status;
		}
		TimeId staged_latest = 0;
		Status staged_status = staging_->latest_time(symbol_id, &staged_latest);
		if (staged_status.ok())
		{
			if (!found || staged_latest > latest)
			{
				latest = staged_latest;
			}
			found = true;
		}
		else if (staged_status.code() != ErrorCode::NotFound)
		{
			return staged_status;
		}
		TimeId vault_latest = 0;
		Status vault_status = vault_->latest_time(symbol_id, &vault_latest);
		if (vault_status.ok())
		{
			if (!found || vault_latest > latest)
			{
				latest = vault_latest;
			}
			found = true;
		}
		else if (vault_status.code() != ErrorCode::NotFound)
		{
			return vault_status;
		}
		if (!found)
		{
			return Status::Error(ErrorCode::NotFound, "history bar was not found");
		}
		// Convert the resolved TimeId back to a canonical local-time string and
		// fall through to the normal single-bar read path.
		std::string resolved_time;
		Status status = LocalTime(calendar_, frequency_, latest, &resolved_time);
		if (!status.ok()) return status;
		return get(symbol_id, resolved_time, out);
	}
	TimeId time_id = 0;
	TimeId block_id = 0;
	BlockOff block_offset = 0;
	Status status = ResolveTime(calendar_, frequency_, time,
						&time_id, &block_id, &block_offset);
	if (!status.ok()) {
		return status;
	}
	BlockBar active_bar = {};
	BlockBar staged_bar = {};
	BlockBar vault_bar = {};
	const Status active_status = active_->get(symbol_id, block_id, block_offset, &active_bar);
	const Status staged_status = staging_->get(symbol_id, time_id, &staged_bar);
	const Status vault_status = vault_->get(symbol_id, time_id, &vault_bar);
	if ((!active_status.ok() && active_status.code() != ErrorCode::NotFound) ||
		(!staged_status.ok() && staged_status.code() != ErrorCode::NotFound) ||
		(!vault_status.ok() && vault_status.code() != ErrorCode::NotFound)) {
		// A corrupt target must never expose a partially reconstructed single bar.
		if (active_status.code() != ErrorCode::NotFound && !active_status.ok()) {
			return active_status;
		}
		if (staged_status.code() != ErrorCode::NotFound && !staged_status.ok()) {
			return staged_status;
		}
		return vault_status;
	}
	const bool has_active = active_status.ok();
	const bool has_staged = staged_status.ok();
	const bool has_vault = vault_status.ok();
	const bool layer_conflict =
		(has_active && has_staged && !SameBlockBar(active_bar, staged_bar)) ||
		(has_active && has_vault && !SameBlockBar(active_bar, vault_bar)) ||
		(has_staged && has_vault && !SameBlockBar(staged_bar, vault_bar));
	BlockBar block_bar = has_active ? active_bar : (has_staged ? staged_bar : vault_bar);
	if (!has_active && !has_staged && !has_vault) {
		return Status::Error(ErrorCode::NotFound, "history bar was not found");
	}
	if (layer_conflict) {
		return Status::Error(ErrorCode::CorruptData, "history layers contain conflicting bars");
	}
	std::string canonical_time;
	status = LocalTime(calendar_, frequency_, time_id, &canonical_time);
	if (!status.ok()) {
		return status;
	}
	Bar result = {symbol_id, frequency_, canonical_time, block_bar.state, block_bar.open,
		block_bar.high, block_bar.low, block_bar.close, block_bar.volume};
	*out = result;
	return Status::Ok();
}

Status History::get(SymbolId symbol_id,
			    const std::string& begin,
			    const std::string& end,
			    AdjustMode adjust_mode,
			    std::vector<Bar>* out) const {
	std::vector<SymbolId> symbol_ids(1, symbol_id);
	return get(symbol_ids, begin, end, adjust_mode, out);
}

Status History::get(const std::vector<SymbolId>& symbol_ids,
			    const std::string& begin,
			    const std::string& end,
			    AdjustMode adjust_mode,
			    std::vector<Bar>* out) const {
	if (!status_.ok()) {
		return status_;
	}
	if (out == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "history range output is required");
	}
	TimeId begin_time_id = 0;
	TimeId ignored_block_id = 0;
	BlockOff ignored_block_offset = 0;
	Status status = ResolveTime(calendar_, frequency_, begin,
						&begin_time_id, &ignored_block_id, &ignored_block_offset);
	if (!status.ok()) {
		return status;
	}
	TimeId end_time_id = 0;
	status = ResolveTime(calendar_, frequency_, end,
						&end_time_id, &ignored_block_id, &ignored_block_offset);
	if (!status.ok()) {
		return status;
	}
	if (begin_time_id > end_time_id) {
		return Status::Error(ErrorCode::InvalidArgument, "history range begins after it ends");
	}
	for (size_t i = 0; i < symbol_ids.size(); ++i) {
		if (symbol_ids[i] == kInvalidSymbolId) {
			return Status::Error(ErrorCode::InvalidArgument, "history range contains an invalid symbol");
		}
	}
	// Layer reads are independent. Retain successful work from every layer and
	// report corruption only after converting the merged, sorted partial result.
	std::vector<ActiveBar> vault_bars;
	std::vector<ActiveBar> staged_bars;
	std::vector<ActiveBar> active_bars;
	bool corrupt = false;
	status = vault_->range(symbol_ids, begin_time_id, end_time_id, &vault_bars);
	if (!status.ok()) {
		corrupt = true;
	}
	status = staging_->range(symbol_ids, begin_time_id, end_time_id, &staged_bars);
	if (!status.ok()) {
		corrupt = true;
	}
	status = active_->range(symbol_ids, begin_time_id, end_time_id, &active_bars);
	if (!status.ok()) {
		corrupt = true;
	}
	std::map<std::pair<TimeId, SymbolId>, ActiveBar> merged;
	for (size_t i = 0; i < vault_bars.size(); ++i) {
		merged[std::make_pair(vault_bars[i].time_id, vault_bars[i].symbol_id)] = vault_bars[i];
	}
	for (size_t i = 0; i < staged_bars.size(); ++i) {
		const std::pair<TimeId, SymbolId> key =
			std::make_pair(staged_bars[i].time_id, staged_bars[i].symbol_id);
		std::map<std::pair<TimeId, SymbolId>, ActiveBar>::iterator existing = merged.find(key);
		if (existing != merged.end() && !SameBlockBar(existing->second.bar, staged_bars[i].bar)) {
			corrupt = true;
		}
		merged[key] = staged_bars[i];
	}
	for (size_t i = 0; i < active_bars.size(); ++i) {
		const std::pair<TimeId, SymbolId> key =
			std::make_pair(active_bars[i].time_id, active_bars[i].symbol_id);
		std::map<std::pair<TimeId, SymbolId>, ActiveBar>::iterator existing = merged.find(key);
		if (existing != merged.end() && !SameBlockBar(existing->second.bar, active_bars[i].bar)) {
			corrupt = true;
		}
		merged[key] = active_bars[i];
	}
	out->clear();
	out->reserve(merged.size());
	for (std::map<std::pair<TimeId, SymbolId>, ActiveBar>::const_iterator it = merged.begin();
		 it != merged.end(); ++it) {
		const ActiveBar& active_bar = it->second;
		std::string local_time;
		status = LocalTime(calendar_, frequency_, active_bar.time_id, &local_time);
		if (!status.ok()) {
			return status;
		}
		Bar bar = {active_bar.symbol_id, frequency_, local_time,
			active_bar.bar.state, active_bar.bar.open,
			active_bar.bar.high, active_bar.bar.low,
			active_bar.bar.close, active_bar.bar.volume};
		status = actions_.adjust(active_bar.symbol_id, local_time, adjust_mode, &bar);
		if (!status.ok()) {
			return status;
		}
		out->push_back(bar);
	}
	return corrupt ? Status::Error(ErrorCode::CorruptData,
		"one or more history range blocks are corrupt") : Status::Ok();
}

Status History::flush()
{
	if (!status_.ok())
		return status_;
	return active_->flush();
}

// WriteSealMarker atomically creates a seal transaction marker. The marker
// stores the cutoff time_id so recovery knows which time range was being
// sealed. Recovery logic is not yet implemented; the marker currently serves
// as a crash-detection signal only.
//
// File format (little-endian):
//   magic  : 4 bytes  'Z' 'T' 'X' 'N'  (shared transaction marker magic)
//   type   : 1 byte   0x01 = seal
//   cutoff : 4 bytes  TimeId (uint32) of the seal cutoff
//
// The marker lives inside the daily/ or hourly/ directory, so frequency is
// implied by location; it does not need to be encoded in the file.
static Status WriteSealMarker(const std::string &frequency_path, TimeId cutoff_time_id)
{
	const std::string marker_path = frequency_path + "/seal.txn";
	const std::string temp_path = frequency_path + "/seal.txn.tmp";
	std::ofstream output(temp_path.c_str(), std::ios::binary | std::ios::trunc);
	if (!output)
		return Status::Error(ErrorCode::IoError, "cannot create seal marker");
	std::vector<uint8_t> bytes;
	bytes.push_back('Z');
	bytes.push_back('T');
	bytes.push_back('X');
	bytes.push_back('N');
	bytes.push_back(0x01);  // type = seal
	PutU32(&bytes, cutoff_time_id);
	output.write(reinterpret_cast<const char *>(&bytes[0]), bytes.size());
	output.close();
	if (!output || rename(temp_path.c_str(), marker_path.c_str()) != 0)
	{
		unlink(temp_path.c_str());
		return Status::Error(ErrorCode::IoError, "cannot publish seal marker");
	}
	return Status::Ok();
}

// RemoveSealMarker deletes the seal transaction marker after a successful
// seal. Absence of the marker is not an error — the caller may have already
// cleaned it up, or the operation may have been a no-op.
static Status RemoveSealMarker(const std::string &frequency_path)
{
	const std::string marker_path = frequency_path + "/seal.txn";
	if (unlink(marker_path.c_str()) != 0 && errno != ENOENT)
		return Status::Error(ErrorCode::IoError, "cannot remove seal marker");
	return Status::Ok();
}

// Seal moves completed blocks from ActiveStore into StagingStore. The operation
// is crash-recoverable through a transaction marker (seal.txn) with forward
// recovery semantics:
//
// Crash recovery strategy (not yet implemented, marker only):
//   If seal.txn exists on startup, an in-flight seal is detected. Recovery:
//   1. Read cutoff_time_id from the marker.
//   2. Check whether staging contains the sealed blocks (by inspecting its
//      index for keys before the cutoff).
//   3a. If staging has none of the sealed data:
//         - Staging append either didn't start or was truncated on replay.
//         - The data is still fully in active.data.
//         - Action: delete seal.txn; nothing more to do.
//   3b. If staging has the sealed data (staging index references them):
//         - Staging accept completed successfully.
//         - Active may or may not have been trimmed; if not, data is duplicated.
//         - Manifest may or may not be up to date.
//         - Action:
//           1. If active still has the data → call remove_before() to trim it.
//           2. Republish manifest (covers both size and index changes).
//           3. Delete seal.txn.
//         This completes the seal forward.
//
// Rationale for forward recovery (not rollback):
//   - Staging pages are append-only; rolling them back would require tracking
//     exact byte offsets and truncating segment files, which is fragile.
//   - Active truncation is already a full rewrite (tmp + rename), so redoing
//     it is straightforward and idempotent.
//   - Worst case after crash: data exists in BOTH stores (duplicate, not lost).
//     Forward recovery removes the duplicate; data integrity is never violated.
//
// Ordering guarantee:
//   marker created → staging accept (append pages + atomic index swap) →
//   active remove_before (atomic rewrite) → publish manifest → marker deleted.
//   At no point can data be lost (it's always in at least one store).
//   The marker is removed LAST so any crash during publish leaves a recovery
//   signal on disk even though both data stores are already consistent.
Status History::seal_before(const std::string &time)
{
	if (!status_.ok())
		return status_;
	TimeId time_id = 0;
	TimeId ignored_block_id = 0;
	BlockOff ignored_block_offset = 0;
	Status status = active_->flush();
	if (!status.ok())
		return status;
	status = ResolveTime(calendar_, frequency_, time,
						&time_id, &ignored_block_id, &ignored_block_offset);
	if (!status.ok())
		return status;

	// Collect what we're about to seal first; if there's nothing to do, skip
	// the marker entirely so no recovery state is created for a no-op.
	std::vector<StockTimeBlock> sealed;
	std::vector<ActiveBar> sealed_bars;
	status = active_->collect_before(time_id, &sealed, &sealed_bars);
	if (!status.ok())
		return status;
	if (sealed.empty())
		return Status::Ok();

	// Atomically create the seal transaction marker. After this point, a crash
	// leaves the marker on disk for post-crash recovery detection.
	status = WriteSealMarker(frequency_path_, time_id);
	if (!status.ok())
		return status;

	status = staging_->accept(sealed);
	if (!status.ok())
	{
		// Staging failed before any durable change took effect (accept either
		// fully commits via index swap or leaves no index-visible data).
		// Clean up the marker and report the error.
		RemoveSealMarker(frequency_path_);
		return status;
	}
	status = active_->remove_before(time_id);
	if (!status.ok())
	{
		// Active rewrite failed — staging already has the data, but we leave
		// the marker in place so forward recovery can finish the job later.
		return status;
	}
	// Seal is fully committed. Remove the transaction marker.
	return RemoveSealMarker(frequency_path_);
}

// =============================================================================
// Offline Vault Compaction
//
// Compaction rewrites logical block records, not their on-disk frames. The run
// files are private, short-lived transport between bounded sorting batches and
// the final merge; Vault and Staging output is always written by their native
// stores so their public persistent formats remain unchanged.
// =============================================================================

static const size_t kCompactionBatchBytes = 64U * 1024U * 1024U;

enum class CompactionDestination : uint8_t {
	Vault = 0,
	Staging = 1
};

enum class CompactionSource : uint8_t {
	Vault = 0,
	Staging = 1
};

// The unit of work for offline Vault compaction.
// The frame bytes are preserved verbatim without decode or re-encode
struct CompactionRecord {
	CompactionDestination destination;
	CompactionSource source;
	SymbolId symbol_id;
	TimeId time_block_id;
	uint64_t day_presence;
	BlockOff position_count;
	std::vector<uint8_t> frame_bytes;
};

static bool CompactionRecordOrder(const CompactionRecord& left,
							  const CompactionRecord& right) {
	if (left.destination != right.destination) {
		return static_cast<uint8_t>(left.destination) < static_cast<uint8_t>(right.destination);
	}
	if (left.symbol_id != right.symbol_id)
	{
		return left.symbol_id < right.symbol_id;
	}
	if (left.time_block_id != right.time_block_id)
	{
		return left.time_block_id < right.time_block_id;
	}
	return static_cast<uint8_t>(left.source) < static_cast<uint8_t>(right.source);
}

static Status SerializeCompactionRecord(const CompactionRecord& record,
										std::vector<uint8_t>* bytes) {
	if (bytes == NULL || record.symbol_id == kInvalidSymbolId ||
		record.frame_bytes.size() > std::numeric_limits<uint32_t>::max())
	{
		return Status::Error(ErrorCode::InvalidArgument, "invalid compaction record");
	}
	bytes->clear();
	PutU8(bytes, 1);
	PutU8(bytes, static_cast<uint8_t>(record.destination));
	PutU8(bytes, static_cast<uint8_t>(record.source));
	PutU8(bytes, 0);  // reserved
	PutU32(bytes, record.symbol_id);
	PutU32(bytes, record.time_block_id);
	PutU64(bytes, record.day_presence);
	PutU16(bytes, record.position_count);
	PutU32(bytes, static_cast<uint32_t>(record.frame_bytes.size()));
	bytes->insert(bytes->end(), record.frame_bytes.begin(), record.frame_bytes.end());
	return Status::Ok();
}

static Status ParseCompactionRecord(const std::vector<uint8_t> &bytes, CompactionRecord *record)
{
	if (record == NULL)
		return Status::Error(ErrorCode::InvalidArgument, "compaction record output is required");
	size_t offset = 0;
	uint8_t version = 0;
	uint8_t destination = 0;
	uint8_t source = 0;
	uint8_t reserved = 0;
	uint32_t frame_size = 0;
	if (!GetU8(bytes, &offset, &version) || !GetU8(bytes, &offset, &destination) ||
		!GetU8(bytes, &offset, &source) || !GetU8(bytes, &offset, &reserved) ||
		!GetU32(bytes, &offset, &record->symbol_id) ||
		!GetU32(bytes, &offset, &record->time_block_id) ||
		!GetU64(bytes, &offset, &record->day_presence) ||
		!GetU16(bytes, &offset, &record->position_count) ||
		!GetU32(bytes, &offset, &frame_size) || version != 1 || reserved != 0 ||
		destination > static_cast<uint8_t>(CompactionDestination::Staging) ||
		source > static_cast<uint8_t>(CompactionSource::Staging) ||
		record->symbol_id == kInvalidSymbolId || record->position_count == 0 ||
		offset + frame_size > bytes.size())
	{
		return Status::Error(ErrorCode::CorruptData, "invalid compaction run record");
	}
	record->destination = static_cast<CompactionDestination>(destination);
	record->source = static_cast<CompactionSource>(source);
	record->frame_bytes.assign(bytes.begin() + offset, bytes.begin() + offset + frame_size);
	offset += frame_size;
	return offset == bytes.size() ? Status::Ok() :
		Status::Error(ErrorCode::CorruptData, "trailing compaction run bytes");
}

static Status WriteCompactionRun(const std::string& path,
								 std::vector<CompactionRecord>* records,
								 uint64_t* written_bytes) {
	std::sort(records->begin(), records->end(), CompactionRecordOrder);
	std::ofstream output(path.c_str(), std::ios::binary | std::ios::trunc);
	if (!output) {
		return Status::Error(ErrorCode::IoError, "cannot create compaction run");
	}
	for (size_t i = 0; i < records->size(); ++i) {
		std::vector<uint8_t> bytes;
		Status status = SerializeCompactionRecord((*records)[i], &bytes);
		if (!status.ok() || bytes.size() > std::numeric_limits<uint32_t>::max()) {
			return status.ok() ? Status::Error(ErrorCode::InvalidArgument, "compaction record is too large") : status;
		}
		std::vector<uint8_t> length;
		PutU32(&length, static_cast<uint32_t>(bytes.size()));
		output.write(reinterpret_cast<const char*>(&length[0]), length.size());
		output.write(reinterpret_cast<const char*>(&bytes[0]), bytes.size());
		if (!output) {
			return Status::Error(ErrorCode::IoError, "cannot write compaction run");
		}
		*written_bytes += length.size() + bytes.size();
	}
	records->clear();
	return Status::Ok();
}

struct CompactionRunReader {
	std::ifstream input;
	CompactionRecord record;
	bool has_record;
};

static Status ReadCompactionRunRecord(CompactionRunReader* reader) {
	uint8_t length_bytes[4] = {};
	reader->input.read(reinterpret_cast<char*>(length_bytes), sizeof(length_bytes));
	if (reader->input.eof() && reader->input.gcount() == 0) {
		reader->has_record = false;
		return Status::Ok();
	}
	if (reader->input.gcount() != static_cast<std::streamsize>(sizeof(length_bytes))) {
		return Status::Error(ErrorCode::CorruptData, "truncated compaction run length");
	}
	const uint32_t length = static_cast<uint32_t>(length_bytes[0]) |
		(static_cast<uint32_t>(length_bytes[1]) << 8) |
		(static_cast<uint32_t>(length_bytes[2]) << 16) |
		(static_cast<uint32_t>(length_bytes[3]) << 24);
	if (length == 0 || length > kCompactionBatchBytes) {
		return Status::Error(ErrorCode::CorruptData, "invalid compaction run length");
	}
	std::vector<uint8_t> bytes(length);
	reader->input.read(reinterpret_cast<char*>(&bytes[0]), bytes.size());
	if (reader->input.gcount() != static_cast<std::streamsize>(bytes.size())) {
		return Status::Error(ErrorCode::CorruptData, "truncated compaction run record");
	}
	Status status = ParseCompactionRecord(bytes, &reader->record);
	if (!status.ok()) {
		return status;
	}
	reader->has_record = true;
	return Status::Ok();
}

static uint64_t FileBytes(const std::string& path) {
	struct stat metadata = {};
	return stat(path.c_str(), &metadata) == 0 && metadata.st_size > 0 ?
		static_cast<uint64_t>(metadata.st_size) : 0;
}

static bool IsStoreFile(const std::string& name, CompactionDestination destination) {
	if (destination == CompactionDestination::Vault) {
		return name == "vault-index" || name == "vault-index.tmp" ||
			name.compare(0, 6, "vault-") == 0;
	}
	return name == "staging-index" || name.compare(0, 14, "staging-pages-") == 0;
}

static Status StoreFiles(const std::string& directory,
					 CompactionDestination destination,
					 std::vector<std::string>* files) {
	files->clear();
	DIR* handle = opendir(directory.c_str());
	if (handle == NULL) {
		return errno == ENOENT ? Status::Ok() :
			Status::Error(ErrorCode::IoError, "cannot inspect store directory");
	}
	for (dirent* entry = readdir(handle); entry != NULL; entry = readdir(handle)) {
		const std::string name(entry->d_name);
		if (IsStoreFile(name, destination)) {
			files->push_back(name);
		}
	}
	closedir(handle);
	std::sort(files->begin(), files->end());
	return Status::Ok();
}

static void RemoveCompactionTree(const std::string& directory) {
	DIR* handle = opendir(directory.c_str());
	if (handle == NULL) {
		return;
	}
	for (dirent* entry = readdir(handle); entry != NULL; entry = readdir(handle)) {
		const std::string name(entry->d_name);
		if (name == "." || name == "..") {
			continue;
		}
		const std::string path = directory + "/" + name;
		struct stat metadata = {};
		if (stat(path.c_str(), &metadata) == 0 && S_ISDIR(metadata.st_mode)) {
			RemoveCompactionTree(path);
		} else {
			unlink(path.c_str());
		}
	}
	closedir(handle);
	rmdir(directory.c_str());
}

static Status PublishCompactedStore(const std::string& frequency_path,
									 const std::string& build_path,
									 const std::string& backup_path,
									 CompactionDestination destination) {
	if (mkdir(backup_path.c_str(), 0755) != 0) {
		return Status::Error(ErrorCode::IoError, "cannot create store backup directory");
	}
	std::vector<std::string> old_files;
	std::vector<std::string> new_files;
	Status status = StoreFiles(frequency_path, destination, &old_files);
	if (!status.ok()) {
		return status;
	}
	status = StoreFiles(build_path, destination, &new_files);
	if (!status.ok()) {
		return status;
	}
	std::vector<std::string> moved_old;
	for (size_t i = 0; i < old_files.size(); ++i) {
		if (rename((frequency_path + "/" + old_files[i]).c_str(),
			(backup_path + "/" + old_files[i]).c_str()) != 0) {
			for (size_t rollback = moved_old.size(); rollback > 0; --rollback) {
				const std::string& name = moved_old[rollback - 1];
				rename((backup_path + "/" + name).c_str(), (frequency_path + "/" + name).c_str());
			}
			return Status::Error(ErrorCode::IoError, "cannot preserve old store file");
		}
		moved_old.push_back(old_files[i]);
	}
	std::vector<std::string> moved_new;
	for (size_t i = 0; i < new_files.size(); ++i) {
		if (rename((build_path + "/" + new_files[i]).c_str(),
			(frequency_path + "/" + new_files[i]).c_str()) != 0) {
			for (size_t rollback = moved_new.size(); rollback > 0; --rollback) {
				const std::string& name = moved_new[rollback - 1];
				rename((frequency_path + "/" + name).c_str(), (build_path + "/" + name).c_str());
			}
			for (size_t rollback = moved_old.size(); rollback > 0; --rollback) {
				const std::string& name = moved_old[rollback - 1];
				rename((backup_path + "/" + name).c_str(), (frequency_path + "/" + name).c_str());
			}
			return Status::Error(ErrorCode::IoError, "cannot publish compacted store file");
		}
		moved_new.push_back(new_files[i]);
	}
	return Status::Ok();
}

// WriteCompactionMarker creates a transaction marker file atomically. Its
// presence after a crash indicates an in-flight compaction that needs
// recovery. The marker stores the compaction token so recovery can locate the
// backup directories and roll forward or back. Recovery logic is not yet
// implemented; the marker currently serves only as a crash-detection signal.
static Status WriteCompactionMarker(const std::string &frequency_path, uint64_t token)
{
	const std::string marker_path = frequency_path + "/compact.txn";
	const std::string temp_path = frequency_path + "/compact.txn.tmp";
	std::ofstream output(temp_path.c_str(), std::ios::binary | std::ios::trunc);
	if (!output)
		return Status::Error(ErrorCode::IoError, "cannot create compaction marker");
	std::vector<uint8_t> bytes;
	bytes.push_back('Z');
	bytes.push_back('T');
	bytes.push_back('X');
	bytes.push_back('N');
	PutU64(&bytes, token);
	output.write(reinterpret_cast<const char *>(&bytes[0]), bytes.size());
	output.close();
	if (!output || rename(temp_path.c_str(), marker_path.c_str()) != 0)
	{
		unlink(temp_path.c_str());
		return Status::Error(ErrorCode::IoError, "cannot publish compaction marker");
	}
	return Status::Ok();
}

// RemoveCompactionMarker deletes the transaction marker after successful
// compaction. If the marker is already absent the call succeeds so callers do
// not need to distinguish "never started" from "already cleaned up".
static Status RemoveCompactionMarker(const std::string &frequency_path)
{
	const std::string marker_path = frequency_path + "/compact.txn";
	if (unlink(marker_path.c_str()) != 0 && errno != ENOENT)
	{
		return Status::Error(ErrorCode::IoError, "cannot remove compaction marker");
	}
	return Status::Ok();
}

// A compaction publishes Vault and Staging as one logical operation. If the
// second publication fails, move the new Vault files back to the build tree
// and restore the preserved files before returning the error.
static Status RestoreCompactedStore(const std::string& frequency_path,
									const std::string& build_path,
									const std::string& backup_path,
									CompactionDestination destination) {
	std::vector<std::string> current_files;
	Status status = StoreFiles(frequency_path, destination, &current_files);
	if (!status.ok()) {
		return status;
	}
	for (size_t i = 0; i < current_files.size(); ++i) {
		if (rename((frequency_path + "/" + current_files[i]).c_str(),
			(build_path + "/" + current_files[i]).c_str()) != 0) {
			return Status::Error(ErrorCode::IoError, "cannot stage compacted store rollback");
		}
	}
	std::vector<std::string> backup_files;
	status = StoreFiles(backup_path, destination, &backup_files);
	if (!status.ok()) {
		return status;
	}
	for (size_t i = 0; i < backup_files.size(); ++i) {
		if (rename((backup_path + "/" + backup_files[i]).c_str(),
			(frequency_path + "/" + backup_files[i]).c_str()) != 0) {
			return Status::Error(ErrorCode::IoError, "cannot restore compacted store backup");
		}
	}
	return Status::Ok();
}

Status CompactVault(const std::string& config_path,
					const std::string& market_name,
					Frequency frequency,
					const std::string& cutoff_time,
					VaultCompactionStats* stats) {
	if (stats == NULL || config_path.empty() || market_name.empty()) {
		return Status::Error(ErrorCode::InvalidArgument, "config path, market name, and compaction stats are required");
	}
	*stats = {};
	const std::chrono::steady_clock::time_point started = std::chrono::steady_clock::now();

	// Read root_path and markets list from the unified config file.
	struct ConfigHandle : public config_t {
		ConfigHandle() { config_init(this); }
		~ConfigHandle() { config_destroy(this); }
	};
	ConfigHandle cfg;
	if (config_read_file(&cfg, config_path.c_str()) != CONFIG_TRUE) {
		return Status::Error(ErrorCode::IoError, "cannot read config file");
	}
	const char* root_path_cstr = NULL;
	if (!config_lookup_string(&cfg, "root_path", &root_path_cstr) ||
		root_path_cstr == NULL || *root_path_cstr == '\0') {
		return Status::Error(ErrorCode::InvalidArgument, "config: root_path is required");
	}
	const std::string root_path(root_path_cstr);

	std::vector<MarketDef> market_defs;
	Status load_status = LoadMarketsConfig(config_path, &market_defs);
	if (!load_status.ok()) return load_status;

	// Compaction changes two immutable stores as one offline operation. Opening
	// the configured root verifies the published input generation; sync below
	// exposes replacement files only after both store publications complete.
	Markets markets(root_path, market_defs);
	if (!markets.status().ok()) {
		return markets.status();
	}
	Market* market = NULL;
	Status status = markets.get(market_name, &market);
	if (!status.ok()) {
		return status;
	}
	Calendar calendar(market->schedule());
	TimeId cutoff = 0;
	TimeId cutoff_block = 0;
	BlockOff unused_offset = 0;
	status = ResolveTime(calendar, frequency, cutoff_time, &cutoff, &cutoff_block, &unused_offset);
	if (!status.ok()) {
		return status;
	}
	const std::string frequency_path = FrequencyPath(market->path(), frequency);
	if (frequency_path.empty()) {
		return Status::Error(ErrorCode::IoError, "cannot create frequency directory");
	}
	StagingStore old_staging(frequency, calendar, frequency_path, NextRuntimeMarketId());
	VaultStore old_vault(frequency, calendar, frequency_path, NextRuntimeMarketId());
	std::vector<BlockKey> staging_keys;
	std::vector<uint64_t> staging_day_presence;
	std::vector<BlockOff> staging_position_counts;
	std::vector<std::vector<uint8_t> > staging_frames;
	std::vector<BlockKey> vault_keys;
	std::vector<uint64_t> vault_day_presence;
	std::vector<BlockOff> vault_position_counts;
	std::vector<std::vector<uint8_t> > vault_frames;
	status = old_staging.snapshot(&staging_keys, &staging_day_presence,
		&staging_position_counts, &staging_frames);
	if (!status.ok()) {
		return status;
	}
	status = old_vault.snapshot(&vault_keys, &vault_day_presence,
		&vault_position_counts, &vault_frames);
	if (!status.ok()) {
		return status;
	}
	stats->input_blocks = staging_keys.size() + vault_keys.size();
	if (staging_frames.size() != staging_keys.size() || vault_frames.size() != vault_keys.size()) {
		return Status::Error(ErrorCode::CorruptData, "compaction frames do not match block count");
	}
	std::map<std::pair<SymbolId, TimeId>, std::vector<uint8_t> > staging_frames_by_block;
	std::map<std::pair<SymbolId, TimeId>, std::vector<uint8_t> > vault_frames_by_block;
	std::map<std::pair<SymbolId, TimeId>, uint64_t> staging_day_presence_by_block;
	std::map<std::pair<SymbolId, TimeId>, uint64_t> vault_day_presence_by_block;
	std::map<std::pair<SymbolId, TimeId>, BlockOff> staging_position_count_by_block;
	std::map<std::pair<SymbolId, TimeId>, BlockOff> vault_position_count_by_block;
	for (size_t i = 0; i < staging_keys.size(); ++i) {
		const std::pair<SymbolId, TimeId> key(staging_keys[i].symbol_id, staging_keys[i].time_block_id);
		staging_frames_by_block[key] = staging_frames[i];
		staging_day_presence_by_block[key] = staging_day_presence[i];
		staging_position_count_by_block[key] = staging_position_counts[i];
	}
	for (size_t i = 0; i < vault_keys.size(); ++i) {
		const std::pair<SymbolId, TimeId> key(vault_keys[i].symbol_id, vault_keys[i].time_block_id);
		vault_frames_by_block[key] = vault_frames[i];
		vault_day_presence_by_block[key] = vault_day_presence[i];
		vault_position_count_by_block[key] = vault_position_counts[i];
	}
	const uint64_t token = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
		std::chrono::system_clock::now().time_since_epoch()).count());
	const std::string build_root = frequency_path + "/makevault.build." + std::to_string(token);
	if (mkdir(build_root.c_str(), 0755) != 0) {
		return Status::Error(ErrorCode::IoError, "cannot create compaction build directory");
	}
	const std::string vault_build = build_root + "/vault";
	const std::string staging_build = build_root + "/staging";
	if (mkdir(vault_build.c_str(), 0755) != 0 || mkdir(staging_build.c_str(), 0755) != 0) {
		RemoveCompactionTree(build_root);
		return Status::Error(ErrorCode::IoError, "cannot create compacted store directories");
	}
	std::vector<CompactionRecord> batch;
	std::vector<std::string> runs;
	size_t batch_bytes = 0;
	uint32_t run_id = 0;
	const auto append_record = [&](const CompactionRecord& record) -> Status {
		std::vector<uint8_t> bytes;
		Status append_status = SerializeCompactionRecord(record, &bytes);
		if (!append_status.ok()) {
			return append_status;
		}
		const size_t record_bytes = bytes.size() + sizeof(uint32_t);
		if (record_bytes > kCompactionBatchBytes) {
			return Status::Error(ErrorCode::InvalidArgument, "compaction record exceeds 64 MiB batch limit");
		}
		if (!batch.empty() && batch_bytes + record_bytes > kCompactionBatchBytes) {
			const std::string run = build_root + "/run-" + std::to_string(run_id++) + ".bin";
			Status flush_status = WriteCompactionRun(run, &batch, &stats->temporary_bytes);
			if (!flush_status.ok()) {
				return flush_status;
			}
			runs.push_back(run);
			batch_bytes = 0;
		}
		batch.push_back(record);
		batch_bytes += record_bytes;
		return Status::Ok();
	};
	for (size_t i = 0; i < vault_keys.size(); ++i) {
		const std::pair<SymbolId, TimeId> key(vault_keys[i].symbol_id, vault_keys[i].time_block_id);
		std::map<std::pair<SymbolId, TimeId>, std::vector<uint8_t> >::const_iterator frame =
			vault_frames_by_block.find(key);
		if (frame == vault_frames_by_block.end())
		{
			RemoveCompactionTree(build_root);
			return Status::Error(ErrorCode::CorruptData, "vault block missing its frame");
		}
		CompactionRecord record = {};
		record.destination = CompactionDestination::Vault;
		record.source = CompactionSource::Vault;
		record.symbol_id = vault_keys[i].symbol_id;
		record.time_block_id = vault_keys[i].time_block_id;
		record.day_presence = vault_day_presence[i];
		record.position_count = vault_position_counts[i];
		record.frame_bytes = frame->second;
		status = append_record(record);
		if (!status.ok())
		{
			RemoveCompactionTree(build_root);
			return status;
		}
	}
	// The cutoff block itself stays in Staging so the compactor never splits
	// one frame between store ownership layers.
	for (size_t i = 0; i < staging_keys.size(); ++i) {
		const std::pair<SymbolId, TimeId> key(staging_keys[i].symbol_id, staging_keys[i].time_block_id);
		std::map<std::pair<SymbolId, TimeId>, std::vector<uint8_t> >::const_iterator frame =
			staging_frames_by_block.find(key);
		if (frame == staging_frames_by_block.end())
		{
			RemoveCompactionTree(build_root);
			return Status::Error(ErrorCode::CorruptData, "staging block missing its frame");
		}
		CompactionRecord record = {};
		record.source = CompactionSource::Staging;
		record.symbol_id = staging_keys[i].symbol_id;
		record.time_block_id = staging_keys[i].time_block_id;
		record.day_presence = staging_day_presence[i];
		record.position_count = staging_position_counts[i];
		record.frame_bytes = frame->second;
		record.destination = staging_keys[i].time_block_id < cutoff_block ?
			CompactionDestination::Vault : CompactionDestination::Staging;
		status = append_record(record);
		if (!status.ok())
		{
			RemoveCompactionTree(build_root);
			return status;
		}
	}
	if (!batch.empty()) {
		const std::string run = build_root + "/run-" + std::to_string(run_id++) + ".bin";
		status = WriteCompactionRun(run, &batch, &stats->temporary_bytes);
		if (!status.ok()) {
			RemoveCompactionTree(build_root);
			return status;
		}
		runs.push_back(run);
	}
	VaultStore new_vault(frequency, calendar, vault_build, NextRuntimeMarketId());
	StagingStore new_staging(frequency, calendar, staging_build, NextRuntimeMarketId());
	std::vector<CompactionRunReader> readers(runs.size());
	struct EarlierRun {
		const std::vector<CompactionRunReader>* readers;
		bool operator()(size_t left, size_t right) const {
			return CompactionRecordOrder((*readers)[right].record, (*readers)[left].record);
		}
	};
	EarlierRun earlier = {&readers};
	std::priority_queue<size_t, std::vector<size_t>, EarlierRun> heap(earlier);
	for (size_t i = 0; i < runs.size(); ++i) {
		readers[i].input.open(runs[i].c_str(), std::ios::binary);
		if (!readers[i].input || !(status = ReadCompactionRunRecord(&readers[i])).ok()) {
			RemoveCompactionTree(build_root);
			return status.ok() ? Status::Error(ErrorCode::IoError, "cannot read compaction run") : status;
		}
		if (readers[i].has_record) {
			heap.push(i);
		}
	}
	std::vector<StockTimeBlock> vault_batch;
	std::vector<std::vector<uint8_t> > vault_batch_frames;
	std::vector<StockTimeBlock> staging_batch;
	std::vector<std::vector<uint8_t> > staging_batch_frames;
	const auto flush_output = [&](CompactionDestination destination) -> Status {
		if (destination == CompactionDestination::Vault) {
			if (vault_batch.empty()) {
				return Status::Ok();
			}
			Status flush_status = new_vault.ingest(vault_batch, &vault_batch_frames);
			vault_batch.clear();
			vault_batch_frames.clear();
			return flush_status;
		}
		if (staging_batch.empty()) {
			return Status::Ok();
		}
		Status flush_status = new_staging.accept(staging_batch, &staging_batch_frames);
		staging_batch.clear();
		staging_batch_frames.clear();
		return flush_status;
	};
	CompactionDestination last_destination = CompactionDestination::Vault;
	bool have_destination = false;
	while (!heap.empty()) {
		const CompactionRecord first = readers[heap.top()].record;
		const CompactionDestination destination = first.destination;
		const SymbolId symbol_id = first.symbol_id;
		const TimeId block_id = first.time_block_id;
		bool have_vault = false;
		bool have_staging = false;
		CompactionRecord chosen;
		while (!heap.empty()) {
			const size_t index = heap.top();
			const CompactionRecord& candidate = readers[index].record;
			if (candidate.destination != destination || candidate.symbol_id != symbol_id ||
				candidate.time_block_id != block_id) {
				break;
			}
			heap.pop();
			if (candidate.source == CompactionSource::Vault) {
				if (have_vault) {
					RemoveCompactionTree(build_root);
					return Status::Error(ErrorCode::Conflict, "conflicting duplicate Vault block");
				}
				have_vault = true;
				chosen = candidate;
			} else {
				if (have_staging) {
					RemoveCompactionTree(build_root);
					return Status::Error(ErrorCode::Conflict, "conflicting duplicate Staging block");
				}
				have_staging = true;
				chosen = candidate;
			}
			status = ReadCompactionRunRecord(&readers[index]);
			if (!status.ok()) {
				RemoveCompactionTree(build_root);
				return status;
			}
			if (readers[index].has_record) {
				heap.push(index);
			}
		}
		if (have_destination && destination != last_destination) {
			status = flush_output(last_destination);
			if (!status.ok()) {
				RemoveCompactionTree(build_root);
				return status;
			}
		}
		have_destination = true;
		last_destination = destination;
		StockTimeBlock batch_block = {};
		batch_block.key.symbol_id = chosen.symbol_id;
		batch_block.key.time_block_id = chosen.time_block_id;
		batch_block.day_presence = chosen.day_presence;
		batch_block.positions.assign(chosen.position_count, MissingBlockBar());
		if (destination == CompactionDestination::Vault)
		{
			vault_batch.push_back(batch_block);
			vault_batch_frames.push_back(chosen.frame_bytes);
		}
		else
		{
			staging_batch.push_back(batch_block);
			staging_batch_frames.push_back(chosen.frame_bytes);
		}
		++stats->output_blocks;
	}
	if (have_destination && !(status = flush_output(last_destination)).ok()) {
		RemoveCompactionTree(build_root);
		return status;
	}
	std::vector<std::string> files;
	status = StoreFiles(frequency_path, CompactionDestination::Vault, &files);
	if (!status.ok()) {
		RemoveCompactionTree(build_root);
		return status;
	}
	for (size_t i = 0; i < files.size(); ++i) {
		stats->io_bytes += FileBytes(frequency_path + "/" + files[i]);
	}
	status = StoreFiles(frequency_path, CompactionDestination::Staging, &files);
	if (!status.ok()) {
		RemoveCompactionTree(build_root);
		return status;
	}
	for (size_t i = 0; i < files.size(); ++i) {
		stats->io_bytes += FileBytes(frequency_path + "/" + files[i]);
	}
	stats->io_bytes += stats->temporary_bytes * 2;
	status = StoreFiles(vault_build, CompactionDestination::Vault, &files);
	if (!status.ok()) {
		RemoveCompactionTree(build_root);
		return status;
	}
	for (size_t i = 0; i < files.size(); ++i) {
		stats->io_bytes += FileBytes(vault_build + "/" + files[i]);
	}
	status = StoreFiles(staging_build, CompactionDestination::Staging, &files);
	if (!status.ok()) {
		RemoveCompactionTree(build_root);
		return status;
	}
	for (size_t i = 0; i < files.size(); ++i) {
		stats->io_bytes += FileBytes(staging_build + "/" + files[i]);
	}
	status = WriteCompactionMarker(frequency_path, token);
	if (!status.ok())
	{
		RemoveCompactionTree(build_root);
		return status;
	}
	status = PublishCompactedStore(frequency_path, vault_build,
		frequency_path + "/vault." + std::to_string(token), CompactionDestination::Vault);
	const bool vault_published = status.ok();
	if (vault_published)
	{
		status = PublishCompactedStore(frequency_path, staging_build,
			frequency_path + "/staging." + std::to_string(token), CompactionDestination::Staging);
		if (!status.ok())
		{
			const Status rollback_status = RestoreCompactedStore(frequency_path, vault_build,
				frequency_path + "/vault." + std::to_string(token), CompactionDestination::Vault);
			if (!rollback_status.ok())
				status = Status::Error(ErrorCode::IoError, "compaction publication and rollback both failed");
		}
	}
	RemoveCompactionTree(build_root);
	if (!status.ok())
		return status;
	status = RemoveCompactionMarker(frequency_path);
	if (!status.ok())
		return status;
	stats->elapsed_milliseconds = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
		std::chrono::steady_clock::now() - started).count());
	return Status::Ok();
}

}  // namespace zstfs
