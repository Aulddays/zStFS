// store.cpp
//
// Implements the mutable ActiveStore and immutable StagingStore/VaultStore
// layers. These stores own their persistence formats; History coordinates them.
//
#include "store.h"

#include "codec.h"

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

// =============================================================================
// Active-Store Validation
//
// Validates the complete bar records accepted by History before they enter the
// mutable active store or its recovery log.
// =============================================================================


bool ValidState(BarState state) {
	return state >= BarState::Normal && state <= BarState::Missing;
}

bool ValidBar(const BlockBar& bar) {
	return bar.state != BarState::Normal ||
		(std::isfinite(bar.open) && bar.open > 0.0 &&
		 std::isfinite(bar.high) && bar.high >= 0.0 &&
		 std::isfinite(bar.low) && bar.low >= 0.0 &&
		 std::isfinite(bar.close) && bar.close > 0.0 &&
		 std::isfinite(bar.volume) && bar.volume >= 0.0 &&
		 bar.high >= std::max(bar.open, bar.close) &&
		 bar.low <= std::min(bar.open, bar.close));
}


// =============================================================================
// Active Store, Recovery Log, and Public History API
//
// Active owns the only mutable copy of a bar. Data is grouped by time block so
// a completed block can be handed to Staging without reshaping it. The append
// log records individual accepted positions because random writes are the
// Active workload; M4 keeps sealed records in the log until persistent Staging exists.
// =============================================================================

BlockBar MissingBlockBar() {
	BlockBar bar = {};
	bar.state = BarState::Missing;
	return bar;
}

bool SerializeActiveRecord(const ActiveRecord& record,
				  std::vector<uint8_t>* bytes) {
	if (bytes == NULL || !ValidState(record.bar.state) ||
		record.symbol_id == kInvalidSymbolId || !ValidBar(record.bar)) {
		return false;
	}
	bytes->clear();
	PutU32(bytes, kActiveRecordMagic);
	PutU8(bytes, kActiveRecordVersion);
	PutU8(bytes, static_cast<uint8_t>(record.frequency));
	PutU8(bytes, static_cast<uint8_t>(record.bar.state));
	PutU8(bytes, 0);
	PutU32(bytes, record.symbol_id);
	PutU32(bytes, record.time_id);
	PutFloat(bytes, record.bar.open);
	PutFloat(bytes, record.bar.high);
	PutFloat(bytes, record.bar.low);
	PutFloat(bytes, record.bar.close);
	PutFloat(bytes, record.bar.volume);
	return bytes->size() == kActiveRecordBytes;
}

Status ParseActiveRecord(const std::vector<uint8_t>& bytes,
				size_t offset,
				ActiveRecord* record) {
	if (record == NULL || offset > bytes.size() ||
		bytes.size() - offset < kActiveRecordBytes) {
		return Status::Error(ErrorCode::CorruptData, "truncated active record");
	}
	std::vector<uint8_t> data(bytes.begin() + offset,
					  bytes.begin() + offset + kActiveRecordBytes);
	size_t cursor = 0;
	uint32_t magic = 0;
	uint8_t version = 0;
	uint8_t frequency = 0;
	uint8_t state = 0;
	uint8_t reserved = 0;
	if (!GetU32(data, &cursor, &magic) || !GetU8(data, &cursor, &version) ||
		!GetU8(data, &cursor, &frequency) || !GetU8(data, &cursor, &state) ||
		!GetU8(data, &cursor, &reserved) ||
		!GetU32(data, &cursor, &record->symbol_id) ||
		!GetU32(data, &cursor, &record->time_id) ||
		!GetFloat(data, &cursor, &record->bar.open) ||
		!GetFloat(data, &cursor, &record->bar.high) ||
		!GetFloat(data, &cursor, &record->bar.low) ||
		!GetFloat(data, &cursor, &record->bar.close) ||
		!GetFloat(data, &cursor, &record->bar.volume) ||
		cursor != data.size() || magic != kActiveRecordMagic ||
		version != kActiveRecordVersion || reserved != 0 || frequency > 1 ||
		record->symbol_id == kInvalidSymbolId) {
		return Status::Error(ErrorCode::CorruptData, "invalid active record header");
	}
	record->frequency = static_cast<Frequency>(frequency);
	record->bar.state = static_cast<BarState>(state);
	if (!ValidState(record->bar.state) || !ValidBar(record->bar)) {
		return Status::Error(ErrorCode::CorruptData, "invalid active record values");
	}
	return Status::Ok();
}

Status ResolveTime(const Calendar& calendar,
			  Frequency frequency,
			  const std::string& local_time,
			  TimeId* time_id,
			  TimeId* block_id,
			  BlockOff* block_offset) {
	if (time_id == NULL || block_id == NULL || block_offset == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "time outputs are required");
	}
	TimeId day_time_id = 0;
	Status status = Status::Ok();
	if (frequency == Frequency::Daily) {
		status = calendar.time_id(local_time, &day_time_id);
		if (!status.ok()) {
			return status;
		}
		*time_id = day_time_id;
	} else {
		HourSlot slot = 0;
		status = calendar.time_id(local_time.substr(0, 8), &day_time_id);
		if (!status.ok()) {
			return status;
		}
		status = calendar.hour_slot(local_time, &slot);
		if (!status.ok()) {
			return status;
		}
		*time_id = hourly_bar_id(time_day(day_time_id), slot);
	}
	return calendar.block_offset(frequency, local_time, block_id, block_offset);
}

Status LocalTime(const Calendar& calendar,
			    Frequency frequency,
			    TimeId time_id,
			    std::string* out) {
	if (out == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "local time output is required");
	}
	const TimeId day_time_id = daily_bar_id(time_day(time_id));
	if (frequency == Frequency::Daily) {
		if (time_slot(time_id) != 0) {
			return Status::Error(ErrorCode::CorruptData, "daily active record has an hourly slot");
		}
		return calendar.date(day_time_id, out);
	}
	return calendar.local_time(day_time_id, time_slot(time_id), out);
}

bool ActiveBarOrder(const ActiveBar& left, const ActiveBar& right) {
	return left.time_id != right.time_id ? left.time_id < right.time_id :
		left.symbol_id < right.symbol_id;
}

ActiveStore::ActiveStore(Frequency frequency,
				 const Calendar& calendar,
				 const std::string& market_path,
				 size_t flush_bytes,
				 uint64_t flush_interval_milliseconds)
	: frequency_(frequency),
	  calendar_(calendar),
	  path_(market_path + "/active.data"),
	  replay_status_(Status::Ok()),
	  flush_bytes_(flush_bytes == 0 ? 1 : flush_bytes),
	  flush_interval_milliseconds_(flush_interval_milliseconds == 0 ? 1 :
		flush_interval_milliseconds),
	  dirty_bytes_(0),
	  stop_flush_timer_(false) {
	std::ifstream input(path_.c_str(), std::ios::binary);
	if (!input) {
		flush_timer_ = std::thread(&ActiveStore::run_flush_timer, this);
		return;
	}
	std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(input)),
					   std::istreambuf_iterator<char>());
	const size_t complete_bytes = bytes.size() - bytes.size() % kActiveRecordBytes;
	if (complete_bytes != bytes.size()) {
		// A partial append has no record semantics. Remove it before a future
		// flush so the next complete record cannot be appended after bad bytes.
		input.close();
		std::ofstream repaired(path_.c_str(), std::ios::binary | std::ios::trunc);
		if (!repaired) {
			replay_status_ = Status::Error(ErrorCode::IoError,
				"cannot repair truncated active log");
			return;
		}
		if (complete_bytes > 0) {
			repaired.write(reinterpret_cast<const char*>(&bytes[0]), complete_bytes);
		}
		repaired.flush();
		if (!repaired) {
			replay_status_ = Status::Error(ErrorCode::IoError,
				"cannot finish active log repair");
			return;
		}
	}
	for (size_t offset = 0; offset < complete_bytes; offset += kActiveRecordBytes) {
		ActiveRecord record;
		replay_status_ = ParseActiveRecord(bytes, offset, &record);
		if (!replay_status_.ok()) {
			return;
		}
		if (record.frequency != frequency_) {
			continue;
		}
		std::string local_time;
		replay_status_ = LocalTime(calendar_, frequency_, record.time_id, &local_time);
		if (!replay_status_.ok()) {
			return;
		}
		TimeId time_id = 0;
		TimeId block_id = 0;
		BlockOff block_offset = 0;
		replay_status_ = ResolveTime(calendar_, frequency_, local_time,
							 &time_id, &block_id, &block_offset);
		if (!replay_status_.ok() || time_id != record.time_id) {
			if (replay_status_.ok()) {
				replay_status_ = Status::Error(ErrorCode::CorruptData,
					"active record does not map to its stored time");
			}
			return;
		}
		replay_status_ = put(record.symbol_id, time_id, block_id, block_offset,
							 record.bar, true);
		if (!replay_status_.ok()) {
			return;
		}
	}
	flush_timer_ = std::thread(&ActiveStore::run_flush_timer, this);
}

ActiveStore::~ActiveStore() {
	{
		// Timer shutdown must be synchronized with the timer thread.
		std::lock_guard<std::mutex> lock(mutex_);
		stop_flush_timer_ = true;
	}
	flush_condition_.notify_one();
	if (flush_timer_.joinable()) {
		flush_timer_.join();
	}
	flush();
}

Status ActiveStore::put(SymbolId symbol_id,
				TimeId time_id,
				TimeId block_id,
				BlockOff block_offset,
				const BlockBar& bar,
				bool replay) {
	// This write must be synchronized with reads, sealing, and timer flushes.
	std::lock_guard<std::mutex> lock(mutex_);
	if (!replay_status_.ok()) {
		return replay_status_;
	}
	if (symbol_id == kInvalidSymbolId || !ValidState(bar.state) || !ValidBar(bar)) {
		return Status::Error(ErrorCode::InvalidArgument, "invalid active bar");
	}
	BlockOff block_length = 0;
	Status status = calendar_.block_length(frequency_, block_id, &block_length);
	if (!status.ok()) {
		return status;
	}
	if (block_offset >= block_length) {
		return Status::Error(ErrorCode::InvalidArgument, "active bar offset is outside its block");
	}
	std::map<TimeId, ActiveTimeBlock>::iterator block_it = blocks_.find(block_id);
	if (block_it == blocks_.end()) {
		ActiveTimeBlock block = {};
		block.position_count = block_length;
		block_it = blocks_.insert(std::make_pair(block_id, block)).first;
	} else if (block_it->second.position_count != block_length) {
		return Status::Error(ErrorCode::CorruptData, "active block length changed during use");
	}
	std::map<SymbolId, ActiveStockBlock>::iterator stock_it =
		block_it->second.stocks.find(symbol_id);
	if (stock_it == block_it->second.stocks.end()) {
		ActiveStockBlock stock;
		stock.positions.assign(block_length, MissingBlockBar());
		stock.time_ids.assign(block_length, 0);
		stock.present.assign(block_length, false);
		stock.dirty.assign(block_length, false);
		stock_it = block_it->second.stocks.insert(std::make_pair(symbol_id, stock)).first;
	}
	ActiveStockBlock& stock = stock_it->second;
	if (stock.present[block_offset] && !replay) {
		return Status::Error(ErrorCode::AlreadyPresent, "active bar is already present");
	}
	stock.positions[block_offset] = bar;
	stock.time_ids[block_offset] = time_id;
	stock.present[block_offset] = true;
	stock.dirty[block_offset] = !replay;
	if (!replay) {
		dirty_bytes_ += kActiveRecordBytes;
	}
	return Status::Ok();
}

bool ActiveStore::contains(SymbolId symbol_id, TimeId time_id) const {
	// This duplicate check must be synchronized with writes and timer flushes.
	std::lock_guard<std::mutex> lock(mutex_);
	for (std::map<TimeId, ActiveTimeBlock>::const_iterator block = blocks_.begin();
		 block != blocks_.end(); ++block) {
		std::map<SymbolId, ActiveStockBlock>::const_iterator stock =
			block->second.stocks.find(symbol_id);
		if (stock == block->second.stocks.end()) {
			continue;
		}
		for (size_t i = 0; i < stock->second.present.size(); ++i) {
			if (stock->second.present[i] && stock->second.time_ids[i] == time_id) {
				return true;
			}
		}
	}
	return false;
}

Status ActiveStore::get(SymbolId symbol_id,
				TimeId block_id,
				BlockOff block_offset,
				BlockBar* out) const {
	// This read must be synchronized with writes, sealing, and timer flushes.
	std::lock_guard<std::mutex> lock(mutex_);
	if (out == NULL || symbol_id == kInvalidSymbolId) {
		return Status::Error(ErrorCode::InvalidArgument, "active read output and symbol are required");
	}
	if (!replay_status_.ok()) {
		return replay_status_;
	}
	std::map<TimeId, ActiveTimeBlock>::const_iterator block = blocks_.find(block_id);
	if (block == blocks_.end() || block_offset >= block->second.position_count) {
		return Status::Error(ErrorCode::NotFound, "active bar was not found");
	}
	std::map<SymbolId, ActiveStockBlock>::const_iterator stock =
		block->second.stocks.find(symbol_id);
	if (stock == block->second.stocks.end() || !stock->second.present[block_offset]) {
		return Status::Error(ErrorCode::NotFound, "active bar was not found");
	}
	*out = stock->second.positions[block_offset];
	return Status::Ok();
}

Status ActiveStore::range(const std::vector<SymbolId>& symbol_ids,
				  TimeId begin,
				  TimeId end,
				  std::vector<ActiveBar>* out) const {
	// This range read must be synchronized with writes, sealing, and timer flushes.
	std::lock_guard<std::mutex> lock(mutex_);
	if (out == NULL || begin > end) {
		return Status::Error(ErrorCode::InvalidArgument, "invalid active range");
	}
	if (!replay_status_.ok()) {
		return replay_status_;
	}
	std::set<SymbolId> requested(symbol_ids.begin(), symbol_ids.end());
	out->clear();
	for (std::map<TimeId, ActiveTimeBlock>::const_iterator block = blocks_.begin();
		 block != blocks_.end(); ++block) {
		for (std::map<SymbolId, ActiveStockBlock>::const_iterator stock =
			 block->second.stocks.begin(); stock != block->second.stocks.end(); ++stock) {
			if (requested.find(stock->first) == requested.end()) {
				continue;
			}
			for (size_t i = 0; i < stock->second.present.size(); ++i) {
				if (!stock->second.present[i] || stock->second.time_ids[i] < begin ||
					stock->second.time_ids[i] > end) {
					continue;
				}
				ActiveBar value = {stock->first, stock->second.time_ids[i],
					stock->second.positions[i]};
				out->push_back(value);
			}
		}
	}
	std::sort(out->begin(), out->end(), ActiveBarOrder);
	return Status::Ok();
}

Status ActiveStore::latest_time(SymbolId symbol_id, TimeId *out) const
{
	// Walk blocks in reverse order and find the last position at which the
	// symbol has data. The in-memory block map and per-stock presence vector
	// make this O(blocks * stock_positions) in the worst case, but ActiveStore
	// holds only a small number of unsealed blocks so this is effectively constant.
	std::lock_guard<std::mutex> lock(mutex_);
	if (!replay_status_.ok())
	{
		return replay_status_;
	}
	if (symbol_id == kInvalidSymbolId || out == NULL)
	{
		return Status::Error(ErrorCode::InvalidArgument, "invalid latest_time arguments");
	}
	for (std::map<TimeId, ActiveTimeBlock>::const_reverse_iterator block = blocks_.rbegin();
		 block != blocks_.rend(); ++block)
	{
		std::map<SymbolId, ActiveStockBlock>::const_iterator stock =
			block->second.stocks.find(symbol_id);
		if (stock == block->second.stocks.end()) continue;
		// Find the last present position within this stock's block.
		for (int i = static_cast<int>(stock->second.present.size()) - 1; i >= 0; --i)
		{
			if (stock->second.present[i])
			{
				*out = stock->second.time_ids[i];
				return Status::Ok();
			}
		}
	}
	return Status::Error(ErrorCode::NotFound, "no active history for symbol");
}

// The byte trigger is evaluated after History has completed its current
// accepted batch. The timer uses the same locked writer path, so a threshold
// flush and a periodic flush cannot append overlapping record batches.
Status ActiveStore::flush_if_needed() {
	// This threshold check and flush must be synchronized with writes and the timer.
	std::lock_guard<std::mutex> lock(mutex_);
	if (dirty_bytes_ < flush_bytes_)
		return Status::Ok();
	return flush_locked();
}

Status ActiveStore::flush() {
	// This explicit flush must be synchronized with Active mutations and the timer.
	std::lock_guard<std::mutex> lock(mutex_);
	return flush_locked();
}

void ActiveStore::run_flush_timer() {
	// This timer flush must be synchronized with all foreground Active operations.
	std::unique_lock<std::mutex> lock(mutex_);
	while (!stop_flush_timer_) {
		if (flush_condition_.wait_for(lock,
			std::chrono::milliseconds(flush_interval_milliseconds_),
			[this] { return stop_flush_timer_; })) {
			break;
		}
		flush_locked();
	}
}

Status ActiveStore::flush_locked() {
	if (!replay_status_.ok()) {
		return replay_status_;
	}
	bool has_dirty = false;
	for (std::map<TimeId, ActiveTimeBlock>::const_iterator block = blocks_.begin();
		 block != blocks_.end() && !has_dirty; ++block) {
		for (std::map<SymbolId, ActiveStockBlock>::const_iterator stock =
			 block->second.stocks.begin(); stock != block->second.stocks.end() && !has_dirty;
			 ++stock) {
			for (size_t i = 0; i < stock->second.dirty.size(); ++i) {
				if (stock->second.dirty[i]) {
					has_dirty = true;
					break;
				}
			}
		}
	}
	if (!has_dirty) {
		return Status::Ok();
	}
	std::ofstream output(path_.c_str(), std::ios::binary | std::ios::app);
	if (!output)
	{
		PELOG_LOG((PLV_ERROR, "active store: cannot append %s (errno=%d)\n", path_.c_str(), errno));
		return Status::Error(ErrorCode::IoError, "cannot append active log");
	}
	for (std::map<TimeId, ActiveTimeBlock>::iterator block = blocks_.begin();
		 block != blocks_.end(); ++block) {
		for (std::map<SymbolId, ActiveStockBlock>::iterator stock =
			 block->second.stocks.begin(); stock != block->second.stocks.end(); ++stock) {
			for (size_t i = 0; i < stock->second.dirty.size(); ++i) {
				if (!stock->second.dirty[i]) {
					continue;
				}
				ActiveRecord record = {frequency_, stock->first, stock->second.time_ids[i],
					stock->second.positions[i]};
				std::vector<uint8_t> bytes;
				if (!SerializeActiveRecord(record, &bytes)) {
					return Status::Error(ErrorCode::CorruptData, "invalid dirty active bar");
				}
				output.write(reinterpret_cast<const char*>(&bytes[0]), bytes.size());
				if (!output)
				{
					PELOG_LOG((PLV_ERROR, "active store: write failed %s\n", path_.c_str()));
					return Status::Error(ErrorCode::IoError, "cannot write active log");
				}
			}
		}
	}
	output.flush();
	if (!output)
	{
		PELOG_LOG((PLV_ERROR, "active store: flush failed %s\n", path_.c_str()));
		return Status::Error(ErrorCode::IoError, "cannot flush active log");
	}
	for (std::map<TimeId, ActiveTimeBlock>::iterator block = blocks_.begin();
		 block != blocks_.end(); ++block) {
		for (std::map<SymbolId, ActiveStockBlock>::iterator stock =
			 block->second.stocks.begin(); stock != block->second.stocks.end(); ++stock) {
			std::fill(stock->second.dirty.begin(), stock->second.dirty.end(), false);
		}
	}
	dirty_bytes_ = 0;
	return Status::Ok();
}

Status ActiveStore::collect_before(TimeId time_id,
					       std::vector<StockTimeBlock>* sealed,
					       std::vector<ActiveBar>* sealed_bars) {
	// This snapshot must be synchronized with writes, reads, and timer flushes.
	std::lock_guard<std::mutex> lock(mutex_);
	if (sealed == NULL || sealed_bars == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "sealed outputs are required");
	}
	if (!replay_status_.ok()) {
		return replay_status_;
	}
	Status status = flush_locked();
	if (!status.ok()) {
		return status;
	}
	sealed->clear();
	sealed_bars->clear();
	const TimeId cutoff_day = time_day(time_id);
	const TimeId block_days = frequency_ == Frequency::Daily ?
		kDailyTimeBlockDayLength : kHourlyTimeBlockDayLength;
	for (std::map<TimeId, ActiveTimeBlock>::iterator block = blocks_.begin();
		 block != blocks_.end();) {
		if (time_day(block->first) + block_days > cutoff_day) {
			++block;
			continue;
		}
		for (std::map<SymbolId, ActiveStockBlock>::const_iterator stock =
			 block->second.stocks.begin(); stock != block->second.stocks.end(); ++stock) {
			StockTimeBlock completed = {};
			completed.key.symbol_id = stock->first;
			completed.key.time_block_id = block->first;
			completed.positions = stock->second.positions;
			for (size_t i = 0; i < stock->second.present.size(); ++i) {
				if (!stock->second.present[i]) {
					continue;
				}
				const TimeId day = time_day(stock->second.time_ids[i]);
				const TimeId day_offset = day - time_day(block->first);
				if (day_offset < 64) {
					completed.day_presence |= static_cast<uint64_t>(1) << day_offset;
				}
				ActiveBar active_bar = {stock->first, stock->second.time_ids[i],
					stock->second.positions[i]};
				sealed_bars->push_back(active_bar);
			}
			sealed->push_back(completed);
		}
		++block;
	}
	return Status::Ok();
}

Status ActiveStore::remove_before(TimeId time_id) {
	// This ownership commit must be synchronized with writes and timer flushes.
	std::lock_guard<std::mutex> lock(mutex_);
	if (!replay_status_.ok()) {
		return replay_status_;
	}
	const TimeId cutoff_day = time_day(time_id);
	const TimeId block_days = frequency_ == Frequency::Daily ?
		kDailyTimeBlockDayLength : kHourlyTimeBlockDayLength;
	const std::string temporary_path = path_ + ".tmp";
	std::ofstream output(temporary_path.c_str(), std::ios::binary | std::ios::trunc);
	if (!output)
	{
		PELOG_LOG((PLV_ERROR, "active store: cannot create rewrite temp %s\n", temporary_path.c_str()));
		return Status::Error(ErrorCode::IoError, "cannot rewrite active log");
	}
	for (std::map<TimeId, ActiveTimeBlock>::const_iterator block = blocks_.begin();
		 block != blocks_.end(); ++block) {
		if (time_day(block->first) + block_days <= cutoff_day) {
			continue;
		}
		for (std::map<SymbolId, ActiveStockBlock>::const_iterator stock =
			 block->second.stocks.begin(); stock != block->second.stocks.end(); ++stock) {
			for (size_t offset = 0; offset < stock->second.present.size(); ++offset) {
				if (!stock->second.present[offset]) {
					continue;
				}
				ActiveRecord record = {frequency_, stock->first, stock->second.time_ids[offset],
					stock->second.positions[offset]};
				std::vector<uint8_t> bytes;
				if (!SerializeActiveRecord(record, &bytes)) {
					return Status::Error(ErrorCode::CorruptData, "invalid active bar during rewrite");
				}
				output.write(reinterpret_cast<const char*>(&bytes[0]), bytes.size());
				if (!output)
				{
					PELOG_LOG((PLV_ERROR, "active store: rewrite write failed %s\n", path_.c_str()));
					return Status::Error(ErrorCode::IoError, "cannot rewrite active log");
				}
			}
		}
	}
	output.flush();
	output.close();
	if (!output || std::rename(temporary_path.c_str(), path_.c_str()) != 0)
	{
		std::remove(temporary_path.c_str());
		PELOG_LOG((PLV_ERROR, "active store: cannot publish rewrite %s (errno=%d)\n", path_.c_str(), errno));
		return Status::Error(ErrorCode::IoError, "cannot publish rewritten active log");
	}
	for (std::map<TimeId, ActiveTimeBlock>::iterator block = blocks_.begin();
		 block != blocks_.end();) {
		if (time_day(block->first) + block_days <= cutoff_day) {
			blocks_.erase(block++);
		} else {
			++block;
		}
	}
	return Status::Ok();
}

Status ActiveStore::replay_status() const {
	// This recovery-status read must be synchronized with Active state access.
	std::lock_guard<std::mutex> lock(mutex_);
	return replay_status_;
}

// =============================================================================
// Staging page types and shared cache forward declarations
//
// ParsedStagingRecord and PendingStagingRecord are defined here so the shared
// cache function declarations (below) can reference them. The full cache
// implementation lives in the vault section further down.
// =============================================================================

static const size_t kStagingPageTargetBytes = 64 * 1024;
static const uint64_t kStagingSegmentTargetBytes = 64ULL * 1024 * 1024;
static const uint8_t kStagingVersion = 2;
static const size_t kStagingPageHeaderBytes = 28;

struct PendingStagingRecord {
	StockTimeBlock block;
	std::vector<uint8_t> present;
	std::vector<std::vector<uint8_t> > frame_bytes;
};

struct ParsedStagingRecord {
	StockTimeBlock block;
	std::vector<ActiveBar> bars;
	std::vector<uint8_t> frame_bytes;
};

enum class CacheStoreType : uint8_t {
	Vault = 0,
	Staging = 1
};
static void InsertCompressedCache(CacheStoreType store_type, uint64_t runtime_market_id,
	Frequency frequency, uint32_t segment_id, uint64_t offset, const std::vector<uint8_t>& bytes);
static bool GetCompressedCache(CacheStoreType store_type, uint64_t runtime_market_id,
	Frequency frequency, uint32_t segment_id, uint64_t offset, std::vector<uint8_t>* bytes);
static void InsertStagingDecodedCache(uint64_t runtime_market_id, Frequency frequency,
	uint32_t segment_id, uint64_t page_offset, const std::vector<ParsedStagingRecord>& records);
static bool GetStagingDecodedCache(uint64_t runtime_market_id, Frequency frequency,
	uint32_t segment_id, uint64_t page_offset, std::vector<ParsedStagingRecord>* records);

// =============================================================================
// Staging Pages and Index
//
// Staging pages are variable-length sequential file records. The 64 KiB target
// controls aggregation only: a page closes after appending the first complete
// block that reaches the target, so no padding or special oversized-page format
// is needed. A separate index maps immutable block keys to page locators.
// =============================================================================

static bool PendingStagingOrder(const PendingStagingRecord& left,
					const PendingStagingRecord& right) {
	return left.block.key.time_block_id != right.block.key.time_block_id ?
		left.block.key.time_block_id < right.block.key.time_block_id :
		left.block.key.symbol_id < right.block.key.symbol_id;
}

bool SameBlockBar(const BlockBar& left, const BlockBar& right) {
	return left.state == right.state && left.open == right.open && left.high == right.high &&
		left.low == right.low && left.close == right.close && left.volume == right.volume;
}

static PrecisionProfile StagingPrecisionProfile() {
	PrecisionProfile profile = {};
	profile.price_relative_epsilon = 1e-4;
	profile.volume_relative_epsilon = 0.01;
	return profile;
}

static std::string StagingSegmentPath(const std::string& path, uint32_t segment_id) {
	char name[64];
	std::snprintf(name, sizeof(name), "staging-pages-%04u.seg", segment_id);
	return path + "/" + name;
}

static bool ReadFile(const std::string& path, std::vector<uint8_t>* bytes) {
	std::ifstream input(path.c_str(), std::ios::binary);
	if (!input) {
		return false;
	}
	bytes->assign(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
	return input.good() || input.eof();
}

Status StagingTimeIds(const Calendar& calendar,
				     Frequency frequency,
				     TimeId block_id,
				     BlockOff position_count,
				     std::vector<TimeId>* time_ids) {
	if (time_ids == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "staging time-id output is required");
	}
	time_ids->clear();
	time_ids->reserve(position_count);
	const TimeId first_day = time_day(block_id);
	if (frequency == Frequency::Daily) {
		for (BlockOff offset = 0; offset < position_count; ++offset) {
			time_ids->push_back(daily_bar_id(first_day + offset));
		}
		return Status::Ok();
	}
	for (TimeId day_offset = 0; day_offset < kHourlyTimeBlockDayLength; ++day_offset) {
		const TimeId day_id = daily_bar_id(first_day + day_offset);
		std::string date;
		Status status = calendar.date(day_id, &date);
		if (!status.ok()) {
			return status;
		}
		std::vector<HourSlot> slots;
		status = calendar.slots(date, &slots);
		if (!status.ok()) {
			return status;
		}
		for (size_t slot = 0; slot < slots.size(); ++slot) {
			if (time_ids->size() == position_count) {
				return Status::Error(ErrorCode::CorruptData, "hourly staging block is too short");
			}
			time_ids->push_back(day_id | slots[slot]);
		}
	}
	if (time_ids->size() != position_count) {
		return Status::Error(ErrorCode::CorruptData, "hourly staging block length changed");
	}
	return Status::Ok();
}

// Build a pending staging record from a complete StockTimeBlock.
// The present bitmap is derived from positions[i].state:
// If frame_bytes is provided, it is used as-is, without encoding.
// Otherwise the frame is encoded from positions.
static Status MakePendingStagingRecord(const Calendar& calendar,
					       Frequency frequency,
					       const StockTimeBlock& block,
					       const std::vector<uint8_t>* frame_bytes,
					       PendingStagingRecord* output) {
	if (output == NULL || block.key.symbol_id == kInvalidSymbolId ||
		block.positions.empty() || block.positions.size() > std::numeric_limits<BlockOff>::max())
	{
		return Status::Error(ErrorCode::InvalidArgument, "invalid staging block");
	}
	BlockOff expected_position_count = 0;
	Status status = calendar.block_length(frequency, block.key.time_block_id,
		&expected_position_count);
	if (!status.ok() || block.positions.size() != expected_position_count)
{
		return !status.ok() ? status : Status::Error(ErrorCode::InvalidArgument,
			"staging block does not cover its complete calendar range");
	}
	PendingStagingRecord pending = {};
	pending.block = block;
	pending.present.assign((block.positions.size() + 7) / 8, 0);
	for (size_t offset = 0; offset < block.positions.size(); ++offset)
	{
		if (block.positions[offset].state != BarState::Missing)
		{
			pending.present[offset / 8] |= static_cast<uint8_t>(1U << (offset % 8));
		}
	}
	if (frame_bytes != NULL)
	{
		// When a pre-encoded frame is provided, decode it once to derive the
		// present bitmap (needed for the page directory). The frame bytes
		// themselves are used as-is for the payload, avoiding a re-encode.
		BarBlockFrame frame = {*frame_bytes};
		std::vector<BlockBar> decoded;
		status = DecodeBarBlockFrame(frame, &decoded);
		if (!status.ok() || decoded.size() != block.positions.size())
			return Status::Error(ErrorCode::CorruptData, "invalid source staging bar block frame");
		// Rebuild present from the decoded frame data.
		pending.present.assign((decoded.size() + 7) / 8, 0);
		for (size_t i = 0; i < decoded.size(); ++i)
		{
			if (decoded[i].state != BarState::Missing)
				pending.present[i / 8] |= static_cast<uint8_t>(1U << (i % 8));
		}
		pending.frame_bytes.push_back(*frame_bytes);
	}
	else
	{
		BarBlockFrame frame;
		status = EncodeBarBlockFrame(block.positions, &frame);
		if (!status.ok())
			return status;
		pending.frame_bytes.push_back(frame.bytes);
	}
	*output = pending;
	return Status::Ok();
}

static Status SerializeStagingPage(uint32_t page_id,
					  Frequency frequency,
					  const std::vector<PendingStagingRecord>& records,
					  std::vector<uint8_t>* bytes) {
	if (bytes == NULL || records.empty() || records.size() > std::numeric_limits<uint32_t>::max()) {
		return Status::Error(ErrorCode::InvalidArgument, "invalid staging page");
	}
	std::vector<uint8_t> directory;
	std::vector<uint8_t> payload;
	for (size_t i = 0; i < records.size(); ++i) {
		const PendingStagingRecord& record = records[i];
		if (record.present.size() != (record.block.positions.size() + 7) / 8 ||
			record.frame_bytes.size() > std::numeric_limits<uint16_t>::max()) {
			return Status::Error(ErrorCode::InvalidArgument, "invalid staging page record");
		}
		PutU32(&directory, record.block.key.symbol_id);
		PutU32(&directory, record.block.key.time_block_id);
		PutU16(&directory, static_cast<uint16_t>(record.block.positions.size()));
		PutU64(&directory, record.block.day_presence);
		PutU16(&directory, static_cast<uint16_t>(record.present.size()));
		directory.insert(directory.end(), record.present.begin(), record.present.end());
		PutU16(&directory, static_cast<uint16_t>(record.frame_bytes.size()));
		for (size_t frame = 0; frame < record.frame_bytes.size(); ++frame) {
			if (payload.size() > std::numeric_limits<uint32_t>::max() ||
				record.frame_bytes[frame].size() > std::numeric_limits<uint32_t>::max()) {
				return Status::Error(ErrorCode::InvalidArgument, "staging payload is too large");
			}
			PutU32(&directory, static_cast<uint32_t>(payload.size()));
			PutU32(&directory, static_cast<uint32_t>(record.frame_bytes[frame].size()));
			payload.insert(payload.end(), record.frame_bytes[frame].begin(),
				record.frame_bytes[frame].end());
		}
	}
	if (directory.size() > std::numeric_limits<uint32_t>::max() ||
		payload.size() > std::numeric_limits<uint32_t>::max()) {
		return Status::Error(ErrorCode::InvalidArgument, "staging page is too large");
	}
	bytes->clear();
	bytes->reserve(kStagingPageHeaderBytes + directory.size() + payload.size());
	PutU8(bytes, 'Z');
	PutU8(bytes, 'S');
	PutU8(bytes, 'P');
	PutU8(bytes, '5');
	PutU8(bytes, kStagingVersion);
	PutU8(bytes, static_cast<uint8_t>(frequency));
	PutU16(bytes, 0);
	PutU64(bytes, page_id);
	PutU32(bytes, static_cast<uint32_t>(records.size()));
	PutU32(bytes, static_cast<uint32_t>(directory.size()));
	PutU32(bytes, static_cast<uint32_t>(payload.size()));
	bytes->insert(bytes->end(), directory.begin(), directory.end());
	bytes->insert(bytes->end(), payload.begin(), payload.end());
	return Status::Ok();
}

static Status ParseStagingPage(const Calendar& calendar,
				       Frequency frequency,
				       const std::vector<uint8_t>& bytes,
				       std::vector<ParsedStagingRecord>* records,
				       uint32_t* page_id) {
	if (records == NULL || page_id == NULL || bytes.size() < kStagingPageHeaderBytes) {
		return Status::Error(ErrorCode::CorruptData, "truncated staging page");
	}
	size_t cursor = 0;
	uint8_t magic[4] = {};
	uint8_t version = 0;
	uint8_t stored_frequency = 0;
	uint16_t reserved = 0;
	uint64_t stored_page_id = 0;
	uint32_t count = 0;
	uint32_t directory_size = 0;
	uint32_t payload_size = 0;
	if (!GetU8(bytes, &cursor, &magic[0]) || !GetU8(bytes, &cursor, &magic[1]) ||
		!GetU8(bytes, &cursor, &magic[2]) || !GetU8(bytes, &cursor, &magic[3]) ||
		magic[0] != 'Z' || magic[1] != 'S' || magic[2] != 'P' || magic[3] != '5' ||
		!GetU8(bytes, &cursor, &version) || !GetU8(bytes, &cursor, &stored_frequency) ||
		!GetU16(bytes, &cursor, &reserved) || !GetU64(bytes, &cursor, &stored_page_id) ||
		!GetU32(bytes, &cursor, &count) ||
		!GetU32(bytes, &cursor, &directory_size) || !GetU32(bytes, &cursor, &payload_size) ||
		version != kStagingVersion || stored_frequency != static_cast<uint8_t>(frequency) ||
		reserved != 0 || stored_page_id == 0 ||
		stored_page_id > std::numeric_limits<uint32_t>::max() || count == 0 ||
		kStagingPageHeaderBytes + static_cast<size_t>(directory_size) + payload_size != bytes.size()) {
		return Status::Error(ErrorCode::CorruptData, "invalid staging page header");
	}
	*page_id = static_cast<uint32_t>(stored_page_id);
	const size_t directory_end = cursor + directory_size;
	const size_t payload_start = directory_end;
	records->clear();
	records->reserve(count);
	for (uint32_t record_index = 0; record_index < count; ++record_index) {
		uint32_t symbol_id = 0;
		uint32_t block_id = 0;
		uint16_t position_count = 0;
		uint64_t day_presence = 0;
		uint16_t present_size = 0;
		uint16_t frame_count = 0;
		if (!GetU32(bytes, &cursor, &symbol_id) || !GetU32(bytes, &cursor, &block_id) ||
			!GetU16(bytes, &cursor, &position_count) || !GetU64(bytes, &cursor, &day_presence) ||
			!GetU16(bytes, &cursor, &present_size) || symbol_id == kInvalidSymbolId ||
			position_count == 0 || present_size != (position_count + 7) / 8 ||
			cursor + present_size > directory_end) {
			return Status::Error(ErrorCode::CorruptData, "invalid staging record directory");
		}
		std::vector<uint8_t> present(bytes.begin() + cursor, bytes.begin() + cursor + present_size);
		cursor += present_size;
		if (!GetU16(bytes, &cursor, &frame_count) || frame_count == 0) {
			return Status::Error(ErrorCode::CorruptData, "staging record has no frames");
		}
		if (frame_count != 1) {
			return Status::Error(ErrorCode::CorruptData, "staging record must contain one bar block frame");
		}
		uint32_t offset = 0;
		uint32_t length = 0;
		if (!GetU32(bytes, &cursor, &offset) || !GetU32(bytes, &cursor, &length) ||
			offset > payload_size || length > payload_size - offset) {
			return Status::Error(ErrorCode::CorruptData, "invalid staging frame locator");
		}
		BarBlockFrame frame;
		frame.bytes.assign(bytes.begin() + payload_start + offset,
			bytes.begin() + payload_start + offset + length);
		std::vector<BlockBar> positions;
		Status status = DecodeBarBlockFrame(frame, &positions);
		if (!status.ok()) {
			return status;
		}
		if (positions.size() != position_count) {
			return Status::Error(ErrorCode::CorruptData, "staging bar block position count mismatch");
		}
		std::vector<TimeId> time_ids;
		status = StagingTimeIds(calendar, frequency, block_id, position_count, &time_ids);
		if (!status.ok()) {
			return status;
		}
		ParsedStagingRecord parsed = {};
		parsed.block.key.symbol_id = symbol_id;
		parsed.block.key.time_block_id = block_id;
		parsed.block.day_presence = day_presence;
		parsed.block.positions = positions;
		parsed.frame_bytes = frame.bytes;
		for (size_t offset = 0; offset < positions.size(); ++offset) {
			if ((present[offset / 8] & static_cast<uint8_t>(1U << (offset % 8))) != 0) {
				ActiveBar bar = {symbol_id, time_ids[offset], positions[offset]};
				parsed.bars.push_back(bar);
			}
		}
		records->push_back(parsed);
	}
	if (cursor != directory_end) {
		return Status::Error(ErrorCode::CorruptData, "trailing staging directory bytes");
	}
	return Status::Ok();
}

StagingStore::StagingStore(Frequency frequency,
				   const Calendar& calendar,
				   const std::string& frequency_path,
				   uint64_t runtime_market_id)
	: frequency_(frequency),
	  calendar_(calendar),
	  path_(frequency_path),
	  runtime_market_id_(runtime_market_id),
	  status_(Status::Ok()),
	  next_page_id_(1),
	  current_segment_id_(1) {
	status_ = load();
}

Status StagingStore::load_page_bytes(uint32_t segment_id, uint64_t page_offset, uint32_t page_length,
							  std::vector<uint8_t>* page_bytes) const
{
	if (GetCompressedCache(CacheStoreType::Staging, runtime_market_id_, frequency_,
			segment_id, page_offset, page_bytes))
	{
		return Status::Ok();
	}
	std::ifstream input(StagingSegmentPath(path_, segment_id).c_str(), std::ios::binary);
	if (!input)
	{
		return Status::Error(ErrorCode::IoError, "cannot read staging segment");
	}
	input.seekg(static_cast<std::streamoff>(page_offset));
	page_bytes->assign(page_length, 0);
	input.read(reinterpret_cast<char*>(&(*page_bytes)[0]), page_length);
	if (input.gcount() != static_cast<std::streamsize>(page_length))
	{
		return Status::Error(ErrorCode::CorruptData, "truncated staging page");
	}
	InsertCompressedCache(CacheStoreType::Staging, runtime_market_id_, frequency_,
		segment_id, page_offset, *page_bytes);
	return Status::Ok();
}

Status StagingStore::load_page(uint32_t segment_id, uint64_t page_offset, uint32_t page_length,
					 std::vector<ParsedStagingRecord>* records) const {
	// First try the decoded page cache.
	if (GetStagingDecodedCache(runtime_market_id_, frequency_, segment_id, page_offset, records)) {
		return Status::Ok();
	}
	std::vector<uint8_t> page_bytes;
	Status status = load_page_bytes(segment_id, page_offset, page_length, &page_bytes);
	if (!status.ok()) {
		return status;
	}
	uint32_t page_id = 0;
	status = ParseStagingPage(calendar_, frequency_, page_bytes, records, &page_id);
	if (!status.ok()) {
		return status;
	}
	InsertStagingDecodedCache(runtime_market_id_, frequency_, segment_id, page_offset, *records);
	return Status::Ok();
}

Status StagingStore::load() {
	index_.clear();
	next_page_id_ = 1;
	current_segment_id_ = 1;

	// Load index first; if valid we skip full segment scan and verify lazily.
	std::map<std::pair<TimeId, SymbolId>, Locator> persisted_index;
	bool persisted_index_valid = false;
	std::vector<uint8_t> index_bytes;
	if (ReadFile(path_ + "/staging-index", &index_bytes)) {
		size_t cursor = 0;
		uint8_t magic[4] = {};
		uint8_t version = 0;
		uint8_t stored_frequency = 0;
		uint16_t reserved = 0;
		uint32_t count = 0;
		if (GetU8(index_bytes, &cursor, &magic[0]) && GetU8(index_bytes, &cursor, &magic[1]) &&
			GetU8(index_bytes, &cursor, &magic[2]) && GetU8(index_bytes, &cursor, &magic[3]) &&
			magic[0] == 'Z' && magic[1] == 'S' && magic[2] == 'I' && magic[3] == '5' &&
			GetU8(index_bytes, &cursor, &version) && GetU8(index_bytes, &cursor, &stored_frequency) &&
			GetU16(index_bytes, &cursor, &reserved) && GetU32(index_bytes, &cursor, &count) &&
			version == kStagingVersion && stored_frequency == static_cast<uint8_t>(frequency_) &&
			reserved == 0 && index_bytes.size() == 12 + static_cast<size_t>(count) * 28) {
			persisted_index_valid = true;
			uint32_t max_page_id = 0;
			uint32_t max_segment = 0;
			for (uint32_t i = 0; i < count; ++i) {
				uint32_t block_id = 0;
				uint32_t symbol_id = 0;
				Locator locator = {};
				if (!GetU32(index_bytes, &cursor, &block_id) ||
					!GetU32(index_bytes, &cursor, &symbol_id) ||
					!GetU32(index_bytes, &cursor, &locator.segment_id) ||
					!GetU64(index_bytes, &cursor, &locator.page_offset) ||
					!GetU32(index_bytes, &cursor, &locator.page_length) ||
					!GetU32(index_bytes, &cursor, &locator.record_index) ||
					symbol_id == kInvalidSymbolId ||
					!persisted_index.insert(std::make_pair(
						std::make_pair(block_id, symbol_id), locator)).second) {
					persisted_index_valid = false;
					break;
				}
				max_segment = std::max(max_segment, locator.segment_id);
				// We don't know page_id from index alone; set a safe lower bound.
			}
			if (persisted_index_valid) {
				index_ = persisted_index;
				current_segment_id_ = max_segment == 0 ? 1 : max_segment;
				// next_page_id_ is only used by accept() to assign new ids; we
				// recover it on the first full rebuild path below.
				next_page_id_ = 1;
				return Status::Ok();
			}
		}
	}
	// Fallback: rebuild index by scanning segment files.
	for (uint32_t segment_id = 1;; ++segment_id) {
		const std::string segment_path = StagingSegmentPath(path_, segment_id);
		struct stat information;
		if (stat(segment_path.c_str(), &information) != 0) {
			if (errno == ENOENT) {
				break;
			}
			return Status::Error(ErrorCode::IoError, "cannot inspect staging segment");
		}
		std::vector<uint8_t> segment;
		if (!ReadFile(segment_path, &segment)) {
			return Status::Error(ErrorCode::IoError, "cannot read staging segment");
		}
		size_t offset = 0;
		while (offset < segment.size()) {
			if (segment.size() - offset < kStagingPageHeaderBytes) {
				if (truncate(segment_path.c_str(), static_cast<off_t>(offset)) != 0) {
					return Status::Error(ErrorCode::IoError, "cannot repair staging segment tail");
				}
				break;
			}
			size_t header_cursor = offset + 20;
			uint32_t directory_size = 0;
			uint32_t payload_size = 0;
			if (!GetU32(segment, &header_cursor, &directory_size) ||
				!GetU32(segment, &header_cursor, &payload_size) ||
				directory_size > segment.size() || payload_size > segment.size() - directory_size ||
				kStagingPageHeaderBytes + static_cast<size_t>(directory_size) + payload_size >
					segment.size() - offset) {
				if (truncate(segment_path.c_str(), static_cast<off_t>(offset)) != 0) {
					return Status::Error(ErrorCode::IoError, "cannot repair staging segment tail");
				}
				break;
			}
			const size_t page_length = kStagingPageHeaderBytes + directory_size + payload_size;
			std::vector<uint8_t> page(segment.begin() + offset, segment.begin() + offset + page_length);
			std::vector<ParsedStagingRecord> parsed;
			uint32_t page_id = 0;
			Status status = ParseStagingPage(calendar_, frequency_, page, &parsed, &page_id);
			if (!status.ok()) {
				return status;
			}
			for (size_t record = 0; record < parsed.size(); ++record) {
				const std::pair<TimeId, SymbolId> key(parsed[record].block.key.time_block_id,
					parsed[record].block.key.symbol_id);
				if (index_.find(key) != index_.end()) {
					return Status::Error(ErrorCode::CorruptData, "duplicate staged block");
				}
				Locator locator = {segment_id, static_cast<uint64_t>(offset),
					static_cast<uint32_t>(page_length), static_cast<uint32_t>(record)};
				index_[key] = locator;
			}
			next_page_id_ = std::max(next_page_id_, page_id + 1);
			offset += page_length;
		}
		current_segment_id_ = segment_id;
	}
	return write_index();
}

Status StagingStore::write_index() const {
	std::vector<uint8_t> bytes;
	PutU8(&bytes, 'Z');
	PutU8(&bytes, 'S');
	PutU8(&bytes, 'I');
	PutU8(&bytes, '5');
	PutU8(&bytes, kStagingVersion);
	PutU8(&bytes, static_cast<uint8_t>(frequency_));
	PutU16(&bytes, 0);
	PutU32(&bytes, static_cast<uint32_t>(index_.size()));
	for (std::map<std::pair<TimeId, SymbolId>, Locator>::const_iterator entry = index_.begin();
		 entry != index_.end(); ++entry) {
		PutU32(&bytes, entry->first.first);
		PutU32(&bytes, entry->first.second);
		PutU32(&bytes, entry->second.segment_id);
		PutU64(&bytes, entry->second.page_offset);
		PutU32(&bytes, entry->second.page_length);
		PutU32(&bytes, entry->second.record_index);
	}
	const std::string temporary_path = path_ + "/staging-index.tmp";
	const std::string index_path = path_ + "/staging-index";
	std::ofstream output(temporary_path.c_str(), std::ios::binary | std::ios::trunc);
	if (!output) {
		return Status::Error(ErrorCode::IoError, "cannot write staging index");
	}
	output.write(reinterpret_cast<const char*>(&bytes[0]), bytes.size());
	output.flush();
	output.close();
	if (!output || std::rename(temporary_path.c_str(), index_path.c_str()) != 0) {
		std::remove(temporary_path.c_str());
		return Status::Error(ErrorCode::IoError, "cannot publish staging index");
	}
	return Status::Ok();
}

Status StagingStore::accept(const std::vector<StockTimeBlock>& blocks,
				    const std::vector<std::vector<uint8_t> >* frame_bytes) {
	if (!status_.ok()) {
		return status_;
	}
	if (frame_bytes != NULL && frame_bytes->size() != blocks.size()) {
		return Status::Error(ErrorCode::InvalidArgument, "staging frame bytes do not match blocks");
	}
	std::set<std::pair<TimeId, SymbolId> > batch_keys;
	std::vector<PendingStagingRecord> pending;
	for (size_t i = 0; i < blocks.size(); ++i) {
		const std::pair<TimeId, SymbolId> key(blocks[i].key.time_block_id,
			blocks[i].key.symbol_id);
		if (!batch_keys.insert(key).second) {
			return Status::Error(ErrorCode::Conflict, "duplicate block in staging batch");
		}
		if (index_.find(key) != index_.end()) {
			continue;
		}
		PendingStagingRecord record;
		const std::vector<uint8_t>* source_frame = frame_bytes == NULL || (*frame_bytes)[i].empty() ?
			NULL : &(*frame_bytes)[i];
		Status status = MakePendingStagingRecord(calendar_, frequency_, blocks[i], source_frame, &record);
		if (!status.ok()) {
			return status;
		}
		pending.push_back(record);
	}
	if (pending.empty()) {
		return Status::Ok();
	}
	std::sort(pending.begin(), pending.end(), PendingStagingOrder);
	std::vector<std::vector<PendingStagingRecord> > pages;
	std::vector<PendingStagingRecord> page_records;
	for (size_t i = 0; i < pending.size(); ++i) {
		page_records.push_back(pending[i]);
		std::vector<uint8_t> page_bytes;
		Status status = SerializeStagingPage(next_page_id_, frequency_, page_records, &page_bytes);
		if (!status.ok()) {
			return status;
		}
		if (page_bytes.size() >= kStagingPageTargetBytes) {
			pages.push_back(page_records);
			page_records.clear();
		}
	}
	if (!page_records.empty()) {
		pages.push_back(page_records);
	}
	for (size_t page = 0; page < pages.size(); ++page) {
		const std::string segment_path = StagingSegmentPath(path_, current_segment_id_);
		struct stat information;
		uint64_t offset = 0;
		if (stat(segment_path.c_str(), &information) == 0) {
			offset = static_cast<uint64_t>(information.st_size);
			if (offset >= kStagingSegmentTargetBytes) {
				++current_segment_id_;
				offset = 0;
			}
		} else if (errno != ENOENT) {
			return Status::Error(ErrorCode::IoError, "cannot inspect staging segment");
		}
		const std::string output_path = StagingSegmentPath(path_, current_segment_id_);
		std::vector<uint8_t> page_bytes;
		Status status = SerializeStagingPage(next_page_id_, frequency_, pages[page], &page_bytes);
		if (!status.ok()) {
			return status;
		}
		std::ofstream output(output_path.c_str(), std::ios::binary | std::ios::app);
		if (!output) {
			return Status::Error(ErrorCode::IoError, "cannot append staging page");
		}
		output.write(reinterpret_cast<const char*>(&page_bytes[0]), page_bytes.size());
		output.flush();
		if (!output) {
			return Status::Error(ErrorCode::IoError, "cannot write staging page");
		}
		std::vector<ParsedStagingRecord> parsed;
		uint32_t parsed_page_id = 0;
		status = ParseStagingPage(calendar_, frequency_, page_bytes, &parsed, &parsed_page_id);
		if (!status.ok() || parsed.size() != pages[page].size()) {
			return !status.ok() ? status : Status::Error(ErrorCode::CorruptData,
				"staging page record count changed");
		}
		for (size_t record = 0; record < pages[page].size(); ++record) {
			const std::pair<TimeId, SymbolId> key(pages[page][record].block.key.time_block_id,
				pages[page][record].block.key.symbol_id);
			Locator locator = {current_segment_id_, offset, static_cast<uint32_t>(page_bytes.size()),
				static_cast<uint32_t>(record)};
			index_[key] = locator;
		}
		++next_page_id_;
	}
	return write_index();
}

bool StagingStore::contains(SymbolId symbol_id, TimeId time_id) const {
	if (!status_.ok()) {
		return false;
	}
	// Locate the block+symbol key by scanning index keys. We check the
	// block's time range against the requested time_id using day arithmetic.
	const TimeId day = time_day(time_id);
	const TimeId block_days = frequency_ == Frequency::Daily ?
		kDailyTimeBlockDayLength : kHourlyTimeBlockDayLength;
	for (std::map<std::pair<TimeId, SymbolId>, Locator>::const_iterator it = index_.begin();
		 it != index_.end(); ++it) {
		if (it->first.second != symbol_id) {
			continue;
		}
		const TimeId block_start_day = time_day(it->first.first);
		if (day >= block_start_day && day < block_start_day + block_days) {
			return true;
		}
	}
	return false;
}

Status StagingStore::get(SymbolId symbol_id, TimeId time_id, BlockBar* out) const {
	if (out == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "staging read output is required");
	}
	if (!status_.ok()) {
		return status_;
	}
	// Find the matching block in the index by checking each (block_id, symbol_id)
	// entry's time range.
	const TimeId day = time_day(time_id);
	const TimeId block_days = frequency_ == Frequency::Daily ?
		kDailyTimeBlockDayLength : kHourlyTimeBlockDayLength;
	for (std::map<std::pair<TimeId, SymbolId>, Locator>::const_iterator it = index_.begin();
		 it != index_.end(); ++it) {
		if (it->first.second != symbol_id) {
			continue;
		}
		const TimeId block_start_day = time_day(it->first.first);
		if (day < block_start_day || day >= block_start_day + block_days) {
			continue;
		}
		std::vector<ParsedStagingRecord> records;
		Status status = load_page(it->second.segment_id, it->second.page_offset,
			it->second.page_length, &records);
		if (!status.ok()) {
			return status;
		}
		if (it->second.record_index >= records.size()) {
			return Status::Error(ErrorCode::CorruptData, "staging record index out of range");
		}
		const ParsedStagingRecord& record = records[it->second.record_index];
		for (size_t i = 0; i < record.bars.size(); ++i) {
			if (record.bars[i].time_id == time_id) {
				*out = record.bars[i].bar;
				return Status::Ok();
			}
		}
		break;
	}
	return Status::Error(ErrorCode::NotFound, "staged bar was not found");
}

Status StagingStore::range(const std::vector<SymbolId>& symbol_ids,
				   TimeId begin,
				   TimeId end,
				   std::vector<ActiveBar>* out) const {
	if (out == NULL || begin > end) {
		return Status::Error(ErrorCode::InvalidArgument, "invalid staging range");
	}
	if (!status_.ok()) {
		return status_;
	}
	std::set<SymbolId> requested(symbol_ids.begin(), symbol_ids.end());
	out->clear();
	// Walk the index in order. For each matching record, load its page and
	// collect bars that fall within the requested range.
	std::set<std::pair<uint32_t, uint64_t> > visited_pages;
	for (std::map<std::pair<TimeId, SymbolId>, Locator>::const_iterator it = index_.begin();
		 it != index_.end(); ++it) {
		if (requested.find(it->first.second) == requested.end()) {
			continue;
		}
		const std::pair<uint32_t, uint64_t> page_key(it->second.segment_id, it->second.page_offset);
		std::vector<ParsedStagingRecord> records;
		Status status = load_page(it->second.segment_id, it->second.page_offset,
			it->second.page_length, &records);
		if (!status.ok()) {
			return status;
		}
		// Only scan the page once even if multiple index entries point at it.
		if (visited_pages.insert(page_key).second) {
			for (size_t r = 0; r < records.size(); ++r) {
				for (size_t i = 0; i < records[r].bars.size(); ++i) {
					const ActiveBar& bar = records[r].bars[i];
					if (requested.count(bar.symbol_id) &&
						bar.time_id >= begin && bar.time_id <= end) {
						out->push_back(bar);
					}
				}
			}
		}
	}
	std::sort(out->begin(), out->end(), ActiveBarOrder);
	return Status::Ok();
}

Status StagingStore::latest_time(SymbolId symbol_id, TimeId *out) const
{
	// The staging index is ordered by (time_block_id, symbol_id). Walk in
	// reverse to find the last block that contains data for this symbol, then
	// load its page and scan for the maximum time_id. Since the index alone
	// is always resident, only one page load is needed in the common case.
	if (!status_.ok())
	{
		return status_;
	}
	if (symbol_id == kInvalidSymbolId || out == NULL)
	{
		return Status::Error(ErrorCode::InvalidArgument, "invalid latest_time arguments");
	}
	for (std::map<std::pair<TimeId, SymbolId>, Locator>::const_reverse_iterator it =
		 index_.rbegin(); it != index_.rend(); ++it)
	{
		if (it->first.second != symbol_id) continue;
		std::vector<ParsedStagingRecord> records;
		Status status = load_page(it->second.segment_id, it->second.page_offset,
			it->second.page_length, &records);
		if (!status.ok())
		{
			return status;
		}
		TimeId max_time = 0;
		bool found = false;
		for (size_t r = 0; r < records.size(); ++r)
		{
			for (size_t i = 0; i < records[r].bars.size(); ++i)
			{
				if (records[r].bars[i].symbol_id == symbol_id &&
					records[r].bars[i].time_id > max_time)
				{
					max_time = records[r].bars[i].time_id;
					found = true;
				}
			}
		}
		if (found)
		{
			*out = max_time;
			return Status::Ok();
		}
	}
	return Status::Error(ErrorCode::NotFound, "no staging history for symbol");
}

Status StagingStore::snapshot(std::vector<StockTimeBlock>* blocks,
								 std::vector<ActiveBar>* bars,
								 std::vector<std::vector<uint8_t> >* frame_bytes) const {
	if (blocks == NULL || bars == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "staging snapshot outputs are required");
	}
	if (!status_.ok()) {
		return status_;
	}
	blocks->clear();
	bars->clear();
	if (frame_bytes != NULL) {
		frame_bytes->clear();
	}
	// Walk index in order; load each page once and extract records in index order.
	std::set<std::pair<uint32_t, uint64_t> > visited_pages;
	std::map<std::pair<TimeId, SymbolId>, ParsedStagingRecord> by_key;
	for (std::map<std::pair<TimeId, SymbolId>, Locator>::const_iterator it = index_.begin();
		 it != index_.end(); ++it) {
		const std::pair<uint32_t, uint64_t> page_key(it->second.segment_id, it->second.page_offset);
		if (visited_pages.insert(page_key).second) {
			std::vector<ParsedStagingRecord> records;
			Status status = load_page(it->second.segment_id, it->second.page_offset,
				it->second.page_length, &records);
			if (!status.ok()) {
				return status;
			}
			for (size_t r = 0; r < records.size(); ++r) {
				const std::pair<TimeId, SymbolId> key(
					records[r].block.key.time_block_id, records[r].block.key.symbol_id);
				by_key.insert(std::make_pair(key, records[r]));
			}
		}
	}
	// Emit in index order so snapshot matches index ordering.
	for (std::map<std::pair<TimeId, SymbolId>, Locator>::const_iterator it = index_.begin();
		 it != index_.end(); ++it) {
		std::map<std::pair<TimeId, SymbolId>, ParsedStagingRecord>::iterator found =
			by_key.find(it->first);
		if (found == by_key.end()) {
			return Status::Error(ErrorCode::CorruptData, "staging snapshot missing record");
		}
		blocks->push_back(found->second.block);
		if (frame_bytes != NULL) {
			frame_bytes->push_back(found->second.frame_bytes);
		}
		bars->insert(bars->end(), found->second.bars.begin(), found->second.bars.end());
	}
	std::sort(bars->begin(), bars->end(), ActiveBarOrder);
	return Status::Ok();
}

struct RawStagingRecord
{
	BlockKey key;
	uint64_t day_presence;
	BlockOff position_count;
	std::vector<uint8_t> frame_bytes;
};

static Status ParseStagingPageRaw(const std::vector<uint8_t> &bytes,
							  Frequency frequency,
							  std::vector<RawStagingRecord> *records,
							  uint32_t *page_id)
{
	if (records == NULL || page_id == NULL || bytes.size() < kStagingPageHeaderBytes)
		return Status::Error(ErrorCode::CorruptData, "truncated staging page");
	size_t cursor = 0;
	uint8_t magic[4] = {};
	uint8_t version = 0;
	uint8_t stored_frequency = 0;
	uint16_t reserved = 0;
	uint64_t stored_page_id = 0;
	uint32_t count = 0;
	uint32_t directory_size = 0;
	uint32_t payload_size = 0;
	if (!GetU8(bytes, &cursor, &magic[0]) || !GetU8(bytes, &cursor, &magic[1]) ||
		!GetU8(bytes, &cursor, &magic[2]) || !GetU8(bytes, &cursor, &magic[3]) ||
		magic[0] != 'Z' || magic[1] != 'S' || magic[2] != 'P' || magic[3] != '5' ||
		!GetU8(bytes, &cursor, &version) || !GetU8(bytes, &cursor, &stored_frequency) ||
		!GetU16(bytes, &cursor, &reserved) || !GetU64(bytes, &cursor, &stored_page_id) ||
		!GetU32(bytes, &cursor, &count) ||
		!GetU32(bytes, &cursor, &directory_size) || !GetU32(bytes, &cursor, &payload_size) ||
		version != kStagingVersion || stored_frequency != static_cast<uint8_t>(frequency) ||
		reserved != 0 || stored_page_id == 0 ||
		stored_page_id > std::numeric_limits<uint32_t>::max() || count == 0 ||
		kStagingPageHeaderBytes + static_cast<size_t>(directory_size) + payload_size != bytes.size())
	{
		return Status::Error(ErrorCode::CorruptData, "invalid staging page header");
	}
	*page_id = static_cast<uint32_t>(stored_page_id);
	const size_t directory_end = cursor + directory_size;
	const size_t payload_start = directory_end;
	records->clear();
	records->reserve(count);
	for (uint32_t record_index = 0; record_index < count; ++record_index)
	{
		uint32_t symbol_id = 0;
		uint32_t block_id = 0;
		uint16_t position_count = 0;
		uint64_t record_day_presence = 0;
		uint16_t present_size = 0;
		uint16_t frame_count = 0;
		if (!GetU32(bytes, &cursor, &symbol_id) || !GetU32(bytes, &cursor, &block_id) ||
			!GetU16(bytes, &cursor, &position_count) ||
			!GetU64(bytes, &cursor, &record_day_presence) ||
			!GetU16(bytes, &cursor, &present_size) || symbol_id == kInvalidSymbolId ||
			position_count == 0 || present_size != (position_count + 7) / 8 ||
			cursor + present_size > directory_end)
		{
			return Status::Error(ErrorCode::CorruptData, "invalid staging record directory");
		}
		cursor += present_size;
		if (!GetU16(bytes, &cursor, &frame_count) || frame_count == 0)
			return Status::Error(ErrorCode::CorruptData, "staging record has no frames");
		if (frame_count != 1)
			return Status::Error(ErrorCode::CorruptData,
				"staging record must contain one bar block frame");
		uint32_t frame_offset = 0;
		uint32_t frame_length = 0;
		if (!GetU32(bytes, &cursor, &frame_offset) || !GetU32(bytes, &cursor, &frame_length) ||
			frame_offset > payload_size || frame_length > payload_size - frame_offset)
			return Status::Error(ErrorCode::CorruptData, "invalid staging frame locator");
		RawStagingRecord record = {};
		record.key.symbol_id = symbol_id;
		record.key.time_block_id = block_id;
		record.day_presence = record_day_presence;
		record.position_count = position_count;
		record.frame_bytes.assign(bytes.begin() + payload_start + frame_offset,
			bytes.begin() + payload_start + frame_offset + frame_length);
		records->push_back(record);
	}
	if (cursor != directory_end)
		return Status::Error(ErrorCode::CorruptData, "trailing staging directory bytes");
	return Status::Ok();
}

Status StagingStore::snapshot(std::vector<BlockKey> *keys,
						 std::vector<uint64_t> *day_presence,
						 std::vector<BlockOff> *position_counts,
						 std::vector<std::vector<uint8_t> > *frame_bytes) const
{
	if (keys == NULL || day_presence == NULL || position_counts == NULL || frame_bytes == NULL)
		return Status::Error(ErrorCode::InvalidArgument, "staging raw snapshot outputs are required");
	if (!status_.ok())
		return status_;
	keys->clear();
	day_presence->clear();
	position_counts->clear();
	frame_bytes->clear();
	// Walk the index, loading each page once. Records are keyed so we can emit
	// them in index order below.
	std::set<std::pair<uint32_t, uint64_t> > visited_pages;
	std::map<std::pair<TimeId, SymbolId>, RawStagingRecord> by_key;
	for (std::map<std::pair<TimeId, SymbolId>, Locator>::const_iterator it = index_.begin();
		 it != index_.end(); ++it)
	{
		const std::pair<uint32_t, uint64_t> page_key(it->second.segment_id, it->second.page_offset);
		if (visited_pages.insert(page_key).second)
		{
			std::vector<uint8_t> page_bytes;
			Status status = load_page_bytes(it->second.segment_id, it->second.page_offset,
				it->second.page_length, &page_bytes);
			if (!status.ok())
				return status;
			std::vector<RawStagingRecord> records;
			uint32_t page_id = 0;
			status = ParseStagingPageRaw(page_bytes, frequency_, &records, &page_id);
			if (!status.ok())
				return status;
			for (size_t r = 0; r < records.size(); ++r)
			{
				const std::pair<TimeId, SymbolId> key(
					records[r].key.time_block_id, records[r].key.symbol_id);
				by_key.insert(std::make_pair(key, records[r]));
			}
		}
	}
	// Emit in index order.
	for (std::map<std::pair<TimeId, SymbolId>, Locator>::const_iterator it = index_.begin();
		 it != index_.end(); ++it)
	{
		std::map<std::pair<TimeId, SymbolId>, RawStagingRecord>::iterator found =
			by_key.find(it->first);
		if (found == by_key.end())
			return Status::Error(ErrorCode::CorruptData, "staging raw snapshot missing record");
		keys->push_back(found->second.key);
		day_presence->push_back(found->second.day_presence);
		position_counts->push_back(found->second.position_count);
		frame_bytes->push_back(found->second.frame_bytes);
	}
	return Status::Ok();
}

// =============================================================================
// Vault Blobs, Index, and Shared Read Caches
//
// Vault stores one or more chronologically ordered blobs for each symbol. A
// blob has a compact block directory, a sparse frame locator array, and raw
// ZMF3 frame bytes. The frame codec remains independently owned by codec.cpp;
// ZVB6 only supplies durable framing and selective disk locations.
// =============================================================================

static const uint8_t kVaultVersion = 2;
static const size_t kVaultBlobMaxBytes = 256 * 1024;
static const uint64_t kVaultSegmentTargetBytes = 256ULL * 1024 * 1024;
// Unified page cache shared by Vault and Staging stores. Both stores use the
// same two-level cache design: compressed raw bytes (blob/page) and decoded
// in-memory records. A single LRU counter and shared capacity budget keep the
// replacement policy consistent across layers. Sizes are configurable via
// the public SetCacheSizes() API (declared in zstfs/market.h).
static const size_t kDefaultCompressedCacheBytes = 128 * 1024 * 1024;
static const size_t kDefaultDecodedCacheBytes = 32 * 1024 * 1024;
static size_t g_compressed_cache_limit = kDefaultCompressedCacheBytes;
static size_t g_decoded_cache_limit = kDefaultDecodedCacheBytes;

void SetCacheSizes(size_t compressed_cache_bytes, size_t decoded_cache_bytes) {
	g_compressed_cache_limit = compressed_cache_bytes;
	g_decoded_cache_limit = decoded_cache_bytes;
}

size_t CompressedCacheBytes() {
	return g_compressed_cache_limit;
}

size_t DecodedCacheBytes() {
	return g_decoded_cache_limit;
}

// CachedPage is the compressed-cache entry: one raw blob (vault) or one raw
// page (staging), identified by its store type, market, frequency, segment,
// and byte offset.
struct CachedPage {
	CacheStoreType store_type;
	uint64_t runtime_market_id;
	Frequency frequency;
	uint32_t segment_id;
	uint64_t offset;
	uint64_t last_use;
	std::vector<uint8_t> bytes;
};

// CachedVaultBlock is one decoded block out of a vault blob.
struct CachedVaultBlock {
	uint64_t runtime_market_id;
	Frequency frequency;
	uint32_t segment_id;
	uint64_t blob_offset;
	TimeId time_block_id;
	uint64_t last_use;
	std::vector<ActiveBar> bars;
};

// CachedStagingPage is one fully decoded staging page with all its records.
struct CachedStagingPage {
	uint64_t runtime_market_id;
	Frequency frequency;
	uint32_t segment_id;
	uint64_t page_offset;
	uint64_t last_use;
	std::vector<ParsedStagingRecord> records;
};

struct VaultPendingBlock {
	StockTimeBlock block;
	std::vector<uint8_t> present;
	std::vector<uint8_t> frame_bytes;
};

struct VaultBlockDirectory {
	TimeId time_block_id;
	uint64_t day_presence;
	BlockOff position_count;
	uint32_t present_offset;
	uint16_t present_length;
};

struct VaultFrameDirectory {
	FieldId field;
	TimeId time_block_id;
	BlockOff first_offset;
	BlockOff sample_count;
	uint32_t frame_offset;
	uint32_t frame_length;
};

static std::mutex g_cache_mutex;
static uint64_t g_cache_tick = 0;
static size_t g_compressed_cache_size = 0;
static size_t g_decoded_cache_size = 0;
static std::vector<CachedPage> g_compressed_cache;
static std::vector<CachedVaultBlock> g_vault_decoded_cache;
static std::vector<CachedStagingPage> g_staging_decoded_cache;

static std::string VaultSegmentPath(const std::string& path, uint32_t segment_id) {
	char name[64];
	std::snprintf(name, sizeof(name), "vault-%04u.seg", segment_id);
	return path + "/" + name;
}

static bool VaultBlockContainsTime(TimeId first_block_id,
						   TimeId last_block_id,
						   TimeId time_id) {
	const TimeId day = time_day(time_id);
	// Locator endpoints name block starts. The final block therefore covers its
	// full 64-day calendar address range rather than only its first day.
	return day >= time_day(first_block_id) &&
		day < time_day(last_block_id) + kDailyTimeBlockDayLength;
}

static size_t VaultBarsBytes(const std::vector<ActiveBar>& bars) {
	return bars.size() * sizeof(ActiveBar);
}

static size_t StagingPageDecodedBytes(const std::vector<ParsedStagingRecord>& records) {
	size_t total = 0;
	for (size_t i = 0; i < records.size(); ++i) {
		total += records[i].bars.size() * sizeof(ActiveBar) +
			records[i].block.positions.size() * sizeof(BlockBar) +
			records[i].frame_bytes.size();
	}
	return total;
}

// --- Compressed cache (shared: vault blobs + staging pages) ---

static void InsertCompressedCache(CacheStoreType store_type,
						  uint64_t runtime_market_id,
						  Frequency frequency,
						  uint32_t segment_id,
						  uint64_t offset,
						  const std::vector<uint8_t>& bytes) {
	if (bytes.size() > g_compressed_cache_limit) {
		return;
	}
	std::lock_guard<std::mutex> lock(g_cache_mutex);
	while (!g_compressed_cache.empty() &&
		g_compressed_cache_size + bytes.size() > g_compressed_cache_limit) {
		size_t oldest = 0;
		for (size_t i = 1; i < g_compressed_cache.size(); ++i) {
			if (g_compressed_cache[i].last_use < g_compressed_cache[oldest].last_use) {
				oldest = i;
			}
		}
		g_compressed_cache_size -= g_compressed_cache[oldest].bytes.size();
		g_compressed_cache.erase(g_compressed_cache.begin() + oldest);
	}
	CachedPage entry = {};
	entry.store_type = store_type;
	entry.runtime_market_id = runtime_market_id;
	entry.frequency = frequency;
	entry.segment_id = segment_id;
	entry.offset = offset;
	entry.last_use = ++g_cache_tick;
	entry.bytes = bytes;
	g_compressed_cache_size += entry.bytes.size();
	g_compressed_cache.push_back(entry);
}

static bool GetCompressedCache(CacheStoreType store_type,
						uint64_t runtime_market_id,
						Frequency frequency,
						uint32_t segment_id,
						uint64_t offset,
						std::vector<uint8_t>* bytes) {
	std::lock_guard<std::mutex> lock(g_cache_mutex);
	for (size_t i = 0; i < g_compressed_cache.size(); ++i) {
		CachedPage& entry = g_compressed_cache[i];
		if (entry.store_type == store_type && entry.runtime_market_id == runtime_market_id &&
			entry.frequency == frequency && entry.segment_id == segment_id &&
			entry.offset == offset) {
			entry.last_use = ++g_cache_tick;
			*bytes = entry.bytes;
			return true;
		}
	}
	return false;
}

// --- Eviction helper for decoded cache: finds oldest among both vault and
// staging decoded entries, then removes it. Both caches draw from the same
// capacity budget so they share one LRU clock.
static void EvictOldestDecodedLocked() {
	uint64_t oldest_tick = UINT64_MAX;
	bool is_vault = true;
	size_t oldest_index = 0;
	for (size_t i = 0; i < g_vault_decoded_cache.size(); ++i) {
		if (g_vault_decoded_cache[i].last_use < oldest_tick) {
			oldest_tick = g_vault_decoded_cache[i].last_use;
			oldest_index = i;
			is_vault = true;
		}
	}
	for (size_t i = 0; i < g_staging_decoded_cache.size(); ++i) {
		if (g_staging_decoded_cache[i].last_use < oldest_tick) {
			oldest_tick = g_staging_decoded_cache[i].last_use;
			oldest_index = i;
			is_vault = false;
		}
	}
	if (is_vault && !g_vault_decoded_cache.empty()) {
		g_decoded_cache_size -= VaultBarsBytes(g_vault_decoded_cache[oldest_index].bars);
		g_vault_decoded_cache.erase(g_vault_decoded_cache.begin() + oldest_index);
	} else if (!is_vault && !g_staging_decoded_cache.empty()) {
		g_decoded_cache_size -= StagingPageDecodedBytes(g_staging_decoded_cache[oldest_index].records);
		g_staging_decoded_cache.erase(g_staging_decoded_cache.begin() + oldest_index);
	}
}

// --- Vault decoded cache ---

static void InsertVaultDecodedCache(uint64_t runtime_market_id,
						  Frequency frequency,
						  uint32_t segment_id,
						  uint64_t blob_offset,
						  TimeId time_block_id,
						  const std::vector<ActiveBar>& bars) {
	const size_t bytes = VaultBarsBytes(bars);
	if (bytes > g_decoded_cache_limit) {
		return;
	}
	std::lock_guard<std::mutex> lock(g_cache_mutex);
	while (!g_vault_decoded_cache.empty() && !g_staging_decoded_cache.empty() &&
		g_decoded_cache_size + bytes > g_decoded_cache_limit) {
		EvictOldestDecodedLocked();
	}
	// If one cache is empty but we're still over budget, it means the other
	// cache alone exceeds the limit. Keep evicting from the non-empty one.
	while (g_decoded_cache_size + bytes > g_decoded_cache_limit &&
		(!g_vault_decoded_cache.empty() || !g_staging_decoded_cache.empty())) {
		EvictOldestDecodedLocked();
	}
	CachedVaultBlock entry = {};
	entry.runtime_market_id = runtime_market_id;
	entry.frequency = frequency;
	entry.segment_id = segment_id;
	entry.blob_offset = blob_offset;
	entry.time_block_id = time_block_id;
	entry.last_use = ++g_cache_tick;
	entry.bars = bars;
	g_decoded_cache_size += bytes;
	g_vault_decoded_cache.push_back(entry);
}

static bool GetVaultDecodedCache(uint64_t runtime_market_id,
						 Frequency frequency,
						 uint32_t segment_id,
						 uint64_t blob_offset,
						 TimeId time_block_id,
						 std::vector<ActiveBar>* bars) {
	std::lock_guard<std::mutex> lock(g_cache_mutex);
	for (size_t i = 0; i < g_vault_decoded_cache.size(); ++i) {
		CachedVaultBlock& entry = g_vault_decoded_cache[i];
		if (entry.runtime_market_id == runtime_market_id && entry.frequency == frequency &&
			entry.segment_id == segment_id && entry.blob_offset == blob_offset &&
			entry.time_block_id == time_block_id) {
			entry.last_use = ++g_cache_tick;
			*bars = entry.bars;
			return true;
		}
	}
	return false;
}

// --- Staging decoded cache ---

static void InsertStagingDecodedCache(uint64_t runtime_market_id,
							  Frequency frequency,
							  uint32_t segment_id,
							  uint64_t page_offset,
							  const std::vector<ParsedStagingRecord>& records) {
	const size_t bytes = StagingPageDecodedBytes(records);
	if (bytes > g_decoded_cache_limit) {
		return;
	}
	std::lock_guard<std::mutex> lock(g_cache_mutex);
	while (g_decoded_cache_size + bytes > g_decoded_cache_limit &&
		(!g_vault_decoded_cache.empty() || !g_staging_decoded_cache.empty())) {
		EvictOldestDecodedLocked();
	}
	CachedStagingPage entry = {};
	entry.runtime_market_id = runtime_market_id;
	entry.frequency = frequency;
	entry.segment_id = segment_id;
	entry.page_offset = page_offset;
	entry.last_use = ++g_cache_tick;
	entry.records = records;
	g_decoded_cache_size += bytes;
	g_staging_decoded_cache.push_back(entry);
}

static bool GetStagingDecodedCache(uint64_t runtime_market_id,
							Frequency frequency,
							uint32_t segment_id,
							uint64_t page_offset,
							std::vector<ParsedStagingRecord>* records) {
	std::lock_guard<std::mutex> lock(g_cache_mutex);
	for (size_t i = 0; i < g_staging_decoded_cache.size(); ++i) {
		CachedStagingPage& entry = g_staging_decoded_cache[i];
		if (entry.runtime_market_id == runtime_market_id && entry.frequency == frequency &&
			entry.segment_id == segment_id && entry.page_offset == page_offset) {
			entry.last_use = ++g_cache_tick;
			*records = entry.records;
			return true;
		}
	}
	return false;
}

// Build a pending vault block from StockTimeBlock.
// Similar to MakePendingStagingRecord
static Status MakeVaultPendingBlock(const Calendar& calendar,
							Frequency frequency,
							const StockTimeBlock& block,
							const std::vector<uint8_t>* frame_bytes,
							VaultPendingBlock *output)
{
	if (output == NULL || block.key.symbol_id == kInvalidSymbolId || block.positions.empty())
		return Status::Error(ErrorCode::InvalidArgument, "invalid vault block");
	BlockOff expected_length = 0;
	Status status = calendar.block_length(frequency, block.key.time_block_id, &expected_length);
	if (!status.ok() || block.positions.size() != expected_length)
		return Status::Error(ErrorCode::InvalidArgument, "vault block length does not match calendar");
	output->block = block;
	if (frame_bytes != NULL)
	{
		// When a pre-encoded frame is provided, decode it once to derive the
		// present bitmap. The frame bytes are used as-is for the payload.
		BarBlockFrame frame = {*frame_bytes};
		std::vector<BlockBar> decoded;
		status = DecodeBarBlockFrame(frame, &decoded);
		if (!status.ok() || decoded.size() != block.positions.size())
			return Status::Error(ErrorCode::CorruptData, "invalid source bar block frame");
		output->present.assign((decoded.size() + 7) / 8, 0);
		for (size_t i = 0; i < decoded.size(); ++i)
		{
			if (decoded[i].state != BarState::Missing)
				output->present[i / 8] |= static_cast<uint8_t>(1U << (i % 8));
		}
		output->frame_bytes = *frame_bytes;
		return Status::Ok();
	}
	// No pre-encoded frame: build present from block positions and encode.
	output->present.assign((block.positions.size() + 7) / 8, 0);
	for (size_t offset = 0; offset < block.positions.size(); ++offset)
	{
		if (block.positions[offset].state != BarState::Missing)
			output->present[offset / 8] |= static_cast<uint8_t>(1U << (offset % 8));
	}
	BarBlockFrame frame;
	status = EncodeBarBlockFrame(block.positions, &frame);
	if (status.ok())
		output->frame_bytes.swap(frame.bytes);
	return status;
}

static Status BuildVaultBlob(Frequency frequency,
						 const std::vector<VaultPendingBlock>& blocks,
						 std::vector<uint8_t>* bytes) {
	if (blocks.empty() || bytes == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "vault blob requires blocks");
	}
	const SymbolId symbol_id = blocks[0].block.key.symbol_id;
	std::vector<VaultBlockDirectory> block_directory;
	std::vector<VaultFrameDirectory> frame_directory;
	std::vector<std::vector<uint8_t> > frame_bytes;
	for (size_t i = 0; i < blocks.size(); ++i) {
		if (blocks[i].block.key.symbol_id != symbol_id) {
			return Status::Error(ErrorCode::InvalidArgument, "vault blob mixes symbols");
		}
		VaultBlockDirectory block_entry = {};
		block_entry.time_block_id = blocks[i].block.key.time_block_id;
		block_entry.day_presence = blocks[i].block.day_presence;
		block_entry.position_count = static_cast<BlockOff>(blocks[i].block.positions.size());
		block_entry.present_length = static_cast<uint16_t>(blocks[i].present.size());
		block_directory.push_back(block_entry);
		if (blocks[i].frame_bytes.empty() ||
			blocks[i].frame_bytes.size() > std::numeric_limits<uint32_t>::max()) {
			return Status::Error(ErrorCode::InvalidArgument, "invalid vault bar block frame");
		}
		VaultFrameDirectory frame_entry = {};
		frame_entry.field = FieldId::State;
		frame_entry.time_block_id = blocks[i].block.key.time_block_id;
		frame_entry.first_offset = 0;
		frame_entry.sample_count = block_entry.position_count;
		frame_entry.frame_length = static_cast<uint32_t>(blocks[i].frame_bytes.size());
		frame_directory.push_back(frame_entry);
		frame_bytes.push_back(blocks[i].frame_bytes);
	}
	const size_t header_bytes = 32 + block_directory.size() * 20 + frame_directory.size() * 17;
	if (header_bytes > std::numeric_limits<uint32_t>::max()) {
		return Status::Error(ErrorCode::InvalidArgument, "vault header is too large");
	}
	size_t payload_offset = header_bytes;
	for (size_t i = 0; i < block_directory.size(); ++i) {
		block_directory[i].present_offset = static_cast<uint32_t>(payload_offset);
		payload_offset += blocks[i].present.size();
	}
	for (size_t i = 0; i < frame_directory.size(); ++i) {
		frame_directory[i].frame_offset = static_cast<uint32_t>(payload_offset);
		payload_offset += frame_bytes[i].size();
	}
	if (payload_offset > kVaultBlobMaxBytes || payload_offset > std::numeric_limits<uint32_t>::max()) {
		return Status::Error(ErrorCode::InvalidArgument, "vault blob exceeds 16 MiB");
	}
	bytes->clear();
	bytes->reserve(payload_offset);
	PutU8(bytes, 'Z'); PutU8(bytes, 'V'); PutU8(bytes, 'B'); PutU8(bytes, '6');
	PutU8(bytes, kVaultVersion); PutU8(bytes, static_cast<uint8_t>(frequency)); PutU16(bytes, 0);
	PutU32(bytes, symbol_id);
	PutU32(bytes, block_directory.front().time_block_id);
	PutU32(bytes, block_directory.back().time_block_id);
	PutU32(bytes, static_cast<uint32_t>(block_directory.size()));
	PutU32(bytes, static_cast<uint32_t>(frame_directory.size()));
	PutU32(bytes, static_cast<uint32_t>(header_bytes));
	for (size_t i = 0; i < block_directory.size(); ++i) {
		PutU32(bytes, block_directory[i].time_block_id);
		PutU64(bytes, block_directory[i].day_presence);
		PutU16(bytes, block_directory[i].position_count);
		PutU16(bytes, block_directory[i].present_length);
		PutU32(bytes, block_directory[i].present_offset);
	}
	for (size_t i = 0; i < frame_directory.size(); ++i) {
		PutU8(bytes, static_cast<uint8_t>(frame_directory[i].field));
		PutU32(bytes, frame_directory[i].time_block_id);
		PutU16(bytes, frame_directory[i].first_offset);
		PutU16(bytes, frame_directory[i].sample_count);
		PutU32(bytes, frame_directory[i].frame_offset);
		PutU32(bytes, frame_directory[i].frame_length);
	}
	for (size_t i = 0; i < blocks.size(); ++i) {
		bytes->insert(bytes->end(), blocks[i].present.begin(), blocks[i].present.end());
	}
	for (size_t i = 0; i < frame_bytes.size(); ++i) {
		bytes->insert(bytes->end(), frame_bytes[i].begin(), frame_bytes[i].end());
	}
	return bytes->size() == payload_offset ? Status::Ok() :
		Status::Error(ErrorCode::CorruptData, "vault blob layout mismatch");
}

VaultStore::VaultStore(Frequency frequency,
					   const Calendar& calendar,
					   const std::string& frequency_path,
					   uint64_t runtime_market_id)
	: frequency_(frequency),
	  calendar_(calendar),
	  path_(frequency_path),
	  runtime_market_id_(runtime_market_id),
	  status_(Status::Ok()),
	  current_segment_id_(1) {
	status_ = load();
}

Status VaultStore::load() {
	index_.clear();
	current_segment_id_ = 1;
	const std::string index_path = path_ + "/vault-index";
	if (access(index_path.c_str(), F_OK) != 0) {
		return Status::Ok();
	}
	std::vector<uint8_t> bytes;
	if (!ReadFile(index_path, &bytes)) {
		return Status::Error(ErrorCode::IoError, "cannot read vault index");
	}
	size_t offset = 0;
	uint8_t magic[4] = {};
	uint8_t version = 0;
	uint8_t stored_frequency = 0;
	uint16_t reserved = 0;
	uint32_t count = 0;
	if (!GetU8(bytes, &offset, &magic[0]) || !GetU8(bytes, &offset, &magic[1]) ||
		!GetU8(bytes, &offset, &magic[2]) || !GetU8(bytes, &offset, &magic[3]) ||
		!GetU8(bytes, &offset, &version) || !GetU8(bytes, &offset, &stored_frequency) ||
		!GetU16(bytes, &offset, &reserved) || !GetU32(bytes, &offset, &count) ||
		magic[0] != 'Z' || magic[1] != 'V' || magic[2] != 'I' || magic[3] != '6' ||
		version != kVaultVersion || stored_frequency != static_cast<uint8_t>(frequency_)) {
		return Status::Error(ErrorCode::CorruptData, "invalid vault index");
	}
	for (uint32_t i = 0; i < count; ++i) {
		Locator locator = {};
		if (!GetU32(bytes, &offset, &locator.symbol_id) || !GetU32(bytes, &offset, &locator.first_time_block_id) ||
			!GetU32(bytes, &offset, &locator.last_time_block_id) || !GetU32(bytes, &offset, &locator.segment_id) ||
			!GetU64(bytes, &offset, &locator.blob_offset) || !GetU32(bytes, &offset, &locator.blob_length) ||
			locator.symbol_id == kInvalidSymbolId || locator.blob_length == 0 ||
			locator.blob_length > kVaultBlobMaxBytes) {
			return Status::Error(ErrorCode::CorruptData, "invalid vault locator");
		}
		index_.push_back(locator);
		current_segment_id_ = std::max(current_segment_id_, locator.segment_id);
	}
	if (offset != bytes.size()) {
		return Status::Error(ErrorCode::CorruptData, "trailing vault index bytes");
	}
	for (size_t i = 1; i < index_.size(); ++i) {
		if (index_[i - 1].symbol_id > index_[i].symbol_id ||
			(index_[i - 1].symbol_id == index_[i].symbol_id &&
			index_[i - 1].first_time_block_id > index_[i].first_time_block_id)) {
			return Status::Error(ErrorCode::CorruptData, "vault index is not sorted");
		}
	}
	return Status::Ok();
}

Status VaultStore::write_index() const {
	std::vector<uint8_t> bytes;
	PutU8(&bytes, 'Z'); PutU8(&bytes, 'V'); PutU8(&bytes, 'I'); PutU8(&bytes, '6');
	PutU8(&bytes, kVaultVersion); PutU8(&bytes, static_cast<uint8_t>(frequency_)); PutU16(&bytes, 0);
	PutU32(&bytes, static_cast<uint32_t>(index_.size()));
	for (size_t i = 0; i < index_.size(); ++i) {
		PutU32(&bytes, index_[i].symbol_id);
		PutU32(&bytes, index_[i].first_time_block_id);
		PutU32(&bytes, index_[i].last_time_block_id);
		PutU32(&bytes, index_[i].segment_id);
		PutU64(&bytes, index_[i].blob_offset);
		PutU32(&bytes, index_[i].blob_length);
	}
	const std::string temporary_path = path_ + "/vault-index.tmp";
	std::ofstream output(temporary_path.c_str(), std::ios::binary | std::ios::trunc);
	if (!output) {
		return Status::Error(ErrorCode::IoError, "cannot create vault index");
	}
	output.write(reinterpret_cast<const char*>(&bytes[0]), bytes.size());
	output.flush();
	output.close();
	if (!output || std::rename(temporary_path.c_str(), (path_ + "/vault-index").c_str()) != 0) {
		std::remove(temporary_path.c_str());
		return Status::Error(ErrorCode::IoError, "cannot publish vault index");
	}
	return Status::Ok();
}

Status VaultStore::ingest(const std::vector<StockTimeBlock>& blocks,
					  const std::vector<std::vector<uint8_t> >* frame_bytes) {
	if (!status_.ok()) {
		return status_;
	}
	if (frame_bytes != NULL && frame_bytes->size() != blocks.size()) {
		return Status::Error(ErrorCode::InvalidArgument, "vault frame bytes do not match blocks");
	}

	std::map<SymbolId, std::vector<VaultPendingBlock> > by_symbol;
	for (size_t i = 0; i < blocks.size(); ++i) {
		VaultPendingBlock pending;
		const std::vector<uint8_t>* source_frame = frame_bytes == NULL || (*frame_bytes)[i].empty() ?
			NULL : &(*frame_bytes)[i];
		Status status = MakeVaultPendingBlock(calendar_, frequency_, blocks[i], source_frame, &pending);
		if (!status.ok()) {
			return status;
		}
		by_symbol[blocks[i].key.symbol_id].push_back(pending);
	}
	std::vector<Locator> added;
	const auto append_blob = [&](const std::vector<VaultPendingBlock>& group,
								const std::vector<uint8_t>& blob) -> Status {
		const std::string segment_path = VaultSegmentPath(path_, current_segment_id_);
		std::ifstream existing(segment_path.c_str(), std::ios::binary | std::ios::ate);
		uint64_t segment_size = existing ? static_cast<uint64_t>(existing.tellg()) : 0;
		if (segment_size != 0 && segment_size + blob.size() > kVaultSegmentTargetBytes) {
			++current_segment_id_;
			segment_size = 0;
		}
		const std::string write_path = VaultSegmentPath(path_, current_segment_id_);
		std::ofstream output(write_path.c_str(), std::ios::binary | std::ios::app);
		if (!output) {
			return Status::Error(ErrorCode::IoError, "cannot append vault segment");
		}
		output.write(reinterpret_cast<const char*>(&blob[0]), blob.size());
		output.flush();
		if (!output) {
			return Status::Error(ErrorCode::IoError, "cannot write vault blob");
		}
		Locator locator = {};
		locator.symbol_id = group.front().block.key.symbol_id;
		locator.first_time_block_id = group.front().block.key.time_block_id;
		locator.last_time_block_id = group.back().block.key.time_block_id;
		locator.segment_id = current_segment_id_;
		locator.blob_offset = segment_size;
		locator.blob_length = static_cast<uint32_t>(blob.size());
		added.push_back(locator);
		return Status::Ok();
	};
	for (std::map<SymbolId, std::vector<VaultPendingBlock> >::iterator symbol = by_symbol.begin();
		 symbol != by_symbol.end(); ++symbol) {
		std::sort(symbol->second.begin(), symbol->second.end(),
			[](const VaultPendingBlock& left, const VaultPendingBlock& right) {
				return left.block.key.time_block_id < right.block.key.time_block_id;
			});
		std::vector<VaultPendingBlock> group(symbol->second);
		std::vector<uint8_t> blob;
		Status status = BuildVaultBlob(frequency_, group, &blob);
		if (status.ok()) {
			status = append_blob(group, blob);
			if (!status.ok()) {
				return status;
			}
			continue;
		}

		// Oversized symbols retain the existing 16 MiB blob partitioning rule.
		group.clear();
		for (size_t i = 0; i < symbol->second.size(); ++i) {
			group.push_back(symbol->second[i]);
			blob.clear();
			status = BuildVaultBlob(frequency_, group, &blob);
			if (!status.ok()) {
				if (group.size() == 1) {
					return status;
				}
				group.pop_back();
				status = BuildVaultBlob(frequency_, group, &blob);
				if (!status.ok()) {
					return status;
				}
				i--;
			} else if (i + 1 != symbol->second.size()) {
				continue;
			}
			status = append_blob(group, blob);
			if (!status.ok()) {
				return status;
			}
			group.clear();
		}
	}
	index_.insert(index_.end(), added.begin(), added.end());
	std::sort(index_.begin(), index_.end(), [](const Locator& left, const Locator& right) {
		return left.symbol_id != right.symbol_id ? left.symbol_id < right.symbol_id :
			left.first_time_block_id < right.first_time_block_id;
	});
	return write_index();
}

static Status ReadVaultBlob(const std::string& path,
						uint64_t runtime_market_id,
						Frequency frequency,
						uint32_t segment_id,
						uint64_t blob_offset,
						uint32_t blob_length,
						std::vector<uint8_t>* bytes) {
	if (GetCompressedCache(CacheStoreType::Vault, runtime_market_id, frequency,
			segment_id, blob_offset, bytes)) {
		return Status::Ok();
	}
	std::ifstream input(VaultSegmentPath(path, segment_id).c_str(), std::ios::binary);
	if (!input) {
		return Status::Error(ErrorCode::IoError, "cannot read vault segment");
	}
	input.seekg(static_cast<std::streamoff>(blob_offset));
	bytes->assign(blob_length, 0);
	input.read(reinterpret_cast<char*>(&(*bytes)[0]), blob_length);
	if (input.gcount() != static_cast<std::streamsize>(blob_length)) {
		return Status::Error(ErrorCode::CorruptData, "truncated vault blob");
	}
	InsertCompressedCache(CacheStoreType::Vault, runtime_market_id, frequency,
		segment_id, blob_offset, *bytes);
	return Status::Ok();
}

static Status DecodeVaultBlock(const Calendar& calendar,
						   Frequency frequency,
						   uint64_t runtime_market_id,
						   uint32_t segment_id,
						   uint64_t blob_offset,
						   const std::vector<uint8_t>& bytes,
						   SymbolId symbol_id,
						   TimeId wanted_block_id,
						   std::vector<ActiveBar>* output,
						   std::vector<uint8_t>* frame_bytes) {
	if (frame_bytes == NULL &&
		GetVaultDecodedCache(runtime_market_id, frequency, segment_id, blob_offset, wanted_block_id, output)) {
		return Status::Ok();
	}
	size_t offset = 0;
	uint8_t magic[4] = {};
	uint8_t version = 0;
	uint8_t stored_frequency = 0;
	uint16_t reserved = 0;
	uint32_t stored_symbol = 0;
	uint32_t first_block = 0;
	uint32_t last_block = 0;
	uint32_t block_count = 0;
	uint32_t frame_count = 0;
	uint32_t header_bytes = 0;
	if (!GetU8(bytes, &offset, &magic[0]) || !GetU8(bytes, &offset, &magic[1]) ||
		!GetU8(bytes, &offset, &magic[2]) || !GetU8(bytes, &offset, &magic[3]) ||
		!GetU8(bytes, &offset, &version) || !GetU8(bytes, &offset, &stored_frequency) ||
		!GetU16(bytes, &offset, &reserved) || !GetU32(bytes, &offset, &stored_symbol) ||
		!GetU32(bytes, &offset, &first_block) || !GetU32(bytes, &offset, &last_block) ||
		!GetU32(bytes, &offset, &block_count) || !GetU32(bytes, &offset, &frame_count) ||
		!GetU32(bytes, &offset, &header_bytes) || magic[0] != 'Z' || magic[1] != 'V' ||
		magic[2] != 'B' || magic[3] != '6' || version != kVaultVersion ||
		stored_frequency != static_cast<uint8_t>(frequency) || stored_symbol != symbol_id ||
		header_bytes > bytes.size()) {
		return Status::Error(ErrorCode::CorruptData, "invalid vault blob header");
	}
	std::vector<VaultBlockDirectory> blocks;
	for (uint32_t i = 0; i < block_count; ++i) {
		VaultBlockDirectory entry = {};
		if (!GetU32(bytes, &offset, &entry.time_block_id) || !GetU64(bytes, &offset, &entry.day_presence) ||
			!GetU16(bytes, &offset, &entry.position_count) || !GetU16(bytes, &offset, &entry.present_length) ||
			!GetU32(bytes, &offset, &entry.present_offset) || entry.position_count == 0 ||
			entry.present_length != (entry.position_count + 7) / 8 ||
			entry.present_offset > bytes.size() || entry.present_length > bytes.size() - entry.present_offset) {
			return Status::Error(ErrorCode::CorruptData, "invalid vault block directory");
		}
		blocks.push_back(entry);
	}
	std::vector<VaultFrameDirectory> frames;
	for (uint32_t i = 0; i < frame_count; ++i) {
		VaultFrameDirectory entry = {};
		uint8_t field = 0;
		if (!GetU8(bytes, &offset, &field) || !GetU32(bytes, &offset, &entry.time_block_id) ||
			!GetU16(bytes, &offset, &entry.first_offset) || !GetU16(bytes, &offset, &entry.sample_count) ||
			!GetU32(bytes, &offset, &entry.frame_offset) || !GetU32(bytes, &offset, &entry.frame_length) ||
			field > static_cast<uint8_t>(FieldId::Volume) || entry.frame_length == 0 ||
			entry.frame_offset > bytes.size() || entry.frame_length > bytes.size() - entry.frame_offset) {
			return Status::Error(ErrorCode::CorruptData, "invalid vault frame locator");
		}
		entry.field = static_cast<FieldId>(field);
		frames.push_back(entry);
	}
	if (offset != header_bytes || wanted_block_id < first_block || wanted_block_id > last_block) {
		return Status::Error(ErrorCode::CorruptData, "invalid vault blob directory size");
	}
	const VaultBlockDirectory* block = NULL;
	for (size_t i = 0; i < blocks.size(); ++i) {
		if (blocks[i].time_block_id == wanted_block_id) {
			block = &blocks[i];
			break;
		}
	}
	if (block == NULL) {
		return Status::Error(ErrorCode::NotFound, "vault block was not found");
	}
	const VaultFrameDirectory* frame_directory = NULL;
	for (size_t i = 0; i < frames.size(); ++i) {
		if (frames[i].time_block_id != wanted_block_id) {
			continue;
		}
		if (frame_directory != NULL || frames[i].field != FieldId::State ||
			frames[i].first_offset != 0 || frames[i].sample_count != block->position_count) {
			return Status::Error(ErrorCode::CorruptData, "invalid vault bar block locator");
		}
		frame_directory = &frames[i];
	}
	if (frame_directory == NULL) {
		return Status::Error(ErrorCode::CorruptData, "vault block has no bar block frame");
	}
	BarBlockFrame frame;
	frame.bytes.assign(bytes.begin() + frame_directory->frame_offset,
		bytes.begin() + frame_directory->frame_offset + frame_directory->frame_length);
	std::vector<BlockBar> positions;
	Status status = DecodeBarBlockFrame(frame, &positions);
	if (!status.ok() || positions.size() != block->position_count) {
		return Status::Error(ErrorCode::CorruptData, "cannot decode vault block");
	}
	if (frame_bytes != NULL) {
		*frame_bytes = frame.bytes;
	}
	std::vector<TimeId> time_ids;
	status = StagingTimeIds(calendar, frequency, wanted_block_id, block->position_count, &time_ids);
	if (!status.ok()) {
		return Status::Error(ErrorCode::CorruptData, "cannot map vault block times");
	}
	output->clear();
	for (size_t i = 0; i < positions.size(); ++i) {
		if ((bytes[block->present_offset + i / 8] & static_cast<uint8_t>(1U << (i % 8))) == 0) {
			continue;
		}
		ActiveBar bar = {};
		bar.symbol_id = symbol_id;
		bar.time_id = time_ids[i];
		bar.bar = positions[i];
		output->push_back(bar);
	}
	InsertVaultDecodedCache(runtime_market_id, frequency, segment_id, blob_offset,
		wanted_block_id, *output);
	return Status::Ok();
}

bool VaultStore::contains(SymbolId symbol_id, TimeId time_id) const {
	BlockBar bar;
	return get(symbol_id, time_id, &bar).ok();
}

Status VaultStore::get(SymbolId symbol_id, TimeId time_id, BlockBar* out) const {
	if (out == NULL || symbol_id == kInvalidSymbolId) {
		return Status::Error(ErrorCode::InvalidArgument, "vault read output and symbol are required");
	}
	if (!status_.ok()) {
		return status_;
	}
	Status first_error = Status::Error(ErrorCode::NotFound, "vault bar was not found");
	for (size_t i = 0; i < index_.size(); ++i) {
		const Locator& locator = index_[i];
		if (locator.symbol_id != symbol_id || !VaultBlockContainsTime(locator.first_time_block_id,
			locator.last_time_block_id, time_id)) {
			continue;
		}
		std::vector<uint8_t> bytes;
		Status status = ReadVaultBlob(path_, runtime_market_id_, frequency_, locator.segment_id,
			locator.blob_offset, locator.blob_length, &bytes);
		if (!status.ok()) {
			return status.code() == ErrorCode::IoError ? status :
				Status::Error(ErrorCode::CorruptData, status.message());
		}
		std::vector<ActiveBar> bars;
		const TimeId block_day = time_day(time_id) -
			(time_day(time_id) % kDailyTimeBlockDayLength);
		const TimeId block_id = daily_bar_id(block_day);
		status = DecodeVaultBlock(calendar_, frequency_, runtime_market_id_, locator.segment_id,
			locator.blob_offset, bytes, symbol_id, block_id, &bars, NULL);
		if (!status.ok()) {
			if (status.code() == ErrorCode::NotFound) {
				continue;
			}
			return Status::Error(ErrorCode::CorruptData, status.message());
		}
		for (size_t j = 0; j < bars.size(); ++j) {
			if (bars[j].time_id == time_id) {
				*out = bars[j].bar;
				return Status::Ok();
			}
		}
	}
	return first_error;
}

Status VaultStore::range(const std::vector<SymbolId>& symbol_ids,
					 TimeId begin,
					 TimeId end,
					 std::vector<ActiveBar>* out) const {
	if (out == NULL || begin > end) {
		return Status::Error(ErrorCode::InvalidArgument, "invalid vault range");
	}
	out->clear();
	if (!status_.ok()) {
		return status_;
	}
	std::set<SymbolId> requested(symbol_ids.begin(), symbol_ids.end());
	bool corrupt = false;
	for (size_t i = 0; i < index_.size(); ++i) {
		const Locator& locator = index_[i];
		if (requested.find(locator.symbol_id) == requested.end() ||
			time_day(locator.last_time_block_id) + kDailyTimeBlockDayLength <= time_day(begin) ||
			time_day(locator.first_time_block_id) > time_day(end)) {
			continue;
		}
		std::vector<uint8_t> bytes;
		Status status = ReadVaultBlob(path_, runtime_market_id_, frequency_, locator.segment_id,
			locator.blob_offset, locator.blob_length, &bytes);
		if (!status.ok()) {
			corrupt = true;
			continue;
		}
		for (TimeId block_id = locator.first_time_block_id; block_id <= locator.last_time_block_id;
			 block_id = daily_bar_id(time_day(block_id) + 64)) {
			std::vector<ActiveBar> bars;
			status = DecodeVaultBlock(calendar_, frequency_, runtime_market_id_, locator.segment_id,
				locator.blob_offset, bytes, locator.symbol_id, block_id, &bars, NULL);
			if (!status.ok()) {
				if (status.code() != ErrorCode::NotFound) {
					corrupt = true;
				}
				continue;
			}
			for (size_t j = 0; j < bars.size(); ++j) {
				if (bars[j].time_id >= begin && bars[j].time_id <= end) {
					out->push_back(bars[j]);
				}
			}
			if (block_id > std::numeric_limits<TimeId>::max() - kTimeIdDayStep * 64) {
				break;
			}
		}
	}
	std::sort(out->begin(), out->end(), ActiveBarOrder);
	return corrupt ? Status::Error(ErrorCode::CorruptData, "one or more vault blocks are corrupt") : Status::Ok();
}

Status VaultStore::latest_time(SymbolId symbol_id, TimeId *out) const
{
	// The vault index is a flat vector ordered by symbol. Walk it to find the
	// locator with the highest last_time_block_id for this symbol, then load
	// that blob and scan for the maximum time_id. In the common case the
	// symbol has one blob and we pay exactly one blob load.
	if (!status_.ok())
	{
		return status_;
	}
	if (symbol_id == kInvalidSymbolId || out == NULL)
	{
		return Status::Error(ErrorCode::InvalidArgument, "invalid latest_time arguments");
	}
	int best_index = -1;
	TimeId best_last_block = 0;
	for (size_t i = 0; i < index_.size(); ++i)
	{
		const Locator &locator = index_[i];
		if (locator.symbol_id != symbol_id) continue;
		if (best_index == -1 || locator.last_time_block_id > best_last_block)
		{
			best_index = static_cast<int>(i);
			best_last_block = locator.last_time_block_id;
		}
	}
	if (best_index == -1)
	{
		return Status::Error(ErrorCode::NotFound, "no vault history for symbol");
	}
	const Locator &best = index_[best_index];
	std::vector<uint8_t> bytes;
	Status status = ReadVaultBlob(path_, runtime_market_id_, frequency_, best.segment_id,
		best.blob_offset, best.blob_length, &bytes);
	if (!status.ok())
	{
		return status.code() == ErrorCode::IoError ? status :
			Status::Error(ErrorCode::CorruptData, status.message());
	}
	TimeId max_time = 0;
	bool found = false;
	for (TimeId block_id = best.first_time_block_id; block_id <= best.last_time_block_id;
		 block_id = daily_bar_id(time_day(block_id) + 64))
	{
		std::vector<ActiveBar> bars;
		Status decode_status = DecodeVaultBlock(calendar_, frequency_, runtime_market_id_,
			best.segment_id, best.blob_offset, bytes, symbol_id, block_id, &bars, NULL);
		if (!decode_status.ok())
		{
			if (decode_status.code() == ErrorCode::NotFound) continue;
			return Status::Error(ErrorCode::CorruptData, decode_status.message());
		}
		for (size_t j = 0; j < bars.size(); ++j)
		{
			if (bars[j].time_id > max_time)
			{
				max_time = bars[j].time_id;
				found = true;
			}
		}
		if (block_id > std::numeric_limits<TimeId>::max() - kTimeIdDayStep * 64)
		{
			break;
		}
	}
	if (!found)
	{
		return Status::Error(ErrorCode::NotFound, "no vault history for symbol");
	}
	*out = max_time;
	return Status::Ok();
}

// Reconstruct logical blocks from explicit bars when copying an immutable
// store. The persistent codecs already retain complete positions, but this
// helper keeps compaction independent from their private blob/page directories.
TimeId CompactionBlockDayLength(Frequency frequency) {
	return frequency == Frequency::Daily ? kDailyTimeBlockDayLength :
		kHourlyTimeBlockDayLength;
}

TimeId CompactionBlockId(Frequency frequency, TimeId time_id) {
	const TimeId length = CompactionBlockDayLength(frequency);
	return daily_bar_id((time_day(time_id) / length) * length);
}

static Status BlocksFromBars(const Calendar& calendar,
					 Frequency frequency,
					 const std::vector<ActiveBar>& bars,
					 std::vector<StockTimeBlock>* blocks) {
	if (blocks == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "block reconstruction output is required");
	}
	std::map<std::pair<SymbolId, TimeId>, std::vector<ActiveBar> > grouped;
	for (size_t i = 0; i < bars.size(); ++i) {
		const TimeId block_id = CompactionBlockId(frequency, bars[i].time_id);
		grouped[std::make_pair(bars[i].symbol_id, block_id)].push_back(bars[i]);
	}
	blocks->clear();
	for (std::map<std::pair<SymbolId, TimeId>, std::vector<ActiveBar> >::const_iterator group =
		 grouped.begin(); group != grouped.end(); ++group) {
		BlockOff position_count = 0;
		Status status = calendar.block_length(frequency, group->first.second, &position_count);
		if (!status.ok()) {
			return status;
		}
		std::vector<TimeId> time_ids;
		status = StagingTimeIds(calendar, frequency, group->first.second, position_count, &time_ids);
		if (!status.ok()) {
			return status;
		}
		StockTimeBlock block = {};
		block.key.symbol_id = group->first.first;
		block.key.time_block_id = group->first.second;
		block.positions.assign(position_count, MissingBlockBar());
		std::vector<bool> occupied(position_count, false);
		for (size_t i = 0; i < group->second.size(); ++i) {
			std::vector<TimeId>::const_iterator position = std::lower_bound(time_ids.begin(),
				time_ids.end(), group->second[i].time_id);
			if (position == time_ids.end() || *position != group->second[i].time_id) {
				return Status::Error(ErrorCode::CorruptData, "stored bar does not fit its block");
			}
			const size_t offset = static_cast<size_t>(position - time_ids.begin());
			if (occupied[offset]) {
				return Status::Error(ErrorCode::Conflict, "duplicate bar in immutable block");
			}
			occupied[offset] = true;
			block.positions[offset] = group->second[i].bar;
			const TimeId day_offset = time_day(group->second[i].time_id) -
				time_day(block.key.time_block_id);
			block.day_presence |= static_cast<uint64_t>(1) << day_offset;
		}
		blocks->push_back(block);
	}
	return Status::Ok();
}

Status VaultStore::snapshot(std::vector<StockTimeBlock>* blocks,
							  std::vector<ActiveBar>* bars,
							  std::vector<std::vector<uint8_t> >* frame_bytes) const {
	if (blocks == NULL || bars == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "vault snapshot outputs are required");
	}
	if (!status_.ok()) {
		return status_;
	}
	std::vector<SymbolId> symbols;
	for (size_t i = 0; i < index_.size(); ++i) {
		if (symbols.empty() || symbols.back() != index_[i].symbol_id) {
			symbols.push_back(index_[i].symbol_id);
		}
	}
	Status status = range(symbols, 0, std::numeric_limits<TimeId>::max(), bars);
	if (!status.ok()) {
		return status;
	}
	status = BlocksFromBars(calendar_, frequency_, *bars, blocks);
	if (!status.ok() || frame_bytes == NULL) {
		return status;
	}

	frame_bytes->clear();
	std::map<std::pair<TimeId, SymbolId>, std::vector<uint8_t> > frames_by_block;
	for (size_t i = 0; i < index_.size(); ++i) {
		const Locator& locator = index_[i];
		std::vector<uint8_t> bytes;
		status = ReadVaultBlob(path_, runtime_market_id_, frequency_, locator.segment_id,
			locator.blob_offset, locator.blob_length, &bytes);
		if (!status.ok()) {
			return status;
		}
		for (TimeId block_id = locator.first_time_block_id;
			 block_id <= locator.last_time_block_id;
			 block_id = daily_bar_id(time_day(block_id) + kDailyTimeBlockDayLength)) {
			std::vector<ActiveBar> decoded;
			std::vector<uint8_t> frame;
			status = DecodeVaultBlock(calendar_, frequency_, runtime_market_id_, locator.segment_id,
				locator.blob_offset, bytes, locator.symbol_id, block_id, &decoded, &frame);
			if (!status.ok()) {
				if (status.code() == ErrorCode::NotFound) {
					continue;
				}
				return status;
			}
			const std::pair<TimeId, SymbolId> key(block_id, locator.symbol_id);
			if (!frames_by_block.insert(std::make_pair(key, frame)).second) {
				return Status::Error(ErrorCode::CorruptData, "duplicate vault block frame");
			}
		}
	}
	for (size_t i = 0; i < blocks->size(); ++i) {
		const std::pair<TimeId, SymbolId> key((*blocks)[i].key.time_block_id,
			(*blocks)[i].key.symbol_id);
		std::map<std::pair<TimeId, SymbolId>, std::vector<uint8_t> >::const_iterator found =
			frames_by_block.find(key);
		if (found == frames_by_block.end()) {
			return Status::Error(ErrorCode::CorruptData, "vault block frame is missing");
		}
		frame_bytes->push_back(found->second);
	}
	return Status::Ok();
}

Status VaultStore::snapshot(std::vector<BlockKey> *keys,
						std::vector<uint64_t> *day_presence,
						std::vector<BlockOff> *position_counts,
						std::vector<std::vector<uint8_t>> *frame_bytes) const
{
	if (keys == NULL || day_presence == NULL || position_counts == NULL || frame_bytes == NULL)
		return Status::Error(ErrorCode::InvalidArgument, "vault raw snapshot outputs are required");
	if (!status_.ok())
		return status_;
	keys->clear();
	day_presence->clear();
	position_counts->clear();
	frame_bytes->clear();
	// Walk the index in order, reading each blob once.
	for (size_t i = 0; i < index_.size(); ++i)
	{
		const Locator &locator = index_[i];
		std::vector<uint8_t> blob;
		Status status = ReadVaultBlob(path_, runtime_market_id_, frequency_,
			locator.segment_id, locator.blob_offset, locator.blob_length, &blob);
		if (!status.ok())
			return status;
		// Parse blob header + directories only; do not decode frame content.
		if (blob.size() < 32)
			return Status::Error(ErrorCode::CorruptData, "truncated vault blob header");
		size_t cursor = 0;
		uint8_t magic[4] = {};
		uint8_t version = 0;
		uint8_t stored_frequency = 0;
		uint16_t reserved = 0;
		uint32_t symbol_id = 0;
		uint32_t first_block = 0;
		uint32_t last_block = 0;
		uint32_t block_count = 0;
		uint32_t frame_count = 0;
		uint32_t header_bytes = 0;
		if (!GetU8(blob, &cursor, &magic[0]) || !GetU8(blob, &cursor, &magic[1]) ||
			!GetU8(blob, &cursor, &magic[2]) || !GetU8(blob, &cursor, &magic[3]) ||
			magic[0] != 'Z' || magic[1] != 'V' || magic[2] != 'B' || magic[3] != '6' ||
			!GetU8(blob, &cursor, &version) || !GetU8(blob, &cursor, &stored_frequency) ||
			!GetU16(blob, &cursor, &reserved) || !GetU32(blob, &cursor, &symbol_id) ||
			!GetU32(blob, &cursor, &first_block) || !GetU32(blob, &cursor, &last_block) ||
			!GetU32(blob, &cursor, &block_count) || !GetU32(blob, &cursor, &frame_count) ||
			!GetU32(blob, &cursor, &header_bytes) || version != kVaultVersion ||
			stored_frequency != static_cast<uint8_t>(frequency_) || reserved != 0 ||
			symbol_id == kInvalidSymbolId || block_count == 0 || frame_count != block_count ||
			header_bytes < 32 + block_count * 20 + frame_count * 17 ||
			header_bytes > blob.size())
		{
			return Status::Error(ErrorCode::CorruptData, "invalid vault blob header");
		}
		// Read block directory (20 bytes each).
		size_t block_dir_cursor = 32;
		for (uint32_t b = 0; b < block_count; ++b)
		{
			uint32_t time_block_id = 0;
			uint64_t block_day_presence = 0;
			uint16_t position_count = 0;
			uint16_t present_length = 0;
			uint32_t present_offset = 0;
			if (!GetU32(blob, &block_dir_cursor, &time_block_id) ||
				!GetU64(blob, &block_dir_cursor, &block_day_presence) ||
				!GetU16(blob, &block_dir_cursor, &position_count) ||
				!GetU16(blob, &block_dir_cursor, &present_length) ||
				!GetU32(blob, &block_dir_cursor, &present_offset) ||
				position_count == 0 || present_length != (position_count + 7) / 8 ||
				present_offset >= blob.size() || present_offset + present_length > blob.size())
			{
				return Status::Error(ErrorCode::CorruptData, "invalid vault block directory");
			}
			BlockKey key = {};
			key.symbol_id = symbol_id;
			key.time_block_id = time_block_id;
			keys->push_back(key);
			day_presence->push_back(block_day_presence);
			position_counts->push_back(position_count);
		}
		// Read frame directory (17 bytes each) and extract frame bytes.
		size_t frame_dir_cursor = 32 + block_count * 20;
		for (uint32_t f = 0; f < frame_count; ++f)
		{
			uint8_t field = 0;
			uint32_t frame_block_id = 0;
			uint16_t first_offset = 0;
			uint16_t sample_count = 0;
			uint32_t frame_offset = 0;
			uint32_t frame_length = 0;
			if (!GetU8(blob, &frame_dir_cursor, &field) ||
				!GetU32(blob, &frame_dir_cursor, &frame_block_id) ||
				!GetU16(blob, &frame_dir_cursor, &first_offset) ||
				!GetU16(blob, &frame_dir_cursor, &sample_count) ||
				!GetU32(blob, &frame_dir_cursor, &frame_offset) ||
				!GetU32(blob, &frame_dir_cursor, &frame_length) ||
				field != static_cast<uint8_t>(FieldId::State) || first_offset != 0 ||
				frame_offset >= blob.size() || frame_length > blob.size() - frame_offset)
			{
				return Status::Error(ErrorCode::CorruptData, "invalid vault frame directory");
			}
			frame_bytes->push_back(std::vector<uint8_t>(
				blob.begin() + frame_offset, blob.begin() + frame_offset + frame_length));
		}
	}
	return Status::Ok();
}

}  // namespace zstfs
