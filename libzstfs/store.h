// store.h
//
// Defines the calendar-addressed block model and the internal Active, Staging,
// and Vault storage layers used by History. Public callers only operate on
// complete Bar records.

#pragma once

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "zstfs/data.h"
#include "zstfs/status.h"
#include "codec.h"
#include "calendar.h"

namespace zstfs {

struct BlockKey {
	SymbolId symbol_id;
	// The block key is the daily TimeId of the block's first trading day.
	TimeId time_block_id;
};

struct StockTimeBlock {
	BlockKey key;
	// One bit identifies whether this symbol has any data on a day in the
	// block. The hourly payload uses the market slot layout only when the
	// corresponding day bit is set; intra-day gaps remain explicit payload
	// positions rather than expanding the day into a fixed 256-slot grid.
	uint64_t day_presence;
	// Entries are addressed by BlockOff, not by adding the offset to the
	// block's TimeId. Hourly entries follow the compact market slot layout.
	std::vector<BlockBar> positions;
};


// ActiveStore keeps mutable positions grouped by block so sealing can hand one
// complete StockTimeBlock to the next layer. It also owns the low-frequency
// recovery-log scheduler: byte thresholds are checked after accepted batches,
// while a private timer flushes dirty records that remain below the threshold.
// The time ID vector is retained
// only for positions that have been written: hourly block offsets are compact
// and cannot be converted back to TimeId with arithmetic alone.
struct ActiveStockBlock {
	std::vector<BlockBar> positions;
	std::vector<TimeId> time_ids;
	std::vector<bool> present;
	std::vector<bool> dirty;
};

struct ActiveTimeBlock {
	BlockOff position_count;
	std::map<SymbolId, ActiveStockBlock> stocks;
};

struct ActiveBar {
	SymbolId symbol_id;
	TimeId time_id;
	BlockBar bar;
};

// ActiveStore is accessed by the foreground History calls and by its private
// periodic flush thread. `mutex_` protects the complete mutable Active state:
// block contents, presence and dirty bitmaps, replay_status_, dirty_bytes_, and
// timer shutdown state. It also covers active.data writes so a flush cannot
// race with a put, read, seal, or another flush. The public methods acquire the
// mutex before touching that state; `flush_locked()` is private and may only be
// called while the mutex is already held. The timer waits on the condition
// variable with the same lock and releases it only while sleeping. Destruction
// first marks the timer stopped under the lock, wakes and joins the timer, then
// performs the final flush after no other thread can access the store.
class ActiveStore {
public:
	ActiveStore(Frequency frequency,
	            const Calendar& calendar,
	            const std::string& market_path,
	            size_t flush_bytes = 1024 * 1024,
	            uint64_t flush_interval_milliseconds = 60 * 1000);
	~ActiveStore();

	Status put(SymbolId symbol_id,
	           TimeId time_id,
	           TimeId block_id,
	           BlockOff block_offset,
	           const BlockBar& bar,
	           bool replay);
	bool contains(SymbolId symbol_id, TimeId time_id) const;
	Status get(SymbolId symbol_id,
	           TimeId block_id,
	           BlockOff block_offset,
	           BlockBar* out) const;
	Status range(const std::vector<SymbolId>& symbol_ids,
	             TimeId begin,
	             TimeId end,
	             std::vector<ActiveBar>* out) const;
	Status flush_if_needed();
	Status flush();
	// collect_before snapshots complete blocks without changing their Active
	// ownership. History removes them only after Staging has published them.
	Status collect_before(TimeId time_id,
	                      std::vector<StockTimeBlock>* sealed,
	                      std::vector<ActiveBar>* sealed_bars);
	Status remove_before(TimeId time_id);
	Status replay_status() const;

private:
	Status flush_locked();
	void run_flush_timer();

	Frequency frequency_;
	const Calendar& calendar_;
	std::string path_;
	std::map<TimeId, ActiveTimeBlock> blocks_;
	Status replay_status_;
	size_t flush_bytes_;
	uint64_t flush_interval_milliseconds_;
	size_t dirty_bytes_;
	bool stop_flush_timer_;
	mutable std::mutex mutex_;
	std::condition_variable flush_condition_;
	std::thread flush_timer_;
};

struct ParsedStagingRecord;

// StagingStore owns immutable complete blocks after Active sealing. Its index
// is ordered by (time_block_id, symbol_id), while pages retain the encoded
// frames and the presence bitmap needed to distinguish padded Missing values
// from explicitly written Missing bars. Page data is loaded on demand through
// the shared compressed+decoded cache; the index alone is always resident.
class StagingStore {
public:
	StagingStore(Frequency frequency,
	             const Calendar& calendar,
	             const std::string& frequency_path,
	             uint64_t runtime_market_id);

	Status accept(const std::vector<StockTimeBlock>& blocks,
	              const std::vector<ActiveBar>& bars,
	              const std::vector<std::vector<uint8_t> >* frame_bytes = NULL);
	bool contains(SymbolId symbol_id, TimeId time_id) const;
	Status get(SymbolId symbol_id, TimeId time_id, BlockBar* out) const;
	Status range(const std::vector<SymbolId>& symbol_ids,
	             TimeId begin,
	             TimeId end,
	             std::vector<ActiveBar>* out) const;
	// snapshot exposes complete logical records to the offline compactor without
	// exposing the persistent Staging page layout outside this implementation.
	// When requested, frame_bytes is aligned with blocks and contains immutable
	// BarBlockFrame bytes for direct Vault migration.
	Status snapshot(std::vector<StockTimeBlock>* blocks,
	                std::vector<ActiveBar>* bars,
	                std::vector<std::vector<uint8_t> >* frame_bytes = NULL) const;

private:
	struct Locator {
		uint32_t segment_id;
		uint64_t page_offset;
		uint32_t page_length;
		uint32_t record_index;
	};

	Status load();
	Status write_index() const;
	// Loads and decodes one page, returning its parsed records. Uses the
	// shared decoded cache (and compressed cache as fallback).
	Status load_page(uint32_t segment_id, uint64_t page_offset, uint32_t page_length,
		std::vector<ParsedStagingRecord>* records) const;

	Frequency frequency_;
	const Calendar& calendar_;
	std::string path_;
	uint64_t runtime_market_id_;
	Status status_;
	uint32_t next_page_id_;
	uint32_t current_segment_id_;
	std::map<std::pair<TimeId, SymbolId>, Locator> index_;
};

// VaultStore holds compacted immutable blocks ordered by symbol history. Its
// ingest entry point is intentionally internal: a later compactor can pass its
// sorted completed blocks directly without coupling the Vault layout to the
// mutable Active or time-oriented Staging stores.
class VaultStore {
public:
	VaultStore(Frequency frequency,
	           const Calendar& calendar,
	           const std::string& frequency_path,
	           uint64_t runtime_market_id);

	// Appends completed blocks as symbol-ordered ZVB6 blobs and publishes their
	// locators in the persistent Vault index.
	Status ingest(const std::vector<StockTimeBlock>& blocks,
	              const std::vector<ActiveBar>& bars,
	              const std::vector<std::vector<uint8_t> >* frame_bytes = NULL);
	bool contains(SymbolId symbol_id, TimeId time_id) const;
	Status get(SymbolId symbol_id, TimeId time_id, BlockBar* out) const;
	Status range(const std::vector<SymbolId>& symbol_ids,
	             TimeId begin,
	             TimeId end,
	             std::vector<ActiveBar>* out) const;
	// snapshot reconstructs complete logical records from immutable Vault blobs.
	// frame_bytes, when requested, is aligned with blocks for byte-preserving
	// compaction into a replacement Vault generation.
	Status snapshot(std::vector<StockTimeBlock>* blocks,
	                std::vector<ActiveBar>* bars,
	                std::vector<std::vector<uint8_t> >* frame_bytes = NULL) const;

private:
	struct Locator {
		SymbolId symbol_id;
		TimeId first_time_block_id;
		TimeId last_time_block_id;
		uint32_t segment_id;
		uint64_t blob_offset;
		uint32_t blob_length;
	};

	Status load();
	Status write_index() const;

	Frequency frequency_;
	const Calendar& calendar_;
	std::string path_;
	uint64_t runtime_market_id_;
	Status status_;
	uint32_t current_segment_id_;
	std::vector<Locator> index_;
};

// =============================================================================
// Shared helpers
//
// Utility constants, structs, and functions used by both the store
// implementation and the History coordination layer. These are internal
// implementation details, not part of the public API.

// Recovery-log record magic ("ZTA1" in little-endian bytes).
static const uint32_t kActiveRecordMagic = 0x3141545a;
static const uint8_t kActiveRecordVersion = 1;
// Fixed serialized size of one active recovery-log record. Must match the
// layout written by SerializeActiveRecord() and read by ParseActiveRecord():
//   magic(4) + version(1) + frequency(1) + state(1) + reserved(1)
//   + symbol_id(4) + time_id(4) + open(4) + high(4) + low(4) + close(4) + volume(4)
//   = 36 bytes
static const size_t kActiveRecordBytes = 36;

// One mutable active-store record before serialization.
struct ActiveRecord {
	Frequency frequency;
	SymbolId symbol_id;
	TimeId time_id;
	BlockBar bar;
};

// A bar fully resolved to its time_id, block_id, and block_offset so callers
// do not repeatedly look up the calendar.
struct ResolvedBar {
	Bar bar;
	TimeId time_id;
	TimeId block_id;
	BlockOff block_offset;
};

bool ValidState(BarState state);
bool ValidBar(const BlockBar& bar);
BlockBar MissingBlockBar();

bool SerializeActiveRecord(const ActiveRecord& record,
                           std::vector<uint8_t>* bytes);
Status ParseActiveRecord(const std::vector<uint8_t>& bytes,
                         size_t offset,
                         ActiveRecord* record);

Status ResolveTime(const Calendar& calendar,
                   Frequency frequency,
                   const std::string& local_time,
                   TimeId* time_id,
                   TimeId* block_id,
                   BlockOff* block_offset);
Status LocalTime(const Calendar& calendar,
                 Frequency frequency,
                 TimeId time_id,
                 std::string* out);

bool ActiveBarOrder(const ActiveBar& left, const ActiveBar& right);
bool SameBlockBar(const BlockBar& left, const BlockBar& right);

Status StagingTimeIds(const Calendar& calendar,
                      Frequency frequency,
                      TimeId block_id,
                      BlockOff position_count,
                      std::vector<TimeId>* out);

TimeId CompactionBlockId(Frequency frequency, TimeId time_id);
TimeId CompactionBlockDayLength(Frequency frequency);

}  // namespace zstfs

