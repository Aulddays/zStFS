// history.h
//
// Defines the calendar-addressed block and frame structures used inside
// History. Public callers only operate on complete Bar records.

#pragma once

#include <condition_variable>
#include <cstddef>
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

struct BlockBar {
	BarState state;
	double open;
	double high;
	double low;
	double close;
	double volume;
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

// Microblock is one field's sequence from one StockTimeBlock.
struct Microblock {
	FieldId field;
	BlockOff first_offset;
	std::vector<int64_t> values;
};

// EncodeFieldFrames turns a block's positions into independent normal runs.
// Invalid normal values are treated as missing positions and therefore split
// the output just like an explicit non-normal state.
Status EncodeFieldFrames(FieldId field,
                         BlockOff first_offset,
                         const std::vector<BlockBar>& positions,
                         const PrecisionProfile& profile,
                         std::vector<MicroblockFrame>* output);

// State frames cover all positions, including non-normal values, and use a
// compact run representation independent of numeric quantization.
Status EncodeStateFrame(BlockOff first_offset,
                        const std::vector<BlockBar>& positions,
                        MicroblockFrame* output);
Status DecodeStateFrame(const MicroblockFrame& frame,
                        std::vector<BarState>* states);

// A complete block uses one state frame plus independent field frames. The
// combined decoder restores the OHLC ordering constraints after quantization.
Status EncodeOhlcvFrames(BlockOff first_offset,
                         const std::vector<BlockBar>& positions,
                         const PrecisionProfile& profile,
                         std::vector<MicroblockFrame>* output);
Status DecodeOhlcvFrames(BlockOff first_offset,
                         BlockOff position_count,
                         const std::vector<MicroblockFrame>& frames,
                         const PrecisionProfile& profile,
                         std::vector<BlockBar>* positions);

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

// StagingStore owns immutable complete blocks after Active sealing. Its index
// is ordered by (time_block_id, symbol_id), while pages retain the encoded
// frames and the presence bitmap needed to distinguish padded Missing values
// from explicitly written Missing bars.
class StagingStore {
public:
	StagingStore(Frequency frequency,
	             const Calendar& calendar,
	             const std::string& frequency_path);

	Status accept(const std::vector<StockTimeBlock>& blocks,
	              const std::vector<ActiveBar>& bars);
	bool contains(SymbolId symbol_id, TimeId time_id) const;
	Status get(SymbolId symbol_id, TimeId time_id, BlockBar* out) const;
	Status range(const std::vector<SymbolId>& symbol_ids,
	             TimeId begin,
	             TimeId end,
	             std::vector<ActiveBar>* out) const;
	// snapshot exposes complete logical records to the offline compactor without
	// exposing the persistent Staging page layout outside this implementation.
	Status snapshot(std::vector<StockTimeBlock>* blocks,
	                std::vector<ActiveBar>* bars) const;

private:
	struct Locator {
		uint32_t segment_id;
		uint64_t page_offset;
		uint32_t page_length;
		uint32_t record_index;
	};

	struct Entry {
		StockTimeBlock block;
		std::vector<ActiveBar> bars;
	};

	Status load();
	Status write_index() const;

	Frequency frequency_;
	const Calendar& calendar_;
	std::string path_;
	Status status_;
	uint32_t next_page_id_;
	uint32_t current_segment_id_;
	std::map<std::pair<TimeId, SymbolId>, Locator> index_;
	std::map<std::pair<TimeId, SymbolId>, Entry> entries_;
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
	              const std::vector<ActiveBar>& bars);
	bool contains(SymbolId symbol_id, TimeId time_id) const;
	Status get(SymbolId symbol_id, TimeId time_id, BlockBar* out) const;
	Status range(const std::vector<SymbolId>& symbol_ids,
	             TimeId begin,
	             TimeId end,
	             std::vector<ActiveBar>* out) const;
	// snapshot reconstructs complete logical records from immutable Vault blobs.
	Status snapshot(std::vector<StockTimeBlock>* blocks,
	                std::vector<ActiveBar>* bars) const;

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

}  // namespace zstfs

