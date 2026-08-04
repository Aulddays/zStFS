// history.h
//
// Defines the calendar-addressed block and frame structures used inside
// History. Public callers only operate on complete Bar records.

#pragma once

#include <cstdint>
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

// ActiveStore holds the current writable time window for one History.
class ActiveStore {
public:
	explicit ActiveStore(Frequency frequency);

private:
	Frequency frequency_;
};

// StagingStore holds sealed immutable blocks ordered for recent range reads.
class StagingStore {
public:
	explicit StagingStore(Frequency frequency);

private:
	Frequency frequency_;
};

// VaultStore holds compacted immutable blocks ordered by symbol history.
class VaultStore {
public:
	explicit VaultStore(Frequency frequency);

private:
	Frequency frequency_;
};

}  // namespace zstfs

