// history_internal.h
//
// Defines the calendar-addressed block and frame structures used inside
// History. Public callers only operate on complete Bar records.

#ifndef ZSTFS_HISTORY_INTERNAL_H_
#define ZSTFS_HISTORY_INTERNAL_H_

#include <cstdint>
#include <vector>

#include "zstfs/data.h"

namespace zstfs {

struct BlockKey {
	SymbolId symbol_id;
	TimeBlockId time_block_id;
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
	std::vector<BlockBar> positions;
};

// Microblock is one field's sequence from one StockTimeBlock.
struct Microblock {
	FieldId field;
	Position first_position;
	std::vector<int64_t> values;
};

// MicroblockFrame is the independently decodable encoded part of a
// Microblock. Its codec fields make the on-disk representation self-describing.
struct MicroblockFrame {
	FieldId field;
	Position first_position;
	Position sample_count;
	uint8_t codec_id;
	uint8_t predictor_id;
	uint8_t quantizer_id;
	std::vector<uint8_t> quantizer_parameters;
	int64_t anchor;
	std::vector<uint8_t> payload;
	uint32_t checksum;
};

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

#endif  // ZSTFS_HISTORY_INTERNAL_H_
