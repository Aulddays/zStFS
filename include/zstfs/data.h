// zstfs/data.h
//
// Declares the stable value types shared by the public Market API and the
// internal history pipeline. Public callers use market-local time strings;
// Calendar converts those strings into compact internal time identifiers.

#ifndef ZSTFS_DATA_H_
#define ZSTFS_DATA_H_

#include <cstdint>
#include <string>

namespace zstfs {

typedef uint32_t SymbolId;
typedef uint32_t ActionId;
typedef uint32_t TimeId;
typedef uint32_t TimeBlockId;
typedef uint16_t Position;

const SymbolId kInvalidSymbolId = 0;
const ActionId kInvalidActionId = 0;

enum class Frequency {
	Daily,
	Hourly
};

enum class BarState {
	Normal,
	NotListed,
	Delisted,
	Suspended,
	MarketClosed,
	Missing
};

enum class SymbolState {
	Active,
	Retired
};

enum class ActionType {
	Split,
	ReverseSplit,
	StockDividend,
	CashDividend,
	RightsIssue
};

enum class AdjustMode {
	Raw,
	Forward,
	Backward
};

enum class FieldId {
	State,
	Open,
	High,
	Low,
	Close,
	Volume
};

// Bar is one complete daily or hourly OHLCV sample. local_time uses YYYYMMDD
// for daily data and YYYYMMDD-HHMM for hourly data.
struct Bar {
	SymbolId symbol_id;
	Frequency frequency;
	std::string local_time;
	BarState state;
	double open;
	double high;
	double low;
	double close;
	double volume;
};

struct PrecisionProfile {
	double absolute_epsilon;
	double relative_epsilon;
	double volume_relative_error;
};

struct Symbol {
	SymbolId id;
	std::string code;
	std::string name;
	std::string security_type;
	std::string industry;
	std::string list_date;
	std::string delist_date;
	uint64_t share_capital;
	uint64_t tradable_share;
	uint32_t volume_unit;
	SymbolState state;
};

struct Action {
	ActionId id;
	SymbolId symbol_id;
	std::string effective_date;
	ActionType type;
	double factor;
	double cash_value;
};

}  // namespace zstfs

#endif  // ZSTFS_DATA_H_
