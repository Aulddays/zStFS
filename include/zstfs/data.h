// zstfs/data.h
//
// Declares the stable value types exposed by the public Market API. Public
// callers use market-local time strings; the private Calendar owns all compact
// time coordinates used by the history pipeline.

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace zstfs {

typedef uint32_t SymbolId;
typedef uint32_t ActionId;

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

// PrecisionProfile controls lossy numeric quantization without exposing the
// internal frame or codec format. Price precision is relative to the value's
// magnitude; no fixed decimal scale is imposed on high-priced instruments.
struct PrecisionProfile {
	double price_relative_epsilon;
	double volume_relative_epsilon;
};

struct SymbolAlias {
	std::string code;
	std::string begin_date;
	std::string end_date;
};

struct Symbol {
	SymbolId id;
	std::string code;
	std::string name;
	std::string security_type;	// index/stock/etf...
	std::string exchange;	// SH/SZ/BJ...
	std::string board;	// Main/STAR/ChiNext...
	std::string industry;
	std::string list_date;
	std::string delist_date;
	uint64_t share_capital;
	uint64_t tradable_share;
	SymbolState state;
	std::string trade_state;	// purchase/redeem status
	std::vector<SymbolAlias> aliases;
};

struct Action {
	ActionId id;
	// external_event_key identifies one source event across retries and
	// corrections. It is unique within its Symbol and remains stable while the
	// event representation changes; id is the library-owned physical key.
	std::string external_event_key;
	SymbolId symbol_id;
	std::string effective_date;
	ActionType type;
	double factor;
	double cash_value;
};

}  // namespace zstfs

