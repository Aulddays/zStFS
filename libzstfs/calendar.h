#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "zstfs/data.h"
#include "zstfs/status.h"

namespace zstfs {

// These coordinates are private to the storage library. The public Market API
// accepts only market-local time strings and never exposes their encoded form.
typedef uint8_t HourSlot;
typedef uint32_t TimeId;

// BlockOff is a compact offset inside one time block. It is not a TimeId: the
// block TimeId selects the block, while BlockOff selects a position in that
// block's daily or hourly layout. The two values must not be added directly.
typedef uint16_t BlockOff;

const TimeId kDailyTimeBlockDayLength = 64;
const TimeId kHourlyTimeBlockDayLength = 64;
const TimeId kEpochTimeId = 0;
const TimeId kTimeIdDayStep = 1 << 8;

// Every time coordinate reserves the low byte for the ten-minute HHM slot. A
// daily bar and a block boundary use slot zero; an hourly bar uses its actual
// slot. The high bits always identify the trading-day number.
inline TimeId daily_bar_id(TimeId day_number) {
	return day_number << 8;
}

inline TimeId hourly_bar_id(TimeId day_number, HourSlot slot) {
	return (day_number << 8) | static_cast<TimeId>(slot);
}

inline TimeId time_day(TimeId time_id) {
	return time_id >> 8;
}

inline HourSlot time_slot(TimeId time_id) {
	return static_cast<HourSlot>(time_id & 0xff);
}

// Calendar is the private translation layer between local time strings and the
// compact coordinates used by History. The date axis is shared by a Market;
// its intraday slots come from date-aware market type rules and can be
// overridden on individual dates without renumbering existing day portions of TimeId values.
class Calendar {
public:
	explicit Calendar(const std::string& market_type);

	const std::string& market_type() const;

	// The returned TimeId is a daily coordinate with a zero low-byte slot.
	// Weekends have no coordinate; weekday holidays retain their coordinate and
	// are represented by an empty date override in the slot table.
	Status time_id(const std::string& date, TimeId* out) const;
	Status date(TimeId day_time_id, std::string* out) const;

	// The minute component is rounded down to the ten-minute grid before it is
	// encoded as HHM, so 09:39 becomes slot 93.
	Status hour_slot(const std::string& local_time, HourSlot* out) const;
	Status local_time(TimeId day_time_id,
	                  HourSlot slot,
	                  std::string* out) const;

	// Returns the market's intraday slot list for the given date. The market
	// type rule determines the layout; it may vary by date range for historical
	// schedule changes. Returns an empty vector when the market is closed that day.
	Status slots(const std::string& date,
	             std::vector<HourSlot>* out) const;

	// Each frequency uses its own fixed trading-day block length. The block ID
	// identifies the block's first day; BlockOff identifies a location within its
	// layout. Hourly offsets count only actual slots on preceding dates and do
	// not reserve 256 slots per day.
	Status block_offset(Frequency frequency,
	                    const std::string& local_time,
	                    TimeId* block_id,
	                    BlockOff* block_offset) const;
	// Compute block coordinates directly from a TimeId without going through
	// a local-time string. Same result as the string overload after a round
	// trip through date()/time_id(), but avoids string allocation and parsing.
	Status block_offset(Frequency frequency,
		TimeId time_id,
		TimeId *block_id,
		BlockOff *block_offset) const;
	Status block_length(Frequency frequency,
	                    TimeId block_id,
	                    BlockOff* out) const;

	// Persistence: only the market type name is stored; slot rules are compiled
	// into the binary and looked up by name at load time.
	Status load(const std::string& file_path);
	Status save(const std::string& file_path) const;

private:
	std::string market_type_;
};

// Implementations receive a validated weekday date and must replace `out` with
// that market type's normal sorted, unique ten-minute HHM slots. A function may
// choose layouts by date to preserve historical market schedules. Rules for past
// dates are append-only: keep every historical branch and add a later effective
// date for each new layout. Holidays and temporary closures belong in Calendar's
// date overrides, not this function. Register the function under the type name
// used by Markets configuration before constructing Calendars that use it.
typedef Status (*MarketSlotsFunction)(const std::string& date,
                                      std::vector<HourSlot>* out);

Status register_market_type(const std::string& name,
                            MarketSlotsFunction slots_function);

}  // namespace zstfs

