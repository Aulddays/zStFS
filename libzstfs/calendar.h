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

	// An explicit date override takes precedence over the market type's normal
	// slots. An empty slot list means that the weekday is closed. The normal rule
	// itself may select different layouts by date; once a layout applies to a past
	// date, its rule must remain unchanged and later layouts are appended with a
	// future effective date.
	Status slots(const std::string& date,
	             std::vector<HourSlot>* out) const;
	Status set_closed(const std::string& date);
	Status set_slots(const std::string& date,
	                 const std::vector<HourSlot>& slots);

	// Each frequency uses its own fixed trading-day block length. The block ID
	// identifies the block's first day; BlockOff identifies a location within its
	// layout. Hourly offsets count only actual slots on preceding dates and do
	// not reserve 256 slots per day.
	Status block_offset(Frequency frequency,
	                    const std::string& local_time,
	                    TimeId* block_id,
	                    BlockOff* block_offset) const;
	Status block_length(Frequency frequency,
	                    TimeId block_id,
	                    BlockOff* out) const;

	// Only date-specific overrides are persisted. Normal slots remain in the
	// compiled market type, so the file does not contain a full date table.
	Status load(const std::string& file_path);
	Status save(const std::string& file_path) const;

private:
	std::string market_type_;
	std::map<std::string, std::vector<HourSlot> > override_slots_;
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

