#pragma once

#include <map>
#include <string>
#include <vector>

#include "zstfs/data.h"
#include "zstfs/status.h"

namespace zstfs {

// Calendar is the private translation layer between local time strings and the
// compact coordinates used by History. The date axis is shared by a Market;
// its intraday slots come from the market type and can be overridden on
// individual dates without renumbering existing DayId values.
class Calendar {
public:
	explicit Calendar(const std::string& market_type);

	const std::string& market_type() const;

	// Weekends have no DayId. Weekday holidays retain their DayId and are
	// represented by an empty date override in the slot table.
	Status day_id(const std::string& date, DayId* out) const;
	Status date(DayId day_id, std::string* out) const;

	// The minute component is rounded down to the ten-minute grid before it is
	// encoded as HHM, so 09:39 becomes slot 93.
	Status hour_slot(const std::string& local_time, HourSlot* out) const;
	Status local_time(DayId day_id,
	                  HourSlot slot,
	                  std::string* out) const;

	// An explicit date override takes precedence over the market type's normal
	// slots. An empty slot list means that the weekday is closed.
	Status slots(const std::string& date,
	             std::vector<HourSlot>* out) const;
	Status set_closed(const std::string& date);
	Status set_slots(const std::string& date,
	                 const std::vector<HourSlot>& slots);

	// Both frequencies use 64-day block boundaries. Hourly positions count only
	// the slots on preceding dates; they do not reserve 256 slots per day.
	Status block_position(Frequency frequency,
	                      const std::string& local_time,
	                      TimeBlockId* block_id,
	                      Position* position) const;
	Status block_length(Frequency frequency,
	                    TimeBlockId block_id,
	                    Position* out) const;

	// Only date-specific overrides are persisted. Normal slots remain in the
	// compiled market type, so the file does not contain a full date table.
	Status load(const std::string& file_path);
	Status save(const std::string& file_path) const;

private:
	std::string market_type_;
	std::map<std::string, std::vector<HourSlot> > override_slots_;
};

// Implementations receive a validated weekday date and must replace `out` with
// that market type's normal sorted, unique ten-minute HHM slots. Holidays and
// temporary closures belong in Calendar's date overrides, not this function.
// Register the function under the type name used by Markets configuration
// before constructing Calendars that use it.
typedef Status (*MarketSlotsFunction)(const std::string& date,
                                      std::vector<HourSlot>* out);

Status register_market_type(const std::string& name,
                            MarketSlotsFunction slots_function);

}  // namespace zstfs

