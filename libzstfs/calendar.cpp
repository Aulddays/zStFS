#include "calendar.h"

#include "serialization.h"

#include <algorithm>
#include <cstdio>
#include <fstream>
#include <iterator>
#include <limits>
#include <map>
#include <zstfs/pe_log.h>

namespace zstfs {

// Gregorian date arithmetic
//
// 1900-01-01 was a Monday. We first measure ordinary calendar days from that
// epoch, then remove only Saturday and Sunday from the compact TimeId axis.
// Weekday holidays deliberately remain on the axis, preserving every existing
// storage coordinate when a holiday or special slot layout is later added.
struct DateParts {
	int year;
	int month;
	int day;
};

static bool IsLeapYear(int year) {
	return (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
}

static int DaysInMonth(int year, int month) {
	static const int kDays[] = {
		31, 28, 31, 30, 31, 30,
		31, 31, 30, 31, 30, 31
	};
	if (month == 2 && IsLeapYear(year)) {
		return 29;
	}
	return kDays[month - 1];
}

static bool ParseDate(const std::string& value, DateParts* out) {
	if (out == NULL || value.size() != 8) {
		return false;
	}
	for (std::string::const_iterator it = value.begin(); it != value.end(); ++it) {
		if (*it < '0' || *it > '9') {
			return false;
		}
	}
	out->year = std::atoi(value.substr(0, 4).c_str());
	out->month = std::atoi(value.substr(4, 2).c_str());
	out->day = std::atoi(value.substr(6, 2).c_str());
	return out->year >= 1900 && out->month >= 1 && out->month <= 12 &&
	       out->day >= 1 && out->day <= DaysInMonth(out->year, out->month);
}

static bool ParseHourlyTime(const std::string& value,
                     std::string* date,
                     int* hour,
                     int* minute) {
	if (date == NULL || hour == NULL || minute == NULL || value.size() != 13 ||
	    value[8] != '-') {
		return false;
	}
	*date = value.substr(0, 8);
	DateParts date_parts;
	if (!ParseDate(*date, &date_parts)) {
		return false;
	}
	for (size_t i = 9; i < value.size(); ++i) {
		if (value[i] < '0' || value[i] > '9') {
			return false;
		}
	}
	*hour = std::atoi(value.substr(9, 2).c_str());
	*minute = std::atoi(value.substr(11, 2).c_str());
	return *hour >= 0 && *hour <= 23 && *minute >= 0 && *minute <= 59;
}

static int LeapYearsBefore(int year) {
	const int previous_year = year - 1;
	return previous_year / 4 - previous_year / 100 + previous_year / 400;
}

static int DaysBeforeYear(int year) {
	return (year - 1900) * 365 +
		LeapYearsBefore(year) - LeapYearsBefore(1900);
}

// Converts an ordinary Gregorian-day offset from 1900-01-01 without walking
// every elapsed day. Calendar::date uses this after expanding a compact
// weekday-only TimeId back to its ordinary-day coordinate.
static bool DateFromOrdinal(int ordinal, DateParts* out) {
	if (out == NULL || ordinal < 0 || ordinal >= DaysBeforeYear(10000)) {
		return false;
	}
	int lower_year = 1900;
	int upper_year = 10000;
	while (lower_year + 1 < upper_year) {
		const int middle_year = lower_year + (upper_year - lower_year) / 2;
		if (DaysBeforeYear(middle_year) <= ordinal) {
			lower_year = middle_year;
		} else {
			upper_year = middle_year;
		}
	}
	out->year = lower_year;
	int day_of_year = ordinal - DaysBeforeYear(lower_year);
	out->month = 1;
	while (day_of_year >= DaysInMonth(out->year, out->month)) {
		day_of_year -= DaysInMonth(out->year, out->month);
		++out->month;
	}
	out->day = day_of_year + 1;
	return true;
}

static int DaysBeforeDate(const DateParts& date) {
	int total = DaysBeforeYear(date.year);
	for (int month = 1; month < date.month; ++month) {
		total += DaysInMonth(date.year, month);
	}
	return total + date.day - 1;
}

static bool IsWeekendOrdinal(int ordinal) {
	return ordinal % 7 >= 5;
}

static bool ValidSlots(const std::vector<HourSlot>& slots) {
	if (!std::is_sorted(slots.begin(), slots.end()) ||
		std::adjacent_find(slots.begin(), slots.end()) != slots.end()) {
		return false;
	}
	for (std::vector<HourSlot>::const_iterator slot = slots.begin();
	     slot != slots.end(); ++slot) {
		if (*slot > 235 || *slot % 10 > 5) {
			return false;
		}
	}
	return true;
}

// CNA normal trading slots are the starts of the two morning and two
// afternoon trading hours. Historical branches remain in this function when a
// real schedule change occurs; a new branch must use its actual future
// effective date and must not alter an existing date range.
static Status CNASlots(const std::string& date, std::vector<HourSlot>* out) {
	if (out == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "slot output is required");
	}
	out->clear();
	out->push_back(93);   // 09:30
	out->push_back(103);  // 10:30
	out->push_back(130);  // 13:00
	out->push_back(140);  // 14:00

	// Future schedule change example, intentionally inactive. Replace the
	// placeholder date and slot with a real effective rule when one is needed;
	// keep the existing branch unchanged for all earlier dates.
	// if (date >= "YYYYMMDD") {
	// 	out->push_back(120);
	// }
	return Status::Ok();
}

// Markets configuration names a type such as "CNA"; this registry maps that
// name to the compiled function that generates normal weekday slots. Calendar
// calls the function only after date overrides have been checked. The function
// receives the date so it can preserve every historical layout while appending
// only future-effective changes. It clears out and returns sorted, unique HHM
// slots; holidays and exceptional layouts stay in Calendar's date overrides.
static std::map<std::string, MarketSlotsFunction>& MarketTypes() {
	static std::map<std::string, MarketSlotsFunction> functions;
	if (functions.empty()) {
		functions["CNA"] = CNASlots;
	}
	return functions;
}

static Status GetSlots(const std::string& type,
                  const std::string& date,
                  std::vector<HourSlot>* out) {
	std::map<std::string, MarketSlotsFunction>& functions = MarketTypes();
	std::map<std::string, MarketSlotsFunction>::const_iterator found =
		functions.find(type);
	if (found == functions.end()) {
		return Status::Error(ErrorCode::InvalidArgument,
		                     "unknown market type: " + type);
	}
	Status status = found->second(date, out);
	if (!status.ok()) {
		return status;
	}
	if (out == NULL || !ValidSlots(*out)) {
		return Status::Error(ErrorCode::InvalidArgument,
		                     "market type returned invalid slots");
	}
	return Status::Ok();
}

Status register_market_type(const std::string& name,
                            MarketSlotsFunction slots_function) {
	if (name.empty() || slots_function == NULL) {
		return Status::Error(ErrorCode::InvalidArgument,
		                     "market type name and function are required");
	}
	MarketTypes()[name] = slots_function;
	return Status::Ok();
}

Calendar::Calendar(const std::string& market_type)
	: market_type_(market_type) {
}

const std::string& Calendar::market_type() const {
	return market_type_;
}

Status Calendar::time_id(const std::string& value, TimeId* out) const {
	DateParts date_parts;
	if (out == NULL || !ParseDate(value, &date_parts))
	{
		PELOG_ERROR_RETURN((PLV_ERROR, "Calendar::time_id %s invalid date\n", value.c_str()),
			Status::Error(ErrorCode::InvalidArgument, "invalid input date"));
	}
	const int ordinal = DaysBeforeDate(date_parts);
	if (IsWeekendOrdinal(ordinal))
	{
		PELOG_ERROR_RETURN((PLV_ERROR, "Calendar::time_id %s is weekend\n", value.c_str()),
			Status::Error(ErrorCode::NotFound, "input date is a weekend"));
	}
	const int day_number = ordinal - (ordinal / 7) * 2;
	if (day_number < 0 ||
			static_cast<TimeId>(day_number) > (std::numeric_limits<TimeId>::max() >> 8))
	{
		PELOG_ERROR_RETURN((PLV_ERROR, "Calendar::time_id %s out of range\n", value.c_str()),
			Status::Error(ErrorCode::Conflict, "time identifier is out of range"));
	}
	*out = daily_bar_id(static_cast<TimeId>(day_number));
	return Status::Ok();
}

Status Calendar::date(TimeId value, std::string* out) const {
	if (out == NULL || time_slot(value) != 0) {
		return Status::Error(ErrorCode::InvalidArgument,
		                     "daily time identifier is required");
	}
	const int compact_day_number = static_cast<int>(time_day(value));
	const int ordinal = (compact_day_number / 5) * 7 + compact_day_number % 5;
	DateParts date_parts = {};
	if (!DateFromOrdinal(ordinal, &date_parts)) {
		return Status::Error(ErrorCode::Conflict, "time identifier is outside the calendar range");
	}
	char buffer[9];
	std::snprintf(buffer, sizeof(buffer), "%04d%02d%02d",
	              date_parts.year, date_parts.month, date_parts.day);
	*out = buffer;
	return Status::Ok();
}

Status Calendar::hour_slot(const std::string& value, HourSlot* out) const {
	std::string date_value;
	int hour = 0;
	int minute = 0;
	if (out == NULL || !ParseHourlyTime(value, &date_value, &hour, &minute)) {
		return Status::Error(ErrorCode::InvalidArgument, "invalid local hourly time");
	}
	TimeId ignored_time_id = 0;
	Status status = time_id(date_value, &ignored_time_id);
	if (!status.ok()) {
		return status;
	}
	*out = static_cast<HourSlot>(hour * 10 + minute / 10);
	return Status::Ok();
}

Status Calendar::local_time(TimeId value,
                            HourSlot slot,
                            std::string* out) const {
	if (out == NULL || slot > 235 || slot % 10 > 5) {
		return Status::Error(ErrorCode::InvalidArgument, "invalid hourly slot");
	}
	std::string date_value;
	Status status = date(value, &date_value);
	if (!status.ok()) {
		return status;
	}
	char buffer[14];
	std::snprintf(buffer, sizeof(buffer), "%s-%02u%u0",
	              date_value.c_str(),
	              static_cast<unsigned int>(slot / 10),
	              static_cast<unsigned int>(slot % 10));
	*out = buffer;
	return Status::Ok();
}

Status Calendar::slots(const std::string& value,
                       std::vector<HourSlot>* out) const {
	if (out == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "slot output is required");
	}
	TimeId ignored_time_id = 0;
	Status status = time_id(value, &ignored_time_id);
	if (!status.ok()) {
		return status;
	}
	std::map<std::string, std::vector<HourSlot> >::const_iterator override_found =
		override_slots_.find(value);
	if (override_found != override_slots_.end()) {
		*out = override_found->second;
		return Status::Ok();
	}
	return GetSlots(market_type_, value, out);
}

// Hourly offsets use the prefix sum of actual slots in the block; closed
// days and shorter slot layouts therefore consume no artificial 256-slot space.
Status Calendar::block_offset(Frequency frequency,
                                const std::string& value,
                                TimeId* block_id,
                                BlockOff* block_offset) const {
	if (block_id == NULL || block_offset == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "block outputs are required");
	}
	std::string date_value = frequency == Frequency::Daily ? value : value.substr(0, 8);
	TimeId value_day_time_id = 0;
	Status status = time_id(date_value, &value_day_time_id);
	if (!status.ok()) {
		return status;
	}
	const TimeId day_number = time_day(value_day_time_id);
	const TimeId block_day_length = frequency == Frequency::Daily
		? kDailyTimeBlockDayLength : kHourlyTimeBlockDayLength;
	const BlockOff day_offset = day_number % block_day_length;
	*block_id = value_day_time_id -
		static_cast<TimeId>(day_offset) * kTimeIdDayStep;
	if (frequency == Frequency::Daily) {
		*block_offset = day_offset;
		return Status::Ok();
	}

	HourSlot requested_slot = 0;
	status = hour_slot(value, &requested_slot);
	if (!status.ok()) {
		return status;
	}
	std::vector<HourSlot> date_slots;
	BlockOff compact_offset = 0;
	TimeId current_day_time_id = *block_id;
	for (BlockOff offset = 0; offset <= day_offset; ++offset) {
		std::string current_date;
		status = date(current_day_time_id, &current_date);
		if (!status.ok()) {
			return status;
		}
		status = slots(current_date, &date_slots);
		if (!status.ok()) {
			return status;
		}
		if (offset == day_offset) {
			std::vector<HourSlot>::const_iterator found =
				std::find(date_slots.begin(), date_slots.end(), requested_slot);
			if (found == date_slots.end()) {
				return Status::Error(ErrorCode::NotFound,
				                     "hour slot is outside the market slots");
			}
			compact_offset += static_cast<BlockOff>(found - date_slots.begin());
			*block_offset = compact_offset;
			return Status::Ok();
		}
		compact_offset += static_cast<BlockOff>(date_slots.size());
		current_day_time_id += kTimeIdDayStep;
	}
	return Status::Error(ErrorCode::Conflict, "failed to locate hourly offset");
}

Status Calendar::block_length(Frequency frequency,
                              TimeId block_id,
                              BlockOff* out) const {
	if (out == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "block length output is required");
	}
	const TimeId block_day_length = frequency == Frequency::Daily
		? kDailyTimeBlockDayLength : kHourlyTimeBlockDayLength;
	if (time_slot(block_id) != 0 || time_day(block_id) % block_day_length != 0) {
		return Status::Error(ErrorCode::InvalidArgument,
		                     "invalid time block identifier");
	}
	if (frequency == Frequency::Daily) {
		*out = block_day_length;
		return Status::Ok();
	}
	BlockOff total = 0;
	TimeId current_day_time_id = block_id;
	for (BlockOff offset = 0; offset < block_day_length; ++offset) {
		std::string current_date;
		Status status = date(current_day_time_id, &current_date);
		if (!status.ok()) {
			return status;
		}
		std::vector<HourSlot> date_slots;
		status = slots(current_date, &date_slots);
		if (!status.ok()) {
			return status;
		}
		total += static_cast<BlockOff>(date_slots.size());
		current_day_time_id += kTimeIdDayStep;
	}
	*out = total;
	return Status::Ok();
}

Status Calendar::set_closed(const std::string& value) {
	TimeId ignored_time_id = 0;
	Status status = time_id(value, &ignored_time_id);
	if (!status.ok()) {
		return status;
	}
	override_slots_[value] = std::vector<HourSlot>();
	return Status::Ok();
}

Status Calendar::set_slots(const std::string& value,
                                   const std::vector<HourSlot>& slots) {
	TimeId ignored_time_id = 0;
	Status status = time_id(value, &ignored_time_id);
	if (!status.ok()) {
		return status;
	}
	if (!ValidSlots(slots)) {
		return Status::Error(ErrorCode::InvalidArgument,
		                     "invalid slot layout");
	}
	override_slots_[value] = slots;
	return Status::Ok();
}

// Calendar load/save functions and helpers
static const uint16_t kCalendarVersion = 1;

// calendar.bin saves three sections:
//   1. format header: magic, version, reserved field;
//   2. market type name; it selects, but does not store, the compiled default slots;
//   3. date overrides: each date and its ordered HHM slot list.
Status Calendar::save(const std::string& file_path) const {
	std::vector<uint8_t> data;
	data.push_back('Z');
	data.push_back('C');
	data.push_back('A');
	data.push_back('L');
	PutU16(&data, kCalendarVersion);
	PutU16(&data, 0);
	if (!PutString(&data, market_type_)) {
		return Status::Error(ErrorCode::InvalidArgument, "market type is too long");
	}
	PutU32(&data, static_cast<uint32_t>(override_slots_.size()));
	for (std::map<std::string, std::vector<HourSlot> >::const_iterator it =
		     override_slots_.begin(); it != override_slots_.end(); ++it) {
		std::vector<uint8_t> record;
		if (!PutString(&record, it->first) ||
		    it->second.size() > std::numeric_limits<uint16_t>::max()) {
			return Status::Error(ErrorCode::InvalidArgument,
			                     "calendar record is too large");
		}
		PutU16(&record, static_cast<uint16_t>(it->second.size()));
		record.insert(record.end(), it->second.begin(), it->second.end());
		PutU32(&data, static_cast<uint32_t>(record.size()));
		data.insert(data.end(), record.begin(), record.end());
	}

	std::ofstream output(file_path.c_str(), std::ios::binary | std::ios::trunc);
	if (!output) {
		return Status::Error(ErrorCode::IoError, "failed to open calendar file");
	}
	output.write(reinterpret_cast<const char*>(&data[0]), data.size());
	if (!output) {
		return Status::Error(ErrorCode::IoError, "failed to write calendar file");
	}
	return Status::Ok();
}

Status Calendar::load(const std::string& file_path) {
	std::ifstream input(file_path.c_str(), std::ios::binary);
	if (!input) {
		return Status::Error(ErrorCode::IoError, "failed to open calendar file");
	}
	std::vector<uint8_t> data((std::istreambuf_iterator<char>(input)),
	                          std::istreambuf_iterator<char>());
	if (data.size() < 12 || data[0] != 'Z' || data[1] != 'C' ||
	    data[2] != 'A' || data[3] != 'L') {
		return Status::Error(ErrorCode::CorruptData, "invalid calendar file header");
	}
	size_t offset = 4;
	uint16_t version = 0;
	uint16_t reserved = 0;
	std::string file_type;
	uint32_t record_count = 0;
	if (!GetU16(data, &offset, &version) || !GetU16(data, &offset, &reserved) ||
	    !GetString(data, &offset, &file_type) ||
	    !GetU32(data, &offset, &record_count) ||
	    version != kCalendarVersion || file_type != market_type_) {
		return Status::Error(ErrorCode::CorruptData,
		                     "unsupported or mismatched calendar file");
	}

	std::map<std::string, std::vector<HourSlot> > loaded;
	for (uint32_t index = 0; index < record_count; ++index) {
		uint32_t record_size = 0;
		if (!GetU32(data, &offset, &record_size) ||
		    offset + record_size > data.size()) {
			return Status::Error(ErrorCode::CorruptData,
			                     "truncated calendar record");
		}
		std::vector<uint8_t> record(data.begin() + offset,
		                            data.begin() + offset + record_size);
		offset += record_size;
		size_t record_offset = 0;
		std::string date_value;
		uint16_t slot_count = 0;
		if (!GetString(record, &record_offset, &date_value) ||
		    !GetU16(record, &record_offset, &slot_count) ||
		    record_offset + slot_count != record.size()) {
			return Status::Error(ErrorCode::CorruptData,
			                     "invalid calendar record");
		}
		std::vector<HourSlot> slots(record.begin() + record_offset, record.end());
		TimeId ignored_time_id = 0;
		Status status = time_id(date_value, &ignored_time_id);
		if (!status.ok() || !ValidSlots(slots) ||
		    !loaded.insert(std::make_pair(date_value, slots)).second) {
			return Status::Error(ErrorCode::CorruptData,
			                     "invalid calendar date override");
		}
	}
	if (offset != data.size()) {
		return Status::Error(ErrorCode::CorruptData, "trailing calendar data");
	}
	override_slots_.swap(loaded);
	return Status::Ok();
}

}  // namespace zstfs
