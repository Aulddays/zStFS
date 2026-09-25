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
	return GetSlots(market_type_, value, out);
}

// Hourly offsets use the prefix sum of actual slots in the block; closed
// days and shorter slot layouts therefore consume no artificial 256-slot space.
Status Calendar::block_offset(Frequency frequency,
                                const std::string& value,
                                TimeId *out_block_id,
                                BlockOff *out_block_offset) const
{
	if (out_block_id == NULL || out_block_offset == NULL)
		return Status::Error(ErrorCode::InvalidArgument, "block outputs are required");
	// Convert the string to a TimeId, then delegate to the TimeId overload.
	// This keeps all block-positioning logic in a single place.
	if (frequency == Frequency::Daily)
	{
		TimeId tid = 0;
		Status status = time_id(value, &tid);
		if (!status.ok())
			return status;
		return block_offset(frequency, tid, out_block_id, out_block_offset);
	}
	// Hourly: parse date + optional time portion, then build a TimeId.
	std::string date_value = value.substr(0, 8);
	TimeId day_tid = 0;
	Status status = time_id(date_value, &day_tid);
	if (!status.ok())
		return status;
	HourSlot slot = 0;
	if (value.size() > 8)
	{
		status = hour_slot(value, &slot);
		if (!status.ok())
			return status;
	}
	TimeId tid = hourly_bar_id(time_day(day_tid), slot);
	return block_offset(frequency, tid, out_block_id, out_block_offset);
}

Status Calendar::block_offset(Frequency frequency,
	TimeId time_id,
	TimeId *block_id,
	BlockOff *block_offset) const
{
	if (block_id == NULL || block_offset == NULL)
	{
		return Status::Error(ErrorCode::InvalidArgument, "block outputs are required");
	}
	const TimeId day_number = time_day(time_id);
	const TimeId block_day_length = frequency == Frequency::Daily
		? kDailyTimeBlockDayLength : kHourlyTimeBlockDayLength;
	const BlockOff day_offset = static_cast<BlockOff>(day_number % block_day_length);
	const TimeId block_day = day_number - day_offset;
	*block_id = block_day * kTimeIdDayStep;

	if (frequency == Frequency::Daily)
	{
		if (time_slot(time_id) != 0)
		{
			return Status::Error(ErrorCode::InvalidArgument,
				"daily time_id has a non-zero slot");
		}
		*block_offset = day_offset;
		return Status::Ok();
	}

	// Hourly: compute the compact offset within the block.
	const HourSlot slot = time_slot(time_id);
	const TimeId target_day_id = daily_bar_id(time_day(time_id));
	std::string block_start_date;
	Status status = date(*block_id, &block_start_date);
	if (!status.ok())
		return status;
	std::string target_date;
	status = date(target_day_id, &target_date);
	if (!status.ok())
		return status;
	std::vector<HourSlot> first_day_slots;
	status = slots(block_start_date, &first_day_slots);
	if (!status.ok())
		return status;
	std::vector<HourSlot> target_day_slots;
	status = slots(target_date, &target_day_slots);
	if (!status.ok())
		return status;

	// Fast path: if the first day and target day have the same slot count,
	// the block has a uniform daily slot layout (no schedule change within
	// the block). This is the common case since schedule changes are rare
	// (years apart) and a block is only 64 trading days.
	if (first_day_slots.size() == target_day_slots.size() && day_offset > 0)
	{
		// Still need to verify slot validity and find its index in the day.
		if (slot == 0)
		{
			*block_offset = static_cast<BlockOff>(
				day_offset * first_day_slots.size());
			return Status::Ok();
		}
		std::vector<HourSlot>::const_iterator found =
			std::find(target_day_slots.begin(), target_day_slots.end(), slot);
		if (found == target_day_slots.end())
		{
			return Status::Error(ErrorCode::NotFound,
				"hour slot is outside the market slots");
		}
		*block_offset = static_cast<BlockOff>(
			day_offset * first_day_slots.size() + (found - target_day_slots.begin()));
		return Status::Ok();
	}
	if (day_offset == 0)
	{
		if (slot == 0)
		{
			*block_offset = 0;
			return Status::Ok();
		}
		std::vector<HourSlot>::const_iterator found =
			std::find(target_day_slots.begin(), target_day_slots.end(), slot);
		if (found == target_day_slots.end())
		{
			return Status::Error(ErrorCode::NotFound,
				"hour slot is outside the market slots");
		}
		*block_offset = static_cast<BlockOff>(found - target_day_slots.begin());
		return Status::Ok();
	}

	// Slow path: the block crosses a schedule change. Sum slots day by day.
	std::vector<HourSlot> date_slots;
	BlockOff compact_offset = 0;
	TimeId current_day_time_id = *block_id;
	for (BlockOff offset = 0; offset <= day_offset; ++offset)
	{
		std::string current_date;
		status = date(current_day_time_id, &current_date);
		if (!status.ok())
			return status;
		status = slots(current_date, &date_slots);
		if (!status.ok())
			return status;
		if (offset == day_offset)
		{
			if (slot == 0)
			{
				*block_offset = compact_offset;
				return Status::Ok();
			}
			std::vector<HourSlot>::const_iterator found =
				std::find(date_slots.begin(), date_slots.end(), slot);
			if (found == date_slots.end())
			{
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
	// Fast path: compare first and last day of the block. If slot counts
	// match, the whole block uses the same daily layout (no schedule change
	// inside the 64-day block), so length is just days * slots_per_day.
	std::string first_date;
	Status status = date(block_id, &first_date);
	if (!status.ok())
		return status;
	TimeId last_day_id = block_id + (block_day_length - 1) * kTimeIdDayStep;
	std::string last_date;
	status = date(last_day_id, &last_date);
	if (!status.ok())
		return status;
	std::vector<HourSlot> first_slots;
	status = slots(first_date, &first_slots);
	if (!status.ok())
		return status;
	std::vector<HourSlot> last_slots;
	status = slots(last_date, &last_slots);
	if (!status.ok())
		return status;
	if (first_slots.size() == last_slots.size())
	{
		*out = static_cast<BlockOff>(block_day_length * first_slots.size());
		return Status::Ok();
	}
	// Slow path: block crosses a schedule change, sum day by day.
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

// Calendar load/save functions and helpers
static const uint16_t kCalendarVersion = 1;

// calendar.bin saves two sections:
//   1. format header: magic, version, reserved field;
//   2. market type name; it selects the compiled slot rule function.
// Slot layouts are determined by the market type's compiled rule, which may
// vary by date range for historical schedule changes. There is no per-date
// override table.
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
	if (data.size() < 8 || data[0] != 'Z' || data[1] != 'C' ||
	    data[2] != 'A' || data[3] != 'L') {
		return Status::Error(ErrorCode::CorruptData, "invalid calendar file header");
	}
	size_t offset = 4;
	uint16_t version = 0;
	uint16_t reserved = 0;
	std::string file_type;
	if (!GetU16(data, &offset, &version) || !GetU16(data, &offset, &reserved) ||
	    !GetString(data, &offset, &file_type) ||
	    version != kCalendarVersion || file_type != market_type_) {
		return Status::Error(ErrorCode::CorruptData,
		                     "unsupported or mismatched calendar file");
	}
	if (offset != data.size()) {
		return Status::Error(ErrorCode::CorruptData, "trailing calendar data");
	}
	return Status::Ok();
}

}  // namespace zstfs
