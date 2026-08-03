#include "calendar.h"

#include "serialization.h"

#include <algorithm>
#include <cstdio>
#include <fstream>
#include <iterator>
#include <map>

namespace zstfs {

// Gregorian date arithmetic
//
// 1900-01-01 was a Monday. We first measure ordinary calendar days from that
// epoch, then remove only Saturday and Sunday from the compact DayId axis.
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

static int DaysBeforeYear(int year) {
	int total = 0;
	for (int current = 1900; current < year; ++current) {
		total += IsLeapYear(current) ? 366 : 365;
	}
	return total;
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

static Status CNASlots(const std::string&, std::vector<HourSlot>* out) {
	if (out == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "slot output is required");
	}
	out->clear();
	out->push_back(93);
	out->push_back(103);
	out->push_back(113);
	out->push_back(133);
	out->push_back(143);
	return Status::Ok();
}

// Markets configuration names a type such as "CNA"; this registry maps
// that name to code which generates its normal weekday slots. Calendar calls
// the function only after date overrides have been checked. A function clears
// out and returns sorted, unique HHM slots; holidays and exceptional slot
// layouts stay in Calendar's date override table.
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

Status Calendar::day_id(const std::string& value, DayId* out) const {
	DateParts date_parts;
	if (out == NULL || !ParseDate(value, &date_parts)) {
		return Status::Error(ErrorCode::InvalidArgument, "invalid local date");
	}
	const int ordinal = DaysBeforeDate(date_parts);
	if (IsWeekendOrdinal(ordinal)) {
		return Status::Error(ErrorCode::NotFound, "local date is a weekend");
	}
	const int day_id_value = ordinal - (ordinal / 7) * 2;
	if (day_id_value < 0 || day_id_value > 65535) {
		return Status::Error(ErrorCode::Conflict, "day identifier is out of range");
	}
	*out = static_cast<DayId>(day_id_value);
	return Status::Ok();
}

Status Calendar::date(DayId value, std::string* out) const {
	if (out == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "date output is required");
	}
	int ordinal = 0;
	int remaining = value;
	while (remaining > 0) {
		++ordinal;
		if (!IsWeekendOrdinal(ordinal)) {
			--remaining;
		}
	}
	DateParts date_parts = {1900, 1, 1};
	while (ordinal > 0) {
		++date_parts.day;
		if (date_parts.day > DaysInMonth(date_parts.year, date_parts.month)) {
			date_parts.day = 1;
			++date_parts.month;
			if (date_parts.month > 12) {
				date_parts.month = 1;
				++date_parts.year;
			}
		}
		--ordinal;
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
	DayId ignored_day_id = 0;
	Status status = day_id(date_value, &ignored_day_id);
	if (!status.ok()) {
		return status;
	}
	*out = static_cast<HourSlot>(hour * 10 + minute / 10);
	return Status::Ok();
}

Status Calendar::local_time(DayId value,
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
	DayId ignored_day_id = 0;
	Status status = day_id(value, &ignored_day_id);
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

// Hourly positions use the prefix sum of actual slots in the block; closed
// days and shorter slot layouts therefore consume no artificial 256-slot space.
Status Calendar::block_position(Frequency frequency,
                                const std::string& value,
                                TimeBlockId* block_id,
                                Position* position) const {
	if (block_id == NULL || position == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "block outputs are required");
	}
	std::string date_value = frequency == Frequency::Daily ? value : value.substr(0, 8);
	DayId value_day = 0;
	Status status = day_id(date_value, &value_day);
	if (!status.ok()) {
		return status;
	}
	*block_id = value_day / kTimeBlockDayLength;
	const Position day_offset = value_day % kTimeBlockDayLength;
	if (frequency == Frequency::Daily) {
		*position = day_offset;
		return Status::Ok();
	}

	HourSlot requested_slot = 0;
	status = hour_slot(value, &requested_slot);
	if (!status.ok()) {
		return status;
	}
	std::vector<HourSlot> date_slots;
	Position compact_position = 0;
	for (Position offset = 0; offset <= day_offset; ++offset) {
		std::string current_date;
		status = date((*block_id) * kTimeBlockDayLength + offset, &current_date);
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
			compact_position += static_cast<Position>(found - date_slots.begin());
			*position = compact_position;
			return Status::Ok();
		}
		compact_position += static_cast<Position>(date_slots.size());
	}
	return Status::Error(ErrorCode::Conflict, "failed to locate hourly position");
}

Status Calendar::block_length(Frequency frequency,
                              TimeBlockId block_id,
                              Position* out) const {
	if (out == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "block length output is required");
	}
	if (frequency == Frequency::Daily) {
		*out = kTimeBlockDayLength;
		return Status::Ok();
	}
	Position total = 0;
	for (Position offset = 0; offset < kTimeBlockDayLength; ++offset) {
		std::string current_date;
		Status status = date(block_id * kTimeBlockDayLength + offset, &current_date);
		if (!status.ok()) {
			return status;
		}
		std::vector<HourSlot> date_slots;
		status = slots(current_date, &date_slots);
		if (!status.ok()) {
			return status;
		}
		total += static_cast<Position>(date_slots.size());
	}
	*out = total;
	return Status::Ok();
}

Status Calendar::set_closed(const std::string& value) {
	DayId ignored_day_id = 0;
	Status status = day_id(value, &ignored_day_id);
	if (!status.ok()) {
		return status;
	}
	override_slots_[value] = std::vector<HourSlot>();
	return Status::Ok();
}

Status Calendar::set_slots(const std::string& value,
                                   const std::vector<HourSlot>& slots) {
	DayId ignored_day_id = 0;
	Status status = day_id(value, &ignored_day_id);
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
		DayId ignored_day_id = 0;
		Status status = day_id(date_value, &ignored_day_id);
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
