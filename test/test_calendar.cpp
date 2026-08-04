// test_calendar.cpp
//
// Verifies the market-local calendar coordinate model used by Milestone 2.

#include <assert.h>

#include <vector>

#include "../libzstfs/calendar.h"

static zstfs::Status CustomSlots(const std::string& date,
                                 std::vector<zstfs::HourSlot>* out) {
	out->clear();
	out->push_back(100);
	if (date >= "19000102") {
		out->push_back(110);
	}
	return zstfs::Status::Ok();
}

int main() {
	zstfs::Calendar calendar("CNA");
	zstfs::TimeId day_time_id = 0;
	assert(calendar.time_id("19000101", &day_time_id).ok());
	assert(day_time_id == zstfs::daily_bar_id(0));
	assert(calendar.time_id("19000105", &day_time_id).ok());
	assert(day_time_id == zstfs::daily_bar_id(4));
	assert(calendar.time_id("19000108", &day_time_id).ok());
	assert(day_time_id == zstfs::daily_bar_id(5));
	assert(calendar.time_id("19000106", &day_time_id).code() ==
	       zstfs::ErrorCode::NotFound);

	std::string date;
	assert(calendar.date(zstfs::daily_bar_id(5), &date).ok());
	assert(date == "19000108");

	zstfs::HourSlot slot = 0;
	assert(calendar.hour_slot("19000101-0939", &slot).ok());
	assert(slot == 93);
	assert(calendar.local_time(zstfs::daily_bar_id(0), slot, &date).ok());
	assert(date == "19000101-0930");

	const zstfs::TimeId daily_id = zstfs::daily_bar_id(5);
	assert(daily_id == (static_cast<zstfs::TimeId>(5) << 8));
	assert(zstfs::time_day(daily_id) == 5);
	assert(zstfs::time_slot(daily_id) == 0);
	const zstfs::TimeId hourly_id = zstfs::hourly_bar_id(5, 93);
	assert(hourly_id == (static_cast<zstfs::TimeId>(5) << 8) + 93);
	assert(zstfs::time_day(hourly_id) == 5);
	assert(zstfs::time_slot(hourly_id) == 93);
	assert(calendar.date(hourly_id, &date).code() ==
	       zstfs::ErrorCode::InvalidArgument);

	std::vector<zstfs::HourSlot> slots;
	assert(calendar.slots("19000101", &slots).ok());
	assert(slots.size() == 4);
	assert(slots[0] == 93 && slots[1] == 103);
	assert(slots[2] == 130 && slots[3] == 140);
	assert(calendar.slots("20260731", &slots).ok());
	assert(slots.size() == 4 && slots[2] == 130 && slots[3] == 140);
	assert(calendar.slots("20260803", &slots).ok());
	assert(slots.size() == 4 && slots[2] == 130 && slots[3] == 140);

	zstfs::TimeId block_id = 0;
	zstfs::BlockOff offset = 0;
	assert(calendar.block_offset(zstfs::Frequency::Daily,
	                               "19000108", &block_id, &offset).ok());
	assert(block_id == 0);
	assert(offset == 5);
	assert(calendar.block_offset(zstfs::Frequency::Hourly,
	                               "19000108-1300", &block_id, &offset).ok());
	assert(offset == 22);
	assert(calendar.block_offset(zstfs::Frequency::Hourly,
	                               "19000101-1130", &block_id, &offset).code() ==
	       zstfs::ErrorCode::NotFound);
	assert(calendar.date(zstfs::daily_bar_id(zstfs::kDailyTimeBlockDayLength),
	                     &date).ok());
	assert(calendar.block_offset(zstfs::Frequency::Daily,
	                               date, &block_id, &offset).ok());
	assert(block_id == zstfs::daily_bar_id(zstfs::kDailyTimeBlockDayLength) &&
	       offset == 0);
	assert(calendar.block_offset(zstfs::Frequency::Hourly,
	                               date + "-0930", &block_id, &offset).ok());
	assert(block_id == zstfs::daily_bar_id(zstfs::kHourlyTimeBlockDayLength) &&
	       offset == 0);

	zstfs::BlockOff block_length = 0;
	assert(calendar.block_length(zstfs::Frequency::Hourly, 0, &block_length).ok());
	assert(block_length == 256);

	assert(calendar.set_closed("19000102").ok());
	assert(calendar.slots("19000102", &slots).ok());
	assert(slots.empty());
	assert(calendar.block_length(zstfs::Frequency::Hourly, 0, &block_length).ok());
	assert(block_length == 252);

	const std::string calendar_path = "/tmp/zstfs-calendar-test.bin";
	assert(calendar.save(calendar_path).ok());
	zstfs::Calendar restored_calendar("CNA");
	assert(restored_calendar.load(calendar_path).ok());
	assert(restored_calendar.slots("19000102", &slots).ok());
	assert(slots.empty());

	assert(zstfs::register_market_type("custom", CustomSlots).ok());
	zstfs::Calendar custom("custom");
	assert(custom.slots("19000101", &slots).ok());
	assert(slots.size() == 1 && slots[0] == 100);
	assert(custom.slots("19000102", &slots).ok());
	assert(slots.size() == 2 && slots[1] == 110);
	assert(custom.block_offset(zstfs::Frequency::Hourly,
	                             "19000101-1000", &block_id, &offset).ok());
	assert(block_id == 0 && offset == 0);
	assert(custom.block_offset(zstfs::Frequency::Hourly,
	                             "19000102-1100", &block_id, &offset).ok());
	assert(block_id == 0 && offset == 2);
	return 0;
}
