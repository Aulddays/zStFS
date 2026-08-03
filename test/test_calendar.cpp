// test_calendar.cpp
//
// Verifies the market-local calendar coordinate model used by Milestone 2.

#include <assert.h>

#include <vector>

#include "../libzstfs/calendar.h"

static zstfs::Status CustomSlots(const std::string&, std::vector<zstfs::HourSlot>* out) {
	out->clear();
	out->push_back(100);
	return zstfs::Status::Ok();
}

int main() {
	zstfs::Calendar calendar("CNA");
	zstfs::DayId day_id = 0;
	assert(calendar.day_id("19000101", &day_id).ok());
	assert(day_id == 0);
	assert(calendar.day_id("19000105", &day_id).ok());
	assert(day_id == 4);
	assert(calendar.day_id("19000108", &day_id).ok());
	assert(day_id == 5);
	assert(calendar.day_id("19000106", &day_id).code() == zstfs::ErrorCode::NotFound);

	std::string date;
	assert(calendar.date(5, &date).ok());
	assert(date == "19000108");

	zstfs::HourSlot slot = 0;
	assert(calendar.hour_slot("19000101-0939", &slot).ok());
	assert(slot == 93);
	assert(calendar.local_time(0, slot, &date).ok());
	assert(date == "19000101-0930");

	std::vector<zstfs::HourSlot> slots;
	assert(calendar.slots("19000101", &slots).ok());
	assert(slots.size() == 5);
	assert(slots[0] == 93);

	zstfs::TimeBlockId block_id = 0;
	zstfs::Position position = 0;
	assert(calendar.block_position(zstfs::Frequency::Daily,
	                               "19000108", &block_id, &position).ok());
	assert(block_id == 0);
	assert(position == 5);
	assert(calendar.block_position(zstfs::Frequency::Hourly,
	                               "19000108-1330", &block_id, &position).ok());
	assert(position == 28);
	assert(calendar.date(64, &date).ok());
	assert(calendar.block_position(zstfs::Frequency::Daily,
	                               date, &block_id, &position).ok());
	assert(block_id == 1 && position == 0);
	assert(calendar.block_position(zstfs::Frequency::Hourly,
	                               date + "-0930", &block_id, &position).ok());
	assert(block_id == 1 && position == 0);

	zstfs::Position block_length = 0;
	assert(calendar.block_length(zstfs::Frequency::Hourly, 0, &block_length).ok());
	assert(block_length == 320);

	assert(calendar.set_closed("19000102").ok());
	assert(calendar.slots("19000102", &slots).ok());
	assert(slots.empty());
	assert(calendar.block_length(zstfs::Frequency::Hourly, 0, &block_length).ok());
	assert(block_length == 315);

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
	return 0;
}
