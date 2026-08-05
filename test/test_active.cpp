// test_active.cpp
//
// Exercises the mutable current-window store and its append-only recovery log.

#include <assert.h>

#include <chrono>
#include <cstdio>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

#include "zstfs/market.h"
#include "../libzstfs/history.h"

namespace {

void ExpectOk(const zstfs::Status& status) {
	assert(status.ok());
}

zstfs::Bar BarFor(zstfs::SymbolId symbol_id,
			  zstfs::Frequency frequency,
			  const std::string& local_time,
			  double close) {
	zstfs::Bar bar = {};
	bar.symbol_id = symbol_id;
	bar.frequency = frequency;
	bar.local_time = local_time;
	bar.state = zstfs::BarState::Normal;
	bar.open = close - 0.5;
	bar.high = close + 1.0;
	bar.low = close - 1.0;
	bar.close = close;
	bar.volume = 100.0;
	return bar;
}

zstfs::Bar MissingBar(zstfs::SymbolId symbol_id,
			      zstfs::Frequency frequency,
			      const std::string& local_time) {
	zstfs::Bar bar = {};
	bar.symbol_id = symbol_id;
	bar.frequency = frequency;
	bar.local_time = local_time;
	bar.state = zstfs::BarState::Missing;
	return bar;
}

std::string MakeTestDirectory() {
	char path[] = "/tmp/zstfs-active-XXXXXX";
	assert(mkdtemp(path) != NULL);
	return path;
}

void RemoveTestDirectory(const std::string& path) {
	std::remove((path + "/active.data").c_str());
	assert(rmdir(path.c_str()) == 0);
}

void TestDailyWriteReadAndRecovery() {
	const std::string path = MakeTestDirectory();
	const zstfs::SymbolId symbol_id = 7;
	{
		zstfs::Market market("active-test", path, "CNA");
		zstfs::History& history = market.history(zstfs::Frequency::Daily);
		zstfs::History& hourly = market.history(zstfs::Frequency::Hourly);
		ExpectOk(hourly.put(BarFor(symbol_id, zstfs::Frequency::Hourly,
			"20260803-0930", 10.0)));
		ExpectOk(hourly.flush());
		ExpectOk(history.put(BarFor(symbol_id, zstfs::Frequency::Daily,
			"20260804", 12.0)));
		ExpectOk(history.put(BarFor(symbol_id, zstfs::Frequency::Daily,
			"20260803", 11.0)));
		assert(history.put(BarFor(symbol_id, zstfs::Frequency::Daily,
			"20260803", 11.0)).code() == zstfs::ErrorCode::AlreadyPresent);

		std::vector<zstfs::Bar> batch;
		batch.push_back(BarFor(symbol_id, zstfs::Frequency::Daily, "20260805", 13.0));
		batch.push_back(BarFor(symbol_id, zstfs::Frequency::Daily, "20260806", 14.0));
		ExpectOk(history.put(batch));
		batch.clear();
		batch.push_back(BarFor(symbol_id, zstfs::Frequency::Daily, "20260807", 15.0));
		batch.push_back(BarFor(symbol_id, zstfs::Frequency::Daily, "20260806", 14.0));
		assert(history.put(batch).code() == zstfs::ErrorCode::AlreadyPresent);

		zstfs::Bar bar = {};
		assert(history.get(symbol_id, "20260807", &bar).code() == zstfs::ErrorCode::NotFound);
		ExpectOk(history.get(symbol_id, "20260803", &bar));
		assert(bar.close == 11.0);
		ExpectOk(history.put(MissingBar(symbol_id, zstfs::Frequency::Daily, "20260807")));
		ExpectOk(history.get(symbol_id, "20260807", &bar));
		assert(bar.state == zstfs::BarState::Missing);

		std::vector<zstfs::Bar> values;
		ExpectOk(history.get(symbol_id, "20260803", "20260807",
			zstfs::AdjustMode::Raw, &values));
		assert(values.size() == 5);
		assert(values[0].local_time == "20260803");
		assert(values[4].state == zstfs::BarState::Missing);
		ExpectOk(history.flush());
	}
	std::ofstream tail((path + "/active.data").c_str(),
		std::ios::binary | std::ios::app);
	tail.write("tail", 4);
	tail.close();
	{
		zstfs::Market market("active-test", path, "CNA");
		zstfs::History& history = market.history(zstfs::Frequency::Daily);
		zstfs::Bar bar = {};
		ExpectOk(history.get(symbol_id, "20260804", &bar));
		assert(bar.close == 12.0);
		ExpectOk(history.get(symbol_id, "20260807", &bar));
		assert(bar.state == zstfs::BarState::Missing);
		ExpectOk(history.put(BarFor(symbol_id, zstfs::Frequency::Daily,
			"20260810", 16.0)));
		ExpectOk(history.seal_before("20270101"));
		assert(history.put(BarFor(symbol_id, zstfs::Frequency::Daily,
			"20260803", 99.0)).code() == zstfs::ErrorCode::AlreadyPresent);
		std::vector<zstfs::Bar> late_batch;
		late_batch.push_back(BarFor(symbol_id, zstfs::Frequency::Daily,
			"20260811", 17.0));
		late_batch.push_back(BarFor(symbol_id, zstfs::Frequency::Daily,
			"20260803", 99.0));
		assert(history.put(late_batch).code() == zstfs::ErrorCode::AlreadyPresent);
		assert(history.get(symbol_id, "20260811", &bar).code() == zstfs::ErrorCode::NotFound);
		ExpectOk(history.get(symbol_id, "20260803", &bar));
		assert(bar.close == 11.0);
		zstfs::History& hourly = market.history(zstfs::Frequency::Hourly);
		ExpectOk(hourly.get(symbol_id, "20260803-0930", &bar));
		assert(bar.close == 10.0);
	}
	{
		zstfs::Market market("active-test", path, "CNA");
		zstfs::Bar bar = {};
		ExpectOk(market.history(zstfs::Frequency::Daily).get(
			symbol_id, "20260803", &bar));
		assert(bar.close == 11.0);
		ExpectOk(market.history(zstfs::Frequency::Daily).get(
			symbol_id, "20260810", &bar));
		assert(bar.close == 16.0);
		ExpectOk(market.history(zstfs::Frequency::Hourly).get(
			symbol_id, "20260803-0930", &bar));
		assert(bar.close == 10.0);
	}
	RemoveTestDirectory(path);
}

size_t FileSize(const std::string& path) {
	std::ifstream input(path.c_str(), std::ios::binary | std::ios::ate);
	assert(input);
	return static_cast<size_t>(input.tellg());
}

void TestAutomaticFlush() {
	zstfs::Calendar calendar("CNA");
	zstfs::TimeId time_id = 0;
	zstfs::TimeId block_id = 0;
	zstfs::BlockOff block_offset = 0;
	ExpectOk(calendar.time_id("20260803", &time_id));
	ExpectOk(calendar.block_offset(zstfs::Frequency::Daily, "20260803",
		&block_id, &block_offset));
	zstfs::BlockBar bar = {zstfs::BarState::Normal, 10.0, 11.0, 9.0, 10.5, 100.0};

	const std::string threshold_path = MakeTestDirectory();
	{
		zstfs::ActiveStore active(zstfs::Frequency::Daily, calendar, threshold_path,
			1, 60 * 1000);
		ExpectOk(active.put(9, time_id, block_id, block_offset, bar, false));
		ExpectOk(active.flush_if_needed());
		assert(FileSize(threshold_path + "/active.data") == 56);
	}
	RemoveTestDirectory(threshold_path);

	const std::string timer_path = MakeTestDirectory();
	{
		zstfs::ActiveStore active(zstfs::Frequency::Daily, calendar, timer_path,
			1024 * 1024, 5);
		ExpectOk(active.put(9, time_id, block_id, block_offset, bar, false));
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
		assert(FileSize(timer_path + "/active.data") == 56);
	}
	RemoveTestDirectory(timer_path);
}

void TestHourlyOrderingAndCanonicalSlots() {
	const std::string path = MakeTestDirectory();
	const zstfs::SymbolId symbol_id = 8;
	{
		zstfs::Market market("active-hourly", path, "CNA");
		zstfs::History& history = market.history(zstfs::Frequency::Hourly);
		ExpectOk(history.put(BarFor(symbol_id, zstfs::Frequency::Hourly,
			"20260803-1400", 20.0)));
		ExpectOk(history.put(BarFor(symbol_id, zstfs::Frequency::Hourly,
			"20260803-0939", 19.0)));
		zstfs::Bar bar = {};
		ExpectOk(history.get(symbol_id, "20260803-0930", &bar));
		assert(bar.local_time == "20260803-0930");
		assert(bar.close == 19.0);
		std::vector<zstfs::Bar> values;
		ExpectOk(history.get(symbol_id, "20260803-0930", "20260803-1400",
			zstfs::AdjustMode::Raw, &values));
		assert(values.size() == 2);
		assert(values[0].local_time == "20260803-0930");
		assert(values[1].local_time == "20260803-1400");
		ExpectOk(history.flush());
	}
	RemoveTestDirectory(path);
}

}  // namespace

int main() {
	TestDailyWriteReadAndRecovery();
	TestAutomaticFlush();
	TestHourlyOrderingAndCanonicalSlots();
	return 0;
}
