// test_active.cpp
//
// Exercises the mutable current-window store and its append-only recovery log.

#include <assert.h>

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include <sys/stat.h>
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

void RemoveFrequencyDirectory(const std::string& path) {
	std::remove((path + "/active.data").c_str());
	std::remove((path + "/staging-index").c_str());
	std::remove((path + "/staging-pages-0001.seg").c_str());
	std::remove((path + "/staging-pages-0002.seg").c_str());
	std::remove((path + "/vault-index").c_str());
	std::remove((path + "/vault-index.tmp").c_str());
	std::remove((path + "/vault-0001.seg").c_str());
	std::remove((path + "/vault-0002.seg").c_str());
	const int result = rmdir(path.c_str());
	assert(result == 0 || errno == ENOENT);
}

void RemoveTestDirectory(const std::string& path) {
	std::remove((path + "/active.data").c_str());
	RemoveFrequencyDirectory(path + "/daily");
	RemoveFrequencyDirectory(path + "/hourly");
	assert(rmdir(path.c_str()) == 0);
}

// Builds one complete daily block with a single explicitly present bar. The
// remaining positions are intentionally Missing so the Vault test exercises
// its persisted presence bitmap as well as sparse numeric frame locators.
void IngestVaultBar(zstfs::VaultStore* vault,
					const zstfs::Calendar& calendar,
					zstfs::SymbolId symbol_id,
					const std::string& local_time,
					double close) {
	zstfs::TimeId time_id = 0;
	zstfs::TimeId block_id = 0;
	zstfs::BlockOff block_offset = 0;
	ExpectOk(calendar.time_id(local_time, &time_id));
	ExpectOk(calendar.block_offset(zstfs::Frequency::Daily, local_time,
		&block_id, &block_offset));
	zstfs::BlockOff block_length = 0;
	ExpectOk(calendar.block_length(zstfs::Frequency::Daily, block_id, &block_length));
	zstfs::BlockBar missing = {zstfs::BarState::Missing, 0.0, 0.0, 0.0, 0.0, 0.0};
	zstfs::StockTimeBlock block = {};
	block.key.symbol_id = symbol_id;
	block.key.time_block_id = block_id;
	block.day_presence = 1;
	block.positions.assign(block_length, missing);
	const zstfs::Bar bar = BarFor(symbol_id, zstfs::Frequency::Daily, local_time, close);
	block.positions[block_offset] = {bar.state, bar.open, bar.high, bar.low, bar.close, bar.volume};
	zstfs::ActiveBar active_bar = {symbol_id, time_id, block.positions[block_offset]};
	std::vector<zstfs::StockTimeBlock> blocks(1, block);
	std::vector<zstfs::ActiveBar> bars(1, active_bar);
	ExpectOk(vault->ingest(blocks, bars));
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
	std::ofstream tail((path + "/daily/active.data").c_str(),
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

void TestStagingBatchAndIndexRecovery() {
	const std::string path = MakeTestDirectory();
	const std::string frequency_path = path + "/daily";
	assert(mkdir(frequency_path.c_str(), 0755) == 0);
	zstfs::Calendar calendar("CNA");
	zstfs::TimeId time_id = 0;
	zstfs::TimeId block_id = 0;
	zstfs::BlockOff block_offset = 0;
	ExpectOk(calendar.time_id("20260803", &time_id));
	ExpectOk(calendar.block_offset(zstfs::Frequency::Daily, "20260803", &block_id,
		&block_offset));
	zstfs::BlockOff block_length = 0;
	ExpectOk(calendar.block_length(zstfs::Frequency::Daily, block_id, &block_length));

	zstfs::BlockBar missing = {zstfs::BarState::Missing, 0.0, 0.0, 0.0, 0.0, 0.0};
	zstfs::BlockBar first = {zstfs::BarState::Normal, 9.5, 11.0, 9.0, 10.0, 100.0};
	zstfs::BlockBar second = {zstfs::BarState::Normal, 19.5, 21.0, 19.0, 20.0, 200.0};
	zstfs::StockTimeBlock first_block = {};
	first_block.key.symbol_id = 31;
	first_block.key.time_block_id = block_id;
	first_block.positions.assign(block_length, missing);
	first_block.positions[block_offset] = first;
	first_block.day_presence = static_cast<uint64_t>(1) <<
		(zstfs::time_day(time_id) - zstfs::time_day(block_id));
	zstfs::StockTimeBlock second_block = first_block;
	second_block.key.symbol_id = 32;
	second_block.positions[block_offset] = second;
	zstfs::ActiveBar first_bar = {31, time_id, first};
	zstfs::ActiveBar second_bar = {32, time_id, second};

	{
		zstfs::StagingStore staging(zstfs::Frequency::Daily, calendar, frequency_path);
		std::vector<zstfs::StockTimeBlock> first_batch(1, first_block);
		std::vector<zstfs::ActiveBar> first_bars(1, first_bar);
		ExpectOk(staging.accept(first_batch, first_bars));
		std::vector<zstfs::StockTimeBlock> mixed_batch;
		mixed_batch.push_back(first_block);
		mixed_batch.push_back(second_block);
		std::vector<zstfs::ActiveBar> mixed_bars;
		mixed_bars.push_back(first_bar);
		mixed_bars.push_back(second_bar);
		ExpectOk(staging.accept(mixed_batch, mixed_bars));
		zstfs::BlockBar value = {};
		ExpectOk(staging.get(32, time_id, &value));
		assert(value.close > 19.9 && value.close < 20.1);
	}
	assert(FileSize(frequency_path + "/staging-pages-0001.seg") > 0);
	std::ofstream corrupt_index((frequency_path + "/staging-index").c_str(),
		std::ios::binary | std::ios::trunc);
	corrupt_index.write("invalid", 7);
	corrupt_index.close();
	{
		zstfs::StagingStore staging(zstfs::Frequency::Daily, calendar, frequency_path);
		zstfs::BlockBar value = {};
		ExpectOk(staging.get(31, time_id, &value));
		assert(value.close > 9.9 && value.close < 10.1);
		ExpectOk(staging.get(32, time_id, &value));
		assert(value.close > 19.9 && value.close < 20.1);
	}
	std::fstream corrupt_page((frequency_path + "/staging-pages-0001.seg").c_str(),
		std::ios::binary | std::ios::in | std::ios::out);
	corrupt_page.seekp(0);
	corrupt_page.write("X", 1);
	corrupt_page.close();
	{
		zstfs::StagingStore staging(zstfs::Frequency::Daily, calendar, frequency_path);
		zstfs::BlockBar value = {};
		assert(staging.get(31, time_id, &value).code() == zstfs::ErrorCode::CorruptData);
	}
	RemoveTestDirectory(path);
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

void TestVaultPersistenceMergeAndCorruption() {
	const std::string path = MakeTestDirectory();
	const zstfs::SymbolId corrupt_symbol = 41;
	const zstfs::SymbolId intact_symbol = 42;
	{
		// Market creates the per-frequency directories; Vault ingestion then uses
		// the same internal compaction boundary a future offline worker will use.
		zstfs::Market bootstrap("vault-bootstrap", path, "CNA");
	}
	zstfs::Calendar calendar("CNA");
	{
		zstfs::VaultStore vault(zstfs::Frequency::Daily, calendar, path + "/daily", 1001);
		IngestVaultBar(&vault, calendar, corrupt_symbol, "20260805", 12.0);
		IngestVaultBar(&vault, calendar, corrupt_symbol, "20260803", 10.0);
		IngestVaultBar(&vault, calendar, corrupt_symbol, "20260804", 11.0);
		IngestVaultBar(&vault, calendar, intact_symbol, "20260803", 20.0);
		zstfs::TimeId time_id = 0;
		ExpectOk(calendar.time_id("20260803", &time_id));
		zstfs::BlockBar bar = {};
		ExpectOk(vault.get(corrupt_symbol, time_id, &bar));
		assert(bar.close == 10.0);
	}
	{
		zstfs::Market market("vault-read", path, "CNA");
		zstfs::History& history = market.history(zstfs::Frequency::Daily);
		zstfs::Bar bar = {};
		ExpectOk(history.get(corrupt_symbol, "20260803", &bar));
		assert(bar.close == 10.0);

		// Active data is newer than Vault data for the same logical key.
		ExpectOk(history.put(BarFor(corrupt_symbol, zstfs::Frequency::Daily,
			"20260803", 30.0)));
		bar = BarFor(corrupt_symbol, zstfs::Frequency::Daily, "20260803", 999.0);
		assert(history.get(corrupt_symbol, "20260803", &bar).code() ==
			zstfs::ErrorCode::CorruptData);
		assert(bar.close == 999.0);

		// Once sealed, Staging remains newer than the underlying Vault value.
		ExpectOk(history.put(BarFor(corrupt_symbol, zstfs::Frequency::Daily,
			"20260804", 40.0)));
		ExpectOk(history.seal_before("20260805"));
		bar = BarFor(corrupt_symbol, zstfs::Frequency::Daily, "20260804", 999.0);
		assert(history.get(corrupt_symbol, "20260804", &bar).code() ==
			zstfs::ErrorCode::CorruptData);
		assert(bar.close == 999.0);
		std::vector<zstfs::Bar> values;
		std::vector<zstfs::SymbolId> symbols;
		symbols.push_back(intact_symbol);
		symbols.push_back(corrupt_symbol);
		assert(history.get(symbols, "20260803", "20260804",
			zstfs::AdjustMode::Raw, &values).code() == zstfs::ErrorCode::CorruptData);
		assert(values.size() == 3);
		assert(values[0].symbol_id == corrupt_symbol && values[0].close == 30.0);
		assert(values[1].symbol_id == intact_symbol && values[1].close == 20.0);
		assert(values[2].symbol_id == corrupt_symbol && values[2].close == 40.0);
	}

	// Keep only the immutable layer for the corruption phase, so a newer Active
	// or Staging value cannot legitimately mask the damaged Vault target.
	std::remove((path + "/daily/active.data").c_str());
	std::remove((path + "/daily/staging-index").c_str());
	std::remove((path + "/daily/staging-pages-0001.seg").c_str());

	// A fresh runtime namespace cannot reuse the prior compressed cache. Corrupt
	// one blob and verify a target get exposes no result while range retains the
	// independently readable symbol in sorted partial output.
	std::fstream corrupt((path + "/daily/vault-0001.seg").c_str(),
		std::ios::binary | std::ios::in | std::ios::out);
	assert(corrupt);
	corrupt.seekp(0);
	corrupt.write("X", 1);
	corrupt.close();
	{
		zstfs::Market market("vault-corrupt", path, "CNA");
		zstfs::History& history = market.history(zstfs::Frequency::Daily);
		zstfs::Bar bar = BarFor(corrupt_symbol, zstfs::Frequency::Daily, "20260805", 999.0);
		assert(history.get(corrupt_symbol, "20260805", &bar).code() == zstfs::ErrorCode::CorruptData);
		assert(bar.close == 999.0);
		std::vector<zstfs::Bar> values;
		std::vector<zstfs::SymbolId> symbols;
		symbols.push_back(corrupt_symbol);
		symbols.push_back(intact_symbol);
		assert(history.get(symbols, "20260803", "20260805",
			zstfs::AdjustMode::Raw, &values).code() == zstfs::ErrorCode::CorruptData);
		assert(values.size() == 3);
		assert(values[0].symbol_id == corrupt_symbol && values[0].close == 10.0);
		assert(values[1].symbol_id == intact_symbol && values[1].close == 20.0);
		assert(values[2].symbol_id == corrupt_symbol && values[2].close == 11.0);
	}
	RemoveTestDirectory(path);
}

}  // namespace

int main() {
	TestDailyWriteReadAndRecovery();
	TestAutomaticFlush();
	TestStagingBatchAndIndexRecovery();
	TestHourlyOrderingAndCanonicalSlots();
	TestVaultPersistenceMergeAndCorruption();
	return 0;
}
