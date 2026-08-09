// test_active.cpp
//
// Exercises the mutable current-window store and its append-only recovery log.

#include <assert.h>

#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <dirent.h>
#include <fstream>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <sys/stat.h>
#include <unistd.h>

#include "zstfs/market.h"
#include "../libzstfs/history.h"

namespace {

void ExpectOk(const zstfs::Status& status) {
	if (!status.ok()) {
		fprintf(stderr, "unexpected status %d: %s\n", static_cast<int>(status.code()),
			status.message().c_str());
	}
	assert(status.ok());
}

bool CloseEnough(double actual, double expected) {
	return std::abs(actual - expected) <= std::abs(expected) * 1e-4 + 1e-12;
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

void RemoveTree(const std::string& path) {
	DIR* directory = opendir(path.c_str());
	if (directory == NULL) {
		std::remove(path.c_str());
		return;
	}
	for (dirent* entry = readdir(directory); entry != NULL; entry = readdir(directory)) {
		const std::string name(entry->d_name);
		if (name == "." || name == "..") {
			continue;
		}
		const std::string child = path + "/" + name;
		struct stat metadata = {};
		if (stat(child.c_str(), &metadata) == 0 && S_ISDIR(metadata.st_mode)) {
			RemoveTree(child);
		} else {
			std::remove(child.c_str());
		}
	}
	closedir(directory);
	assert(rmdir(path.c_str()) == 0 || errno == ENOENT);
}

void RemoveTestDirectory(const std::string& path) {
	RemoveTree(path);
}

// MarketFixture models the application-owned root configuration boundary. Each
// fixture writes the immutable name,type record before opening Markets, which
// keeps the returned Market valid for all operations in its enclosing scope.
class MarketFixture {
public:
	MarketFixture(const std::string& root_path, const std::string& name,
		const std::string& type)
		: name_(name), markets_(), market_(NULL) {
		std::ofstream config((root_path + "/markets.conf").c_str());
		assert(config);
		config << name << "," << type << "\n";
		config.close();

		markets_.reset(new zstfs::Markets(root_path));
		ExpectOk(markets_->status());
		ExpectOk(markets_->get(name_, &market_));
	}

	zstfs::Market& market() {
		assert(market_ != NULL);
		return *market_;
	}

private:
	std::string name_;
	std::unique_ptr<zstfs::Markets> markets_;
	zstfs::Market* market_;
};

std::string MarketPath(const std::string& root_path, const std::string& name) {
	return root_path + "/markets/" + name;
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
		MarketFixture fixture(path, "active-test", "CNA");
		zstfs::Market& market = fixture.market();
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
		assert(CloseEnough(bar.close, 11.0));
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
	std::ofstream tail((MarketPath(path, "active-test") + "/daily/active.data").c_str(),
		std::ios::binary | std::ios::app);
	tail.write("tail", 4);
	tail.close();
	{
		MarketFixture fixture(path, "active-test", "CNA");
		zstfs::Market& market = fixture.market();
		zstfs::History& history = market.history(zstfs::Frequency::Daily);
		zstfs::Bar bar = {};
		ExpectOk(history.get(symbol_id, "20260804", &bar));
		assert(CloseEnough(bar.close, 12.0));
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
		assert(CloseEnough(bar.close, 11.0));
		zstfs::History& hourly = market.history(zstfs::Frequency::Hourly);
		ExpectOk(hourly.get(symbol_id, "20260803-0930", &bar));
		assert(CloseEnough(bar.close, 10.0));
	}
	{
		MarketFixture fixture(path, "active-test", "CNA");
		zstfs::Market& market = fixture.market();
		zstfs::Bar bar = {};
		ExpectOk(market.history(zstfs::Frequency::Daily).get(
			symbol_id, "20260803", &bar));
		assert(CloseEnough(bar.close, 11.0));
		ExpectOk(market.history(zstfs::Frequency::Daily).get(
			symbol_id, "20260810", &bar));
		assert(CloseEnough(bar.close, 16.0));
		ExpectOk(market.history(zstfs::Frequency::Hourly).get(
			symbol_id, "20260803-0930", &bar));
		assert(CloseEnough(bar.close, 10.0));
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
		MarketFixture fixture(path, "active-hourly", "CNA");
		zstfs::Market& market = fixture.market();
		zstfs::History& history = market.history(zstfs::Frequency::Hourly);
		ExpectOk(history.put(BarFor(symbol_id, zstfs::Frequency::Hourly,
			"20260803-1400", 20.0)));
		ExpectOk(history.put(BarFor(symbol_id, zstfs::Frequency::Hourly,
			"20260803-0939", 19.0)));
		zstfs::Bar bar = {};
		ExpectOk(history.get(symbol_id, "20260803-0930", &bar));
		assert(bar.local_time == "20260803-0930");
		assert(CloseEnough(bar.close, 19.0));
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
	zstfs::Calendar calendar("CNA");
	{
		// This fixture uses VaultStore directly to exercise its persistent format.
		// Market::sync publishes the completed internal write as a valid generation
		// before the next Market open verifies and consumes it.
		MarketFixture fixture(path, "vault-test", "CNA");
		zstfs::Market& bootstrap = fixture.market();
		{
			zstfs::VaultStore vault(zstfs::Frequency::Daily, calendar,
				MarketPath(path, "vault-test") + "/daily", 1001);
			IngestVaultBar(&vault, calendar, corrupt_symbol, "20260805", 12.0);
			IngestVaultBar(&vault, calendar, corrupt_symbol, "20260803", 10.0);
			IngestVaultBar(&vault, calendar, corrupt_symbol, "20260804", 11.0);
			IngestVaultBar(&vault, calendar, intact_symbol, "20260803", 20.0);
			zstfs::TimeId time_id = 0;
			ExpectOk(calendar.time_id("20260803", &time_id));
			zstfs::BlockBar bar = {};
			ExpectOk(vault.get(corrupt_symbol, time_id, &bar));
			assert(CloseEnough(bar.close, 10.0));
		}
		ExpectOk(bootstrap.sync());
	}
	{
		MarketFixture fixture(path, "vault-test", "CNA");
		zstfs::Market& market = fixture.market();
		zstfs::History& history = market.history(zstfs::Frequency::Daily);
		zstfs::Bar bar = {};
		ExpectOk(history.get(corrupt_symbol, "20260803", &bar));
		assert(CloseEnough(bar.close, 10.0));

		// Active data is newer than Vault data for the same logical key.
		ExpectOk(history.put(BarFor(corrupt_symbol, zstfs::Frequency::Daily,
			"20260803", 30.0)));
		bar = BarFor(corrupt_symbol, zstfs::Frequency::Daily, "20260803", 999.0);
		assert(history.get(corrupt_symbol, "20260803", &bar).code() ==
			zstfs::ErrorCode::CorruptData);
		assert(CloseEnough(bar.close, 999.0));

		// Once sealed, Staging remains newer than the underlying Vault value.
		ExpectOk(history.put(BarFor(corrupt_symbol, zstfs::Frequency::Daily,
			"20260804", 40.0)));
		ExpectOk(history.seal_before("20260805"));
		bar = BarFor(corrupt_symbol, zstfs::Frequency::Daily, "20260804", 999.0);
		assert(history.get(corrupt_symbol, "20260804", &bar).code() ==
			zstfs::ErrorCode::CorruptData);
		assert(CloseEnough(bar.close, 999.0));
		std::vector<zstfs::Bar> values;
		std::vector<zstfs::SymbolId> symbols;
		symbols.push_back(intact_symbol);
		symbols.push_back(corrupt_symbol);
		assert(history.get(symbols, "20260803", "20260804",
			zstfs::AdjustMode::Raw, &values).code() == zstfs::ErrorCode::CorruptData);
		assert(values.size() == 3);
		assert(values[0].symbol_id == corrupt_symbol && CloseEnough(values[0].close, 30.0));
		assert(values[1].symbol_id == intact_symbol && CloseEnough(values[1].close, 20.0));
		assert(values[2].symbol_id == corrupt_symbol && CloseEnough(values[2].close, 40.0));
	}

	// Keep only the immutable layer for the corruption phase, so a newer Active
	// or Staging value cannot legitimately mask the damaged Vault target. Publish
	// the intentionally reduced fixture before corrupting a Vault payload in place.
	{
		MarketFixture fixture(path, "vault-test", "CNA");
		zstfs::Market& cleanup = fixture.market();
		std::remove((MarketPath(path, "vault-test") + "/daily/active.data").c_str());
		std::remove((MarketPath(path, "vault-test") + "/daily/staging-index").c_str());
		std::remove((MarketPath(path, "vault-test") + "/daily/staging-pages-0001.seg").c_str());
		ExpectOk(cleanup.sync());
	}

	// A fresh runtime namespace cannot reuse the prior compressed cache. Corrupt
	// one blob and verify a target get exposes no result while range retains the
	// independently readable symbol in sorted partial output.
	std::fstream corrupt((MarketPath(path, "vault-test") + "/daily/vault-0001.seg").c_str(),
		std::ios::binary | std::ios::in | std::ios::out);
	assert(corrupt);
	corrupt.seekp(0);
	corrupt.write("X", 1);
	corrupt.close();
	{
		MarketFixture fixture(path, "vault-test", "CNA");
		zstfs::Market& market = fixture.market();
		zstfs::History& history = market.history(zstfs::Frequency::Daily);
		zstfs::Bar bar = BarFor(corrupt_symbol, zstfs::Frequency::Daily, "20260805", 999.0);
		assert(history.get(corrupt_symbol, "20260805", &bar).code() == zstfs::ErrorCode::CorruptData);
		assert(CloseEnough(bar.close, 999.0));
		std::vector<zstfs::Bar> values;
		std::vector<zstfs::SymbolId> symbols;
		symbols.push_back(corrupt_symbol);
		symbols.push_back(intact_symbol);
		assert(history.get(symbols, "20260803", "20260805",
			zstfs::AdjustMode::Raw, &values).code() == zstfs::ErrorCode::CorruptData);
		assert(values.size() == 3);
		assert(values[0].symbol_id == corrupt_symbol && CloseEnough(values[0].close, 10.0));
		assert(values[1].symbol_id == intact_symbol && CloseEnough(values[1].close, 20.0));
		assert(values[2].symbol_id == corrupt_symbol && CloseEnough(values[2].close, 11.0));
	}
	RemoveTestDirectory(path);
}

void TestManifestAndActions() {
	const zstfs::SymbolId symbol_id = 73;
	const std::string path = MakeTestDirectory();
	{
		MarketFixture fixture(path, "manifest-actions", "CNA");
		zstfs::Market& market = fixture.market();
		ExpectOk(market.status());
		zstfs::History& history = market.history(zstfs::Frequency::Daily);
		ExpectOk(history.put(BarFor(symbol_id, zstfs::Frequency::Daily, "20260803", 100.0)));
		ExpectOk(history.put(BarFor(symbol_id, zstfs::Frequency::Daily, "20260804", 60.0)));
		ExpectOk(history.put(MissingBar(symbol_id, zstfs::Frequency::Daily, "20260805")));

		zstfs::Action split = {};
		split.external_event_key = "provider-a:split:20260804:73";
		split.symbol_id = symbol_id;
		split.effective_date = "20260804";
		split.type = zstfs::ActionType::Split;
		split.factor = 2.0;
		ExpectOk(market.actions().upsert(split));

		zstfs::Action dividend = {};
		dividend.external_event_key = "provider-a:cash-dividend:20260805:73";
		dividend.symbol_id = symbol_id;
		dividend.effective_date = "20260805";
		dividend.type = zstfs::ActionType::CashDividend;
		dividend.cash_value = 5.0;
		ExpectOk(market.actions().upsert(dividend));

		std::ofstream temporary((MarketPath(path, "manifest-actions") + "/daily/orphan.tmp-page").c_str());
		temporary << "unpublished";
	}
	{
		MarketFixture fixture(path, "manifest-actions", "CNA");
		zstfs::Market& market = fixture.market();
		ExpectOk(market.status());
		std::vector<zstfs::Action> actions;
		ExpectOk(market.actions().get(symbol_id, "20260803", "20260805", &actions));
		assert(actions.size() == 2);
		assert(actions[0].id < actions[1].id);

		zstfs::History& history = market.history(zstfs::Frequency::Daily);
		std::vector<zstfs::Bar> values;
		ExpectOk(history.get(symbol_id, "20260803", "20260803",
			zstfs::AdjustMode::Raw, &values));
		assert(values.size() == 1 && CloseEnough(values[0].close, 100.0));
		ExpectOk(history.get(symbol_id, "20260803", "20260803",
			zstfs::AdjustMode::Backward, &values));
		assert(values.size() == 1 && values[0].close == 45.0 && values[0].volume == 200.0);

		zstfs::Action corrected_split = actions[0];
		corrected_split.factor = 4.0;
		ExpectOk(market.actions().upsert(corrected_split));
		ExpectOk(history.get(symbol_id, "20260803", "20260803",
			zstfs::AdjustMode::Backward, &values));
		assert(values.size() == 1 && values[0].close == 20.0 && values[0].volume == 400.0);
		ExpectOk(history.get(symbol_id, "20260804", "20260804",
			zstfs::AdjustMode::Forward, &values));
		assert(values.size() == 1 && values[0].close == 240.0 && values[0].volume == 25.0);
		ExpectOk(history.get(symbol_id, "20260805", "20260805",
			zstfs::AdjustMode::Backward, &values));
		assert(values.size() == 1 && values[0].state == zstfs::BarState::Missing &&
			values[0].close == 0.0);
	}
	RemoveTestDirectory(path);

	const std::string missing_path = MakeTestDirectory();
	{
		MarketFixture fixture(missing_path, "missing-manifest", "CNA");
		zstfs::Market& market = fixture.market();
		ExpectOk(market.status());
	}
	assert(unlink((MarketPath(missing_path, "missing-manifest") + "/manifest").c_str()) == 0);
	zstfs::Markets missing_markets(missing_path);
	assert(missing_markets.status().code() == zstfs::ErrorCode::CorruptData);
	RemoveTestDirectory(missing_path);

	const std::string corrupt_path = MakeTestDirectory();
	{
		MarketFixture fixture(corrupt_path, "corrupt-manifest", "CNA");
		zstfs::Market& market = fixture.market();
		ExpectOk(market.status());
	}
	std::fstream corrupt((MarketPath(corrupt_path, "corrupt-manifest") + "/manifest").c_str(),
		std::ios::in | std::ios::out | std::ios::binary);
	assert(corrupt);
	corrupt.write("X", 1);
	corrupt.close();
	zstfs::Markets corrupt_markets(corrupt_path);
	assert(corrupt_markets.status().code() == zstfs::ErrorCode::CorruptData);
	RemoveTestDirectory(corrupt_path);
}

// Creates one block that crosses the cutoff and an unrelated newer Vault block.
// The fixture verifies that the compactor retains the cutoff block intact rather
// than treating individual bar times as a coarse retention boundary.
void PopulateCompactionFixture(const std::string& path) {
	const zstfs::SymbolId symbol_id = 61;
	MarketFixture fixture(path, "compaction-test", "CNA");
	zstfs::Market& bootstrap = fixture.market();
	ExpectOk(bootstrap.status());
	zstfs::Calendar calendar("CNA");
	zstfs::TimeId block_id = 0;
	zstfs::BlockOff first_offset = 0;
	ExpectOk(calendar.block_offset(zstfs::Frequency::Daily, "20260803", &block_id, &first_offset));
	zstfs::BlockOff position_count = 0;
	ExpectOk(calendar.block_length(zstfs::Frequency::Daily, block_id, &position_count));
	zstfs::StockTimeBlock staged = {};
	staged.key = {symbol_id, block_id};
	staged.day_presence = 0;
	staged.positions.assign(position_count, zstfs::BlockBar{zstfs::BarState::Missing, 0, 0, 0, 0, 0});
	std::vector<zstfs::ActiveBar> staged_bars;
	const zstfs::Bar bars[] = {
		BarFor(symbol_id, zstfs::Frequency::Daily, "20260803", 10.0),
		MissingBar(symbol_id, zstfs::Frequency::Daily, "20260804"),
		BarFor(symbol_id, zstfs::Frequency::Daily, "20260806", 20.0)
	};
	for (size_t i = 0; i < sizeof(bars) / sizeof(bars[0]); ++i) {
		zstfs::TimeId time_id = 0;
		zstfs::BlockOff offset = 0;
		ExpectOk(calendar.block_offset(zstfs::Frequency::Daily, bars[i].local_time,
			&block_id, &offset));
		ExpectOk(calendar.time_id(bars[i].local_time, &time_id));
		staged.positions[offset] = {bars[i].state, bars[i].open, bars[i].high, bars[i].low,
			bars[i].close, bars[i].volume};
		staged.day_presence |= static_cast<uint64_t>(1) << (zstfs::time_day(time_id) -
			zstfs::time_day(staged.key.time_block_id));
		staged_bars.push_back({symbol_id, time_id, staged.positions[offset]});
	}
	{
		zstfs::StagingStore staging(zstfs::Frequency::Daily, calendar,
			MarketPath(path, "compaction-test") + "/daily");
		ExpectOk(staging.accept(std::vector<zstfs::StockTimeBlock>(1, staged), staged_bars));
		zstfs::VaultStore vault(zstfs::Frequency::Daily, calendar,
			MarketPath(path, "compaction-test") + "/daily", 991);
		IngestVaultBar(&vault, calendar, symbol_id, "20270105", 50.0);
	}
	ExpectOk(bootstrap.sync());
}

bool HasBackupDirectory(const std::string& path, const std::string& prefix) {
	DIR* directory = opendir(path.c_str());
	assert(directory != NULL);
	bool found = false;
	for (dirent* entry = readdir(directory); entry != NULL; entry = readdir(directory)) {
		const std::string name(entry->d_name);
		if (name.compare(0, prefix.size(), prefix) == 0) {
			struct stat metadata = {};
			const std::string candidate = path + "/" + name;
			found = stat(candidate.c_str(), &metadata) == 0 && S_ISDIR(metadata.st_mode);
			if (found) {
				break;
			}
		}
	}
	closedir(directory);
	return found;
}

void TestOfflineVaultCompaction() {
	const std::string path = MakeTestDirectory();
	const std::string duplicate_path = MakeTestDirectory();
	const zstfs::SymbolId symbol_id = 61;
	PopulateCompactionFixture(path);
	PopulateCompactionFixture(duplicate_path);
	zstfs::Calendar calendar("CNA");
	const std::string frequency_path = MarketPath(path, "compaction-test") + "/daily";
	std::map<std::pair<zstfs::SymbolId, zstfs::TimeId>, std::vector<uint8_t> > original_frames;
	{
		zstfs::StagingStore staging(zstfs::Frequency::Daily, calendar, frequency_path);
		zstfs::VaultStore vault(zstfs::Frequency::Daily, calendar, frequency_path, 992);
		std::vector<zstfs::StockTimeBlock> blocks;
		std::vector<zstfs::ActiveBar> bars;
		std::vector<std::vector<uint8_t> > frames;
		ExpectOk(staging.snapshot(&blocks, &bars, &frames));
		assert(blocks.size() == frames.size());
		for (size_t i = 0; i < blocks.size(); ++i) {
			assert(original_frames.insert(std::make_pair(
				std::make_pair(blocks[i].key.symbol_id, blocks[i].key.time_block_id), frames[i])).second);
		}
		ExpectOk(vault.snapshot(&blocks, &bars, &frames));
		assert(blocks.size() == frames.size());
		for (size_t i = 0; i < blocks.size(); ++i) {
			assert(original_frames.insert(std::make_pair(
				std::make_pair(blocks[i].key.symbol_id, blocks[i].key.time_block_id), frames[i])).second);
		}
	}
	zstfs::VaultCompactionStats stats = {};
	zstfs::VaultCompactionStats duplicate_stats = {};
	ExpectOk(zstfs::CompactVault(path, "compaction-test", zstfs::Frequency::Daily, "20260805", &stats));
	ExpectOk(zstfs::CompactVault(duplicate_path, "compaction-test", zstfs::Frequency::Daily, "20260805",
		&duplicate_stats));
	assert(stats.input_blocks >= 2);
	// Compaction preserves complete BarBlockFrame ownership and never splits
	// the cutoff block, so it cannot create an extra partial output block.
	assert(stats.output_blocks >= 2);
	assert(stats.temporary_bytes > 0 && stats.io_bytes > stats.temporary_bytes);
	assert(stats.input_blocks == duplicate_stats.input_blocks);
	assert(stats.output_blocks == duplicate_stats.output_blocks);
	assert(stats.temporary_bytes == duplicate_stats.temporary_bytes);
	assert(stats.io_bytes == duplicate_stats.io_bytes);
	assert(HasBackupDirectory(MarketPath(path, "compaction-test") + "/daily", "vault."));
	assert(HasBackupDirectory(MarketPath(path, "compaction-test") + "/daily", "staging."));
	{
		zstfs::VaultStore vault(zstfs::Frequency::Daily, calendar, frequency_path, 993);
		std::vector<zstfs::StockTimeBlock> blocks;
		std::vector<zstfs::ActiveBar> bars;
		std::vector<std::vector<uint8_t> > frames;
		ExpectOk(vault.snapshot(&blocks, &bars, &frames));
		assert(blocks.size() == frames.size());
		for (size_t i = 0; i < blocks.size(); ++i) {
			const std::pair<zstfs::SymbolId, zstfs::TimeId> key(
				blocks[i].key.symbol_id, blocks[i].key.time_block_id);
			std::map<std::pair<zstfs::SymbolId, zstfs::TimeId>, std::vector<uint8_t> >::const_iterator original =
				original_frames.find(key);
			assert(original != original_frames.end());
			assert(frames[i] == original->second);
		}
	}
	{
		MarketFixture fixture(path, "compaction-test", "CNA");
		zstfs::Market& market = fixture.market();
		zstfs::History& history = market.history(zstfs::Frequency::Daily);
		zstfs::Bar bar = {};
		ExpectOk(history.get(symbol_id, "20260803", &bar));
		assert(CloseEnough(bar.close, 10.0));
		ExpectOk(history.get(symbol_id, "20260804", &bar));
		assert(bar.state == zstfs::BarState::Missing);
		ExpectOk(history.get(symbol_id, "20260806", &bar));
		assert(CloseEnough(bar.close, 20.0));
		ExpectOk(history.get(symbol_id, "20270105", &bar));
		assert(CloseEnough(bar.close, 50.0));
	}
	zstfs::VaultCompactionStats second_stats = {};
	ExpectOk(zstfs::CompactVault(path, "compaction-test", zstfs::Frequency::Daily, "20270105", &second_stats));
	{
		zstfs::VaultStore vault(zstfs::Frequency::Daily, calendar, frequency_path, 994);
		std::vector<zstfs::StockTimeBlock> blocks;
		std::vector<zstfs::ActiveBar> bars;
		std::vector<std::vector<uint8_t> > frames;
		ExpectOk(vault.snapshot(&blocks, &bars, &frames));
		assert(blocks.size() == original_frames.size());
		assert(blocks.size() == frames.size());
		for (size_t i = 0; i < blocks.size(); ++i) {
			const std::pair<zstfs::SymbolId, zstfs::TimeId> key(
				blocks[i].key.symbol_id, blocks[i].key.time_block_id);
			std::map<std::pair<zstfs::SymbolId, zstfs::TimeId>, std::vector<uint8_t> >::const_iterator original =
				original_frames.find(key);
			assert(original != original_frames.end());
			assert(frames[i] == original->second);
		}
	}
	RemoveTestDirectory(path);
	RemoveTestDirectory(duplicate_path);
}

}  // namespace

int main() {
	TestDailyWriteReadAndRecovery();
	TestAutomaticFlush();
	TestStagingBatchAndIndexRecovery();
	TestHourlyOrderingAndCanonicalSlots();
	TestVaultPersistenceMergeAndCorruption();
	TestManifestAndActions();
	TestOfflineVaultCompaction();
	return 0;
}
