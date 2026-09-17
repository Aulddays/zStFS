// test_api.cpp
//
// Exercises the Milestone 1 public API and verifies the library can be linked
// by a separate program.

#include <assert.h>

#include <algorithm>
#include <dirent.h>
#include <errno.h>
#include <fstream>
#include <iterator>
#include <memory>
#include <sys/stat.h>
#include <unistd.h>
#include <string>
#include <vector>

#include "zstfs/market.h"

static zstfs::Symbol NewSymbol(const std::string& code) {
	zstfs::Symbol symbol = {};
	symbol.code = code;
	symbol.name = "Example Security";
	symbol.security_type = "equity";
	symbol.industry = "technology";
	symbol.list_date = "20260803";
	symbol.share_capital = 1000;
	symbol.tradable_share = 900;
	symbol.state = zstfs::SymbolState::Active;
	return symbol;
}

void ExpectOk(const zstfs::Status& status) {
	assert(status.ok());
}

void RemoveTree(const std::string& path) {
	DIR* directory = opendir(path.c_str());
	if (directory == NULL) {
		assert(errno == ENOENT);
		return;
	}
	for (dirent* entry = readdir(directory); entry != NULL; entry = readdir(directory)) {
		const std::string name(entry->d_name);
		if (name == "." || name == "..") {
			continue;
		}
		const std::string child = path + "/" + name;
		struct stat metadata = {};
		assert(stat(child.c_str(), &metadata) == 0);
		if (S_ISDIR(metadata.st_mode)) {
			RemoveTree(child);
		} else {
			assert(unlink(child.c_str()) == 0);
		}
	}
	closedir(directory);
	assert(rmdir(path.c_str()) == 0);
}

class ScopedTreeRemoval {
public:
	explicit ScopedTreeRemoval(const std::string& path) : path_(path) {
	}

	~ScopedTreeRemoval() {
		RemoveTree(path_);
	}

private:
	std::string path_;
};

// MarketsFixture writes one application root configuration and opens its market
// collection. The collection owns every returned Market pointer and reference.
class MarketsFixture {
public:
	MarketsFixture(const std::string& root_path, const std::vector<std::string>& records)
		: markets_() {
		std::ofstream config((root_path + "/markets.conf").c_str());
		assert(config);
		for (std::vector<std::string>::const_iterator record = records.begin();
			record != records.end(); ++record) {
			config << *record << "\n";
		}
		config.close();

		markets_.reset(new zstfs::Markets(root_path));
		ExpectOk(markets_->status());
	}

	zstfs::Markets& markets() {
		return *markets_;
	}

	zstfs::Market& market(const std::string& name) {
		zstfs::Market* result = NULL;
		ExpectOk(markets_->get(name, &result));
		assert(result != NULL);
		return *result;
	}

private:
	std::unique_ptr<zstfs::Markets> markets_;
};

int main() {
	const std::string root_path = "/tmp/zstfs-test-root";
	RemoveTree(root_path);
	assert(mkdir(root_path.c_str(), 0755) == 0);
	ScopedTreeRemoval cleanup(root_path);

	std::vector<std::string> records;
	records.push_back("test-market,CNA");
	records.push_back("shanghai,CNA");
	records.push_back("shenzhen,CNA");
	MarketsFixture fixture(root_path, records);
	zstfs::Markets& markets = fixture.markets();
	zstfs::Market& market = fixture.market("test-market");
	assert(market.type() == "CNA");

	zstfs::Market* shanghai = NULL;
	ExpectOk(markets.get("shanghai", &shanghai));
	assert(shanghai->type() == "CNA");

	{
		std::ofstream config((root_path + "/markets.conf").c_str(), std::ios::trunc);
		assert(config);
		config << "test-market,CNA\nnew-market,CNA\n";
	}
	zstfs::Market* new_market = NULL;
	assert(markets.get("new-market", &new_market).code() == zstfs::ErrorCode::NotFound);
	assert(shanghai->type() == "CNA");

	zstfs::SymbolId symbol_id = zstfs::kInvalidSymbolId;
	ExpectOk(market.symbols().add(NewSymbol("TEST"), &symbol_id));
	assert(symbol_id != zstfs::kInvalidSymbolId);

	zstfs::Symbol symbol = {};
	ExpectOk(market.symbols().get(symbol_id, &symbol));
	assert(symbol.code == "TEST");

	symbol.code = "TEST2";
	ExpectOk(market.symbols().update(symbol_id, symbol));
	ExpectOk(market.symbols().find("TEST", &symbol));
	assert(symbol.id == symbol_id);
	ExpectOk(market.symbols().find("TEST2", &symbol));
	ExpectOk(market.symbols().get(symbol_id, &symbol));
	assert(symbol.aliases.size() == 1);
	assert(symbol.aliases[0].code == "TEST");
	ExpectOk(market.symbols().find("TEST", "20260803", &symbol));

	zstfs::Markets restored_markets(root_path);
	ExpectOk(restored_markets.status());
	zstfs::Market* restored_market = NULL;
	ExpectOk(restored_markets.get("test-market", &restored_market));
	ExpectOk(restored_market->symbols().find("TEST", &symbol));
	assert(symbol.id == symbol_id);
	ExpectOk(restored_market->symbols().find("TEST2", &symbol));
	assert(symbol.name == "Example Security");

	zstfs::Action action = {};
	action.external_event_key = "provider-a:cash-dividend:20260803:TEST2";
	action.symbol_id = symbol_id;
	action.effective_date = "20260803";
	action.type = zstfs::ActionType::CashDividend;
	action.factor = 1.0;
	action.cash_value = 0.5;
	ExpectOk(market.actions().upsert(action));
	ExpectOk(market.actions().upsert(action));

	std::vector<zstfs::Action> actions;
	ExpectOk(market.actions().get(symbol_id, "20260803", "20260803", &actions));
	assert(actions.size() == 1);
	const zstfs::ActionId action_id = actions[0].id;
	assert(actions[0].external_event_key == action.external_event_key);

	action.cash_value = 0.75;
	ExpectOk(market.actions().upsert(action));
	ExpectOk(market.actions().get(symbol_id, "20260803", "20260803", &actions));
	assert(actions.size() == 1 && actions[0].id == action_id);
	assert(actions[0].cash_value == 0.75);

	zstfs::Action other_symbol_event = action;
	other_symbol_event.symbol_id = symbol_id + 1;
	ExpectOk(market.actions().upsert(other_symbol_event));
	ExpectOk(market.actions().get(symbol_id + 1, "20260803", "20260803", &actions));
	assert(actions.size() == 1);
	ExpectOk(market.actions().remove(symbol_id + 1, action.external_event_key));

	zstfs::Action separate_event = action;
	separate_event.external_event_key = "provider-a:cash-dividend:20260803:TEST2:second";
	ExpectOk(market.actions().upsert(separate_event));
	ExpectOk(market.actions().get(symbol_id, "20260803", "20260803", &actions));
	assert(actions.size() == 2);
	ExpectOk(market.actions().remove(symbol_id, separate_event.external_event_key));

	zstfs::Action invalid_action = action;
	invalid_action.external_event_key.clear();
	assert(market.actions().upsert(invalid_action).code() == zstfs::ErrorCode::InvalidArgument);
	ExpectOk(market.actions().remove(symbol_id, action.external_event_key));
	ExpectOk(market.actions().remove(symbol_id, action.external_event_key));
	ExpectOk(market.actions().get(symbol_id, "20260803", "20260803", &actions));
	assert(actions.empty());
	ExpectOk(market.actions().upsert(action));
	ExpectOk(market.actions().get(symbol_id, "20260803", "20260803", &actions));
	assert(actions.size() == 1);
	const zstfs::ActionId reinserted_action_id = actions[0].id;

	zstfs::Markets action_reloaded_markets(root_path);
	ExpectOk(action_reloaded_markets.status());
	zstfs::Market* action_reloaded_market = NULL;
	ExpectOk(action_reloaded_markets.get("test-market", &action_reloaded_market));
	ExpectOk(action_reloaded_market->actions().upsert(action));
	ExpectOk(action_reloaded_market->actions().get(
		symbol_id, "20260803", "20260803", &actions));
	assert(actions.size() == 1 && actions[0].id == reinserted_action_id);

	zstfs::Action first_duplicate = action;
	first_duplicate.external_event_key = "provider-a:duplicate-0001";
	first_duplicate.cash_value = 1.0;
	zstfs::Action second_duplicate = first_duplicate;
	second_duplicate.external_event_key = "provider-a:duplicate-0002";
	second_duplicate.cash_value = 2.0;
	ExpectOk(market.actions().upsert(first_duplicate));
	ExpectOk(market.actions().upsert(second_duplicate));

	const std::string actions_path = root_path + "/markets/test-market/actions.bin";
	std::fstream actions_file(actions_path.c_str(),
		std::ios::in | std::ios::out | std::ios::binary);
	assert(actions_file);
	std::vector<char> action_bytes((std::istreambuf_iterator<char>(actions_file)),
		std::istreambuf_iterator<char>());
	const std::string replaced_key = second_duplicate.external_event_key;
	const std::string duplicate_key = first_duplicate.external_event_key;
	std::vector<char>::size_type key_offset = action_bytes.size();
	for (std::vector<char>::size_type offset = 0;
		offset + replaced_key.size() <= action_bytes.size(); ++offset) {
		if (std::equal(replaced_key.begin(), replaced_key.end(), action_bytes.begin() + offset)) {
			key_offset = offset;
			break;
		}
	}
	assert(key_offset != action_bytes.size());
	actions_file.clear();
	actions_file.seekp(static_cast<std::streamoff>(key_offset));
	actions_file.write(duplicate_key.data(), duplicate_key.size());
	actions_file.close();
	zstfs::Markets corrupted_action_markets(root_path);
	assert(corrupted_action_markets.status().code() == zstfs::ErrorCode::CorruptData);

	assert(market.history(zstfs::Frequency::Daily).frequency() ==
	       zstfs::Frequency::Daily);
	assert(market.history(zstfs::Frequency::Hourly).frequency() ==
	       zstfs::Frequency::Hourly);

	zstfs::Bar bar = {};
	bar.symbol_id = symbol_id;
	bar.frequency = zstfs::Frequency::Daily;
	bar.local_time = "20260803";
	bar.state = zstfs::BarState::Normal;
	assert(!market.history(zstfs::Frequency::Daily).put(bar).ok());

	ExpectOk(market.symbols().remove(symbol_id));
	ExpectOk(market.symbols().get(symbol_id, &symbol));
	assert(symbol.state == zstfs::SymbolState::Retired);
	return 0;
}
