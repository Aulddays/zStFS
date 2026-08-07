// test_api.cpp
//
// Exercises the Milestone 1 public API and verifies the library can be linked
// by a separate program.

#include <assert.h>

#include <dirent.h>
#include <errno.h>
#include <fstream>
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
	symbol.volume_unit = 1;
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

int main() {
	const std::string market_path = "/tmp/zstfs-test-market";
	RemoveTree(market_path);
	ScopedTreeRemoval cleanup(market_path);
	zstfs::Market market("test-market", market_path, "CNA");
	assert(market.type() == "CNA");

	const std::string markets_path = "/tmp/zstfs-markets-test.conf";
	std::ofstream markets_config(markets_path.c_str());
	markets_config << "shanghai,CNA,/tmp/zstfs-shanghai\n";
	markets_config << "shenzhen,CNA,/tmp/zstfs-shenzhen\n";
	markets_config.close();
	zstfs::Markets markets;
	ExpectOk(markets.load(markets_path));
	zstfs::Market* shanghai = NULL;
	ExpectOk(markets.get("shanghai", &shanghai));
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

	const std::string symbols_path = "/tmp/zstfs-symbols-test.bin";
	ExpectOk(market.symbols().save(symbols_path));
	zstfs::Symbols restored_symbols;
	ExpectOk(restored_symbols.load(symbols_path));
	ExpectOk(restored_symbols.find("TEST", &symbol));
	assert(symbol.id == symbol_id);
	ExpectOk(restored_symbols.find("TEST2", &symbol));
	assert(symbol.name == "Example Security");

	zstfs::Action action = {};
	action.symbol_id = symbol_id;
	action.effective_date = "20260803";
	action.type = zstfs::ActionType::CashDividend;
	action.factor = 1.0;
	action.cash_value = 0.5;
	zstfs::ActionId action_id = zstfs::kInvalidActionId;
	ExpectOk(market.actions().add(action, &action_id));

	std::vector<zstfs::Action> actions;
	ExpectOk(market.actions().get(symbol_id, "20260803", "20260803", &actions));
	assert(actions.size() == 1);
	assert(actions[0].id == action_id);

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
