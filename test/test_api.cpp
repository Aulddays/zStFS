// test_api.cpp
//
// Exercises the Milestone 1 public API and verifies the library can be linked
// by a separate program.

#include <assert.h>

#include <string>
#include <vector>

#include "zstfs/market.h"

namespace {

zstfs::Symbol NewSymbol(const std::string& code) {
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

}  // namespace

int main() {
	zstfs::Market market("test-market", "/tmp/zstfs-test-market", "market-local");

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
