#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "zstfs/data.h"
#include "zstfs/status.h"

namespace zstfs {

class Calendar;
class Symbols;
class Actions;
class History;
class ActiveStore;
class StagingStore;
class VaultStore;

// Market owns all data domains for one configured market. It provides access
// to symbols, corporate actions, and independent daily/hourly histories.
class Market {
public:
	Market(const std::string& name,
	       const std::string& path,
	       const std::string& type);
	~Market();

	const std::string& name() const;
	const std::string& path() const;
	const std::string& type() const;

	Symbols& symbols();
	const Symbols& symbols() const;
	Actions& actions();
	const Actions& actions() const;
	History& history(Frequency frequency);
	const History& history(Frequency frequency) const;

private:
	std::string name_;
	std::string path_;
	std::string type_;
	std::unique_ptr<Calendar> calendar_;
	std::unique_ptr<Symbols> symbols_;
	std::unique_ptr<Actions> actions_;
	std::unique_ptr<History> daily_history_;
	std::unique_ptr<History> hourly_history_;
};

// Symbols owns all Symbol records for one Market, including the stable ID map
// and the external code lookup index.
class Symbols {
public:
	Symbols();

	Status add(const Symbol& symbol, SymbolId* out_id);
	Status get(SymbolId id, Symbol* out) const;
	Status find(const std::string& code, Symbol* out) const;
	Status find(const std::string& code,
	            const std::string& date,
	            Symbol* out) const;
	Status update(SymbolId id, const Symbol& symbol);
	Status remove(SymbolId id);
	Status list(std::vector<Symbol>* out) const;
	Status load(const std::string& file_path);
	Status save(const std::string& file_path) const;

private:
	std::map<SymbolId, Symbol> by_id_;
	std::map<std::string, SymbolId> by_code_;
	SymbolId next_id_;
};

// Actions owns the dated corporate-action records for one Market. Its records
// are separate from Symbol metadata because each symbol has many events.
class Actions {
public:
	Actions();

	Status add(const Action& action, ActionId* out_id);
	Status get(SymbolId symbol_id,
	           const std::string& begin,
	           const std::string& end,
	           std::vector<Action>* out) const;
	Status update(ActionId id, const Action& action);
	Status remove(ActionId id);

private:
	std::map<ActionId, Action> by_id_;
	ActionId next_id_;
};

// History manages one frequency of market data. Market owns one daily and one
// hourly instance; its storage layers remain private implementation details.
class History {
public:
	~History();

	Frequency frequency() const;
	Status put(const Bar& bar);
	Status put(const std::vector<Bar>& bars);
	Status get(SymbolId symbol_id, const std::string& local_time, Bar* out) const;
	Status get(SymbolId symbol_id,
	           const std::string& begin,
	           const std::string& end,
	           AdjustMode adjust_mode,
	           std::vector<Bar>* out) const;
	Status get(const std::vector<SymbolId>& symbol_ids,
	           const std::string& begin,
	           const std::string& end,
	           AdjustMode adjust_mode,
	           std::vector<Bar>* out) const;
	Status flush();
	Status seal_before(const std::string& local_time);

private:
	friend class Market;
	explicit History(Frequency frequency);

	Frequency frequency_;
	std::unique_ptr<ActiveStore> active_;
	std::unique_ptr<StagingStore> staging_;
	std::unique_ptr<VaultStore> vault_;
};

// Markets owns the statically configured Market instances for one process.
// Its configuration file contains one comma-separated name,type,path entry per
// line, allowing several market instances to share the same market type.
class Markets {
public:
	Status load(const std::string& file_path);
	Status get(const std::string& name, Market** out);
	Status get(const std::string& name, const Market** out) const;

private:
	std::map<std::string, std::unique_ptr<Market> > by_name_;
};

}  // namespace zstfs

