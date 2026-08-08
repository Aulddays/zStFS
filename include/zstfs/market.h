#pragma once

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <utility>
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
class Markets;

// VaultCompactionStats reports the stable operational totals produced by one
// offline compaction. Elapsed time is observational; the byte and block counts
// are derived from deterministic ordered input and output records.
struct VaultCompactionStats {
	uint64_t input_blocks;
	uint64_t output_blocks;
	uint64_t temporary_bytes;
	uint64_t io_bytes;
	uint64_t elapsed_milliseconds;
};

// CompactVault rebuilds one configured market's immutable Vault from existing
// Vault and Staging records strictly before cutoff_local_time. The caller runs
// it while all writers are stopped because publication replaces store files.
Status CompactVault(const std::string& root_path,
                    const std::string& market_name,
                    Frequency frequency,
                    const std::string& cutoff_local_time,
                    VaultCompactionStats* stats);

// Market owns all data domains for one configured market. It provides access
// to symbols, corporate actions, and independent daily/hourly histories.
class Market {
public:
	~Market();

	const std::string& name() const;
	const std::string& path() const;
	const std::string& type() const;
	// status reports whether the on-disk market generation was accepted during
	// construction. A non-OK status means no history or action mutation may use
	// the market state.
	Status status() const;
	// sync publishes the complete current file set as a new manifest generation.
	// History and Actions mutations publish automatically; maintenance code calls
	// this after writing a fully prepared replacement store while the daemon is stopped.
	Status sync();

	Symbols& symbols();
	const Symbols& symbols() const;
	Actions& actions();
	const Actions& actions() const;
	History& history(Frequency frequency);
	const History& history(Frequency frequency) const;

private:
	friend class Markets;

	Market(const std::string& name,
	       const std::string& path,
	       const std::string& type);

	std::string name_;
	std::string path_;
	std::string type_;
	std::unique_ptr<Calendar> calendar_;
	std::unique_ptr<Symbols> symbols_;
	std::unique_ptr<Actions> actions_;
	std::unique_ptr<History> daily_history_;
	std::unique_ptr<History> hourly_history_;
	Status status_;
	uint64_t manifest_generation_;
	bool manifest_loaded_;

	Status initialize_storage();
	Status load_or_bootstrap_manifest();
	Status publish_manifest();
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

private:
	friend class Market;

	Status configure_persistence(const std::string& file_path,
	                             const std::function<Status()>& publish_manifest,
	                             bool* created);
	Status load();
	Status save(const std::map<SymbolId, Symbol>& symbols,
	            const std::map<std::string, SymbolId>& codes,
	            SymbolId next_id) const;
	Status persist(const std::map<SymbolId, Symbol>& symbols,
	               const std::map<std::string, SymbolId>& codes,
	               SymbolId next_id);

	std::map<SymbolId, Symbol> by_id_;
	std::map<std::string, SymbolId> by_code_;
	SymbolId next_id_;
	std::string path_;
	std::function<Status()> publish_manifest_;
	Status status_;
};

// Actions owns the dated corporate-action records for one Market. Its records
// are separate from Symbol metadata because each symbol has many events.
class Actions {
public:
	Actions();

	// upsert accepts a source event keyed by Action::external_event_key. A retry
	// is a no-op; a changed representation with the same key replaces one action.
	Status upsert(const Action& action);
	Status get(SymbolId symbol_id,
	           const std::string& begin,
	           const std::string& end,
	           std::vector<Action>* out) const;
	// remove retracts a source event by its symbol and external key. Removing an
	// already absent key succeeds so source reconciliation can be retried safely.
	Status remove(SymbolId symbol_id, const std::string& external_event_key);
	// adjust transforms a raw bar to the requested corporate-action basis.
	// Raw actions remain the only persistent representation; the ordered
	// in-memory anchors are rebuilt whenever the action set changes.
	Status adjust(SymbolId symbol_id,
	              const std::string& local_time,
	              AdjustMode mode,
	              Bar* bar) const;
	Status status() const;

private:
	friend class Market;

	Status configure_persistence(const std::string& file_path,
	                             const std::function<Status()>& publish_manifest);
	Status load();
	Status save(const std::map<ActionId, Action>& actions,
	            ActionId next_id) const;
	Status persist(const std::map<ActionId, Action>& actions,
	               ActionId next_id);
	void rebuild_anchors();

	std::map<ActionId, Action> by_id_;
	std::map<std::pair<SymbolId, std::string>, ActionId> by_external_event_key_;
	ActionId next_id_;
	std::map<SymbolId, std::vector<Action> > anchors_;
	std::string path_;
	std::function<Status()> publish_manifest_;
	Status status_;
};

// History manages one frequency of market data. Market owns one daily and one
// hourly instance; its storage layers remain private implementation details.
class History {
public:
	~History();

	Frequency frequency() const;
	Status status() const;
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
	History(Frequency frequency,
	        const Calendar& calendar,
	        const std::string& market_path,
	        const Actions& actions,
	        const std::function<Status()>& publish_manifest,
	        const Status& initial_status);

	Frequency frequency_;
	const Calendar& calendar_;
	const Actions& actions_;
	std::function<Status()> publish_manifest_;
	Status status_;
	std::unique_ptr<ActiveStore> active_;
	std::unique_ptr<StagingStore> staging_;
	std::unique_ptr<VaultStore> vault_;
};

// Markets is the root-level zStFS entry point. It reads root/markets.conf once
// and owns every configured Market and its library-managed storage directory.
class Markets {
public:
	explicit Markets(const std::string& root_path);

	const std::string& root_path() const;
	Status status() const;
	Status get(const std::string& name, Market** out);
	Status get(const std::string& name, const Market** out) const;

private:
	Status load_configuration();

	std::string root_path_;
	std::map<std::string, std::unique_ptr<Market> > by_name_;
	Status status_;
};

}  // namespace zstfs

