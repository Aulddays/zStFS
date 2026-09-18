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

// DataFields determines which bar fields are meaningful on input and how they
// are stored.
//   OHLCV - full OHLCV data (default)
//   CV    - close+volume only; open/high/low are set equal to close on write
enum class DataFields {
	OHLCV = 0,
	CV = 1,
};

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

// SetCacheSizes configures the global unified page cache capacity. Both
// compressed and decoded cache tiers share an LRU eviction policy across all
// stores. Call this before constructing any Markets object for the limits to
// take effect. Passing 0 for either size disables that cache tier.
void SetCacheSizes(size_t compressed_cache_bytes, size_t decoded_cache_bytes);

// Returns the current compressed cache capacity in bytes.
size_t CompressedCacheBytes();

// Returns the current decoded cache capacity in bytes.
size_t DecodedCacheBytes();

// CompactVault rebuilds one configured market's immutable Vault from existing
// Vault and Staging records strictly before cutoff_local_time. The caller runs
// it while all writers are stopped because publication replaces store files.
// config_path is the path to the unified zStFS config file.
Status CompactVault(const std::string& config_path,
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
	const std::string& schedule() const;
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
	       const std::string& schedule,
	       DataFields fields);

	std::string name_;
	std::string path_;
	std::string schedule_;
	DataFields fields_;
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
//
// Mutation modes:
// - Immediate (default): each upsert/add/update/remove persists synchronously.
// - Batched: begin_batch() defers persistence until commit_batch(). During a
//   batch, mutations stay in memory; commit_batch writes the full snapshot once
//   and publishes one manifest generation. If no record actually changed during
//   the batch, commit_batch is a no-op and nothing is written.
//
// upsert(code-based) is the preferred single-entry mutation: it creates a new
// symbol when the code is unknown, updates it when any field differs from the
// stored record, and does nothing when every field already matches.
class Symbols {
public:
	Symbols();

	// upsert inserts or updates one symbol keyed by symbol.code. Returns the
	// resolved SymbolId via out_id when provided. Returns Ok but sets
	// *updated = false when the stored record already matches exactly.
	Status upsert(const Symbol& symbol, SymbolId* out_id, bool* updated);
	// add inserts a new symbol. Prefer upsert for idempotent ingestion.
	Status add(const Symbol& symbol, SymbolId* out_id);
	Status get(SymbolId id, Symbol* out) const;
	Status find(const std::string& code, Symbol* out) const;
	Status find(const std::string& code,
	            const std::string& date,
	            Symbol* out) const;
	// update replaces an existing symbol by id. Prefer upsert for idempotent
	// ingestion; update is useful when the caller already holds a SymbolId.
	Status update(SymbolId id, const Symbol& symbol);
	Status remove(SymbolId id);
	Status list(std::vector<Symbol>* out) const;

	// begin_batch enters deferred-persistence mode. Nested batches are not
	// supported; calling begin_batch while a batch is active returns Conflict.
	Status begin_batch();
	// commit_batch flushes one snapshot if anything changed during the batch and
	// ends the batch. Returns Ok with *changed = false when nothing changed.
	Status commit_batch(bool* changed);
	// abort_batch discards all in-batch mutations and restores the last
	// persisted state. Safe to call when no batch is active (no-op).
	Status abort_batch();

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
	// apply_upsert performs the in-memory insert-or-update comparison. Returns
	// true when the stored state actually changed; false when the record already
	// matched field-by-field. Does not touch disk.
	bool apply_upsert(const Symbol& symbol, SymbolId* out_id);
	// flush_if_dirty persists candidate state outside of a batch. Inside a
	// batch, it records that a change has been observed and returns immediately.
	Status flush_if_dirty(const std::map<SymbolId, Symbol>& symbols,
	                      const std::map<std::string, SymbolId>& codes,
	                      SymbolId next_id);

	std::map<SymbolId, Symbol> by_id_;
	std::map<std::string, SymbolId> by_code_;
	SymbolId next_id_;
	std::string path_;
	std::function<Status()> publish_manifest_;
	Status status_;

	bool in_batch_;
	bool batch_dirty_;
	// Pre-batch snapshot used by abort_batch to roll back in-memory state.
	std::map<SymbolId, Symbol> saved_by_id_;
	std::map<std::string, SymbolId> saved_by_code_;
	SymbolId saved_next_id_;
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
	        const Status& initial_status,
	        DataFields fields);

	Frequency frequency_;
	const Calendar& calendar_;
	const Actions& actions_;
	DataFields fields_;
	std::function<Status()> publish_manifest_;
	Status status_;
	std::unique_ptr<ActiveStore> active_;
	std::unique_ptr<StagingStore> staging_;
	std::unique_ptr<VaultStore> vault_;
};

// MarketDef describes one market to be created: its short name, its schedule
// string (e.g. "CNA") that selects the calendar and trading hours, and which
// bar fields are populated on input (defaults to OHLCV).
struct MarketDef {
	std::string name;
	std::string schedule;
	DataFields fields;

	MarketDef() : fields(DataFields::OHLCV) {}
	MarketDef(const std::string& n, const std::string& s,
	          DataFields f = DataFields::OHLCV)
		: name(n), schedule(s), fields(f) {}
};

// LoadMarketsConfig parses the "markets" group from a libconfig-format config
// file. The caller passes the full config file path; this function reads only
// the markets list and leaves other fields to higher-level parsers.
Status LoadMarketsConfig(const std::string& config_path,
                         std::vector<MarketDef>* out_markets);

// Markets is the root-level zStFS entry point. It owns every configured Market
// and its library-managed storage directory under root_path/markets/.
class Markets {
public:
	// Constructs Markets from an explicit list of market definitions. The
	// markets directory is created under root_path when it does not yet exist.
	Markets(const std::string& root_path, const std::vector<MarketDef>& markets);

	const std::string& root_path() const;
	Status status() const;
	Status get(const std::string& name, Market** out);
	Status get(const std::string& name, const Market** out) const;
	// seal_all_before computes (today - trading_days_back) using each market's
	// own calendar, then calls seal_before on that market's daily and hourly
	// histories. Results are collected per market+frequency so a failure in
	// one does not stop the rest; the overall status is the first error seen.
	Status seal_all_before(const std::string& today_local, int trading_days_back);

private:
	// initialize validates the root path, ensures the markets/ directory
	// exists, and constructs each Market in the provided list.
	Status initialize(const std::vector<MarketDef>& markets);

	std::string root_path_;
	std::map<std::string, std::unique_ptr<Market> > by_name_;
	Status status_;
};

}  // namespace zstfs

