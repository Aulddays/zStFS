// daemon/server.cpp
//
// Implements the daemon's small HTTP transport and its serial request worker.
// The wire protocol uses ordinary HTTP and the repository's nlohmann-json
// header; all storage access remains in libzstfs.

#define ASIO_STANDALONE

#include "server.h"

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <map>
#include <sstream>
#include <stdexcept>
#include <utility>

#include <signal.h>

#include "asio.hpp"
#include "nlohmann-json/json.hpp"

namespace zstfsd {
namespace {

using nlohmann::json;

HttpResponse OkJson(const std::string& body) {
	HttpResponse response = {200, "application/json", body};
	return response;
}

HttpResponse ErrorResponse(int code, const std::string& message) {
	HttpResponse response = {code, "application/json", json{{"error", message}}.dump()};
	return response;
}

int StatusCode(const zstfs::Status& status) {
	if (status.ok()) return 200;
	switch (status.code()) {
	case zstfs::ErrorCode::InvalidArgument: return 400;
	case zstfs::ErrorCode::NotFound: return 404;
	case zstfs::ErrorCode::AlreadyPresent:
	case zstfs::ErrorCode::Conflict: return 409;
	case zstfs::ErrorCode::CorruptData:
	case zstfs::ErrorCode::IoError: return 500;
	case zstfs::ErrorCode::NotImplemented: return 501;
	case zstfs::ErrorCode::Ok: return 200;
	}
	return 500;
}

std::string UrlDecode(const std::string& value) {
	std::string result;
	for (size_t i = 0; i < value.size(); ++i) {
		if (value[i] == '%' && i + 2 < value.size()) {
			char* end = NULL;
			const std::string hex = value.substr(i + 1, 2);
			long number = std::strtol(hex.c_str(), &end, 16);
			if (*end == '\0') {
				result += static_cast<char>(number);
				i += 2;
				continue;
			}
		}
		result += value[i] == '+' ? ' ' : value[i];
	}
	return result;
}

std::map<std::string, std::string> Query(const std::string& target) {
	std::map<std::string, std::string> result;
	const size_t question = target.find('?');
	if (question == std::string::npos) return result;
	std::string query = target.substr(question + 1);
	while (!query.empty()) {
		size_t ampersand = query.find('&');
		std::string item = query.substr(0, ampersand);
		size_t equal = item.find('=');
		if (equal == std::string::npos) {
			result[UrlDecode(item)] = "";
		} else {
			result[UrlDecode(item.substr(0, equal))] =
				UrlDecode(item.substr(equal + 1));
		}
		if (ampersand == std::string::npos) break;
		query.erase(0, ampersand + 1);
	}
	return result;
}

std::vector<std::string> PathParts(const std::string& target) {
	const size_t question = target.find('?');
	const std::string path = target.substr(0, question);
	std::vector<std::string> parts;
	size_t begin = 0;
	while (begin < path.size()) {
		while (begin < path.size() && path[begin] == '/') ++begin;
		if (begin == path.size()) break;
		size_t end = path.find('/', begin);
		if (end == std::string::npos) end = path.size();
		parts.push_back(UrlDecode(path.substr(begin, end - begin)));
		begin = end;
	}
	return parts;
}

bool Number(const std::string& value, double* output) {
	char* end = NULL;
	errno = 0;
	const double parsed = std::strtod(value.c_str(), &end);
	if (end == value.c_str() || *end != '\0' || errno == ERANGE) return false;
	*output = parsed;
	return true;
}

bool Integer(const std::string& value, uint32_t* output) {
	char* end = NULL;
	errno = 0;
	unsigned long parsed = std::strtoul(value.c_str(), &end, 10);
	if (end == value.c_str() || *end != '\0' || errno == ERANGE || parsed > 0xffffffffUL) {
		return false;
	}
	*output = static_cast<uint32_t>(parsed);
	return true;
}

bool JsonString(const std::string& object, const std::string& key, std::string* output) {
	const std::string needle = "\"" + key + "\"";
	size_t position = object.find(needle);
	if (position == std::string::npos) return false;
	position = object.find(':', position + needle.size());
	if (position == std::string::npos) return false;
	++position;
	while (position < object.size() && (object[position] == ' ' || object[position] == '\t')) ++position;
	if (position == object.size() || object[position] != '"') return false;
	++position;
	std::string value;
	bool escaped = false;
	for (; position < object.size(); ++position) {
		const char c = object[position];
		if (escaped) {
			switch (c) {
			case 'n': value += '\n'; break;
			case 'r': value += '\r'; break;
			case 't': value += '\t'; break;
			default: value += c; break;
			}
			escaped = false;
		} else if (c == '\\') {
			escaped = true;
		} else if (c == '"') {
			*output = value;
			return true;
		} else {
			value += c;
		}
	}
	return false;
}

bool JsonToken(const std::string& object, const std::string& key, std::string* output) {
	const std::string needle = "\"" + key + "\"";
	size_t position = object.find(needle);
	if (position == std::string::npos) return false;
	position = object.find(':', position + needle.size());
	if (position == std::string::npos) return false;
	++position;
	while (position < object.size() && (object[position] == ' ' || object[position] == '\t')) ++position;
	size_t end = position;
	while (end < object.size() && object[end] != ',' && object[end] != '}') ++end;
	*output = object.substr(position, end - position);
	while (!output->empty() && ((*output)[output->size() - 1] == ' ' || (*output)[output->size() - 1] == '\t')) {
		output->erase(output->size() - 1);
	}
	return !output->empty();
}

std::vector<std::string> JsonObjects(const std::string& body) {
	std::vector<std::string> result;
	int depth = 0;
	size_t begin = std::string::npos;
	bool quoted = false;
	bool escaped = false;
	for (size_t i = 0; i < body.size(); ++i) {
		const char c = body[i];
		if (quoted) {
			if (escaped) escaped = false;
			else if (c == '\\') escaped = true;
			else if (c == '"') quoted = false;
			continue;
		}
		if (c == '"') quoted = true;
		else if (c == '{') {
			if (depth == 0) begin = i;
			++depth;
		} else if (c == '}' && depth > 0) {
			--depth;
			if (depth == 0 && begin != std::string::npos) {
				result.push_back(body.substr(begin, i - begin + 1));
				begin = std::string::npos;
			}
		}
	}
	return result;
}

bool ParseFrequency(const std::string& value, zstfs::Frequency* output) {
	if (value == "daily") *output = zstfs::Frequency::Daily;
	else if (value == "hourly") *output = zstfs::Frequency::Hourly;
	else return false;
	return true;
}

bool ParseAdjust(const std::string& value, zstfs::AdjustMode* output) {
	if (value.empty() || value == "raw") *output = zstfs::AdjustMode::Raw;
	else if (value == "forward") *output = zstfs::AdjustMode::Forward;
	else if (value == "backward") *output = zstfs::AdjustMode::Backward;
	else return false;
	return true;
}

bool ParseState(const std::string& value, zstfs::BarState* output) {
	if (value == "normal") *output = zstfs::BarState::Normal;
	else if (value == "not_listed") *output = zstfs::BarState::NotListed;
	else if (value == "delisted") *output = zstfs::BarState::Delisted;
	else if (value == "suspended") *output = zstfs::BarState::Suspended;
	else if (value == "market_closed") *output = zstfs::BarState::MarketClosed;
	else if (value == "missing") *output = zstfs::BarState::Missing;
	else return false;
	return true;
}

bool ParseBar(const json& object, zstfs::Bar* bar, std::string* error) {
	if (!object.is_object()) {
		*error = "each bar must be a JSON object";
		return false;
	}
	if (!object.contains("symbol_id") || !object["symbol_id"].is_number_unsigned()) {
		*error = "symbol_id must be an unsigned integer";
		return false;
	}
	const uint64_t symbol_id = object["symbol_id"].get<uint64_t>();
	if (symbol_id == 0 || symbol_id > 0xffffffffULL) {
		*error = "symbol_id is out of range";
		return false;
	}
	bar->symbol_id = static_cast<zstfs::SymbolId>(symbol_id);
	if (!object.contains("frequency") || !object["frequency"].is_string() ||
			!ParseFrequency(object["frequency"].get<std::string>(), &bar->frequency)) {
		*error = "frequency must be daily or hourly";
		return false;
	}
	if (!object.contains("local_time") || !object["local_time"].is_string() ||
			object["local_time"].get<std::string>().empty()) {
		*error = "local_time is required";
		return false;
	}
	bar->local_time = object["local_time"].get<std::string>();
	if (!object.contains("state") || !object["state"].is_string() ||
			!ParseState(object["state"].get<std::string>(), &bar->state)) {
		*error = "state is invalid";
		return false;
	}
	if (bar->state != zstfs::BarState::Normal) {
		bar->open = bar->high = bar->low = bar->close = bar->volume = 0.0;
		return true;
	}
	const char* names[] = {"open", "high", "low", "close", "volume"};
	double* values[] = {&bar->open, &bar->high, &bar->low, &bar->close, &bar->volume};
	for (size_t i = 0; i < 5; ++i) {
		if (!object.contains(names[i]) || !object[names[i]].is_number()) {
			*error = std::string(names[i]) + " must be a number";
			return false;
		}
		*values[i] = object[names[i]].get<double>();
	}
	return true;
}

bool ParseSymbol(const json& object, zstfs::Symbol* symbol, std::string* error) {
	if (!object.is_object() || !object.contains("code") || !object["code"].is_string() || object["code"].get<std::string>().empty()) {
		*error = "code is required";
		return false;
	}
	symbol->id = 0;
	symbol->code = object["code"].get<std::string>();
	symbol->name = object.value("name", std::string());
	symbol->security_type = object.value("security_type", std::string());
	symbol->industry = object.value("industry", std::string());
	symbol->list_date = object.value("list_date", std::string());
	symbol->delist_date = object.value("delist_date", std::string());
	symbol->share_capital = object.value("share_capital", static_cast<uint64_t>(0));
	symbol->tradable_share = object.value("tradable_share", static_cast<uint64_t>(0));
	symbol->volume_unit = object.value("volume_unit", static_cast<uint32_t>(1));
	symbol->state = zstfs::SymbolState::Active;
	if (object.contains("state")) {
		if (!object["state"].is_string() || object["state"].get<std::string>() != "active" && object["state"].get<std::string>() != "retired") {
			*error = "state must be active or retired";
			return false;
		}
		if (object["state"].get<std::string>() == "retired") symbol->state = zstfs::SymbolState::Retired;
	}
	return true;
}

bool ParseAction(const json& object, zstfs::Action* action, std::string* error) {
	if (!object.is_object() || !object.contains("external_event_key") || !object["external_event_key"].is_string() || object["external_event_key"].get<std::string>().empty()) {
		*error = "external_event_key is required";
		return false;
	}
	if (!object.contains("symbol_id") || !object["symbol_id"].is_number_unsigned()) {
		*error = "symbol_id must be an unsigned integer";
		return false;
	}
	const uint64_t symbol_id = object["symbol_id"].get<uint64_t>();
	if (symbol_id == 0 || symbol_id > 0xffffffffULL || !object.contains("effective_date") || !object["effective_date"].is_string()) {
		*error = "symbol_id and effective_date are required";
		return false;
	}
	action->id = 0;
	action->external_event_key = object["external_event_key"].get<std::string>();
	action->symbol_id = static_cast<zstfs::SymbolId>(symbol_id);
	action->effective_date = object["effective_date"].get<std::string>();
	const std::string type = object.value("type", std::string());
	if (type == "split") action->type = zstfs::ActionType::Split;
	else if (type == "reverse_split") action->type = zstfs::ActionType::ReverseSplit;
	else if (type == "stock_dividend") action->type = zstfs::ActionType::StockDividend;
	else if (type == "cash_dividend") action->type = zstfs::ActionType::CashDividend;
	else if (type == "rights_issue") action->type = zstfs::ActionType::RightsIssue;
	else {
		*error = "type is invalid";
		return false;
	}
	if (!object.contains("factor") || !object["factor"].is_number() || !object.contains("cash_value") || !object["cash_value"].is_number()) {
		*error = "factor and cash_value are required numbers";
		return false;
	}
	action->factor = object["factor"].get<double>();
	action->cash_value = object["cash_value"].get<double>();
	return true;
}

std::string ActionTypeName(zstfs::ActionType type) {
	switch (type) {
	case zstfs::ActionType::Split: return "split";
	case zstfs::ActionType::ReverseSplit: return "reverse_split";
	case zstfs::ActionType::StockDividend: return "stock_dividend";
	case zstfs::ActionType::CashDividend: return "cash_dividend";
	case zstfs::ActionType::RightsIssue: return "rights_issue";
	}
	return "unknown";
}

json ActionJson(const zstfs::Action& action) {
	return json{{"external_event_key", action.external_event_key}, {"symbol_id", action.symbol_id},
		{"effective_date", action.effective_date}, {"type", ActionTypeName(action.type)},
		{"factor", action.factor}, {"cash_value", action.cash_value}};
}

std::string FrequencyName(zstfs::Frequency frequency) {
	return frequency == zstfs::Frequency::Daily ? "daily" : "hourly";
}

std::string StateName(zstfs::BarState state) {
	switch (state) {
	case zstfs::BarState::Normal: return "normal";
	case zstfs::BarState::NotListed: return "not_listed";
	case zstfs::BarState::Delisted: return "delisted";
	case zstfs::BarState::Suspended: return "suspended";
	case zstfs::BarState::MarketClosed: return "market_closed";
	case zstfs::BarState::Missing: return "missing";
	}
	return "unknown";
}

json SymbolJson(const zstfs::Symbol& symbol) {
	json result = {
		{"id", symbol.id},
		{"code", symbol.code},
		{"name", symbol.name},
		{"security_type", symbol.security_type},
		{"industry", symbol.industry},
		{"list_date", symbol.list_date},
		{"delist_date", symbol.delist_date},
		{"share_capital", symbol.share_capital},
		{"tradable_share", symbol.tradable_share},
		{"volume_unit", symbol.volume_unit},
		{"state", symbol.state == zstfs::SymbolState::Active ? "active" : "retired"}
	};
	json aliases = json::array();
	for (size_t i = 0; i < symbol.aliases.size(); ++i) {
		aliases.push_back({
			{"code", symbol.aliases[i].code},
			{"begin_date", symbol.aliases[i].begin_date},
			{"end_date", symbol.aliases[i].end_date}
		});
	}
	result["aliases"] = aliases;
	return result;
}

json BarJson(const zstfs::Bar& bar) {
	json result = {
		{"symbol_id", bar.symbol_id},
		{"frequency", FrequencyName(bar.frequency)},
		{"local_time", bar.local_time},
		{"state", StateName(bar.state)}
	};
	if (bar.state == zstfs::BarState::Normal) {
		result["open"] = bar.open;
		result["high"] = bar.high;
		result["low"] = bar.low;
		result["close"] = bar.close;
		result["volume"] = bar.volume;
	}
	return result;
}

HttpResponse HandleRequest(zstfs::Markets* markets, const HttpRequest& request) {
	const std::vector<std::string> parts = PathParts(request.target);
	if (parts.size() == 2 && parts[0] == "v1" && parts[1] == "health" && request.method == "GET") {
		return OkJson(json{{"status", "ok"}}.dump());
	}
	if (parts.size() < 3 || parts[0] != "v1" || parts[1] != "markets") {
		return ErrorResponse(404, "endpoint not found");
	}
	zstfs::Market* market = NULL;
	zstfs::Status status = markets->get(parts[2], &market);
	if (!status.ok()) return ErrorResponse(StatusCode(status), status.message());

	if (parts.size() == 4 && parts[3] == "symbols" && request.method == "GET") {
		std::vector<zstfs::Symbol> symbols;
		status = market->symbols().list(&symbols);
		if (!status.ok()) return ErrorResponse(StatusCode(status), status.message());
		json result;
		result["symbols"] = json::array();
		for (size_t i = 0; i < symbols.size(); ++i) {
			result["symbols"].push_back(SymbolJson(symbols[i]));
		}
		return OkJson(result.dump());
	}

	if (parts.size() == 5 && parts[3] == "symbols" && request.method == "GET") {
		const std::string& code = parts[4];
		zstfs::Symbol symbol = {};
		status = market->symbols().find(code, &symbol);
		if (!status.ok()) return ErrorResponse(StatusCode(status), status.message());
		return OkJson(json{{"symbol", SymbolJson(symbol)}}.dump());
	}

	if (parts.size() == 4 && parts[3] == "symbols" && request.method == "POST") {
		// Symbols upsert accepts a single object or an array. Both paths go
		// through the batch mechanism so a single-record write still behaves
		// atomically; the difference is only whether changed/unchanged counts
		// are returned alongside the id.
		json payload;
		try { payload = json::parse(request.body); }
		catch (const std::exception& error) { return ErrorResponse(400, std::string("invalid JSON: ") + error.what()); }
		json items;
		bool single = false;
		if (payload.is_array()) {
			items = payload;
		} else if (payload.is_object() && payload.contains("symbols") && payload["symbols"].is_array()) {
			items = payload["symbols"];
		} else if (payload.is_object()) {
			items = json::array({payload});
			single = true;
		} else {
			return ErrorResponse(400, "request body must contain a symbol object or array");
		}
		if (items.empty()) return ErrorResponse(400, "request body must contain at least one symbol");
		status = market->symbols().begin_batch();
		if (!status.ok()) return ErrorResponse(StatusCode(status), status.message());
		uint32_t changed_count = 0;
		uint32_t unchanged_count = 0;
		zstfs::SymbolId first_id = 0;
		bool first_changed = false;
		bool batch_ok = true;
		std::string batch_error;
		for (size_t i = 0; i < items.size(); ++i) {
			zstfs::Symbol symbol = {};
			std::string perror;
			if (!ParseSymbol(items[i], &symbol, &perror)) {
				batch_error = "symbol[" + std::to_string(i) + "]: " + perror;
				batch_ok = false;
				break;
			}
			bool changed = false;
			zstfs::SymbolId out_id = 0;
			zstfs::Status s = market->symbols().upsert(symbol, &out_id, &changed);
			if (!s.ok()) {
				batch_error = "symbol[" + std::to_string(i) + "]: " + s.message();
				batch_ok = false;
				break;
			}
			if (i == 0) {
				first_id = out_id;
				first_changed = changed;
			}
			if (changed) ++changed_count;
			else ++unchanged_count;
		}
		if (!batch_ok) {
			market->symbols().abort_batch();
			return ErrorResponse(400, batch_error);
		}
		bool batch_changed = false;
		status = market->symbols().commit_batch(&batch_changed);
		if (!status.ok()) return ErrorResponse(StatusCode(status), status.message());
		if (single) {
			return OkJson(json{{"id", first_id}, {"updated", first_changed}}.dump());
		}
		return OkJson(json{
			{"accepted", items.size()},
			{"changed", changed_count},
			{"unchanged", unchanged_count}
		}.dump());
	}

	if (parts.size() == 5 && parts[3] == "symbols" && request.method == "DELETE") {
		const std::string& code = parts[4];
		zstfs::Symbol symbol = {};
		status = market->symbols().find(code, &symbol);
		if (!status.ok()) return ErrorResponse(StatusCode(status), status.message());
		status = market->symbols().remove(symbol.id);
		if (!status.ok()) return ErrorResponse(StatusCode(status), status.message());
		return OkJson("{\"removed\":true}");
	}

	if (parts.size() == 4 && parts[3] == "actions") {
		if (request.method == "DELETE") {
			const std::map<std::string, std::string> query = Query(request.target);
			std::map<std::string, std::string>::const_iterator symbol_it = query.find("symbol_id");
			std::map<std::string, std::string>::const_iterator key_it = query.find("external_event_key");
			uint32_t symbol_id = 0;
			if (symbol_it == query.end() || key_it == query.end() || key_it->second.empty() || !Integer(symbol_it->second, &symbol_id)) {
				return ErrorResponse(400, "symbol_id and external_event_key are required");
			}
			status = market->actions().remove(symbol_id, key_it->second);
			if (!status.ok()) return ErrorResponse(StatusCode(status), status.message());
			return OkJson("{\"removed\":true}");
		}
		if (request.method == "POST") {
			json payload;
			try { payload = json::parse(request.body); }
			catch (const std::exception& error) { return ErrorResponse(400, std::string("invalid JSON: ") + error.what()); }
			zstfs::Action action = {};
			std::string error;
			if (!ParseAction(payload, &action, &error)) return ErrorResponse(400, error);
			status = market->actions().upsert(action);
			if (!status.ok()) return ErrorResponse(StatusCode(status), status.message());
			return OkJson("{\"accepted\":1}");
		}
		if (request.method == "GET") {
			const std::map<std::string, std::string> query = Query(request.target);
			std::map<std::string, std::string>::const_iterator symbol_it = query.find("symbol_id");
			std::map<std::string, std::string>::const_iterator begin_it = query.find("begin");
			std::map<std::string, std::string>::const_iterator end_it = query.find("end");
			uint32_t symbol_id = 0;
			std::vector<zstfs::Action> actions;
			if (symbol_it == query.end() || begin_it == query.end() || end_it == query.end() ||
					!Integer(symbol_it->second, &symbol_id)) return ErrorResponse(400, "symbol_id, begin and end are required");
			status = market->actions().get(symbol_id, begin_it->second, end_it->second, &actions);
			if (!status.ok()) return ErrorResponse(StatusCode(status), status.message());
			json result;
			result["actions"] = json::array();
			for (size_t i = 0; i < actions.size(); ++i) result["actions"].push_back(ActionJson(actions[i]));
			return OkJson(result.dump());
		}
		return ErrorResponse(405, "method not allowed");
	}

	if (parts.size() != 4 || parts[3] != "bars") return ErrorResponse(404, "endpoint not found");
	if (request.method == "POST") {
		json payload;
		try {
			payload = json::parse(request.body);
		} catch (const std::exception& error) {
			return ErrorResponse(400, std::string("invalid JSON: ") + error.what());
		}
		json items;
		if (payload.is_array()) items = payload;
		else if (payload.is_object() && payload.contains("bars") && payload["bars"].is_array()) items = payload["bars"];
		else if (payload.is_object()) items = json::array({payload});
		else return ErrorResponse(400, "request body must contain a bar object or array");
		if (items.empty()) return ErrorResponse(400, "request body must contain at least one bar");
		std::vector<zstfs::Bar> bars;
		for (json::const_iterator item = items.begin(); item != items.end(); ++item) {
			zstfs::Bar bar = {};
			std::string error;
			if (!ParseBar(*item, &bar, &error)) return ErrorResponse(400, error);
			bars.push_back(bar);
		}
		zstfs::Frequency frequency = bars[0].frequency;
		for (size_t i = 1; i < bars.size(); ++i) {
			if (bars[i].frequency != frequency) return ErrorResponse(400, "a batch must use one frequency");
		}
		status = market->history(frequency).put(bars);
		if (!status.ok()) return ErrorResponse(StatusCode(status), status.message());
		return OkJson(json{{"accepted", bars.size()}}.dump());
	}
	if (request.method != "GET") return ErrorResponse(405, "method not allowed");

	const std::map<std::string, std::string> query = Query(request.target);
	std::map<std::string, std::string>::const_iterator it = query.find("symbol_id");
	uint32_t symbol_id = 0;
	if (it == query.end() || !Integer(it->second, &symbol_id) || symbol_id == 0) {
		return ErrorResponse(400, "symbol_id is required");
	}
	it = query.find("frequency");
	zstfs::Frequency frequency = zstfs::Frequency::Daily;
	if (it == query.end() || !ParseFrequency(it->second, &frequency)) return ErrorResponse(400, "frequency is required");
	it = query.find("adjust");
	zstfs::AdjustMode adjust = zstfs::AdjustMode::Raw;
	if (it != query.end() && !ParseAdjust(it->second, &adjust)) return ErrorResponse(400, "adjust is invalid");
	it = query.find("time");
	if (it != query.end()) {
		zstfs::Bar bar = {};
		status = market->history(frequency).get(symbol_id, it->second, &bar);
		if (!status.ok()) return ErrorResponse(StatusCode(status), status.message());
		return OkJson(json{{"bar", BarJson(bar)}}.dump());
	}
	const std::string begin = query.count("begin") ? query.find("begin")->second : "";
	const std::string end = query.count("end") ? query.find("end")->second : "";
	if (begin.empty() || end.empty()) return ErrorResponse(400, "begin and end are required");
	std::vector<zstfs::Bar> bars;
	status = market->history(frequency).get(symbol_id, begin, end, adjust, &bars);
	if (!status.ok()) return ErrorResponse(StatusCode(status), status.message());
	json result;
	result["bars"] = json::array();
	for (size_t i = 0; i < bars.size(); ++i) result["bars"].push_back(BarJson(bars[i]));
	return OkJson(result.dump());
}

std::string Reason(int status) {
	switch (status) {
	case 200: return "OK";
	case 400: return "Bad Request";
	case 404: return "Not Found";
	case 405: return "Method Not Allowed";
	case 409: return "Conflict";
	case 500: return "Internal Server Error";
	case 501: return "Not Implemented";
	default: return "Error";
	}
}

class Session : public std::enable_shared_from_this<Session> {
public:
	Session(asio::ip::tcp::socket socket,
	        asio::io_context* io,
	        RequestWorker* worker)
		: socket_(std::move(socket)), io_(io), worker_(worker) {}

	void start() {
		const std::shared_ptr<Session> self = shared_from_this();
		asio::async_read_until(socket_, buffer_, "\r\n\r\n",
			[self](const asio::error_code& error, std::size_t) {
				self->read_headers_done(error);
			});
	}

private:
	void read_headers_done(const asio::error_code& error) {
		if (error) return;
		std::istream input(&buffer_);
		std::string line;
		if (!std::getline(input, line) || line.size() < 2) return fail(400, "invalid request line");
		if (line[line.size() - 1] == '\r') line.erase(line.size() - 1);
		std::istringstream first(line);
		std::string version;
		if (!(first >> request_method_ >> request_target_ >> version) || version != "HTTP/1.1") return fail(400, "HTTP/1.1 required");
		size_t content_length = 0;
		while (std::getline(input, line) && line != "\r") {
			if (!line.empty() && line[line.size() - 1] == '\r') line.erase(line.size() - 1);
			const size_t colon = line.find(':');
			if (colon == std::string::npos) return fail(400, "invalid header");
			std::string name = line.substr(0, colon);
			std::string value = line.substr(colon + 1);
			std::transform(name.begin(), name.end(), name.begin(), ::tolower);
			while (!value.empty() && (value[0] == ' ' || value[0] == '\t')) value.erase(0, 1);
			if (name == "content-length") {
				char* end = NULL;
				unsigned long length = std::strtoul(value.c_str(), &end, 10);
				if (end == value.c_str() || *end != '\0' || length > 16 * 1024 * 1024UL) return fail(413, "request body too large");
				content_length = static_cast<size_t>(length);
			}
		}
		request_body_.assign(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
		if (request_body_.size() > content_length) request_body_.resize(content_length);
		if (request_body_.size() == content_length) return submit();
		const std::shared_ptr<Session> self = shared_from_this();
		asio::async_read(socket_, buffer_, asio::transfer_exactly(content_length - request_body_.size()),
			[self](const asio::error_code& read_error, std::size_t) {
				self->read_body_done(read_error);
			});
	}

	void read_body_done(const asio::error_code& error) {
		if (error) return;
		std::istream input(&buffer_);
		request_body_.append(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
		submit();
	}

	void submit() {
		HttpRequest request;
		request.method = request_method_;
		request.target = request_target_;
		request.body = request_body_;
		const std::shared_ptr<Session> self = shared_from_this();
		request.complete = [self](const std::string& body, int code, const std::string& type) {
			asio::post(*self->io_, [self, body, code, type]() { self->write(body, code, type); });
		};
		worker_->submit(request);
	}

	void fail(int code, const std::string& message) {
		write(json{{"error", message}}.dump(), code, "application/json");
	}

	void write(const std::string& body, int code, const std::string& type) {
		std::ostringstream output;
		output << "HTTP/1.1 " << code << ' ' << Reason(code) << "\r\n"
			<< "Content-Type: " << type << "\r\n"
			<< "Content-Length: " << body.size() << "\r\n"
			<< "Connection: close\r\n\r\n" << body;
		response_ = output.str();
		const std::shared_ptr<Session> self = shared_from_this();
		asio::async_write(socket_, asio::buffer(response_), [self](const asio::error_code&, std::size_t) {
			asio::error_code ignored;
			self->socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ignored);
			self->socket_.close(ignored);
		});
	}

	asio::ip::tcp::socket socket_;
	asio::io_context* io_;
	RequestWorker* worker_;
	asio::streambuf buffer_;
	std::string request_method_;
	std::string request_target_;
	std::string request_body_;
	std::string response_;
};

}  // namespace

RequestWorker::RequestWorker(const std::string& root_path)
	: root_path_(root_path), stopping_(false) {}

RequestWorker::~RequestWorker() { stop(); }

void RequestWorker::start() {
	thread_ = std::thread(&RequestWorker::run, this);
}

void RequestWorker::stop() {
	{
		std::lock_guard<std::mutex> lock(mutex_);
		if (stopping_) return;
		stopping_ = true;
	}
	condition_.notify_all();
	if (thread_.joinable()) thread_.join();
}

void RequestWorker::submit(const HttpRequest& request) {
	{
		std::lock_guard<std::mutex> lock(mutex_);
		if (stopping_) return;
		requests_.push(request);
	}
	condition_.notify_one();
}

void RequestWorker::run() {
	markets_.reset(new zstfs::Markets(root_path_));
	for (;;) {
		HttpRequest request;
		{
			std::unique_lock<std::mutex> lock(mutex_);
			condition_.wait(lock, [this]() { return stopping_ || !requests_.empty(); });
			if (requests_.empty() && stopping_) break;
			request = requests_.front();
			requests_.pop();
		}
		const HttpResponse response = handle(request);
		if (request.complete) request.complete(response.body, response.status_code, response.content_type);
	}
	markets_.reset();
}

HttpResponse RequestWorker::handle(const HttpRequest& request) {
	if (!markets_ || !markets_->status().ok()) {
		return ErrorResponse(500, markets_ ? markets_->status().message() : "market initialization failed");
	}
	return HandleRequest(markets_.get(), request);
}

class HttpServer::Impl {
public:
	Impl(const std::string& root_path, unsigned short port)
		: acceptor_(io_, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), port)),
		  worker_(root_path), stopped_(false) {}

	int run() {
		worker_.start();
		accept();
		signals_ = std::unique_ptr<asio::signal_set>(new asio::signal_set(io_, SIGINT, SIGTERM));
		signals_->async_wait([this](const asio::error_code&, int) { stop(); });
		io_.run();
		worker_.stop();
		return 0;
	}

	void stop() {
		if (stopped_) return;
		stopped_ = true;
		asio::error_code ignored;
		acceptor_.close(ignored);
		worker_.stop();
		io_.stop();
	}

private:
	void accept() {
		acceptor_.async_accept([this](const asio::error_code& error, asio::ip::tcp::socket socket) {
			if (!error) std::make_shared<Session>(std::move(socket), &io_, &worker_)->start();
			if (!stopped_) accept();
		});
	}

	asio::io_context io_;
	asio::ip::tcp::acceptor acceptor_;
	std::unique_ptr<asio::signal_set> signals_;
	RequestWorker worker_;
	bool stopped_;
};

HttpServer::HttpServer(const std::string& root_path, unsigned short port)
	: impl_(new Impl(root_path, port)) {}

HttpServer::~HttpServer() { stop(); }

int HttpServer::run() { return impl_->run(); }

void HttpServer::stop() { impl_->stop(); }

}  // namespace zstfsd
