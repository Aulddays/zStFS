// history.cpp
//
// Implements History's current store lifecycle shells and block-level frame
// orchestration. A StockTimeBlock is assembled as one state stream plus five
// independent OHLCV field streams. State covers every BlockOff and determines
// which offsets may have numeric values; numeric frames cover only contiguous
// Normal offsets.
//
// This module owns the policy around the strict numeric codec: invalid Normal
// input becomes Missing in a complete block, and reconstructed High/Low are
// constrained against reconstructed Open/Close. codec.cpp owns frame-local
// quantization, residual algorithms, and the ZMF3 byte format.

#include "history.h"

#include "codec.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <iterator>
#include <limits>
#include <set>
#include <utility>

#include "serialization.h"
#include "zstfs/market.h"

namespace zstfs {

// =============================================================================
// Block Input Policy and Shared Validation
//
// Applies the ingestion rules surrounding the strict codec. A complete block
// owns its state stream; numeric frames exist only for Normal offsets selected
// by that stream.
// =============================================================================

static bool IsNumericField(FieldId field) {
	return field != FieldId::State && field <= FieldId::Volume;
}

static bool ValidState(BarState state) {
	return state >= BarState::Normal && state <= BarState::Missing;
}

static bool ValidBar(const BlockBar& bar) {
	return bar.state != BarState::Normal ||
		(std::isfinite(bar.open) && bar.open > 0.0 &&
		 std::isfinite(bar.high) && bar.high >= 0.0 &&
		 std::isfinite(bar.low) && bar.low >= 0.0 &&
		 std::isfinite(bar.close) && bar.close > 0.0 &&
		 std::isfinite(bar.volume) && bar.volume >= 0.0 &&
		 bar.high >= std::max(bar.open, bar.close) &&
		 bar.low <= std::min(bar.open, bar.close));
}

static double FieldValue(FieldId field, const BlockBar& bar) {
	switch (field) {
	case FieldId::Open:
		return bar.open;
	case FieldId::High:
		return bar.high;
	case FieldId::Low:
		return bar.low;
	case FieldId::Close:
		return bar.close;
	case FieldId::Volume:
		return bar.volume;
	case FieldId::State:
		break;
	}
	return 0.0;
}

// =============================================================================
// Numeric Field Run Assembly
//
// Splits one field into fixed-size codec frames. State gaps and invalid input
// remain gaps at their original BlockOff; they never shift later values.
// =============================================================================

Status EncodeFieldFrames(FieldId field,
			 BlockOff first_offset,
			 const std::vector<BlockBar>& positions,
			 const PrecisionProfile& profile,
			 std::vector<MicroblockFrame>* output) {
	if (output == NULL || !IsNumericField(field) || !ValidPrecisionProfile(profile)) {
		return Status::Error(ErrorCode::InvalidArgument, "invalid field frame input");
	}
	output->clear();
	std::vector<double> run;
	BlockOff run_offset = first_offset;
	for (size_t i = 0; i < positions.size(); ++i) {
		if (i > static_cast<size_t>(std::numeric_limits<BlockOff>::max() - first_offset)) {
			return Status::Error(ErrorCode::InvalidArgument, "field offset overflow");
		}
		BlockOff current_offset = static_cast<BlockOff>(first_offset + i);
		if (positions[i].state != BarState::Normal || !ValidBar(positions[i])) {
			if (!run.empty()) {
				FrameInput input = {field, run_offset, run};
				MicroblockFrame frame;
				Status status = EncodeFrame(input, profile, &frame);
				if (!status.ok()) {
					return status;
				}
				output->push_back(frame);
				run.clear();
			}
			continue;
		}
		if (run.empty()) {
			run_offset = current_offset;
		}
		run.push_back(FieldValue(field, positions[i]));
		if (run.size() == kMaxFrameSamples) {
			FrameInput input = {field, run_offset, run};
			MicroblockFrame frame;
			Status status = EncodeFrame(input, profile, &frame);
			if (!status.ok()) {
				return status;
			}
			output->push_back(frame);
			run.clear();
		}
	}
	if (!run.empty()) {
		FrameInput input = {field, run_offset, run};
		MicroblockFrame frame;
		Status status = EncodeFrame(input, profile, &frame);
		if (!status.ok()) {
			return status;
		}
		output->push_back(frame);
	}
	return Status::Ok();
}

// =============================================================================
// State Stream Assembly
//
// The state frame covers every block position, including gaps. It is separate
// from numeric frames because non-Normal positions intentionally have no OHLCV
// payload and only state can distinguish Missing from other lifecycle states.
// =============================================================================

Status EncodeStateFrame(BlockOff first_offset,
			const std::vector<BlockBar>& positions,
			MicroblockFrame* output) {
	if (output == NULL || positions.empty()) {
		return Status::Error(ErrorCode::InvalidArgument, "state frame requires positions");
	}
	if (positions.size() > std::numeric_limits<BlockOff>::max() ||
		positions.size() > static_cast<size_t>(std::numeric_limits<BlockOff>::max() - first_offset)) {
		return Status::Error(ErrorCode::InvalidArgument, "state frame is too large");
	}
	for (size_t i = 0; i < positions.size(); ++i) {
		if (!ValidState(positions[i].state)) {
			return Status::Error(ErrorCode::InvalidArgument, "invalid bar state");
		}
	}

	output->field = FieldId::State;
	output->first_offset = first_offset;
	output->sample_count = static_cast<BlockOff>(positions.size());
	output->codec_id = kStateRleCodecId;
	output->predictor_id = 0;
	output->quantizer_id = 0;
	output->quantizer_parameters.clear();
	output->anchor = static_cast<int64_t>(positions[0].state);
	output->payload.clear();
	for (size_t i = 0; i < positions.size();) {
		size_t end = i + 1;
		while (end < positions.size() &&
			positions[end].state == positions[i].state &&
			end - i < std::numeric_limits<uint16_t>::max()) {
			++end;
		}
		PutU16(&output->payload, static_cast<uint16_t>(end - i));
		PutU8(&output->payload, static_cast<uint8_t>(positions[i].state));
		i = end;
	}
	return Status::Ok();
}

Status DecodeStateFrame(const MicroblockFrame& frame,
			std::vector<BarState>* states) {
	if (states == NULL || frame.field != FieldId::State ||
		frame.sample_count == 0 || frame.codec_id != kStateRleCodecId ||
		frame.predictor_id != 0 || frame.quantizer_id != 0 ||
		!frame.quantizer_parameters.empty() ||
		!ValidState(static_cast<BarState>(frame.anchor))) {
		return Status::Error(ErrorCode::CorruptData, "invalid state frame header");
	}
	states->clear();
	states->reserve(frame.sample_count);
	size_t cursor = 0;
	while (states->size() < frame.sample_count) {
		uint16_t run = 0;
		uint8_t encoded_state = 0;
		BarState state;
		if (!GetU16(frame.payload, &cursor, &run) || run == 0 ||
			!GetU8(frame.payload, &cursor, &encoded_state) ||
			run > frame.sample_count - states->size()) {
			return Status::Error(ErrorCode::CorruptData, "invalid state frame payload");
		}
		state = static_cast<BarState>(encoded_state);
		if (!ValidState(state)) {
			return Status::Error(ErrorCode::CorruptData, "unknown bar state");
		}
		for (uint16_t i = 0; i < run; ++i) {
			states->push_back(state);
		}
	}
	if (cursor != frame.payload.size() || states->empty() ||
		static_cast<int64_t>((*states)[0]) != frame.anchor) {
		return Status::Error(ErrorCode::CorruptData, "trailing or inconsistent state frame data");
	}
	return Status::Ok();
}

// =============================================================================
// OHLCV Block Assembly and Reconstruction
//
// A complete block first turns invalid Normal bars into Missing, then emits the
// state stream plus independent OHLCV field frames. Price fields use half the
// requested error budget so decode can restore High/Low ordering safely.
// =============================================================================

Status EncodeOhlcvFrames(BlockOff first_offset,
			 const std::vector<BlockBar>& positions,
			 const PrecisionProfile& profile,
			 std::vector<MicroblockFrame>* output) {
	if (output == NULL || positions.empty() || !ValidPrecisionProfile(profile) ||
		positions.size() > std::numeric_limits<BlockOff>::max()) {
		return Status::Error(ErrorCode::InvalidArgument, "invalid OHLCV frame input");
	}
	PrecisionProfile price_profile = profile;
	price_profile.price_relative_epsilon /= 2.0;
	if (price_profile.price_relative_epsilon <= 0.0) {
		return Status::Error(ErrorCode::InvalidArgument, "price precision is out of range");
	}

	std::vector<BlockBar> sanitized = positions;
	for (size_t i = 0; i < sanitized.size(); ++i) {
		if (sanitized[i].state == BarState::Normal && !ValidBar(sanitized[i])) {
			sanitized[i].state = BarState::Missing;
			sanitized[i].open = 0.0;
			sanitized[i].high = 0.0;
			sanitized[i].low = 0.0;
			sanitized[i].close = 0.0;
			sanitized[i].volume = 0.0;
		}
	}

	output->clear();
	MicroblockFrame state_frame;
	Status status = EncodeStateFrame(first_offset, sanitized, &state_frame);
	if (!status.ok()) {
		return status;
	}
	output->push_back(state_frame);
	const FieldId fields[] = {
		FieldId::Open, FieldId::High, FieldId::Low, FieldId::Close, FieldId::Volume
	};
	for (size_t i = 0; i < sizeof(fields) / sizeof(fields[0]); ++i) {
		std::vector<MicroblockFrame> field_frames;
		status = EncodeFieldFrames(fields[i], first_offset, sanitized,
			fields[i] == FieldId::Volume ? profile : price_profile, &field_frames);
		if (!status.ok()) {
			return status;
		}
		output->insert(output->end(), field_frames.begin(), field_frames.end());
	}
	return Status::Ok();
}

Status DecodeOhlcvFrames(BlockOff first_offset,
			 BlockOff position_count,
			 const std::vector<MicroblockFrame>& frames,
			 const PrecisionProfile& profile,
			 std::vector<BlockBar>* positions) {
	if (positions == NULL || position_count == 0 || !ValidPrecisionProfile(profile)) {
		return Status::Error(ErrorCode::InvalidArgument, "invalid OHLCV decode input");
	}
	const MicroblockFrame* state_frame = NULL;
	for (size_t i = 0; i < frames.size(); ++i) {
		if (frames[i].field == FieldId::State) {
			if (state_frame != NULL || frames[i].first_offset != first_offset ||
				frames[i].sample_count != position_count) {
				return Status::Error(ErrorCode::CorruptData, "invalid block state frame");
			}
			state_frame = &frames[i];
		}
	}
	if (state_frame == NULL) {
		return Status::Error(ErrorCode::CorruptData, "missing block state frame");
	}

	std::vector<BarState> states;
	Status status = DecodeStateFrame(*state_frame, &states);
	if (!status.ok()) {
		return status;
	}
	positions->assign(position_count, BlockBar());
	for (size_t i = 0; i < states.size(); ++i) {
		(*positions)[i].state = states[i];
	}
	std::vector<std::vector<bool> > seen(5,
		std::vector<bool>(position_count, false));
	for (size_t frame_index = 0; frame_index < frames.size(); ++frame_index) {
		const MicroblockFrame& frame = frames[frame_index];
		if (frame.field == FieldId::State) {
			continue;
		}
		if (!IsNumericField(frame.field) || frame.first_offset < first_offset) {
			return Status::Error(ErrorCode::CorruptData, "invalid numeric frame offset");
		}
		size_t begin = static_cast<size_t>(frame.first_offset - first_offset);
		if (begin > position_count || frame.sample_count > position_count - begin) {
			return Status::Error(ErrorCode::CorruptData, "numeric frame exceeds block range");
		}
		std::vector<double> values;
		status = DecodeFrame(frame, profile, &values);
		if (!status.ok()) {
			return status;
		}
		size_t field_index = static_cast<size_t>(static_cast<uint8_t>(frame.field) -
			static_cast<uint8_t>(FieldId::Open));
		for (size_t i = 0; i < values.size(); ++i) {
			size_t position = begin + i;
			if ((*positions)[position].state != BarState::Normal ||
				seen[field_index][position]) {
				return Status::Error(ErrorCode::CorruptData, "invalid numeric frame ownership");
			}
			switch (frame.field) {
			case FieldId::Open:
				(*positions)[position].open = values[i];
				break;
			case FieldId::High:
				(*positions)[position].high = values[i];
				break;
			case FieldId::Low:
				(*positions)[position].low = values[i];
				break;
			case FieldId::Close:
				(*positions)[position].close = values[i];
				break;
			case FieldId::Volume:
				(*positions)[position].volume = values[i];
				break;
			case FieldId::State:
				return Status::Error(ErrorCode::CorruptData, "unexpected state frame");
			}
			seen[field_index][position] = true;
		}
	}
	for (size_t i = 0; i < positions->size(); ++i) {
		if ((*positions)[i].state != BarState::Normal) {
			continue;
		}
		for (size_t field = 0; field < seen.size(); ++field) {
			if (!seen[field][i]) {
				return Status::Error(ErrorCode::CorruptData, "missing normal field frame");
			}
		}
		(*positions)[i].high = std::max((*positions)[i].high,
			std::max((*positions)[i].open, (*positions)[i].close));
		(*positions)[i].low = std::min((*positions)[i].low,
			std::min((*positions)[i].open, (*positions)[i].close));
	}
	return Status::Ok();
}


// =============================================================================
// Active Store, Recovery Log, and Public History API
//
// Active owns the only mutable copy of a bar. Data is grouped by time block so
// a completed block can be handed to Staging without reshaping it. The append
// log records individual accepted positions because random writes are the
// Active workload; M4 keeps sealed records in the log until persistent Staging exists.
// =============================================================================

static const uint32_t kActiveRecordMagic = 0x3141545a;  // "ZTA1" in little-endian bytes.
static const uint8_t kActiveRecordVersion = 1;
static const size_t kActiveRecordBytes = 56;

struct ActiveRecord {
	Frequency frequency;
	SymbolId symbol_id;
	TimeId time_id;
	BlockBar bar;
};

struct ResolvedBar {
	Bar bar;
	TimeId time_id;
	TimeId block_id;
	BlockOff block_offset;
};

static BlockBar MissingBlockBar() {
	BlockBar bar = {};
	bar.state = BarState::Missing;
	return bar;
}

static bool SerializeActiveRecord(const ActiveRecord& record,
				  std::vector<uint8_t>* bytes) {
	if (bytes == NULL || !ValidState(record.bar.state) ||
		record.symbol_id == kInvalidSymbolId || !ValidBar(record.bar)) {
		return false;
	}
	bytes->clear();
	PutU32(bytes, kActiveRecordMagic);
	PutU8(bytes, kActiveRecordVersion);
	PutU8(bytes, static_cast<uint8_t>(record.frequency));
	PutU8(bytes, static_cast<uint8_t>(record.bar.state));
	PutU8(bytes, 0);
	PutU32(bytes, record.symbol_id);
	PutU32(bytes, record.time_id);
	PutDouble(bytes, record.bar.open);
	PutDouble(bytes, record.bar.high);
	PutDouble(bytes, record.bar.low);
	PutDouble(bytes, record.bar.close);
	PutDouble(bytes, record.bar.volume);
	return bytes->size() == kActiveRecordBytes;
}

static Status ParseActiveRecord(const std::vector<uint8_t>& bytes,
				size_t offset,
				ActiveRecord* record) {
	if (record == NULL || offset > bytes.size() ||
		bytes.size() - offset < kActiveRecordBytes) {
		return Status::Error(ErrorCode::CorruptData, "truncated active record");
	}
	std::vector<uint8_t> data(bytes.begin() + offset,
					  bytes.begin() + offset + kActiveRecordBytes);
	size_t cursor = 0;
	uint32_t magic = 0;
	uint8_t version = 0;
	uint8_t frequency = 0;
	uint8_t state = 0;
	uint8_t reserved = 0;
	if (!GetU32(data, &cursor, &magic) || !GetU8(data, &cursor, &version) ||
		!GetU8(data, &cursor, &frequency) || !GetU8(data, &cursor, &state) ||
		!GetU8(data, &cursor, &reserved) ||
		!GetU32(data, &cursor, &record->symbol_id) ||
		!GetU32(data, &cursor, &record->time_id) ||
		!GetDouble(data, &cursor, &record->bar.open) ||
		!GetDouble(data, &cursor, &record->bar.high) ||
		!GetDouble(data, &cursor, &record->bar.low) ||
		!GetDouble(data, &cursor, &record->bar.close) ||
		!GetDouble(data, &cursor, &record->bar.volume) ||
		cursor != data.size() || magic != kActiveRecordMagic ||
		version != kActiveRecordVersion || reserved != 0 || frequency > 1 ||
		record->symbol_id == kInvalidSymbolId) {
		return Status::Error(ErrorCode::CorruptData, "invalid active record header");
	}
	record->frequency = static_cast<Frequency>(frequency);
	record->bar.state = static_cast<BarState>(state);
	if (!ValidState(record->bar.state) || !ValidBar(record->bar)) {
		return Status::Error(ErrorCode::CorruptData, "invalid active record values");
	}
	return Status::Ok();
}

static Status ResolveTime(const Calendar& calendar,
			  Frequency frequency,
			  const std::string& local_time,
			  TimeId* time_id,
			  TimeId* block_id,
			  BlockOff* block_offset) {
	if (time_id == NULL || block_id == NULL || block_offset == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "time outputs are required");
	}
	TimeId day_time_id = 0;
	Status status = Status::Ok();
	if (frequency == Frequency::Daily) {
		status = calendar.time_id(local_time, &day_time_id);
		if (!status.ok()) {
			return status;
		}
		*time_id = day_time_id;
	} else {
		HourSlot slot = 0;
		status = calendar.time_id(local_time.substr(0, 8), &day_time_id);
		if (!status.ok()) {
			return status;
		}
		status = calendar.hour_slot(local_time, &slot);
		if (!status.ok()) {
			return status;
		}
		*time_id = hourly_bar_id(time_day(day_time_id), slot);
	}
	return calendar.block_offset(frequency, local_time, block_id, block_offset);
}

static Status LocalTime(const Calendar& calendar,
			    Frequency frequency,
			    TimeId time_id,
			    std::string* out) {
	if (out == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "local time output is required");
	}
	const TimeId day_time_id = daily_bar_id(time_day(time_id));
	if (frequency == Frequency::Daily) {
		if (time_slot(time_id) != 0) {
			return Status::Error(ErrorCode::CorruptData, "daily active record has an hourly slot");
		}
		return calendar.date(day_time_id, out);
	}
	return calendar.local_time(day_time_id, time_slot(time_id), out);
}

static bool ActiveBarOrder(const ActiveBar& left, const ActiveBar& right) {
	return left.time_id != right.time_id ? left.time_id < right.time_id :
		left.symbol_id < right.symbol_id;
}

ActiveStore::ActiveStore(Frequency frequency,
				 const Calendar& calendar,
				 const std::string& market_path,
				 size_t flush_bytes,
				 uint64_t flush_interval_milliseconds)
	: frequency_(frequency),
	  calendar_(calendar),
	  path_(market_path + "/active.data"),
	  replay_status_(Status::Ok()),
	  flush_bytes_(flush_bytes == 0 ? 1 : flush_bytes),
	  flush_interval_milliseconds_(flush_interval_milliseconds == 0 ? 1 :
		flush_interval_milliseconds),
	  dirty_bytes_(0),
	  stop_flush_timer_(false) {
	std::ifstream input(path_.c_str(), std::ios::binary);
	if (!input) {
		flush_timer_ = std::thread(&ActiveStore::run_flush_timer, this);
		return;
	}
	std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(input)),
					   std::istreambuf_iterator<char>());
	const size_t complete_bytes = bytes.size() - bytes.size() % kActiveRecordBytes;
	if (complete_bytes != bytes.size()) {
		// A partial append has no record semantics. Remove it before a future
		// flush so the next complete record cannot be appended after bad bytes.
		input.close();
		std::ofstream repaired(path_.c_str(), std::ios::binary | std::ios::trunc);
		if (!repaired) {
			replay_status_ = Status::Error(ErrorCode::IoError,
				"cannot repair truncated active log");
			return;
		}
		if (complete_bytes > 0) {
			repaired.write(reinterpret_cast<const char*>(&bytes[0]), complete_bytes);
		}
		repaired.flush();
		if (!repaired) {
			replay_status_ = Status::Error(ErrorCode::IoError,
				"cannot finish active log repair");
			return;
		}
	}
	for (size_t offset = 0; offset < complete_bytes; offset += kActiveRecordBytes) {
		ActiveRecord record;
		replay_status_ = ParseActiveRecord(bytes, offset, &record);
		if (!replay_status_.ok()) {
			return;
		}
		if (record.frequency != frequency_) {
			continue;
		}
		std::string local_time;
		replay_status_ = LocalTime(calendar_, frequency_, record.time_id, &local_time);
		if (!replay_status_.ok()) {
			return;
		}
		TimeId time_id = 0;
		TimeId block_id = 0;
		BlockOff block_offset = 0;
		replay_status_ = ResolveTime(calendar_, frequency_, local_time,
							 &time_id, &block_id, &block_offset);
		if (!replay_status_.ok() || time_id != record.time_id) {
			if (replay_status_.ok()) {
				replay_status_ = Status::Error(ErrorCode::CorruptData,
					"active record does not map to its stored time");
			}
			return;
		}
		replay_status_ = put(record.symbol_id, time_id, block_id, block_offset,
							 record.bar, true);
		if (!replay_status_.ok()) {
			return;
		}
	}
	flush_timer_ = std::thread(&ActiveStore::run_flush_timer, this);
}

ActiveStore::~ActiveStore() {
	{
		// Timer shutdown must be synchronized with the timer thread.
		std::lock_guard<std::mutex> lock(mutex_);
		stop_flush_timer_ = true;
	}
	flush_condition_.notify_one();
	if (flush_timer_.joinable()) {
		flush_timer_.join();
	}
	flush();
}

Status ActiveStore::put(SymbolId symbol_id,
				TimeId time_id,
				TimeId block_id,
				BlockOff block_offset,
				const BlockBar& bar,
				bool replay) {
	// This write must be synchronized with reads, sealing, and timer flushes.
	std::lock_guard<std::mutex> lock(mutex_);
	if (!replay_status_.ok()) {
		return replay_status_;
	}
	if (symbol_id == kInvalidSymbolId || !ValidState(bar.state) || !ValidBar(bar)) {
		return Status::Error(ErrorCode::InvalidArgument, "invalid active bar");
	}
	BlockOff block_length = 0;
	Status status = calendar_.block_length(frequency_, block_id, &block_length);
	if (!status.ok()) {
		return status;
	}
	if (block_offset >= block_length) {
		return Status::Error(ErrorCode::InvalidArgument, "active bar offset is outside its block");
	}
	std::map<TimeId, ActiveTimeBlock>::iterator block_it = blocks_.find(block_id);
	if (block_it == blocks_.end()) {
		ActiveTimeBlock block = {};
		block.position_count = block_length;
		block_it = blocks_.insert(std::make_pair(block_id, block)).first;
	} else if (block_it->second.position_count != block_length) {
		return Status::Error(ErrorCode::CorruptData, "active block length changed during use");
	}
	std::map<SymbolId, ActiveStockBlock>::iterator stock_it =
		block_it->second.stocks.find(symbol_id);
	if (stock_it == block_it->second.stocks.end()) {
		ActiveStockBlock stock;
		stock.positions.assign(block_length, MissingBlockBar());
		stock.time_ids.assign(block_length, 0);
		stock.present.assign(block_length, false);
		stock.dirty.assign(block_length, false);
		stock_it = block_it->second.stocks.insert(std::make_pair(symbol_id, stock)).first;
	}
	ActiveStockBlock& stock = stock_it->second;
	if (stock.present[block_offset] && !replay) {
		return Status::Error(ErrorCode::AlreadyPresent, "active bar is already present");
	}
	stock.positions[block_offset] = bar;
	stock.time_ids[block_offset] = time_id;
	stock.present[block_offset] = true;
	stock.dirty[block_offset] = !replay;
	if (!replay) {
		dirty_bytes_ += kActiveRecordBytes;
	}
	return Status::Ok();
}

bool ActiveStore::contains(SymbolId symbol_id, TimeId time_id) const {
	// This duplicate check must be synchronized with writes and timer flushes.
	std::lock_guard<std::mutex> lock(mutex_);
	for (std::map<TimeId, ActiveTimeBlock>::const_iterator block = blocks_.begin();
		 block != blocks_.end(); ++block) {
		std::map<SymbolId, ActiveStockBlock>::const_iterator stock =
			block->second.stocks.find(symbol_id);
		if (stock == block->second.stocks.end()) {
			continue;
		}
		for (size_t i = 0; i < stock->second.present.size(); ++i) {
			if (stock->second.present[i] && stock->second.time_ids[i] == time_id) {
				return true;
			}
		}
	}
	return false;
}

Status ActiveStore::get(SymbolId symbol_id,
				TimeId block_id,
				BlockOff block_offset,
				BlockBar* out) const {
	// This read must be synchronized with writes, sealing, and timer flushes.
	std::lock_guard<std::mutex> lock(mutex_);
	if (out == NULL || symbol_id == kInvalidSymbolId) {
		return Status::Error(ErrorCode::InvalidArgument, "active read output and symbol are required");
	}
	if (!replay_status_.ok()) {
		return replay_status_;
	}
	std::map<TimeId, ActiveTimeBlock>::const_iterator block = blocks_.find(block_id);
	if (block == blocks_.end() || block_offset >= block->second.position_count) {
		return Status::Error(ErrorCode::NotFound, "active bar was not found");
	}
	std::map<SymbolId, ActiveStockBlock>::const_iterator stock =
		block->second.stocks.find(symbol_id);
	if (stock == block->second.stocks.end() || !stock->second.present[block_offset]) {
		return Status::Error(ErrorCode::NotFound, "active bar was not found");
	}
	*out = stock->second.positions[block_offset];
	return Status::Ok();
}

Status ActiveStore::range(const std::vector<SymbolId>& symbol_ids,
				  TimeId begin,
				  TimeId end,
				  std::vector<ActiveBar>* out) const {
	// This range read must be synchronized with writes, sealing, and timer flushes.
	std::lock_guard<std::mutex> lock(mutex_);
	if (out == NULL || begin > end) {
		return Status::Error(ErrorCode::InvalidArgument, "invalid active range");
	}
	if (!replay_status_.ok()) {
		return replay_status_;
	}
	std::set<SymbolId> requested(symbol_ids.begin(), symbol_ids.end());
	out->clear();
	for (std::map<TimeId, ActiveTimeBlock>::const_iterator block = blocks_.begin();
		 block != blocks_.end(); ++block) {
		for (std::map<SymbolId, ActiveStockBlock>::const_iterator stock =
			 block->second.stocks.begin(); stock != block->second.stocks.end(); ++stock) {
			if (requested.find(stock->first) == requested.end()) {
				continue;
			}
			for (size_t i = 0; i < stock->second.present.size(); ++i) {
				if (!stock->second.present[i] || stock->second.time_ids[i] < begin ||
					stock->second.time_ids[i] > end) {
					continue;
				}
				ActiveBar value = {stock->first, stock->second.time_ids[i],
					stock->second.positions[i]};
				out->push_back(value);
			}
		}
	}
	std::sort(out->begin(), out->end(), ActiveBarOrder);
	return Status::Ok();
}

// The byte trigger is evaluated after History has completed its current
// accepted batch. The timer uses the same locked writer path, so a threshold
// flush and a periodic flush cannot append overlapping record batches.
Status ActiveStore::flush_if_needed() {
	// This threshold check and flush must be synchronized with writes and the timer.
	std::lock_guard<std::mutex> lock(mutex_);
	if (dirty_bytes_ < flush_bytes_) {
		return Status::Ok();
	}
	return flush_locked();
}

Status ActiveStore::flush() {
	// This explicit flush must be synchronized with Active mutations and the timer.
	std::lock_guard<std::mutex> lock(mutex_);
	return flush_locked();
}

void ActiveStore::run_flush_timer() {
	// This timer flush must be synchronized with all foreground Active operations.
	std::unique_lock<std::mutex> lock(mutex_);
	while (!stop_flush_timer_) {
		if (flush_condition_.wait_for(lock,
			std::chrono::milliseconds(flush_interval_milliseconds_),
			[this] { return stop_flush_timer_; })) {
			break;
		}
		flush_locked();
	}
}

Status ActiveStore::flush_locked() {
	if (!replay_status_.ok()) {
		return replay_status_;
	}
	bool has_dirty = false;
	for (std::map<TimeId, ActiveTimeBlock>::const_iterator block = blocks_.begin();
		 block != blocks_.end() && !has_dirty; ++block) {
		for (std::map<SymbolId, ActiveStockBlock>::const_iterator stock =
			 block->second.stocks.begin(); stock != block->second.stocks.end() && !has_dirty;
			 ++stock) {
			for (size_t i = 0; i < stock->second.dirty.size(); ++i) {
				if (stock->second.dirty[i]) {
					has_dirty = true;
					break;
				}
			}
		}
	}
	if (!has_dirty) {
		return Status::Ok();
	}
	std::ofstream output(path_.c_str(), std::ios::binary | std::ios::app);
	if (!output) {
		return Status::Error(ErrorCode::IoError, "cannot append active log");
	}
	for (std::map<TimeId, ActiveTimeBlock>::iterator block = blocks_.begin();
		 block != blocks_.end(); ++block) {
		for (std::map<SymbolId, ActiveStockBlock>::iterator stock =
			 block->second.stocks.begin(); stock != block->second.stocks.end(); ++stock) {
			for (size_t i = 0; i < stock->second.dirty.size(); ++i) {
				if (!stock->second.dirty[i]) {
					continue;
				}
				ActiveRecord record = {frequency_, stock->first, stock->second.time_ids[i],
					stock->second.positions[i]};
				std::vector<uint8_t> bytes;
				if (!SerializeActiveRecord(record, &bytes)) {
					return Status::Error(ErrorCode::CorruptData, "invalid dirty active bar");
				}
				output.write(reinterpret_cast<const char*>(&bytes[0]), bytes.size());
				if (!output) {
					return Status::Error(ErrorCode::IoError, "cannot write active log");
				}
			}
		}
	}
	output.flush();
	if (!output) {
		return Status::Error(ErrorCode::IoError, "cannot flush active log");
	}
	for (std::map<TimeId, ActiveTimeBlock>::iterator block = blocks_.begin();
		 block != blocks_.end(); ++block) {
		for (std::map<SymbolId, ActiveStockBlock>::iterator stock =
			 block->second.stocks.begin(); stock != block->second.stocks.end(); ++stock) {
			std::fill(stock->second.dirty.begin(), stock->second.dirty.end(), false);
		}
	}
	dirty_bytes_ = 0;
	return Status::Ok();
}

Status ActiveStore::seal_before(TimeId time_id,
					std::vector<StockTimeBlock>* sealed,
					std::vector<ActiveBar>* sealed_bars) {
	// This seal operation must be synchronized with writes, reads, and timer flushes.
	std::lock_guard<std::mutex> lock(mutex_);
	if (sealed == NULL || sealed_bars == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "sealed outputs are required");
	}
	if (!replay_status_.ok()) {
		return replay_status_;
	}
	Status status = flush_locked();
	if (!status.ok()) {
		return status;
	}
	sealed->clear();
	sealed_bars->clear();
	const TimeId cutoff_day = time_day(time_id);
	const TimeId block_days = frequency_ == Frequency::Daily ?
		kDailyTimeBlockDayLength : kHourlyTimeBlockDayLength;
	for (std::map<TimeId, ActiveTimeBlock>::iterator block = blocks_.begin();
		 block != blocks_.end();) {
		if (time_day(block->first) + block_days > cutoff_day) {
			++block;
			continue;
		}
		for (std::map<SymbolId, ActiveStockBlock>::const_iterator stock =
			 block->second.stocks.begin(); stock != block->second.stocks.end(); ++stock) {
			StockTimeBlock completed = {};
			completed.key.symbol_id = stock->first;
			completed.key.time_block_id = block->first;
			completed.positions = stock->second.positions;
			for (size_t i = 0; i < stock->second.present.size(); ++i) {
				if (!stock->second.present[i]) {
					continue;
				}
				const TimeId day = time_day(stock->second.time_ids[i]);
				const TimeId day_offset = day - time_day(block->first);
				if (day_offset < 64) {
					completed.day_presence |= static_cast<uint64_t>(1) << day_offset;
				}
				ActiveBar active_bar = {stock->first, stock->second.time_ids[i],
					stock->second.positions[i]};
				sealed_bars->push_back(active_bar);
			}
			sealed->push_back(completed);
		}
		blocks_.erase(block++);
	}
	return Status::Ok();
}

Status ActiveStore::replay_status() const {
	// This recovery-status read must be synchronized with Active state access.
	std::lock_guard<std::mutex> lock(mutex_);
	return replay_status_;
}

StagingStore::StagingStore(Frequency frequency)
	: frequency_(frequency) {
}

void StagingStore::accept(const std::vector<StockTimeBlock>& blocks,
				  const std::vector<ActiveBar>& bars) {
	blocks_.insert(blocks_.end(), blocks.begin(), blocks.end());
	bars_.insert(bars_.end(), bars.begin(), bars.end());
}

bool StagingStore::contains(SymbolId symbol_id, TimeId time_id) const {
	for (size_t i = 0; i < bars_.size(); ++i) {
		if (bars_[i].symbol_id == symbol_id && bars_[i].time_id == time_id) {
			return true;
		}
	}
	return false;
}

Status StagingStore::get(SymbolId symbol_id, TimeId time_id, BlockBar* out) const {
	if (out == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "staging read output is required");
	}
	for (size_t i = 0; i < bars_.size(); ++i) {
		if (bars_[i].symbol_id == symbol_id && bars_[i].time_id == time_id) {
			*out = bars_[i].bar;
			return Status::Ok();
		}
	}
	return Status::Error(ErrorCode::NotFound, "staged bar was not found");
}

Status StagingStore::range(const std::vector<SymbolId>& symbol_ids,
				   TimeId begin,
				   TimeId end,
				   std::vector<ActiveBar>* out) const {
	if (out == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "staging range output is required");
	}
	std::set<SymbolId> requested(symbol_ids.begin(), symbol_ids.end());
	out->clear();
	for (size_t i = 0; i < bars_.size(); ++i) {
		if (requested.find(bars_[i].symbol_id) != requested.end() &&
			bars_[i].time_id >= begin && bars_[i].time_id <= end) {
			out->push_back(bars_[i]);
		}
	}
	std::sort(out->begin(), out->end(), ActiveBarOrder);
	return Status::Ok();
}

VaultStore::VaultStore(Frequency frequency)
	: frequency_(frequency) {
}

History::History(Frequency frequency,
			 const Calendar& calendar,
			 const std::string& market_path)
	: frequency_(frequency),
	  calendar_(calendar),
	  active_(new ActiveStore(frequency, calendar, market_path)),
	  staging_(new StagingStore(frequency)),
	  vault_(new VaultStore(frequency)) {
}

History::~History() {
}

Frequency History::frequency() const {
	return frequency_;
}

Status History::put(const Bar& bar) {
	if (bar.frequency != frequency_ || bar.symbol_id == kInvalidSymbolId ||
		!ValidState(bar.state) || !ValidBar(BlockBar{bar.state, bar.open, bar.high,
		bar.low, bar.close, bar.volume})) {
		return Status::Error(ErrorCode::InvalidArgument, "invalid history bar");
	}
	TimeId time_id = 0;
	TimeId block_id = 0;
	BlockOff block_offset = 0;
	Status status = ResolveTime(calendar_, frequency_, bar.local_time,
						&time_id, &block_id, &block_offset);
	if (!status.ok()) {
		return status;
	}
	if (active_->contains(bar.symbol_id, time_id) ||
		staging_->contains(bar.symbol_id, time_id)) {
		return Status::Error(ErrorCode::AlreadyPresent, "history bar is already present");
	}
	BlockBar block_bar = {bar.state, bar.open, bar.high, bar.low, bar.close, bar.volume};
	status = active_->put(bar.symbol_id, time_id, block_id, block_offset, block_bar, false);
	if (!status.ok()) {
		return status;
	}
	return active_->flush_if_needed();
}

Status History::put(const std::vector<Bar>& bars) {
	std::vector<ResolvedBar> resolved;
	std::set<std::pair<SymbolId, TimeId> > seen;
	resolved.reserve(bars.size());
	for (size_t i = 0; i < bars.size(); ++i) {
		const Bar& bar = bars[i];
		if (bar.frequency != frequency_ || bar.symbol_id == kInvalidSymbolId ||
			!ValidState(bar.state) || !ValidBar(BlockBar{bar.state, bar.open, bar.high,
			bar.low, bar.close, bar.volume})) {
			return Status::Error(ErrorCode::InvalidArgument, "invalid history batch bar");
		}
		ResolvedBar item = {};
		item.bar = bar;
		Status status = ResolveTime(calendar_, frequency_, bar.local_time,
								&item.time_id, &item.block_id, &item.block_offset);
		if (!status.ok()) {
			return status;
		}
		if (!seen.insert(std::make_pair(bar.symbol_id, item.time_id)).second ||
			active_->contains(bar.symbol_id, item.time_id) ||
			staging_->contains(bar.symbol_id, item.time_id)) {
			return Status::Error(ErrorCode::AlreadyPresent, "history batch contains an existing bar");
		}
		resolved.push_back(item);
	}
	for (size_t i = 0; i < resolved.size(); ++i) {
		const ResolvedBar& item = resolved[i];
		BlockBar block_bar = {item.bar.state, item.bar.open, item.bar.high,
			item.bar.low, item.bar.close, item.bar.volume};
		Status status = active_->put(item.bar.symbol_id, item.time_id, item.block_id,
							 item.block_offset, block_bar, false);
		if (!status.ok()) {
			return status;
		}
	}
	return active_->flush_if_needed();
}

Status History::get(SymbolId symbol_id,
			    const std::string& local_time,
			    Bar* out) const {
	if (out == NULL || symbol_id == kInvalidSymbolId) {
		return Status::Error(ErrorCode::InvalidArgument, "history read output and symbol are required");
	}
	TimeId time_id = 0;
	TimeId block_id = 0;
	BlockOff block_offset = 0;
	Status status = ResolveTime(calendar_, frequency_, local_time,
						&time_id, &block_id, &block_offset);
	if (!status.ok()) {
		return status;
	}
	BlockBar block_bar;
	status = active_->get(symbol_id, block_id, block_offset, &block_bar);
	if (status.code() == ErrorCode::NotFound) {
		status = staging_->get(symbol_id, time_id, &block_bar);
	}
	if (!status.ok()) {
		return status;
	}
	std::string canonical_time;
	status = LocalTime(calendar_, frequency_, time_id, &canonical_time);
	if (!status.ok()) {
		return status;
	}
	Bar result = {symbol_id, frequency_, canonical_time, block_bar.state, block_bar.open,
		block_bar.high, block_bar.low, block_bar.close, block_bar.volume};
	*out = result;
	return Status::Ok();
}

Status History::get(SymbolId symbol_id,
			    const std::string& begin,
			    const std::string& end,
			    AdjustMode adjust_mode,
			    std::vector<Bar>* out) const {
	std::vector<SymbolId> symbol_ids(1, symbol_id);
	return get(symbol_ids, begin, end, adjust_mode, out);
}

Status History::get(const std::vector<SymbolId>& symbol_ids,
			    const std::string& begin,
			    const std::string& end,
			    AdjustMode adjust_mode,
			    std::vector<Bar>* out) const {
	if (out == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "history range output is required");
	}
	if (adjust_mode != AdjustMode::Raw) {
		return Status::Error(ErrorCode::NotImplemented, "adjusted history reads are not implemented");
	}
	TimeId begin_time_id = 0;
	TimeId ignored_block_id = 0;
	BlockOff ignored_block_offset = 0;
	Status status = ResolveTime(calendar_, frequency_, begin,
						&begin_time_id, &ignored_block_id, &ignored_block_offset);
	if (!status.ok()) {
		return status;
	}
	TimeId end_time_id = 0;
	status = ResolveTime(calendar_, frequency_, end,
						&end_time_id, &ignored_block_id, &ignored_block_offset);
	if (!status.ok()) {
		return status;
	}
	if (begin_time_id > end_time_id) {
		return Status::Error(ErrorCode::InvalidArgument, "history range begins after it ends");
	}
	for (size_t i = 0; i < symbol_ids.size(); ++i) {
		if (symbol_ids[i] == kInvalidSymbolId) {
			return Status::Error(ErrorCode::InvalidArgument, "history range contains an invalid symbol");
		}
	}
	std::vector<ActiveBar> active_bars;
	status = active_->range(symbol_ids, begin_time_id, end_time_id, &active_bars);
	if (!status.ok()) {
		return status;
	}
	std::vector<ActiveBar> staged_bars;
	status = staging_->range(symbol_ids, begin_time_id, end_time_id, &staged_bars);
	if (!status.ok()) {
		return status;
	}
	active_bars.insert(active_bars.end(), staged_bars.begin(), staged_bars.end());
	std::sort(active_bars.begin(), active_bars.end(), ActiveBarOrder);
	out->clear();
	out->reserve(active_bars.size());
	for (size_t i = 0; i < active_bars.size(); ++i) {
		std::string local_time;
		status = LocalTime(calendar_, frequency_, active_bars[i].time_id, &local_time);
		if (!status.ok()) {
			return status;
		}
		Bar bar = {active_bars[i].symbol_id, frequency_, local_time,
			active_bars[i].bar.state, active_bars[i].bar.open,
			active_bars[i].bar.high, active_bars[i].bar.low,
			active_bars[i].bar.close, active_bars[i].bar.volume};
		out->push_back(bar);
	}
	return Status::Ok();
}

Status History::flush() {
	return active_->flush();
}

Status History::seal_before(const std::string& local_time) {
	TimeId time_id = 0;
	TimeId ignored_block_id = 0;
	BlockOff ignored_block_offset = 0;
	Status status = ResolveTime(calendar_, frequency_, local_time,
						&time_id, &ignored_block_id, &ignored_block_offset);
	if (!status.ok()) {
		return status;
	}
	std::vector<StockTimeBlock> sealed;
	std::vector<ActiveBar> sealed_bars;
	status = active_->seal_before(time_id, &sealed, &sealed_bars);
	if (!status.ok()) {
		return status;
	}
	staging_->accept(sealed, sealed_bars);
	return Status::Ok();
}

}  // namespace zstfs
