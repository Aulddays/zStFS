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
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <dirent.h>
#include <fstream>
#include <iterator>
#include <limits>
#include <queue>
#include <set>
#include <sys/stat.h>
#include <unistd.h>
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

Status ActiveStore::collect_before(TimeId time_id,
					       std::vector<StockTimeBlock>* sealed,
					       std::vector<ActiveBar>* sealed_bars) {
	// This snapshot must be synchronized with writes, reads, and timer flushes.
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
		++block;
	}
	return Status::Ok();
}

Status ActiveStore::remove_before(TimeId time_id) {
	// This ownership commit must be synchronized with writes and timer flushes.
	std::lock_guard<std::mutex> lock(mutex_);
	if (!replay_status_.ok()) {
		return replay_status_;
	}
	const TimeId cutoff_day = time_day(time_id);
	const TimeId block_days = frequency_ == Frequency::Daily ?
		kDailyTimeBlockDayLength : kHourlyTimeBlockDayLength;
	const std::string temporary_path = path_ + ".tmp";
	std::ofstream output(temporary_path.c_str(), std::ios::binary | std::ios::trunc);
	if (!output) {
		return Status::Error(ErrorCode::IoError, "cannot rewrite active log");
	}
	for (std::map<TimeId, ActiveTimeBlock>::const_iterator block = blocks_.begin();
		 block != blocks_.end(); ++block) {
		if (time_day(block->first) + block_days <= cutoff_day) {
			continue;
		}
		for (std::map<SymbolId, ActiveStockBlock>::const_iterator stock =
			 block->second.stocks.begin(); stock != block->second.stocks.end(); ++stock) {
			for (size_t offset = 0; offset < stock->second.present.size(); ++offset) {
				if (!stock->second.present[offset]) {
					continue;
				}
				ActiveRecord record = {frequency_, stock->first, stock->second.time_ids[offset],
					stock->second.positions[offset]};
				std::vector<uint8_t> bytes;
				if (!SerializeActiveRecord(record, &bytes)) {
					return Status::Error(ErrorCode::CorruptData, "invalid active bar during rewrite");
				}
				output.write(reinterpret_cast<const char*>(&bytes[0]), bytes.size());
				if (!output) {
					return Status::Error(ErrorCode::IoError, "cannot rewrite active log");
				}
			}
		}
	}
	output.flush();
	output.close();
	if (!output || std::rename(temporary_path.c_str(), path_.c_str()) != 0) {
		std::remove(temporary_path.c_str());
		return Status::Error(ErrorCode::IoError, "cannot publish rewritten active log");
	}
	for (std::map<TimeId, ActiveTimeBlock>::iterator block = blocks_.begin();
		 block != blocks_.end();) {
		if (time_day(block->first) + block_days <= cutoff_day) {
			blocks_.erase(block++);
		} else {
			++block;
		}
	}
	return Status::Ok();
}

Status ActiveStore::replay_status() const {
	// This recovery-status read must be synchronized with Active state access.
	std::lock_guard<std::mutex> lock(mutex_);
	return replay_status_;
}

// =============================================================================
// Staging Pages and Index
//
// Staging pages are variable-length sequential file records. The 64 KiB target
// controls aggregation only: a page closes after appending the first complete
// block that reaches the target, so no padding or special oversized-page format
// is needed. A separate index maps immutable block keys to page locators.
// =============================================================================

static const size_t kStagingPageTargetBytes = 64 * 1024;
static const uint64_t kStagingSegmentTargetBytes = 64ULL * 1024 * 1024;
static const uint8_t kStagingVersion = 1;
static const size_t kStagingPageHeaderBytes = 28;

struct PendingStagingRecord {
	StockTimeBlock block;
	std::vector<uint8_t> present;
	std::vector<std::vector<uint8_t> > frame_bytes;
};

struct ParsedStagingRecord {
	StockTimeBlock block;
	std::vector<ActiveBar> bars;
};

static bool PendingStagingOrder(const PendingStagingRecord& left,
					const PendingStagingRecord& right) {
	return left.block.key.time_block_id != right.block.key.time_block_id ?
		left.block.key.time_block_id < right.block.key.time_block_id :
		left.block.key.symbol_id < right.block.key.symbol_id;
}

static bool SameBlockBar(const BlockBar& left, const BlockBar& right) {
	return left.state == right.state && left.open == right.open && left.high == right.high &&
		left.low == right.low && left.close == right.close && left.volume == right.volume;
}

static PrecisionProfile StagingPrecisionProfile() {
	PrecisionProfile profile = {};
	profile.price_relative_epsilon = 5e-4;
	profile.volume_relative_epsilon = 0.04;
	return profile;
}

static std::string StagingSegmentPath(const std::string& path, uint32_t segment_id) {
	char name[64];
	std::snprintf(name, sizeof(name), "staging-pages-%04u.seg", segment_id);
	return path + "/" + name;
}

static bool ReadFile(const std::string& path, std::vector<uint8_t>* bytes) {
	std::ifstream input(path.c_str(), std::ios::binary);
	if (!input) {
		return false;
	}
	bytes->assign(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
	return input.good() || input.eof();
}

static Status StagingTimeIds(const Calendar& calendar,
				     Frequency frequency,
				     TimeId block_id,
				     BlockOff position_count,
				     std::vector<TimeId>* time_ids) {
	if (time_ids == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "staging time-id output is required");
	}
	time_ids->clear();
	time_ids->reserve(position_count);
	const TimeId first_day = time_day(block_id);
	if (frequency == Frequency::Daily) {
		for (BlockOff offset = 0; offset < position_count; ++offset) {
			time_ids->push_back(daily_bar_id(first_day + offset));
		}
		return Status::Ok();
	}
	for (TimeId day_offset = 0; day_offset < kHourlyTimeBlockDayLength; ++day_offset) {
		const TimeId day_id = daily_bar_id(first_day + day_offset);
		std::string date;
		Status status = calendar.date(day_id, &date);
		if (!status.ok()) {
			return status;
		}
		std::vector<HourSlot> slots;
		status = calendar.slots(date, &slots);
		if (!status.ok()) {
			return status;
		}
		for (size_t slot = 0; slot < slots.size(); ++slot) {
			if (time_ids->size() == position_count) {
				return Status::Error(ErrorCode::CorruptData, "hourly staging block is too short");
			}
			time_ids->push_back(day_id | slots[slot]);
		}
	}
	if (time_ids->size() != position_count) {
		return Status::Error(ErrorCode::CorruptData, "hourly staging block length changed");
	}
	return Status::Ok();
}

static Status MakePendingStagingRecord(const Calendar& calendar,
					       Frequency frequency,
					       const StockTimeBlock& block,
					       const std::vector<ActiveBar>& bars,
					       PendingStagingRecord* output) {
	if (output == NULL || block.key.symbol_id == kInvalidSymbolId ||
		block.positions.empty() || block.positions.size() > std::numeric_limits<BlockOff>::max()) {
		return Status::Error(ErrorCode::InvalidArgument, "invalid staging block");
	}
	BlockOff expected_position_count = 0;
	Status status = calendar.block_length(frequency, block.key.time_block_id,
		&expected_position_count);
	if (!status.ok() || block.positions.size() != expected_position_count) {
		return !status.ok() ? status : Status::Error(ErrorCode::InvalidArgument,
			"staging block does not cover its complete calendar range");
	}
	std::vector<TimeId> time_ids;
	status = StagingTimeIds(calendar, frequency, block.key.time_block_id,
		expected_position_count, &time_ids);
	if (!status.ok()) {
		return status;
	}
	PendingStagingRecord pending = {};
	pending.block = block;
	pending.present.assign((block.positions.size() + 7) / 8, 0);
	for (size_t i = 0; i < bars.size(); ++i) {
		if (bars[i].symbol_id != block.key.symbol_id) {
			continue;
		}
		std::vector<TimeId>::const_iterator position = std::lower_bound(time_ids.begin(),
			time_ids.end(), bars[i].time_id);
		if (position == time_ids.end() || *position != bars[i].time_id) {
			continue;
		}
		const size_t offset = static_cast<size_t>(position - time_ids.begin());
		const uint8_t bit = static_cast<uint8_t>(1U << (offset % 8));
		if ((pending.present[offset / 8] & bit) != 0 ||
			!SameBlockBar(block.positions[offset], bars[i].bar)) {
			return Status::Error(ErrorCode::InvalidArgument, "inconsistent staging bar record");
		}
		pending.present[offset / 8] |= bit;
	}
	for (size_t offset = 0; offset < block.positions.size(); ++offset) {
		if (block.positions[offset].state != BarState::Missing &&
			(pending.present[offset / 8] & static_cast<uint8_t>(1U << (offset % 8))) == 0) {
			return Status::Error(ErrorCode::InvalidArgument, "staging block has an unrecorded position");
		}
	}
	std::vector<MicroblockFrame> frames;
	status = EncodeOhlcvFrames(0, block.positions, StagingPrecisionProfile(), &frames);
	if (!status.ok()) {
		return status;
	}
	pending.frame_bytes.reserve(frames.size());
	for (size_t i = 0; i < frames.size(); ++i) {
		std::vector<uint8_t> frame_bytes;
		status = SerializeFrame(frames[i], &frame_bytes);
		if (!status.ok()) {
			return status;
		}
		pending.frame_bytes.push_back(frame_bytes);
	}
	*output = pending;
	return Status::Ok();
}

static Status SerializeStagingPage(uint32_t page_id,
					  Frequency frequency,
					  const std::vector<PendingStagingRecord>& records,
					  std::vector<uint8_t>* bytes) {
	if (bytes == NULL || records.empty() || records.size() > std::numeric_limits<uint32_t>::max()) {
		return Status::Error(ErrorCode::InvalidArgument, "invalid staging page");
	}
	std::vector<uint8_t> directory;
	std::vector<uint8_t> payload;
	for (size_t i = 0; i < records.size(); ++i) {
		const PendingStagingRecord& record = records[i];
		if (record.present.size() != (record.block.positions.size() + 7) / 8 ||
			record.frame_bytes.size() > std::numeric_limits<uint16_t>::max()) {
			return Status::Error(ErrorCode::InvalidArgument, "invalid staging page record");
		}
		PutU32(&directory, record.block.key.symbol_id);
		PutU32(&directory, record.block.key.time_block_id);
		PutU16(&directory, static_cast<uint16_t>(record.block.positions.size()));
		PutU64(&directory, record.block.day_presence);
		PutU16(&directory, static_cast<uint16_t>(record.present.size()));
		directory.insert(directory.end(), record.present.begin(), record.present.end());
		PutU16(&directory, static_cast<uint16_t>(record.frame_bytes.size()));
		for (size_t frame = 0; frame < record.frame_bytes.size(); ++frame) {
			if (payload.size() > std::numeric_limits<uint32_t>::max() ||
				record.frame_bytes[frame].size() > std::numeric_limits<uint32_t>::max()) {
				return Status::Error(ErrorCode::InvalidArgument, "staging payload is too large");
			}
			PutU32(&directory, static_cast<uint32_t>(payload.size()));
			PutU32(&directory, static_cast<uint32_t>(record.frame_bytes[frame].size()));
			payload.insert(payload.end(), record.frame_bytes[frame].begin(),
				record.frame_bytes[frame].end());
		}
	}
	if (directory.size() > std::numeric_limits<uint32_t>::max() ||
		payload.size() > std::numeric_limits<uint32_t>::max()) {
		return Status::Error(ErrorCode::InvalidArgument, "staging page is too large");
	}
	bytes->clear();
	bytes->reserve(kStagingPageHeaderBytes + directory.size() + payload.size());
	PutU8(bytes, 'Z');
	PutU8(bytes, 'S');
	PutU8(bytes, 'P');
	PutU8(bytes, '5');
	PutU8(bytes, kStagingVersion);
	PutU8(bytes, static_cast<uint8_t>(frequency));
	PutU16(bytes, 0);
	PutU64(bytes, page_id);
	PutU32(bytes, static_cast<uint32_t>(records.size()));
	PutU32(bytes, static_cast<uint32_t>(directory.size()));
	PutU32(bytes, static_cast<uint32_t>(payload.size()));
	bytes->insert(bytes->end(), directory.begin(), directory.end());
	bytes->insert(bytes->end(), payload.begin(), payload.end());
	return Status::Ok();
}

static Status ParseStagingPage(const Calendar& calendar,
				       Frequency frequency,
				       const std::vector<uint8_t>& bytes,
				       std::vector<ParsedStagingRecord>* records,
				       uint32_t* page_id) {
	if (records == NULL || page_id == NULL || bytes.size() < kStagingPageHeaderBytes) {
		return Status::Error(ErrorCode::CorruptData, "truncated staging page");
	}
	size_t cursor = 0;
	uint8_t magic[4] = {};
	uint8_t version = 0;
	uint8_t stored_frequency = 0;
	uint16_t reserved = 0;
	uint64_t stored_page_id = 0;
	uint32_t count = 0;
	uint32_t directory_size = 0;
	uint32_t payload_size = 0;
	if (!GetU8(bytes, &cursor, &magic[0]) || !GetU8(bytes, &cursor, &magic[1]) ||
		!GetU8(bytes, &cursor, &magic[2]) || !GetU8(bytes, &cursor, &magic[3]) ||
		magic[0] != 'Z' || magic[1] != 'S' || magic[2] != 'P' || magic[3] != '5' ||
		!GetU8(bytes, &cursor, &version) || !GetU8(bytes, &cursor, &stored_frequency) ||
		!GetU16(bytes, &cursor, &reserved) || !GetU64(bytes, &cursor, &stored_page_id) ||
		!GetU32(bytes, &cursor, &count) ||
		!GetU32(bytes, &cursor, &directory_size) || !GetU32(bytes, &cursor, &payload_size) ||
		version != kStagingVersion || stored_frequency != static_cast<uint8_t>(frequency) ||
		reserved != 0 || stored_page_id == 0 ||
		stored_page_id > std::numeric_limits<uint32_t>::max() || count == 0 ||
		kStagingPageHeaderBytes + static_cast<size_t>(directory_size) + payload_size != bytes.size()) {
		return Status::Error(ErrorCode::CorruptData, "invalid staging page header");
	}
	*page_id = static_cast<uint32_t>(stored_page_id);
	const size_t directory_end = cursor + directory_size;
	const size_t payload_start = directory_end;
	records->clear();
	records->reserve(count);
	for (uint32_t record_index = 0; record_index < count; ++record_index) {
		uint32_t symbol_id = 0;
		uint32_t block_id = 0;
		uint16_t position_count = 0;
		uint64_t day_presence = 0;
		uint16_t present_size = 0;
		uint16_t frame_count = 0;
		if (!GetU32(bytes, &cursor, &symbol_id) || !GetU32(bytes, &cursor, &block_id) ||
			!GetU16(bytes, &cursor, &position_count) || !GetU64(bytes, &cursor, &day_presence) ||
			!GetU16(bytes, &cursor, &present_size) || symbol_id == kInvalidSymbolId ||
			position_count == 0 || present_size != (position_count + 7) / 8 ||
			cursor + present_size > directory_end) {
			return Status::Error(ErrorCode::CorruptData, "invalid staging record directory");
		}
		std::vector<uint8_t> present(bytes.begin() + cursor, bytes.begin() + cursor + present_size);
		cursor += present_size;
		if (!GetU16(bytes, &cursor, &frame_count) || frame_count == 0) {
			return Status::Error(ErrorCode::CorruptData, "staging record has no frames");
		}
		std::vector<MicroblockFrame> frames;
		frames.reserve(frame_count);
		for (uint16_t frame_index = 0; frame_index < frame_count; ++frame_index) {
			uint32_t offset = 0;
			uint32_t length = 0;
			if (!GetU32(bytes, &cursor, &offset) || !GetU32(bytes, &cursor, &length) ||
				offset > payload_size || length > payload_size - offset) {
				return Status::Error(ErrorCode::CorruptData, "invalid staging frame locator");
			}
			std::vector<uint8_t> frame_bytes(bytes.begin() + payload_start + offset,
				bytes.begin() + payload_start + offset + length);
			MicroblockFrame frame;
			Status status = ParseFrame(frame_bytes, &frame);
			if (!status.ok()) {
				return status;
			}
			frames.push_back(frame);
		}
		std::vector<BlockBar> positions;
		Status status = DecodeOhlcvFrames(0, position_count, frames, StagingPrecisionProfile(),
			&positions);
		if (!status.ok()) {
			return status;
		}
		std::vector<TimeId> time_ids;
		status = StagingTimeIds(calendar, frequency, block_id, position_count, &time_ids);
		if (!status.ok()) {
			return status;
		}
		ParsedStagingRecord parsed = {};
		parsed.block.key.symbol_id = symbol_id;
		parsed.block.key.time_block_id = block_id;
		parsed.block.day_presence = day_presence;
		parsed.block.positions = positions;
		for (size_t offset = 0; offset < positions.size(); ++offset) {
			if ((present[offset / 8] & static_cast<uint8_t>(1U << (offset % 8))) != 0) {
				ActiveBar bar = {symbol_id, time_ids[offset], positions[offset]};
				parsed.bars.push_back(bar);
			}
		}
		records->push_back(parsed);
	}
	if (cursor != directory_end) {
		return Status::Error(ErrorCode::CorruptData, "trailing staging directory bytes");
	}
	return Status::Ok();
}

StagingStore::StagingStore(Frequency frequency,
				   const Calendar& calendar,
				   const std::string& frequency_path)
	: frequency_(frequency),
	  calendar_(calendar),
	  path_(frequency_path),
	  status_(Status::Ok()),
	  next_page_id_(1),
	  current_segment_id_(1) {
	status_ = load();
}

Status StagingStore::load() {
	entries_.clear();
	index_.clear();
	next_page_id_ = 1;
	current_segment_id_ = 1;

	// A valid index directly locates pages for the M5 in-memory query state.
	// Pages remain the source of truth when an index must be rebuilt.
	std::map<std::pair<TimeId, SymbolId>, Locator> persisted_index;
	bool persisted_index_valid = false;
	std::vector<uint8_t> index_bytes;
	if (ReadFile(path_ + "/staging-index", &index_bytes)) {
		size_t cursor = 0;
		uint8_t magic[4] = {};
		uint8_t version = 0;
		uint8_t stored_frequency = 0;
		uint16_t reserved = 0;
		uint32_t count = 0;
		if (GetU8(index_bytes, &cursor, &magic[0]) && GetU8(index_bytes, &cursor, &magic[1]) &&
			GetU8(index_bytes, &cursor, &magic[2]) && GetU8(index_bytes, &cursor, &magic[3]) &&
			magic[0] == 'Z' && magic[1] == 'S' && magic[2] == 'I' && magic[3] == '5' &&
			GetU8(index_bytes, &cursor, &version) && GetU8(index_bytes, &cursor, &stored_frequency) &&
			GetU16(index_bytes, &cursor, &reserved) && GetU32(index_bytes, &cursor, &count) &&
			version == kStagingVersion && stored_frequency == static_cast<uint8_t>(frequency_) &&
			reserved == 0 && index_bytes.size() == 12 + static_cast<size_t>(count) * 28) {
			persisted_index_valid = true;
			for (uint32_t i = 0; i < count; ++i) {
				uint32_t block_id = 0;
				uint32_t symbol_id = 0;
				Locator locator = {};
				if (!GetU32(index_bytes, &cursor, &block_id) ||
					!GetU32(index_bytes, &cursor, &symbol_id) ||
					!GetU32(index_bytes, &cursor, &locator.segment_id) ||
					!GetU64(index_bytes, &cursor, &locator.page_offset) ||
					!GetU32(index_bytes, &cursor, &locator.page_length) ||
					!GetU32(index_bytes, &cursor, &locator.record_index) ||
					symbol_id == kInvalidSymbolId ||
					!persisted_index.insert(std::make_pair(
						std::make_pair(block_id, symbol_id), locator)).second) {
					persisted_index_valid = false;
					break;
				}
			}
		}
	}
	if (persisted_index_valid) {
		std::map<uint32_t, std::vector<uint8_t> > segment_bytes;
		for (std::map<std::pair<TimeId, SymbolId>, Locator>::const_iterator locator =
			 persisted_index.begin(); locator != persisted_index.end(); ++locator) {
			std::map<uint32_t, std::vector<uint8_t> >::iterator segment =
				segment_bytes.find(locator->second.segment_id);
			if (segment == segment_bytes.end()) {
				std::vector<uint8_t> bytes;
				if (!ReadFile(StagingSegmentPath(path_, locator->second.segment_id), &bytes)) {
					persisted_index_valid = false;
					break;
				}
				segment = segment_bytes.insert(std::make_pair(locator->second.segment_id, bytes)).first;
			}
			if (locator->second.page_offset > segment->second.size() ||
				locator->second.page_length > segment->second.size() - locator->second.page_offset) {
				persisted_index_valid = false;
				break;
			}
			const size_t page_offset = static_cast<size_t>(locator->second.page_offset);
			std::vector<uint8_t> page(segment->second.begin() + page_offset,
				segment->second.begin() + page_offset + locator->second.page_length);
			std::vector<ParsedStagingRecord> parsed;
			uint32_t page_id = 0;
			Status status = ParseStagingPage(calendar_, frequency_, page, &parsed, &page_id);
			if (!status.ok() || locator->second.record_index >= parsed.size() ||
				parsed[locator->second.record_index].block.key.time_block_id != locator->first.first ||
				parsed[locator->second.record_index].block.key.symbol_id != locator->first.second) {
				persisted_index_valid = false;
				break;
			}
			next_page_id_ = std::max(next_page_id_, page_id + 1);
			current_segment_id_ = std::max(current_segment_id_, locator->second.segment_id);
			Entry entry = {parsed[locator->second.record_index].block,
				parsed[locator->second.record_index].bars};
			entries_[locator->first] = entry;
		}
		if (persisted_index_valid) {
			index_ = persisted_index;
			return Status::Ok();
		}
		entries_.clear();
		index_.clear();
		next_page_id_ = 1;
		current_segment_id_ = 1;
	}
	for (uint32_t segment_id = 1;; ++segment_id) {
		const std::string segment_path = StagingSegmentPath(path_, segment_id);
		struct stat information;
		if (stat(segment_path.c_str(), &information) != 0) {
			if (errno == ENOENT) {
				break;
			}
			return Status::Error(ErrorCode::IoError, "cannot inspect staging segment");
		}
		std::vector<uint8_t> segment;
		if (!ReadFile(segment_path, &segment)) {
			return Status::Error(ErrorCode::IoError, "cannot read staging segment");
		}
		size_t offset = 0;
		while (offset < segment.size()) {
			if (segment.size() - offset < kStagingPageHeaderBytes) {
				if (truncate(segment_path.c_str(), static_cast<off_t>(offset)) != 0) {
					return Status::Error(ErrorCode::IoError, "cannot repair staging segment tail");
				}
				break;
			}
			size_t header_cursor = offset + 20;
			uint32_t directory_size = 0;
			uint32_t payload_size = 0;
			if (!GetU32(segment, &header_cursor, &directory_size) ||
				!GetU32(segment, &header_cursor, &payload_size) ||
				directory_size > segment.size() || payload_size > segment.size() - directory_size ||
				kStagingPageHeaderBytes + static_cast<size_t>(directory_size) + payload_size >
					segment.size() - offset) {
				if (truncate(segment_path.c_str(), static_cast<off_t>(offset)) != 0) {
					return Status::Error(ErrorCode::IoError, "cannot repair staging segment tail");
				}
				break;
			}
			const size_t page_length = kStagingPageHeaderBytes + directory_size + payload_size;
			std::vector<uint8_t> page(segment.begin() + offset, segment.begin() + offset + page_length);
			std::vector<ParsedStagingRecord> parsed;
			uint32_t page_id = 0;
			Status status = ParseStagingPage(calendar_, frequency_, page, &parsed, &page_id);
			if (!status.ok()) {
				return status;
			}
			for (size_t record = 0; record < parsed.size(); ++record) {
				const std::pair<TimeId, SymbolId> key(parsed[record].block.key.time_block_id,
					parsed[record].block.key.symbol_id);
				if (index_.find(key) != index_.end()) {
					return Status::Error(ErrorCode::CorruptData, "duplicate staged block");
				}
				Locator locator = {segment_id, static_cast<uint64_t>(offset),
					static_cast<uint32_t>(page_length), static_cast<uint32_t>(record)};
				index_[key] = locator;
				Entry entry = {parsed[record].block, parsed[record].bars};
				entries_[key] = entry;
			}
			next_page_id_ = std::max(next_page_id_, page_id + 1);
			offset += page_length;
		}
		current_segment_id_ = segment_id;
	}
	if (persisted_index_valid && persisted_index.size() == index_.size()) {
		std::map<std::pair<TimeId, SymbolId>, Locator>::const_iterator expected =
			persisted_index.begin();
		std::map<std::pair<TimeId, SymbolId>, Locator>::const_iterator actual = index_.begin();
		for (; expected != persisted_index.end(); ++expected, ++actual) {
			if (expected->first != actual->first ||
				expected->second.segment_id != actual->second.segment_id ||
				expected->second.page_offset != actual->second.page_offset ||
				expected->second.page_length != actual->second.page_length ||
				expected->second.record_index != actual->second.record_index) {
				persisted_index_valid = false;
				break;
			}
		}
	} else {
		persisted_index_valid = false;
	}
	return persisted_index_valid ? Status::Ok() : write_index();
}

Status StagingStore::write_index() const {
	std::vector<uint8_t> bytes;
	PutU8(&bytes, 'Z');
	PutU8(&bytes, 'S');
	PutU8(&bytes, 'I');
	PutU8(&bytes, '5');
	PutU8(&bytes, kStagingVersion);
	PutU8(&bytes, static_cast<uint8_t>(frequency_));
	PutU16(&bytes, 0);
	PutU32(&bytes, static_cast<uint32_t>(index_.size()));
	for (std::map<std::pair<TimeId, SymbolId>, Locator>::const_iterator entry = index_.begin();
		 entry != index_.end(); ++entry) {
		PutU32(&bytes, entry->first.first);
		PutU32(&bytes, entry->first.second);
		PutU32(&bytes, entry->second.segment_id);
		PutU64(&bytes, entry->second.page_offset);
		PutU32(&bytes, entry->second.page_length);
		PutU32(&bytes, entry->second.record_index);
	}
	const std::string temporary_path = path_ + "/staging-index.tmp";
	const std::string index_path = path_ + "/staging-index";
	std::ofstream output(temporary_path.c_str(), std::ios::binary | std::ios::trunc);
	if (!output) {
		return Status::Error(ErrorCode::IoError, "cannot write staging index");
	}
	output.write(reinterpret_cast<const char*>(&bytes[0]), bytes.size());
	output.flush();
	output.close();
	if (!output || std::rename(temporary_path.c_str(), index_path.c_str()) != 0) {
		std::remove(temporary_path.c_str());
		return Status::Error(ErrorCode::IoError, "cannot publish staging index");
	}
	return Status::Ok();
}

Status StagingStore::accept(const std::vector<StockTimeBlock>& blocks,
				    const std::vector<ActiveBar>& bars) {
	if (!status_.ok()) {
		return status_;
	}
	std::set<std::pair<TimeId, SymbolId> > batch_keys;
	std::vector<PendingStagingRecord> pending;
	for (size_t i = 0; i < blocks.size(); ++i) {
		const std::pair<TimeId, SymbolId> key(blocks[i].key.time_block_id,
			blocks[i].key.symbol_id);
		if (!batch_keys.insert(key).second) {
			return Status::Error(ErrorCode::Conflict, "duplicate block in staging batch");
		}
		if (index_.find(key) != index_.end()) {
			continue;
		}
		PendingStagingRecord record;
		Status status = MakePendingStagingRecord(calendar_, frequency_, blocks[i], bars, &record);
		if (!status.ok()) {
			return status;
		}
		pending.push_back(record);
	}
	if (pending.empty()) {
		return Status::Ok();
	}
	std::sort(pending.begin(), pending.end(), PendingStagingOrder);
	std::vector<std::vector<PendingStagingRecord> > pages;
	std::vector<PendingStagingRecord> page_records;
	for (size_t i = 0; i < pending.size(); ++i) {
		page_records.push_back(pending[i]);
		std::vector<uint8_t> page_bytes;
		Status status = SerializeStagingPage(next_page_id_, frequency_, page_records, &page_bytes);
		if (!status.ok()) {
			return status;
		}
		if (page_bytes.size() >= kStagingPageTargetBytes) {
			pages.push_back(page_records);
			page_records.clear();
		}
	}
	if (!page_records.empty()) {
		pages.push_back(page_records);
	}
	for (size_t page = 0; page < pages.size(); ++page) {
		const std::string segment_path = StagingSegmentPath(path_, current_segment_id_);
		struct stat information;
		uint64_t offset = 0;
		if (stat(segment_path.c_str(), &information) == 0) {
			offset = static_cast<uint64_t>(information.st_size);
			if (offset >= kStagingSegmentTargetBytes) {
				++current_segment_id_;
				offset = 0;
			}
		} else if (errno != ENOENT) {
			return Status::Error(ErrorCode::IoError, "cannot inspect staging segment");
		}
		const std::string output_path = StagingSegmentPath(path_, current_segment_id_);
		std::vector<uint8_t> page_bytes;
		Status status = SerializeStagingPage(next_page_id_, frequency_, pages[page], &page_bytes);
		if (!status.ok()) {
			return status;
		}
		std::ofstream output(output_path.c_str(), std::ios::binary | std::ios::app);
		if (!output) {
			return Status::Error(ErrorCode::IoError, "cannot append staging page");
		}
		output.write(reinterpret_cast<const char*>(&page_bytes[0]), page_bytes.size());
		output.flush();
		if (!output) {
			return Status::Error(ErrorCode::IoError, "cannot write staging page");
		}
		std::vector<ParsedStagingRecord> parsed;
		uint32_t parsed_page_id = 0;
		status = ParseStagingPage(calendar_, frequency_, page_bytes, &parsed, &parsed_page_id);
		if (!status.ok() || parsed.size() != pages[page].size()) {
			return !status.ok() ? status : Status::Error(ErrorCode::CorruptData,
				"staging page record count changed");
		}
		for (size_t record = 0; record < pages[page].size(); ++record) {
			const std::pair<TimeId, SymbolId> key(pages[page][record].block.key.time_block_id,
				pages[page][record].block.key.symbol_id);
			Locator locator = {current_segment_id_, offset, static_cast<uint32_t>(page_bytes.size()),
				static_cast<uint32_t>(record)};
			index_[key] = locator;
			Entry entry = {parsed[record].block, parsed[record].bars};
			entries_[key] = entry;
		}
		++next_page_id_;
	}
	return write_index();
}

bool StagingStore::contains(SymbolId symbol_id, TimeId time_id) const {
	if (!status_.ok()) {
		return false;
	}
	for (std::map<std::pair<TimeId, SymbolId>, Entry>::const_iterator entry = entries_.begin();
		 entry != entries_.end(); ++entry) {
		for (size_t i = 0; i < entry->second.bars.size(); ++i) {
			if (entry->second.bars[i].symbol_id == symbol_id &&
				entry->second.bars[i].time_id == time_id) {
				return true;
			}
		}
	}
	return false;
}

Status StagingStore::get(SymbolId symbol_id, TimeId time_id, BlockBar* out) const {
	if (out == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "staging read output is required");
	}
	if (!status_.ok()) {
		return status_;
	}
	for (std::map<std::pair<TimeId, SymbolId>, Entry>::const_iterator entry = entries_.begin();
		 entry != entries_.end(); ++entry) {
		for (size_t i = 0; i < entry->second.bars.size(); ++i) {
			if (entry->second.bars[i].symbol_id == symbol_id &&
				entry->second.bars[i].time_id == time_id) {
				*out = entry->second.bars[i].bar;
				return Status::Ok();
			}
		}
	}
	return Status::Error(ErrorCode::NotFound, "staged bar was not found");
}

Status StagingStore::range(const std::vector<SymbolId>& symbol_ids,
				   TimeId begin,
				   TimeId end,
				   std::vector<ActiveBar>* out) const {
	if (out == NULL || begin > end) {
		return Status::Error(ErrorCode::InvalidArgument, "invalid staging range");
	}
	if (!status_.ok()) {
		return status_;
	}
	std::set<SymbolId> requested(symbol_ids.begin(), symbol_ids.end());
	out->clear();
	for (std::map<std::pair<TimeId, SymbolId>, Entry>::const_iterator entry = entries_.begin();
		 entry != entries_.end(); ++entry) {
		for (size_t i = 0; i < entry->second.bars.size(); ++i) {
			const ActiveBar& bar = entry->second.bars[i];
			if (requested.find(bar.symbol_id) != requested.end() &&
				bar.time_id >= begin && bar.time_id <= end) {
				out->push_back(bar);
			}
		}
	}
	std::sort(out->begin(), out->end(), ActiveBarOrder);
	return Status::Ok();
}

Status StagingStore::snapshot(std::vector<StockTimeBlock>* blocks,
								 std::vector<ActiveBar>* bars) const {
	if (blocks == NULL || bars == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "staging snapshot outputs are required");
	}
	if (!status_.ok()) {
		return status_;
	}
	blocks->clear();
	bars->clear();
	for (std::map<std::pair<TimeId, SymbolId>, Entry>::const_iterator entry = entries_.begin();
		 entry != entries_.end(); ++entry) {
		blocks->push_back(entry->second.block);
		bars->insert(bars->end(), entry->second.bars.begin(), entry->second.bars.end());
	}
	std::sort(bars->begin(), bars->end(), ActiveBarOrder);
	return Status::Ok();
}

// =============================================================================
// Vault Blobs, Index, and Shared Read Caches
//
// Vault stores one or more chronologically ordered blobs for each symbol. A
// blob has a compact block directory, a sparse frame locator array, and raw
// ZMF3 frame bytes. The frame codec remains independently owned by codec.cpp;
// ZVB6 only supplies durable framing and selective disk locations.
// =============================================================================

static const uint8_t kVaultVersion = 1;
static const size_t kVaultBlobMaxBytes = 16 * 1024 * 1024;
static const uint64_t kVaultSegmentTargetBytes = 256ULL * 1024 * 1024;
static const size_t kCompressedFrameCacheBytes = 32 * 1024 * 1024;
static const size_t kDecodedFieldCacheBytes = 16 * 1024 * 1024;

struct VaultPendingBlock {
	StockTimeBlock block;
	std::vector<uint8_t> present;
	std::vector<MicroblockFrame> frames;
};

struct VaultBlockDirectory {
	TimeId time_block_id;
	uint64_t day_presence;
	BlockOff position_count;
	uint32_t present_offset;
	uint16_t present_length;
};

struct VaultFrameDirectory {
	FieldId field;
	TimeId time_block_id;
	BlockOff first_offset;
	BlockOff sample_count;
	uint32_t frame_offset;
	uint32_t frame_length;
};

struct CachedVaultBlob {
	uint64_t runtime_market_id;
	Frequency frequency;
	uint32_t segment_id;
	uint64_t blob_offset;
	uint64_t last_use;
	std::vector<uint8_t> bytes;
};

struct CachedVaultBlock {
	uint64_t runtime_market_id;
	Frequency frequency;
	uint32_t segment_id;
	uint64_t blob_offset;
	TimeId time_block_id;
	uint64_t last_use;
	std::vector<ActiveBar> bars;
};

static std::mutex g_vault_cache_mutex;
static uint64_t g_vault_cache_tick = 0;
static size_t g_compressed_cache_size = 0;
static size_t g_decoded_cache_size = 0;
static std::vector<CachedVaultBlob> g_compressed_cache;
static std::vector<CachedVaultBlock> g_decoded_cache;

static std::string VaultSegmentPath(const std::string& path, uint32_t segment_id) {
	char name[64];
	std::snprintf(name, sizeof(name), "vault-%04u.seg", segment_id);
	return path + "/" + name;
}

static bool VaultBlockContainsTime(TimeId first_block_id,
						   TimeId last_block_id,
						   TimeId time_id) {
	const TimeId day = time_day(time_id);
	// Locator endpoints name block starts. The final block therefore covers its
	// full 64-day calendar address range rather than only its first day.
	return day >= time_day(first_block_id) &&
		day < time_day(last_block_id) + kDailyTimeBlockDayLength;
}

static size_t VaultBarsBytes(const std::vector<ActiveBar>& bars) {
	return bars.size() * sizeof(ActiveBar);
}

static void InsertCompressedCache(uint64_t runtime_market_id,
						  Frequency frequency,
						  uint32_t segment_id,
						  uint64_t blob_offset,
						  const std::vector<uint8_t>& bytes) {
	if (bytes.size() > kCompressedFrameCacheBytes) {
		return;
	}
	std::lock_guard<std::mutex> lock(g_vault_cache_mutex);
	while (!g_compressed_cache.empty() &&
		g_compressed_cache_size + bytes.size() > kCompressedFrameCacheBytes) {
		size_t oldest = 0;
		for (size_t i = 1; i < g_compressed_cache.size(); ++i) {
			if (g_compressed_cache[i].last_use < g_compressed_cache[oldest].last_use) {
				oldest = i;
			}
		}
		g_compressed_cache_size -= g_compressed_cache[oldest].bytes.size();
		g_compressed_cache.erase(g_compressed_cache.begin() + oldest);
	}
	CachedVaultBlob entry = {};
	entry.runtime_market_id = runtime_market_id;
	entry.frequency = frequency;
	entry.segment_id = segment_id;
	entry.blob_offset = blob_offset;
	entry.last_use = ++g_vault_cache_tick;
	entry.bytes = bytes;
	g_compressed_cache_size += entry.bytes.size();
	g_compressed_cache.push_back(entry);
}

static bool GetCompressedCache(uint64_t runtime_market_id,
						 Frequency frequency,
						 uint32_t segment_id,
						 uint64_t blob_offset,
						 std::vector<uint8_t>* bytes) {
	std::lock_guard<std::mutex> lock(g_vault_cache_mutex);
	for (size_t i = 0; i < g_compressed_cache.size(); ++i) {
		CachedVaultBlob& entry = g_compressed_cache[i];
		if (entry.runtime_market_id == runtime_market_id && entry.frequency == frequency &&
			entry.segment_id == segment_id && entry.blob_offset == blob_offset) {
			entry.last_use = ++g_vault_cache_tick;
			*bytes = entry.bytes;
			return true;
		}
	}
	return false;
}

static void InsertDecodedCache(uint64_t runtime_market_id,
						  Frequency frequency,
						  uint32_t segment_id,
						  uint64_t blob_offset,
						  TimeId time_block_id,
						  const std::vector<ActiveBar>& bars) {
	const size_t bytes = VaultBarsBytes(bars);
	if (bytes > kDecodedFieldCacheBytes) {
		return;
	}
	std::lock_guard<std::mutex> lock(g_vault_cache_mutex);
	while (!g_decoded_cache.empty() && g_decoded_cache_size + bytes > kDecodedFieldCacheBytes) {
		size_t oldest = 0;
		for (size_t i = 1; i < g_decoded_cache.size(); ++i) {
			if (g_decoded_cache[i].last_use < g_decoded_cache[oldest].last_use) {
				oldest = i;
			}
		}
		g_decoded_cache_size -= VaultBarsBytes(g_decoded_cache[oldest].bars);
		g_decoded_cache.erase(g_decoded_cache.begin() + oldest);
	}
	CachedVaultBlock entry = {};
	entry.runtime_market_id = runtime_market_id;
	entry.frequency = frequency;
	entry.segment_id = segment_id;
	entry.blob_offset = blob_offset;
	entry.time_block_id = time_block_id;
	entry.last_use = ++g_vault_cache_tick;
	entry.bars = bars;
	g_decoded_cache_size += bytes;
	g_decoded_cache.push_back(entry);
}

static bool GetDecodedCache(uint64_t runtime_market_id,
						 Frequency frequency,
						 uint32_t segment_id,
						 uint64_t blob_offset,
						 TimeId time_block_id,
						 std::vector<ActiveBar>* bars) {
	std::lock_guard<std::mutex> lock(g_vault_cache_mutex);
	for (size_t i = 0; i < g_decoded_cache.size(); ++i) {
		CachedVaultBlock& entry = g_decoded_cache[i];
		if (entry.runtime_market_id == runtime_market_id && entry.frequency == frequency &&
			entry.segment_id == segment_id && entry.blob_offset == blob_offset &&
			entry.time_block_id == time_block_id) {
			entry.last_use = ++g_vault_cache_tick;
			*bars = entry.bars;
			return true;
		}
	}
	return false;
}

static void SerializeVaultFrame(const MicroblockFrame& frame, std::vector<uint8_t>* bytes) {
	PutU8(bytes, static_cast<uint8_t>(frame.field));
	PutU16(bytes, frame.first_offset);
	PutU16(bytes, frame.sample_count);
	PutU8(bytes, frame.codec_id);
	PutU8(bytes, frame.predictor_id);
	PutU8(bytes, frame.quantizer_id);
	PutU16(bytes, static_cast<uint16_t>(frame.quantizer_parameters.size()));
	PutU64(bytes, static_cast<uint64_t>(frame.anchor));
	PutU32(bytes, static_cast<uint32_t>(frame.payload.size()));
	bytes->insert(bytes->end(), frame.quantizer_parameters.begin(), frame.quantizer_parameters.end());
	bytes->insert(bytes->end(), frame.payload.begin(), frame.payload.end());
}

static bool ParseVaultFrame(const std::vector<uint8_t>& bytes,
						 size_t offset,
						 size_t length,
						 MicroblockFrame* frame) {
	if (frame == NULL || offset > bytes.size() || length > bytes.size() - offset) {
		return false;
	}
	const size_t end = offset + length;
	uint8_t field = 0;
	uint16_t parameter_length = 0;
	uint64_t anchor = 0;
	uint32_t payload_length = 0;
	if (!GetU8(bytes, &offset, &field) || !GetU16(bytes, &offset, &frame->first_offset) ||
		!GetU16(bytes, &offset, &frame->sample_count) || !GetU8(bytes, &offset, &frame->codec_id) ||
		!GetU8(bytes, &offset, &frame->predictor_id) || !GetU8(bytes, &offset, &frame->quantizer_id) ||
		!GetU16(bytes, &offset, &parameter_length) || !GetU64(bytes, &offset, &anchor) ||
		!GetU32(bytes, &offset, &payload_length) || field > static_cast<uint8_t>(FieldId::Volume) ||
		offset > end || parameter_length > end - offset) {
		return false;
	}
	frame->field = static_cast<FieldId>(field);
	frame->anchor = static_cast<int64_t>(anchor);
	frame->quantizer_parameters.assign(bytes.begin() + offset,
		bytes.begin() + offset + parameter_length);
	offset += parameter_length;
	if (payload_length > end - offset || offset + payload_length != end) {
		return false;
	}
	frame->payload.assign(bytes.begin() + offset, bytes.begin() + end);
	return true;
}

static Status MakeVaultPendingBlock(const Calendar& calendar,
							Frequency frequency,
							const StockTimeBlock& block,
							const std::vector<ActiveBar>& bars,
							VaultPendingBlock* output) {
	if (output == NULL || block.key.symbol_id == kInvalidSymbolId || block.positions.empty()) {
		return Status::Error(ErrorCode::InvalidArgument, "invalid vault block");
	}
	BlockOff expected_length = 0;
	Status status = calendar.block_length(frequency, block.key.time_block_id, &expected_length);
	if (!status.ok() || block.positions.size() != expected_length) {
		return Status::Error(ErrorCode::InvalidArgument, "vault block length does not match calendar");
	}
	output->block = block;
	output->present.assign((block.positions.size() + 7) / 8, 0);
	std::vector<TimeId> time_ids;
	status = StagingTimeIds(calendar, frequency, block.key.time_block_id, expected_length, &time_ids);
	if (!status.ok()) {
		return status;
	}
	for (size_t i = 0; i < bars.size(); ++i) {
		if (bars[i].symbol_id != block.key.symbol_id) {
			continue;
		}
		std::vector<TimeId>::const_iterator position = std::lower_bound(time_ids.begin(), time_ids.end(),
			bars[i].time_id);
		if (position == time_ids.end() || *position != bars[i].time_id) {
			continue;
		}
		const size_t index = static_cast<size_t>(position - time_ids.begin());
		const uint8_t bit = static_cast<uint8_t>(1U << (index % 8));
		if ((output->present[index / 8] & bit) != 0) {
			return Status::Error(ErrorCode::Conflict, "duplicate vault bar");
		}
		output->present[index / 8] |= bit;
	}
	return EncodeOhlcvFrames(0, block.positions, StagingPrecisionProfile(), &output->frames);
}

static Status BuildVaultBlob(Frequency frequency,
						 const std::vector<VaultPendingBlock>& blocks,
						 std::vector<uint8_t>* bytes) {
	if (blocks.empty() || bytes == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "vault blob requires blocks");
	}
	const SymbolId symbol_id = blocks[0].block.key.symbol_id;
	std::vector<VaultBlockDirectory> block_directory;
	std::vector<VaultFrameDirectory> frame_directory;
	std::vector<std::vector<uint8_t> > frame_bytes;
	for (size_t i = 0; i < blocks.size(); ++i) {
		if (blocks[i].block.key.symbol_id != symbol_id) {
			return Status::Error(ErrorCode::InvalidArgument, "vault blob mixes symbols");
		}
		VaultBlockDirectory block_entry = {};
		block_entry.time_block_id = blocks[i].block.key.time_block_id;
		block_entry.day_presence = blocks[i].block.day_presence;
		block_entry.position_count = static_cast<BlockOff>(blocks[i].block.positions.size());
		block_entry.present_length = static_cast<uint16_t>(blocks[i].present.size());
		block_directory.push_back(block_entry);
		for (size_t j = 0; j < blocks[i].frames.size(); ++j) {
			std::vector<uint8_t> frame;
			SerializeVaultFrame(blocks[i].frames[j], &frame);
			VaultFrameDirectory frame_entry = {};
			frame_entry.field = blocks[i].frames[j].field;
			frame_entry.time_block_id = blocks[i].block.key.time_block_id;
			frame_entry.first_offset = blocks[i].frames[j].first_offset;
			frame_entry.sample_count = blocks[i].frames[j].sample_count;
			frame_entry.frame_length = static_cast<uint32_t>(frame.size());
			frame_directory.push_back(frame_entry);
			frame_bytes.push_back(frame);
		}
	}
	const size_t header_bytes = 32 + block_directory.size() * 20 + frame_directory.size() * 17;
	if (header_bytes > std::numeric_limits<uint32_t>::max()) {
		return Status::Error(ErrorCode::InvalidArgument, "vault header is too large");
	}
	size_t payload_offset = header_bytes;
	for (size_t i = 0; i < block_directory.size(); ++i) {
		block_directory[i].present_offset = static_cast<uint32_t>(payload_offset);
		payload_offset += blocks[i].present.size();
	}
	for (size_t i = 0; i < frame_directory.size(); ++i) {
		frame_directory[i].frame_offset = static_cast<uint32_t>(payload_offset);
		payload_offset += frame_bytes[i].size();
	}
	if (payload_offset > kVaultBlobMaxBytes || payload_offset > std::numeric_limits<uint32_t>::max()) {
		return Status::Error(ErrorCode::InvalidArgument, "vault blob exceeds 16 MiB");
	}
	bytes->clear();
	bytes->reserve(payload_offset);
	PutU8(bytes, 'Z'); PutU8(bytes, 'V'); PutU8(bytes, 'B'); PutU8(bytes, '6');
	PutU8(bytes, kVaultVersion); PutU8(bytes, static_cast<uint8_t>(frequency)); PutU16(bytes, 0);
	PutU32(bytes, symbol_id);
	PutU32(bytes, block_directory.front().time_block_id);
	PutU32(bytes, block_directory.back().time_block_id);
	PutU32(bytes, static_cast<uint32_t>(block_directory.size()));
	PutU32(bytes, static_cast<uint32_t>(frame_directory.size()));
	PutU32(bytes, static_cast<uint32_t>(header_bytes));
	for (size_t i = 0; i < block_directory.size(); ++i) {
		PutU32(bytes, block_directory[i].time_block_id);
		PutU64(bytes, block_directory[i].day_presence);
		PutU16(bytes, block_directory[i].position_count);
		PutU16(bytes, block_directory[i].present_length);
		PutU32(bytes, block_directory[i].present_offset);
	}
	for (size_t i = 0; i < frame_directory.size(); ++i) {
		PutU8(bytes, static_cast<uint8_t>(frame_directory[i].field));
		PutU32(bytes, frame_directory[i].time_block_id);
		PutU16(bytes, frame_directory[i].first_offset);
		PutU16(bytes, frame_directory[i].sample_count);
		PutU32(bytes, frame_directory[i].frame_offset);
		PutU32(bytes, frame_directory[i].frame_length);
	}
	for (size_t i = 0; i < blocks.size(); ++i) {
		bytes->insert(bytes->end(), blocks[i].present.begin(), blocks[i].present.end());
	}
	for (size_t i = 0; i < frame_bytes.size(); ++i) {
		bytes->insert(bytes->end(), frame_bytes[i].begin(), frame_bytes[i].end());
	}
	return bytes->size() == payload_offset ? Status::Ok() :
		Status::Error(ErrorCode::CorruptData, "vault blob layout mismatch");
}

VaultStore::VaultStore(Frequency frequency,
					   const Calendar& calendar,
					   const std::string& frequency_path,
					   uint64_t runtime_market_id)
	: frequency_(frequency),
	  calendar_(calendar),
	  path_(frequency_path),
	  runtime_market_id_(runtime_market_id),
	  status_(Status::Ok()),
	  current_segment_id_(1) {
	status_ = load();
}

Status VaultStore::load() {
	index_.clear();
	current_segment_id_ = 1;
	const std::string index_path = path_ + "/vault-index";
	if (access(index_path.c_str(), F_OK) != 0) {
		return Status::Ok();
	}
	std::vector<uint8_t> bytes;
	if (!ReadFile(index_path, &bytes)) {
		return Status::Error(ErrorCode::IoError, "cannot read vault index");
	}
	size_t offset = 0;
	uint8_t magic[4] = {};
	uint8_t version = 0;
	uint8_t stored_frequency = 0;
	uint16_t reserved = 0;
	uint32_t count = 0;
	if (!GetU8(bytes, &offset, &magic[0]) || !GetU8(bytes, &offset, &magic[1]) ||
		!GetU8(bytes, &offset, &magic[2]) || !GetU8(bytes, &offset, &magic[3]) ||
		!GetU8(bytes, &offset, &version) || !GetU8(bytes, &offset, &stored_frequency) ||
		!GetU16(bytes, &offset, &reserved) || !GetU32(bytes, &offset, &count) ||
		magic[0] != 'Z' || magic[1] != 'V' || magic[2] != 'I' || magic[3] != '6' ||
		version != kVaultVersion || stored_frequency != static_cast<uint8_t>(frequency_)) {
		return Status::Error(ErrorCode::CorruptData, "invalid vault index");
	}
	for (uint32_t i = 0; i < count; ++i) {
		Locator locator = {};
		if (!GetU32(bytes, &offset, &locator.symbol_id) || !GetU32(bytes, &offset, &locator.first_time_block_id) ||
			!GetU32(bytes, &offset, &locator.last_time_block_id) || !GetU32(bytes, &offset, &locator.segment_id) ||
			!GetU64(bytes, &offset, &locator.blob_offset) || !GetU32(bytes, &offset, &locator.blob_length) ||
			locator.symbol_id == kInvalidSymbolId || locator.blob_length == 0 ||
			locator.blob_length > kVaultBlobMaxBytes) {
			return Status::Error(ErrorCode::CorruptData, "invalid vault locator");
		}
		index_.push_back(locator);
		current_segment_id_ = std::max(current_segment_id_, locator.segment_id);
	}
	if (offset != bytes.size()) {
		return Status::Error(ErrorCode::CorruptData, "trailing vault index bytes");
	}
	for (size_t i = 1; i < index_.size(); ++i) {
		if (index_[i - 1].symbol_id > index_[i].symbol_id ||
			(index_[i - 1].symbol_id == index_[i].symbol_id &&
			index_[i - 1].first_time_block_id > index_[i].first_time_block_id)) {
			return Status::Error(ErrorCode::CorruptData, "vault index is not sorted");
		}
	}
	return Status::Ok();
}

Status VaultStore::write_index() const {
	std::vector<uint8_t> bytes;
	PutU8(&bytes, 'Z'); PutU8(&bytes, 'V'); PutU8(&bytes, 'I'); PutU8(&bytes, '6');
	PutU8(&bytes, kVaultVersion); PutU8(&bytes, static_cast<uint8_t>(frequency_)); PutU16(&bytes, 0);
	PutU32(&bytes, static_cast<uint32_t>(index_.size()));
	for (size_t i = 0; i < index_.size(); ++i) {
		PutU32(&bytes, index_[i].symbol_id);
		PutU32(&bytes, index_[i].first_time_block_id);
		PutU32(&bytes, index_[i].last_time_block_id);
		PutU32(&bytes, index_[i].segment_id);
		PutU64(&bytes, index_[i].blob_offset);
		PutU32(&bytes, index_[i].blob_length);
	}
	const std::string temporary_path = path_ + "/vault-index.tmp";
	std::ofstream output(temporary_path.c_str(), std::ios::binary | std::ios::trunc);
	if (!output) {
		return Status::Error(ErrorCode::IoError, "cannot create vault index");
	}
	output.write(reinterpret_cast<const char*>(&bytes[0]), bytes.size());
	output.flush();
	output.close();
	if (!output || std::rename(temporary_path.c_str(), (path_ + "/vault-index").c_str()) != 0) {
		std::remove(temporary_path.c_str());
		return Status::Error(ErrorCode::IoError, "cannot publish vault index");
	}
	return Status::Ok();
}

Status VaultStore::ingest(const std::vector<StockTimeBlock>& blocks,
					  const std::vector<ActiveBar>& bars) {
	if (!status_.ok()) {
		return status_;
	}
	const TimeId block_day_length = frequency_ == Frequency::Daily
		? kDailyTimeBlockDayLength : kHourlyTimeBlockDayLength;
	// A compaction batch carries bars for many complete blocks. Route them once
	// so each pending Vault block validates only its own local positions.
	std::map<std::pair<SymbolId, TimeId>, std::vector<ActiveBar> > bars_by_block;
	for (size_t i = 0; i < bars.size(); ++i) {
		const TimeId day = time_day(bars[i].time_id);
		const TimeId block_id = daily_bar_id(day - day % block_day_length);
		bars_by_block[std::make_pair(bars[i].symbol_id, block_id)].push_back(bars[i]);
	}

	std::map<SymbolId, std::vector<VaultPendingBlock> > by_symbol;
	const std::vector<ActiveBar> empty_bars;
	for (size_t i = 0; i < blocks.size(); ++i) {
		const std::pair<SymbolId, TimeId> key(blocks[i].key.symbol_id,
			blocks[i].key.time_block_id);
		std::map<std::pair<SymbolId, TimeId>, std::vector<ActiveBar> >::const_iterator found =
			bars_by_block.find(key);
		const std::vector<ActiveBar>& block_bars = found == bars_by_block.end()
			? empty_bars : found->second;
		VaultPendingBlock pending;
		Status status = MakeVaultPendingBlock(calendar_, frequency_, blocks[i], block_bars, &pending);
		if (!status.ok()) {
			return status;
		}
		by_symbol[blocks[i].key.symbol_id].push_back(pending);
	}
	std::vector<Locator> added;
	const auto append_blob = [&](const std::vector<VaultPendingBlock>& group,
								const std::vector<uint8_t>& blob) -> Status {
		const std::string segment_path = VaultSegmentPath(path_, current_segment_id_);
		std::ifstream existing(segment_path.c_str(), std::ios::binary | std::ios::ate);
		uint64_t segment_size = existing ? static_cast<uint64_t>(existing.tellg()) : 0;
		if (segment_size != 0 && segment_size + blob.size() > kVaultSegmentTargetBytes) {
			++current_segment_id_;
			segment_size = 0;
		}
		const std::string write_path = VaultSegmentPath(path_, current_segment_id_);
		std::ofstream output(write_path.c_str(), std::ios::binary | std::ios::app);
		if (!output) {
			return Status::Error(ErrorCode::IoError, "cannot append vault segment");
		}
		output.write(reinterpret_cast<const char*>(&blob[0]), blob.size());
		output.flush();
		if (!output) {
			return Status::Error(ErrorCode::IoError, "cannot write vault blob");
		}
		Locator locator = {};
		locator.symbol_id = group.front().block.key.symbol_id;
		locator.first_time_block_id = group.front().block.key.time_block_id;
		locator.last_time_block_id = group.back().block.key.time_block_id;
		locator.segment_id = current_segment_id_;
		locator.blob_offset = segment_size;
		locator.blob_length = static_cast<uint32_t>(blob.size());
		added.push_back(locator);
		return Status::Ok();
	};
	for (std::map<SymbolId, std::vector<VaultPendingBlock> >::iterator symbol = by_symbol.begin();
		 symbol != by_symbol.end(); ++symbol) {
		std::sort(symbol->second.begin(), symbol->second.end(),
			[](const VaultPendingBlock& left, const VaultPendingBlock& right) {
				return left.block.key.time_block_id < right.block.key.time_block_id;
			});
		std::vector<VaultPendingBlock> group(symbol->second);
		std::vector<uint8_t> blob;
		Status status = BuildVaultBlob(frequency_, group, &blob);
		if (status.ok()) {
			status = append_blob(group, blob);
			if (!status.ok()) {
				return status;
			}
			continue;
		}

		// Oversized symbols retain the existing 16 MiB blob partitioning rule.
		group.clear();
		for (size_t i = 0; i < symbol->second.size(); ++i) {
			group.push_back(symbol->second[i]);
			blob.clear();
			status = BuildVaultBlob(frequency_, group, &blob);
			if (!status.ok()) {
				if (group.size() == 1) {
					return status;
				}
				group.pop_back();
				status = BuildVaultBlob(frequency_, group, &blob);
				if (!status.ok()) {
					return status;
				}
				i--;
			} else if (i + 1 != symbol->second.size()) {
				continue;
			}
			status = append_blob(group, blob);
			if (!status.ok()) {
				return status;
			}
			group.clear();
		}
	}
	index_.insert(index_.end(), added.begin(), added.end());
	std::sort(index_.begin(), index_.end(), [](const Locator& left, const Locator& right) {
		return left.symbol_id != right.symbol_id ? left.symbol_id < right.symbol_id :
			left.first_time_block_id < right.first_time_block_id;
	});
	return write_index();
}

static Status ReadVaultBlob(const std::string& path,
						uint64_t runtime_market_id,
						Frequency frequency,
						uint32_t segment_id,
						uint64_t blob_offset,
						uint32_t blob_length,
						std::vector<uint8_t>* bytes) {
	if (GetCompressedCache(runtime_market_id, frequency, segment_id, blob_offset, bytes)) {
		return Status::Ok();
	}
	std::ifstream input(VaultSegmentPath(path, segment_id).c_str(), std::ios::binary);
	if (!input) {
		return Status::Error(ErrorCode::IoError, "cannot read vault segment");
	}
	input.seekg(static_cast<std::streamoff>(blob_offset));
	bytes->assign(blob_length, 0);
	input.read(reinterpret_cast<char*>(&(*bytes)[0]), blob_length);
	if (input.gcount() != static_cast<std::streamsize>(blob_length)) {
		return Status::Error(ErrorCode::CorruptData, "truncated vault blob");
	}
	InsertCompressedCache(runtime_market_id, frequency, segment_id, blob_offset, *bytes);
	return Status::Ok();
}

static Status DecodeVaultBlock(const Calendar& calendar,
						   Frequency frequency,
						   uint64_t runtime_market_id,
						   uint32_t segment_id,
						   uint64_t blob_offset,
						   const std::vector<uint8_t>& bytes,
						   SymbolId symbol_id,
						   TimeId wanted_block_id,
						   std::vector<ActiveBar>* output) {
	if (GetDecodedCache(runtime_market_id, frequency, segment_id, blob_offset, wanted_block_id, output)) {
		return Status::Ok();
	}
	size_t offset = 0;
	uint8_t magic[4] = {};
	uint8_t version = 0;
	uint8_t stored_frequency = 0;
	uint16_t reserved = 0;
	uint32_t stored_symbol = 0;
	uint32_t first_block = 0;
	uint32_t last_block = 0;
	uint32_t block_count = 0;
	uint32_t frame_count = 0;
	uint32_t header_bytes = 0;
	if (!GetU8(bytes, &offset, &magic[0]) || !GetU8(bytes, &offset, &magic[1]) ||
		!GetU8(bytes, &offset, &magic[2]) || !GetU8(bytes, &offset, &magic[3]) ||
		!GetU8(bytes, &offset, &version) || !GetU8(bytes, &offset, &stored_frequency) ||
		!GetU16(bytes, &offset, &reserved) || !GetU32(bytes, &offset, &stored_symbol) ||
		!GetU32(bytes, &offset, &first_block) || !GetU32(bytes, &offset, &last_block) ||
		!GetU32(bytes, &offset, &block_count) || !GetU32(bytes, &offset, &frame_count) ||
		!GetU32(bytes, &offset, &header_bytes) || magic[0] != 'Z' || magic[1] != 'V' ||
		magic[2] != 'B' || magic[3] != '6' || version != kVaultVersion ||
		stored_frequency != static_cast<uint8_t>(frequency) || stored_symbol != symbol_id ||
		header_bytes > bytes.size()) {
		return Status::Error(ErrorCode::CorruptData, "invalid vault blob header");
	}
	std::vector<VaultBlockDirectory> blocks;
	for (uint32_t i = 0; i < block_count; ++i) {
		VaultBlockDirectory entry = {};
		if (!GetU32(bytes, &offset, &entry.time_block_id) || !GetU64(bytes, &offset, &entry.day_presence) ||
			!GetU16(bytes, &offset, &entry.position_count) || !GetU16(bytes, &offset, &entry.present_length) ||
			!GetU32(bytes, &offset, &entry.present_offset) || entry.position_count == 0 ||
			entry.present_length != (entry.position_count + 7) / 8 ||
			entry.present_offset > bytes.size() || entry.present_length > bytes.size() - entry.present_offset) {
			return Status::Error(ErrorCode::CorruptData, "invalid vault block directory");
		}
		blocks.push_back(entry);
	}
	std::vector<VaultFrameDirectory> frames;
	for (uint32_t i = 0; i < frame_count; ++i) {
		VaultFrameDirectory entry = {};
		uint8_t field = 0;
		if (!GetU8(bytes, &offset, &field) || !GetU32(bytes, &offset, &entry.time_block_id) ||
			!GetU16(bytes, &offset, &entry.first_offset) || !GetU16(bytes, &offset, &entry.sample_count) ||
			!GetU32(bytes, &offset, &entry.frame_offset) || !GetU32(bytes, &offset, &entry.frame_length) ||
			field > static_cast<uint8_t>(FieldId::Volume) || entry.frame_length == 0 ||
			entry.frame_offset > bytes.size() || entry.frame_length > bytes.size() - entry.frame_offset) {
			return Status::Error(ErrorCode::CorruptData, "invalid vault frame locator");
		}
		entry.field = static_cast<FieldId>(field);
		frames.push_back(entry);
	}
	if (offset != header_bytes || wanted_block_id < first_block || wanted_block_id > last_block) {
		return Status::Error(ErrorCode::CorruptData, "invalid vault blob directory size");
	}
	const VaultBlockDirectory* block = NULL;
	for (size_t i = 0; i < blocks.size(); ++i) {
		if (blocks[i].time_block_id == wanted_block_id) {
			block = &blocks[i];
			break;
		}
	}
	if (block == NULL) {
		return Status::Error(ErrorCode::NotFound, "vault block was not found");
	}
	std::vector<MicroblockFrame> decoded_frames;
	for (size_t i = 0; i < frames.size(); ++i) {
		if (frames[i].time_block_id != wanted_block_id) {
			continue;
		}
		MicroblockFrame frame;
		if (!ParseVaultFrame(bytes, frames[i].frame_offset, frames[i].frame_length, &frame) ||
			frame.field != frames[i].field || frame.first_offset != frames[i].first_offset ||
			frame.sample_count != frames[i].sample_count) {
			return Status::Error(ErrorCode::CorruptData, "invalid vault frame payload");
		}
		decoded_frames.push_back(frame);
	}
	std::vector<BlockBar> positions;
	Status status = DecodeOhlcvFrames(0, block->position_count, decoded_frames,
		StagingPrecisionProfile(), &positions);
	if (!status.ok()) {
		return Status::Error(ErrorCode::CorruptData, "cannot decode vault block");
	}
	std::vector<TimeId> time_ids;
	status = StagingTimeIds(calendar, frequency, wanted_block_id, block->position_count, &time_ids);
	if (!status.ok()) {
		return Status::Error(ErrorCode::CorruptData, "cannot map vault block times");
	}
	output->clear();
	for (size_t i = 0; i < positions.size(); ++i) {
		if ((bytes[block->present_offset + i / 8] & static_cast<uint8_t>(1U << (i % 8))) == 0) {
			continue;
		}
		ActiveBar bar = {};
		bar.symbol_id = symbol_id;
		bar.time_id = time_ids[i];
		bar.bar = positions[i];
		output->push_back(bar);
	}
	InsertDecodedCache(runtime_market_id, frequency, segment_id, blob_offset,
		wanted_block_id, *output);
	return Status::Ok();
}

bool VaultStore::contains(SymbolId symbol_id, TimeId time_id) const {
	BlockBar bar;
	return get(symbol_id, time_id, &bar).ok();
}

Status VaultStore::get(SymbolId symbol_id, TimeId time_id, BlockBar* out) const {
	if (out == NULL || symbol_id == kInvalidSymbolId) {
		return Status::Error(ErrorCode::InvalidArgument, "vault read output and symbol are required");
	}
	if (!status_.ok()) {
		return status_;
	}
	Status first_error = Status::Error(ErrorCode::NotFound, "vault bar was not found");
	for (size_t i = 0; i < index_.size(); ++i) {
		const Locator& locator = index_[i];
		if (locator.symbol_id != symbol_id || !VaultBlockContainsTime(locator.first_time_block_id,
			locator.last_time_block_id, time_id)) {
			continue;
		}
		std::vector<uint8_t> bytes;
		Status status = ReadVaultBlob(path_, runtime_market_id_, frequency_, locator.segment_id,
			locator.blob_offset, locator.blob_length, &bytes);
		if (!status.ok()) {
			return status.code() == ErrorCode::IoError ? status :
				Status::Error(ErrorCode::CorruptData, status.message());
		}
		std::vector<ActiveBar> bars;
		const TimeId block_day = time_day(time_id) -
			(time_day(time_id) % kDailyTimeBlockDayLength);
		const TimeId block_id = daily_bar_id(block_day);
		status = DecodeVaultBlock(calendar_, frequency_, runtime_market_id_, locator.segment_id,
			locator.blob_offset, bytes, symbol_id, block_id, &bars);
		if (!status.ok()) {
			if (status.code() == ErrorCode::NotFound) {
				continue;
			}
			return Status::Error(ErrorCode::CorruptData, status.message());
		}
		for (size_t j = 0; j < bars.size(); ++j) {
			if (bars[j].time_id == time_id) {
				*out = bars[j].bar;
				return Status::Ok();
			}
		}
	}
	return first_error;
}

Status VaultStore::range(const std::vector<SymbolId>& symbol_ids,
					 TimeId begin,
					 TimeId end,
					 std::vector<ActiveBar>* out) const {
	if (out == NULL || begin > end) {
		return Status::Error(ErrorCode::InvalidArgument, "invalid vault range");
	}
	out->clear();
	if (!status_.ok()) {
		return status_;
	}
	std::set<SymbolId> requested(symbol_ids.begin(), symbol_ids.end());
	bool corrupt = false;
	for (size_t i = 0; i < index_.size(); ++i) {
		const Locator& locator = index_[i];
		if (requested.find(locator.symbol_id) == requested.end() ||
			time_day(locator.last_time_block_id) + kDailyTimeBlockDayLength <= time_day(begin) ||
			time_day(locator.first_time_block_id) > time_day(end)) {
			continue;
		}
		std::vector<uint8_t> bytes;
		Status status = ReadVaultBlob(path_, runtime_market_id_, frequency_, locator.segment_id,
			locator.blob_offset, locator.blob_length, &bytes);
		if (!status.ok()) {
			corrupt = true;
			continue;
		}
		for (TimeId block_id = locator.first_time_block_id; block_id <= locator.last_time_block_id;
			 block_id = daily_bar_id(time_day(block_id) + 64)) {
			std::vector<ActiveBar> bars;
			status = DecodeVaultBlock(calendar_, frequency_, runtime_market_id_, locator.segment_id,
				locator.blob_offset, bytes, locator.symbol_id, block_id, &bars);
			if (!status.ok()) {
				if (status.code() != ErrorCode::NotFound) {
					corrupt = true;
				}
				continue;
			}
			for (size_t j = 0; j < bars.size(); ++j) {
				if (bars[j].time_id >= begin && bars[j].time_id <= end) {
					out->push_back(bars[j]);
				}
			}
			if (block_id > std::numeric_limits<TimeId>::max() - kTimeIdDayStep * 64) {
				break;
			}
		}
	}
	std::sort(out->begin(), out->end(), ActiveBarOrder);
	return corrupt ? Status::Error(ErrorCode::CorruptData, "one or more vault blocks are corrupt") : Status::Ok();
}

// Reconstruct logical blocks from explicit bars when copying an immutable
// store. The persistent codecs already retain complete positions, but this
// helper keeps compaction independent from their private blob/page directories.
static TimeId CompactionBlockDayLength(Frequency frequency) {
	return frequency == Frequency::Daily ? kDailyTimeBlockDayLength :
		kHourlyTimeBlockDayLength;
}

static TimeId CompactionBlockId(Frequency frequency, TimeId time_id) {
	const TimeId length = CompactionBlockDayLength(frequency);
	return daily_bar_id((time_day(time_id) / length) * length);
}

static Status BlocksFromBars(const Calendar& calendar,
					 Frequency frequency,
					 const std::vector<ActiveBar>& bars,
					 std::vector<StockTimeBlock>* blocks) {
	if (blocks == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "block reconstruction output is required");
	}
	std::map<std::pair<SymbolId, TimeId>, std::vector<ActiveBar> > grouped;
	for (size_t i = 0; i < bars.size(); ++i) {
		const TimeId block_id = CompactionBlockId(frequency, bars[i].time_id);
		grouped[std::make_pair(bars[i].symbol_id, block_id)].push_back(bars[i]);
	}
	blocks->clear();
	for (std::map<std::pair<SymbolId, TimeId>, std::vector<ActiveBar> >::const_iterator group =
		 grouped.begin(); group != grouped.end(); ++group) {
		BlockOff position_count = 0;
		Status status = calendar.block_length(frequency, group->first.second, &position_count);
		if (!status.ok()) {
			return status;
		}
		std::vector<TimeId> time_ids;
		status = StagingTimeIds(calendar, frequency, group->first.second, position_count, &time_ids);
		if (!status.ok()) {
			return status;
		}
		StockTimeBlock block = {};
		block.key.symbol_id = group->first.first;
		block.key.time_block_id = group->first.second;
		block.positions.assign(position_count, MissingBlockBar());
		std::vector<bool> occupied(position_count, false);
		for (size_t i = 0; i < group->second.size(); ++i) {
			std::vector<TimeId>::const_iterator position = std::lower_bound(time_ids.begin(),
				time_ids.end(), group->second[i].time_id);
			if (position == time_ids.end() || *position != group->second[i].time_id) {
				return Status::Error(ErrorCode::CorruptData, "stored bar does not fit its block");
			}
			const size_t offset = static_cast<size_t>(position - time_ids.begin());
			if (occupied[offset]) {
				return Status::Error(ErrorCode::Conflict, "duplicate bar in immutable block");
			}
			occupied[offset] = true;
			block.positions[offset] = group->second[i].bar;
			const TimeId day_offset = time_day(group->second[i].time_id) -
				time_day(block.key.time_block_id);
			block.day_presence |= static_cast<uint64_t>(1) << day_offset;
		}
		blocks->push_back(block);
	}
	return Status::Ok();
}

Status VaultStore::snapshot(std::vector<StockTimeBlock>* blocks,
							  std::vector<ActiveBar>* bars) const {
	if (blocks == NULL || bars == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "vault snapshot outputs are required");
	}
	if (!status_.ok()) {
		return status_;
	}
	std::vector<SymbolId> symbols;
	for (size_t i = 0; i < index_.size(); ++i) {
		if (symbols.empty() || symbols.back() != index_[i].symbol_id) {
			symbols.push_back(index_[i].symbol_id);
		}
	}
	Status status = range(symbols, 0, std::numeric_limits<TimeId>::max(), bars);
	if (!status.ok()) {
		return status;
	}
	return BlocksFromBars(calendar_, frequency_, *bars, blocks);
}

static std::string FrequencyPath(const std::string& market_path, Frequency frequency) {
	const std::string path = market_path + (frequency == Frequency::Daily ? "/daily" : "/hourly");
	if (mkdir(path.c_str(), 0755) != 0 && errno != EEXIST) {
		return std::string();
	}
	return path;
}

// Runtime IDs keep shared cache entries isolated across independently opened
// markets. They are namespaces only: append-only Vault data leaves cached
// entries valid and therefore requires no generation-based invalidation.
static std::mutex g_runtime_market_id_mutex;
static uint64_t g_next_runtime_market_id = 1;

static uint64_t NextRuntimeMarketId() {
	std::lock_guard<std::mutex> lock(g_runtime_market_id_mutex);
	return g_next_runtime_market_id++;
}

History::History(Frequency frequency,
			 const Calendar& calendar,
			 const std::string& market_path,
			 const Actions& actions,
			 const std::function<Status()>& publish_manifest,
			 const Status& initial_status)
	: frequency_(frequency),
	  calendar_(calendar),
	  actions_(actions),
	  publish_manifest_(publish_manifest),
	  status_(initial_status),
	  active_(),
	  staging_(),
	  vault_() {
	// Manifest validation precedes store construction. A failed open retains a
	// History shell so the existing accessor remains source compatible, but all
	// operations return the original CorruptData or I/O status.
	if (!status_.ok()) {
		return;
	}
	const std::string frequency_path = FrequencyPath(market_path, frequency);
	if (frequency_path.empty()) {
		status_ = Status::Error(ErrorCode::IoError, "cannot create frequency directory");
		return;
	}
	active_.reset(new ActiveStore(frequency, calendar, frequency_path));
	staging_.reset(new StagingStore(frequency, calendar, frequency_path));
	vault_.reset(new VaultStore(frequency, calendar, frequency_path,
		NextRuntimeMarketId()));
}

Status History::status() const {
	return status_;
}

History::~History() {
}

Frequency History::frequency() const {
	return frequency_;
}

Status History::put(const Bar& bar) {
	if (!status_.ok()) {
		return status_;
	}
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
	status = active_->flush();
	if (!status.ok()) {
		return status;
	}
	return publish_manifest_ ? publish_manifest_() : Status::Ok();
}

Status History::put(const std::vector<Bar>& bars) {
	if (!status_.ok()) {
		return status_;
	}
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
	Status status = active_->flush();
	if (!status.ok()) {
		return status;
	}
	return publish_manifest_ ? publish_manifest_() : Status::Ok();
}

Status History::get(SymbolId symbol_id,
			    const std::string& local_time,
			    Bar* out) const {
	if (!status_.ok()) {
		return status_;
	}
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
	BlockBar active_bar = {};
	BlockBar staged_bar = {};
	BlockBar vault_bar = {};
	const Status active_status = active_->get(symbol_id, block_id, block_offset, &active_bar);
	const Status staged_status = staging_->get(symbol_id, time_id, &staged_bar);
	const Status vault_status = vault_->get(symbol_id, time_id, &vault_bar);
	if ((!active_status.ok() && active_status.code() != ErrorCode::NotFound) ||
		(!staged_status.ok() && staged_status.code() != ErrorCode::NotFound) ||
		(!vault_status.ok() && vault_status.code() != ErrorCode::NotFound)) {
		// A corrupt target must never expose a partially reconstructed single bar.
		if (active_status.code() != ErrorCode::NotFound && !active_status.ok()) {
			return active_status;
		}
		if (staged_status.code() != ErrorCode::NotFound && !staged_status.ok()) {
			return staged_status;
		}
		return vault_status;
	}
	const bool has_active = active_status.ok();
	const bool has_staged = staged_status.ok();
	const bool has_vault = vault_status.ok();
	const bool layer_conflict =
		(has_active && has_staged && !SameBlockBar(active_bar, staged_bar)) ||
		(has_active && has_vault && !SameBlockBar(active_bar, vault_bar)) ||
		(has_staged && has_vault && !SameBlockBar(staged_bar, vault_bar));
	BlockBar block_bar = has_active ? active_bar : (has_staged ? staged_bar : vault_bar);
	if (!has_active && !has_staged && !has_vault) {
		return Status::Error(ErrorCode::NotFound, "history bar was not found");
	}
	if (layer_conflict) {
		return Status::Error(ErrorCode::CorruptData, "history layers contain conflicting bars");
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
	if (!status_.ok()) {
		return status_;
	}
	if (out == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "history range output is required");
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
	// Layer reads are independent. Retain successful work from every layer and
	// report corruption only after converting the merged, sorted partial result.
	std::vector<ActiveBar> vault_bars;
	std::vector<ActiveBar> staged_bars;
	std::vector<ActiveBar> active_bars;
	bool corrupt = false;
	status = vault_->range(symbol_ids, begin_time_id, end_time_id, &vault_bars);
	if (!status.ok()) {
		corrupt = true;
	}
	status = staging_->range(symbol_ids, begin_time_id, end_time_id, &staged_bars);
	if (!status.ok()) {
		corrupt = true;
	}
	status = active_->range(symbol_ids, begin_time_id, end_time_id, &active_bars);
	if (!status.ok()) {
		corrupt = true;
	}
	std::map<std::pair<TimeId, SymbolId>, ActiveBar> merged;
	for (size_t i = 0; i < vault_bars.size(); ++i) {
		merged[std::make_pair(vault_bars[i].time_id, vault_bars[i].symbol_id)] = vault_bars[i];
	}
	for (size_t i = 0; i < staged_bars.size(); ++i) {
		const std::pair<TimeId, SymbolId> key =
			std::make_pair(staged_bars[i].time_id, staged_bars[i].symbol_id);
		std::map<std::pair<TimeId, SymbolId>, ActiveBar>::iterator existing = merged.find(key);
		if (existing != merged.end() && !SameBlockBar(existing->second.bar, staged_bars[i].bar)) {
			corrupt = true;
		}
		merged[key] = staged_bars[i];
	}
	for (size_t i = 0; i < active_bars.size(); ++i) {
		const std::pair<TimeId, SymbolId> key =
			std::make_pair(active_bars[i].time_id, active_bars[i].symbol_id);
		std::map<std::pair<TimeId, SymbolId>, ActiveBar>::iterator existing = merged.find(key);
		if (existing != merged.end() && !SameBlockBar(existing->second.bar, active_bars[i].bar)) {
			corrupt = true;
		}
		merged[key] = active_bars[i];
	}
	out->clear();
	out->reserve(merged.size());
	for (std::map<std::pair<TimeId, SymbolId>, ActiveBar>::const_iterator it = merged.begin();
		 it != merged.end(); ++it) {
		const ActiveBar& active_bar = it->second;
		std::string local_time;
		status = LocalTime(calendar_, frequency_, active_bar.time_id, &local_time);
		if (!status.ok()) {
			return status;
		}
		Bar bar = {active_bar.symbol_id, frequency_, local_time,
			active_bar.bar.state, active_bar.bar.open,
			active_bar.bar.high, active_bar.bar.low,
			active_bar.bar.close, active_bar.bar.volume};
		status = actions_.adjust(active_bar.symbol_id, local_time, adjust_mode, &bar);
		if (!status.ok()) {
			return status;
		}
		out->push_back(bar);
	}
	return corrupt ? Status::Error(ErrorCode::CorruptData,
		"one or more history range blocks are corrupt") : Status::Ok();
}

Status History::flush() {
	if (!status_.ok()) {
		return status_;
	}
	Status status = active_->flush();
	if (!status.ok()) {
		return status;
	}
	return publish_manifest_ ? publish_manifest_() : Status::Ok();
}

Status History::seal_before(const std::string& local_time) {
	if (!status_.ok()) {
		return status_;
	}
	TimeId time_id = 0;
	TimeId ignored_block_id = 0;
	BlockOff ignored_block_offset = 0;
	Status status = active_->flush();
	if (!status.ok()) {
		return status;
	}
	status = ResolveTime(calendar_, frequency_, local_time,
						&time_id, &ignored_block_id, &ignored_block_offset);
	if (!status.ok()) {
		return status;
	}
	std::vector<StockTimeBlock> sealed;
	std::vector<ActiveBar> sealed_bars;
	status = active_->collect_before(time_id, &sealed, &sealed_bars);
	if (!status.ok()) {
		return status;
	}
	status = staging_->accept(sealed, sealed_bars);
	if (!status.ok()) {
		return status;
	}
	status = active_->remove_before(time_id);
	if (!status.ok()) {
		return status;
	}
	return publish_manifest_ ? publish_manifest_() : Status::Ok();
}

// =============================================================================
// Offline Vault Compaction
//
// Compaction rewrites logical block records, not their on-disk frames. The run
// files are private, short-lived transport between bounded sorting batches and
// the final merge; Vault and Staging output is always written by their native
// stores so their public persistent formats remain unchanged.
// =============================================================================

static const size_t kCompactionBatchBytes = 64U * 1024U * 1024U;

enum class CompactionDestination : uint8_t {
	Vault = 0,
	Staging = 1
};

enum class CompactionSource : uint8_t {
	Vault = 0,
	Staging = 1
};

struct CompactionRecord {
	CompactionDestination destination;
	CompactionSource source;
	StockTimeBlock block;
	std::vector<ActiveBar> bars;
};

static bool CompactionRecordOrder(const CompactionRecord& left,
							  const CompactionRecord& right) {
	if (left.destination != right.destination) {
		return static_cast<uint8_t>(left.destination) < static_cast<uint8_t>(right.destination);
	}
	if (left.block.key.symbol_id != right.block.key.symbol_id) {
		return left.block.key.symbol_id < right.block.key.symbol_id;
	}
	if (left.block.key.time_block_id != right.block.key.time_block_id) {
		return left.block.key.time_block_id < right.block.key.time_block_id;
	}
	return static_cast<uint8_t>(left.source) < static_cast<uint8_t>(right.source);
}

static Status SerializeCompactionRecord(const CompactionRecord& record,
										std::vector<uint8_t>* bytes) {
	if (bytes == NULL || record.block.key.symbol_id == kInvalidSymbolId ||
		record.block.positions.size() > std::numeric_limits<uint32_t>::max() ||
		record.bars.size() > std::numeric_limits<uint32_t>::max()) {
		return Status::Error(ErrorCode::InvalidArgument, "invalid compaction record");
	}
	bytes->clear();
	PutU8(bytes, 1);
	PutU8(bytes, static_cast<uint8_t>(record.destination));
	PutU8(bytes, static_cast<uint8_t>(record.source));
	PutU8(bytes, 0);
	PutU32(bytes, record.block.key.symbol_id);
	PutU32(bytes, record.block.key.time_block_id);
	PutU64(bytes, record.block.day_presence);
	PutU32(bytes, static_cast<uint32_t>(record.block.positions.size()));
	for (size_t i = 0; i < record.block.positions.size(); ++i) {
		const BlockBar& bar = record.block.positions[i];
		if (!ValidState(bar.state) || !ValidBar(bar)) {
			return Status::Error(ErrorCode::InvalidArgument, "invalid block bar in compaction record");
		}
		PutU8(bytes, static_cast<uint8_t>(bar.state));
		PutDouble(bytes, bar.open);
		PutDouble(bytes, bar.high);
		PutDouble(bytes, bar.low);
		PutDouble(bytes, bar.close);
		PutDouble(bytes, bar.volume);
	}
	PutU32(bytes, static_cast<uint32_t>(record.bars.size()));
	for (size_t i = 0; i < record.bars.size(); ++i) {
		const ActiveBar& bar = record.bars[i];
		if (bar.symbol_id != record.block.key.symbol_id || !ValidState(bar.bar.state) ||
			!ValidBar(bar.bar)) {
			return Status::Error(ErrorCode::InvalidArgument, "invalid explicit bar in compaction record");
		}
		PutU32(bytes, bar.time_id);
		PutU8(bytes, static_cast<uint8_t>(bar.bar.state));
		PutDouble(bytes, bar.bar.open);
		PutDouble(bytes, bar.bar.high);
		PutDouble(bytes, bar.bar.low);
		PutDouble(bytes, bar.bar.close);
		PutDouble(bytes, bar.bar.volume);
	}
	return Status::Ok();
}

static Status ParseCompactionRecord(const std::vector<uint8_t>& bytes,
									 CompactionRecord* record) {
	if (record == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "compaction record output is required");
	}
	size_t offset = 0;
	uint8_t version = 0;
	uint8_t destination = 0;
	uint8_t source = 0;
	uint8_t reserved = 0;
	uint32_t positions = 0;
	uint32_t bars = 0;
	if (!GetU8(bytes, &offset, &version) || !GetU8(bytes, &offset, &destination) ||
		!GetU8(bytes, &offset, &source) || !GetU8(bytes, &offset, &reserved) ||
		!GetU32(bytes, &offset, &record->block.key.symbol_id) ||
		!GetU32(bytes, &offset, &record->block.key.time_block_id) ||
		!GetU64(bytes, &offset, &record->block.day_presence) ||
		!GetU32(bytes, &offset, &positions) || version != 1 || reserved != 0 ||
		destination > static_cast<uint8_t>(CompactionDestination::Staging) ||
		source > static_cast<uint8_t>(CompactionSource::Staging) ||
		record->block.key.symbol_id == kInvalidSymbolId) {
		return Status::Error(ErrorCode::CorruptData, "invalid compaction run record");
	}
	record->destination = static_cast<CompactionDestination>(destination);
	record->source = static_cast<CompactionSource>(source);
	record->block.positions.assign(positions, MissingBlockBar());
	for (uint32_t i = 0; i < positions; ++i) {
		uint8_t state = 0;
		BlockBar& bar = record->block.positions[i];
		if (!GetU8(bytes, &offset, &state) || !GetDouble(bytes, &offset, &bar.open) ||
			!GetDouble(bytes, &offset, &bar.high) || !GetDouble(bytes, &offset, &bar.low) ||
			!GetDouble(bytes, &offset, &bar.close) || !GetDouble(bytes, &offset, &bar.volume)) {
			return Status::Error(ErrorCode::CorruptData, "truncated compaction block");
		}
		bar.state = static_cast<BarState>(state);
		if (!ValidState(bar.state) || !ValidBar(bar)) {
			return Status::Error(ErrorCode::CorruptData, "invalid compaction block bar");
		}
	}
	if (!GetU32(bytes, &offset, &bars)) {
		return Status::Error(ErrorCode::CorruptData, "truncated compaction presence list");
	}
	record->bars.clear();
	record->bars.reserve(bars);
	for (uint32_t i = 0; i < bars; ++i) {
		ActiveBar bar = {};
		uint8_t state = 0;
		bar.symbol_id = record->block.key.symbol_id;
		if (!GetU32(bytes, &offset, &bar.time_id) || !GetU8(bytes, &offset, &state) ||
			!GetDouble(bytes, &offset, &bar.bar.open) || !GetDouble(bytes, &offset, &bar.bar.high) ||
			!GetDouble(bytes, &offset, &bar.bar.low) || !GetDouble(bytes, &offset, &bar.bar.close) ||
			!GetDouble(bytes, &offset, &bar.bar.volume)) {
			return Status::Error(ErrorCode::CorruptData, "truncated compaction explicit bar");
		}
		bar.bar.state = static_cast<BarState>(state);
		if (!ValidState(bar.bar.state) || !ValidBar(bar.bar)) {
			return Status::Error(ErrorCode::CorruptData, "invalid compaction explicit bar");
		}
		record->bars.push_back(bar);
	}
	return offset == bytes.size() ? Status::Ok() :
		Status::Error(ErrorCode::CorruptData, "trailing compaction run bytes");
}

static Status WriteCompactionRun(const std::string& path,
								 std::vector<CompactionRecord>* records,
								 uint64_t* written_bytes) {
	std::sort(records->begin(), records->end(), CompactionRecordOrder);
	std::ofstream output(path.c_str(), std::ios::binary | std::ios::trunc);
	if (!output) {
		return Status::Error(ErrorCode::IoError, "cannot create compaction run");
	}
	for (size_t i = 0; i < records->size(); ++i) {
		std::vector<uint8_t> bytes;
		Status status = SerializeCompactionRecord((*records)[i], &bytes);
		if (!status.ok() || bytes.size() > std::numeric_limits<uint32_t>::max()) {
			return status.ok() ? Status::Error(ErrorCode::InvalidArgument, "compaction record is too large") : status;
		}
		std::vector<uint8_t> length;
		PutU32(&length, static_cast<uint32_t>(bytes.size()));
		output.write(reinterpret_cast<const char*>(&length[0]), length.size());
		output.write(reinterpret_cast<const char*>(&bytes[0]), bytes.size());
		if (!output) {
			return Status::Error(ErrorCode::IoError, "cannot write compaction run");
		}
		*written_bytes += length.size() + bytes.size();
	}
	records->clear();
	return Status::Ok();
}

struct CompactionRunReader {
	std::ifstream input;
	CompactionRecord record;
	bool has_record;
};

static Status ReadCompactionRunRecord(CompactionRunReader* reader) {
	uint8_t length_bytes[4] = {};
	reader->input.read(reinterpret_cast<char*>(length_bytes), sizeof(length_bytes));
	if (reader->input.eof() && reader->input.gcount() == 0) {
		reader->has_record = false;
		return Status::Ok();
	}
	if (reader->input.gcount() != static_cast<std::streamsize>(sizeof(length_bytes))) {
		return Status::Error(ErrorCode::CorruptData, "truncated compaction run length");
	}
	const uint32_t length = static_cast<uint32_t>(length_bytes[0]) |
		(static_cast<uint32_t>(length_bytes[1]) << 8) |
		(static_cast<uint32_t>(length_bytes[2]) << 16) |
		(static_cast<uint32_t>(length_bytes[3]) << 24);
	if (length == 0 || length > kCompactionBatchBytes) {
		return Status::Error(ErrorCode::CorruptData, "invalid compaction run length");
	}
	std::vector<uint8_t> bytes(length);
	reader->input.read(reinterpret_cast<char*>(&bytes[0]), bytes.size());
	if (reader->input.gcount() != static_cast<std::streamsize>(bytes.size())) {
		return Status::Error(ErrorCode::CorruptData, "truncated compaction run record");
	}
	Status status = ParseCompactionRecord(bytes, &reader->record);
	if (!status.ok()) {
		return status;
	}
	reader->has_record = true;
	return Status::Ok();
}

static Status BuildCompactionPart(const Calendar& calendar,
								  Frequency frequency,
								  const StockTimeBlock& source,
								  const std::vector<ActiveBar>& bars,
								  CompactionDestination destination,
								  CompactionSource origin,
								  CompactionRecord* record) {
	if (record == NULL || bars.empty()) {
		return Status::Error(ErrorCode::InvalidArgument, "compaction part requires explicit bars");
	}
	std::vector<TimeId> time_ids;
	Status status = StagingTimeIds(calendar, frequency, source.key.time_block_id,
		static_cast<BlockOff>(source.positions.size()), &time_ids);
	if (!status.ok()) {
		return status;
	}
	*record = {};
	record->destination = destination;
	record->source = origin;
	record->block.key = source.key;
	record->block.positions.assign(source.positions.size(), MissingBlockBar());
	std::vector<bool> present(source.positions.size(), false);
	for (size_t i = 0; i < bars.size(); ++i) {
		if (bars[i].symbol_id != source.key.symbol_id) {
			return Status::Error(ErrorCode::CorruptData, "bar belongs to another compaction block");
		}
		std::vector<TimeId>::const_iterator position = std::lower_bound(time_ids.begin(),
			time_ids.end(), bars[i].time_id);
		if (position == time_ids.end() || *position != bars[i].time_id) {
			return Status::Error(ErrorCode::CorruptData, "bar does not fit compaction block");
		}
		const size_t offset = static_cast<size_t>(position - time_ids.begin());
		if (present[offset]) {
			return Status::Error(ErrorCode::Conflict, "duplicate bar in compaction block");
		}
		present[offset] = true;
		record->block.positions[offset] = bars[i].bar;
		record->block.day_presence |= static_cast<uint64_t>(1) <<
			(time_day(bars[i].time_id) - time_day(source.key.time_block_id));
		record->bars.push_back(bars[i]);
	}
	return Status::Ok();
}

static uint64_t FileBytes(const std::string& path) {
	struct stat metadata = {};
	return stat(path.c_str(), &metadata) == 0 && metadata.st_size > 0 ?
		static_cast<uint64_t>(metadata.st_size) : 0;
}

static bool IsStoreFile(const std::string& name, CompactionDestination destination) {
	if (destination == CompactionDestination::Vault) {
		return name == "vault-index" || name == "vault-index.tmp" ||
			name.compare(0, 6, "vault-") == 0;
	}
	return name == "staging-index" || name.compare(0, 14, "staging-pages-") == 0;
}

static Status StoreFiles(const std::string& directory,
					 CompactionDestination destination,
					 std::vector<std::string>* files) {
	files->clear();
	DIR* handle = opendir(directory.c_str());
	if (handle == NULL) {
		return errno == ENOENT ? Status::Ok() :
			Status::Error(ErrorCode::IoError, "cannot inspect store directory");
	}
	for (dirent* entry = readdir(handle); entry != NULL; entry = readdir(handle)) {
		const std::string name(entry->d_name);
		if (IsStoreFile(name, destination)) {
			files->push_back(name);
		}
	}
	closedir(handle);
	std::sort(files->begin(), files->end());
	return Status::Ok();
}

static void RemoveCompactionTree(const std::string& directory) {
	DIR* handle = opendir(directory.c_str());
	if (handle == NULL) {
		return;
	}
	for (dirent* entry = readdir(handle); entry != NULL; entry = readdir(handle)) {
		const std::string name(entry->d_name);
		if (name == "." || name == "..") {
			continue;
		}
		const std::string path = directory + "/" + name;
		struct stat metadata = {};
		if (stat(path.c_str(), &metadata) == 0 && S_ISDIR(metadata.st_mode)) {
			RemoveCompactionTree(path);
		} else {
			unlink(path.c_str());
		}
	}
	closedir(handle);
	rmdir(directory.c_str());
}

static Status PublishCompactedStore(const std::string& frequency_path,
									 const std::string& build_path,
									 const std::string& backup_path,
									 CompactionDestination destination) {
	if (mkdir(backup_path.c_str(), 0755) != 0) {
		return Status::Error(ErrorCode::IoError, "cannot create store backup directory");
	}
	std::vector<std::string> old_files;
	std::vector<std::string> new_files;
	Status status = StoreFiles(frequency_path, destination, &old_files);
	if (!status.ok()) {
		return status;
	}
	status = StoreFiles(build_path, destination, &new_files);
	if (!status.ok()) {
		return status;
	}
	std::vector<std::string> moved_old;
	for (size_t i = 0; i < old_files.size(); ++i) {
		if (rename((frequency_path + "/" + old_files[i]).c_str(),
			(backup_path + "/" + old_files[i]).c_str()) != 0) {
			for (size_t rollback = moved_old.size(); rollback > 0; --rollback) {
				const std::string& name = moved_old[rollback - 1];
				rename((backup_path + "/" + name).c_str(), (frequency_path + "/" + name).c_str());
			}
			return Status::Error(ErrorCode::IoError, "cannot preserve old store file");
		}
		moved_old.push_back(old_files[i]);
	}
	std::vector<std::string> moved_new;
	for (size_t i = 0; i < new_files.size(); ++i) {
		if (rename((build_path + "/" + new_files[i]).c_str(),
			(frequency_path + "/" + new_files[i]).c_str()) != 0) {
			for (size_t rollback = moved_new.size(); rollback > 0; --rollback) {
				const std::string& name = moved_new[rollback - 1];
				rename((frequency_path + "/" + name).c_str(), (build_path + "/" + name).c_str());
			}
			for (size_t rollback = moved_old.size(); rollback > 0; --rollback) {
				const std::string& name = moved_old[rollback - 1];
				rename((backup_path + "/" + name).c_str(), (frequency_path + "/" + name).c_str());
			}
			return Status::Error(ErrorCode::IoError, "cannot publish compacted store file");
		}
		moved_new.push_back(new_files[i]);
	}
	return Status::Ok();
}

// A compaction publishes Vault and Staging as one logical operation. If the
// second publication fails, move the new Vault files back to the build tree
// and restore the preserved files before returning the error.
static Status RestoreCompactedStore(const std::string& frequency_path,
									const std::string& build_path,
									const std::string& backup_path,
									CompactionDestination destination) {
	std::vector<std::string> current_files;
	Status status = StoreFiles(frequency_path, destination, &current_files);
	if (!status.ok()) {
		return status;
	}
	for (size_t i = 0; i < current_files.size(); ++i) {
		if (rename((frequency_path + "/" + current_files[i]).c_str(),
			(build_path + "/" + current_files[i]).c_str()) != 0) {
			return Status::Error(ErrorCode::IoError, "cannot stage compacted store rollback");
		}
	}
	std::vector<std::string> backup_files;
	status = StoreFiles(backup_path, destination, &backup_files);
	if (!status.ok()) {
		return status;
	}
	for (size_t i = 0; i < backup_files.size(); ++i) {
		if (rename((backup_path + "/" + backup_files[i]).c_str(),
			(frequency_path + "/" + backup_files[i]).c_str()) != 0) {
			return Status::Error(ErrorCode::IoError, "cannot restore compacted store backup");
		}
	}
	return Status::Ok();
}

Status CompactVault(const std::string& root_path,
					const std::string& market_name,
					Frequency frequency,
					const std::string& cutoff_local_time,
					VaultCompactionStats* stats) {
	if (stats == NULL || root_path.empty() || market_name.empty()) {
		return Status::Error(ErrorCode::InvalidArgument, "root path, market name, and compaction stats are required");
	}
	*stats = {};
	const std::chrono::steady_clock::time_point started = std::chrono::steady_clock::now();
	// Compaction changes two immutable stores as one offline operation. Opening
	// the configured root verifies the published input generation; sync below
	// exposes replacement files only after both store publications complete.
	Markets markets(root_path);
	if (!markets.status().ok()) {
		return markets.status();
	}
	Market* market = NULL;
	Status status = markets.get(market_name, &market);
	if (!status.ok()) {
		return status;
	}
	Calendar calendar(market->type());
	TimeId cutoff = 0;
	TimeId unused_block = 0;
	BlockOff unused_offset = 0;
	status = ResolveTime(calendar, frequency, cutoff_local_time, &cutoff, &unused_block, &unused_offset);
	if (!status.ok()) {
		return status;
	}
	const std::string frequency_path = FrequencyPath(market->path(), frequency);
	if (frequency_path.empty()) {
		return Status::Error(ErrorCode::IoError, "cannot create frequency directory");
	}
	StagingStore old_staging(frequency, calendar, frequency_path);
	VaultStore old_vault(frequency, calendar, frequency_path, NextRuntimeMarketId());
	std::vector<StockTimeBlock> staging_blocks;
	std::vector<ActiveBar> staging_bars;
	std::vector<StockTimeBlock> vault_blocks;
	std::vector<ActiveBar> vault_bars;
	status = old_staging.snapshot(&staging_blocks, &staging_bars);
	if (!status.ok()) {
		return status;
	}
	status = old_vault.snapshot(&vault_blocks, &vault_bars);
	if (!status.ok()) {
		return status;
	}
	stats->input_blocks = staging_blocks.size() + vault_blocks.size();
	std::map<std::pair<SymbolId, TimeId>, std::vector<ActiveBar> > staging_by_block;
	std::map<std::pair<SymbolId, TimeId>, std::vector<ActiveBar> > vault_by_block;
	for (size_t i = 0; i < staging_bars.size(); ++i) {
		const TimeId block_id = CompactionBlockId(frequency, staging_bars[i].time_id);
		staging_by_block[std::make_pair(staging_bars[i].symbol_id, block_id)].push_back(staging_bars[i]);
	}
	for (size_t i = 0; i < vault_bars.size(); ++i) {
		const TimeId block_id = CompactionBlockId(frequency, vault_bars[i].time_id);
		vault_by_block[std::make_pair(vault_bars[i].symbol_id, block_id)].push_back(vault_bars[i]);
	}
	const uint64_t token = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
		std::chrono::system_clock::now().time_since_epoch()).count());
	const std::string build_root = frequency_path + "/makevault.build." + std::to_string(token);
	if (mkdir(build_root.c_str(), 0755) != 0) {
		return Status::Error(ErrorCode::IoError, "cannot create compaction build directory");
	}
	const std::string vault_build = build_root + "/vault";
	const std::string staging_build = build_root + "/staging";
	if (mkdir(vault_build.c_str(), 0755) != 0 || mkdir(staging_build.c_str(), 0755) != 0) {
		RemoveCompactionTree(build_root);
		return Status::Error(ErrorCode::IoError, "cannot create compacted store directories");
	}
	std::vector<CompactionRecord> batch;
	std::vector<std::string> runs;
	size_t batch_bytes = 0;
	uint32_t run_id = 0;
	const auto append_record = [&](const CompactionRecord& record) -> Status {
		std::vector<uint8_t> bytes;
		Status append_status = SerializeCompactionRecord(record, &bytes);
		if (!append_status.ok()) {
			return append_status;
		}
		const size_t record_bytes = bytes.size() + sizeof(uint32_t);
		if (record_bytes > kCompactionBatchBytes) {
			return Status::Error(ErrorCode::InvalidArgument, "compaction record exceeds 64 MiB batch limit");
		}
		if (!batch.empty() && batch_bytes + record_bytes > kCompactionBatchBytes) {
			const std::string run = build_root + "/run-" + std::to_string(run_id++) + ".bin";
			Status flush_status = WriteCompactionRun(run, &batch, &stats->temporary_bytes);
			if (!flush_status.ok()) {
				return flush_status;
			}
			runs.push_back(run);
			batch_bytes = 0;
		}
		batch.push_back(record);
		batch_bytes += record_bytes;
		return Status::Ok();
	};
	for (size_t i = 0; i < vault_blocks.size(); ++i) {
		const std::pair<SymbolId, TimeId> key(vault_blocks[i].key.symbol_id, vault_blocks[i].key.time_block_id);
		std::map<std::pair<SymbolId, TimeId>, std::vector<ActiveBar> >::const_iterator bars = vault_by_block.find(key);
		if (bars == vault_by_block.end()) {
			continue;
		}
		CompactionRecord record;
		status = BuildCompactionPart(calendar, frequency, vault_blocks[i], bars->second,
			CompactionDestination::Vault, CompactionSource::Vault, &record);
		if (!status.ok() || !(status = append_record(record)).ok()) {
			RemoveCompactionTree(build_root);
			return status;
		}
	}
	for (size_t i = 0; i < staging_blocks.size(); ++i) {
		const std::pair<SymbolId, TimeId> key(staging_blocks[i].key.symbol_id, staging_blocks[i].key.time_block_id);
		std::map<std::pair<SymbolId, TimeId>, std::vector<ActiveBar> >::const_iterator bars = staging_by_block.find(key);
		if (bars == staging_by_block.end()) {
			continue;
		}
		std::vector<ActiveBar> old_bars;
		std::vector<ActiveBar> new_bars;
		for (size_t j = 0; j < bars->second.size(); ++j) {
			(bars->second[j].time_id < cutoff ? old_bars : new_bars).push_back(bars->second[j]);
		}
		if (!old_bars.empty()) {
			CompactionRecord record;
			status = BuildCompactionPart(calendar, frequency, staging_blocks[i], old_bars,
				CompactionDestination::Vault, CompactionSource::Staging, &record);
			if (!status.ok() || !(status = append_record(record)).ok()) {
				RemoveCompactionTree(build_root);
				return status;
			}
		}
		if (!new_bars.empty()) {
			CompactionRecord record;
			status = BuildCompactionPart(calendar, frequency, staging_blocks[i], new_bars,
				CompactionDestination::Staging, CompactionSource::Staging, &record);
			if (!status.ok() || !(status = append_record(record)).ok()) {
				RemoveCompactionTree(build_root);
				return status;
			}
		}
	}
	if (!batch.empty()) {
		const std::string run = build_root + "/run-" + std::to_string(run_id++) + ".bin";
		status = WriteCompactionRun(run, &batch, &stats->temporary_bytes);
		if (!status.ok()) {
			RemoveCompactionTree(build_root);
			return status;
		}
		runs.push_back(run);
	}
	VaultStore new_vault(frequency, calendar, vault_build, NextRuntimeMarketId());
	StagingStore new_staging(frequency, calendar, staging_build);
	std::vector<CompactionRunReader> readers(runs.size());
	struct EarlierRun {
		const std::vector<CompactionRunReader>* readers;
		bool operator()(size_t left, size_t right) const {
			return CompactionRecordOrder((*readers)[right].record, (*readers)[left].record);
		}
	};
	EarlierRun earlier = {&readers};
	std::priority_queue<size_t, std::vector<size_t>, EarlierRun> heap(earlier);
	for (size_t i = 0; i < runs.size(); ++i) {
		readers[i].input.open(runs[i].c_str(), std::ios::binary);
		if (!readers[i].input || !(status = ReadCompactionRunRecord(&readers[i])).ok()) {
			RemoveCompactionTree(build_root);
			return status.ok() ? Status::Error(ErrorCode::IoError, "cannot read compaction run") : status;
		}
		if (readers[i].has_record) {
			heap.push(i);
		}
	}
	std::vector<StockTimeBlock> vault_batch;
	std::vector<ActiveBar> vault_batch_bars;
	std::vector<StockTimeBlock> staging_batch;
	std::vector<ActiveBar> staging_batch_bars;
	const auto flush_output = [&](CompactionDestination destination) -> Status {
		if (destination == CompactionDestination::Vault) {
			if (vault_batch.empty()) {
				return Status::Ok();
			}
			Status flush_status = new_vault.ingest(vault_batch, vault_batch_bars);
			vault_batch.clear();
			vault_batch_bars.clear();
			return flush_status;
		}
		if (staging_batch.empty()) {
			return Status::Ok();
		}
		Status flush_status = new_staging.accept(staging_batch, staging_batch_bars);
		staging_batch.clear();
		staging_batch_bars.clear();
		return flush_status;
	};
	CompactionDestination last_destination = CompactionDestination::Vault;
	bool have_destination = false;
	while (!heap.empty()) {
		const CompactionRecord first = readers[heap.top()].record;
		const CompactionDestination destination = first.destination;
		const SymbolId symbol_id = first.block.key.symbol_id;
		const TimeId block_id = first.block.key.time_block_id;
		bool have_vault = false;
		bool have_staging = false;
		CompactionRecord chosen;
		while (!heap.empty()) {
			const size_t index = heap.top();
			const CompactionRecord& candidate = readers[index].record;
			if (candidate.destination != destination || candidate.block.key.symbol_id != symbol_id ||
				candidate.block.key.time_block_id != block_id) {
				break;
			}
			heap.pop();
			if (candidate.source == CompactionSource::Vault) {
				if (have_vault) {
					RemoveCompactionTree(build_root);
					return Status::Error(ErrorCode::Conflict, "conflicting duplicate Vault block");
				}
				have_vault = true;
				chosen = candidate;
			} else {
				if (have_staging) {
					RemoveCompactionTree(build_root);
					return Status::Error(ErrorCode::Conflict, "conflicting duplicate Staging block");
				}
				have_staging = true;
				chosen = candidate;
			}
			status = ReadCompactionRunRecord(&readers[index]);
			if (!status.ok()) {
				RemoveCompactionTree(build_root);
				return status;
			}
			if (readers[index].has_record) {
				heap.push(index);
			}
		}
		if (have_destination && destination != last_destination) {
			status = flush_output(last_destination);
			if (!status.ok()) {
				RemoveCompactionTree(build_root);
				return status;
			}
		}
		have_destination = true;
		last_destination = destination;
		if (destination == CompactionDestination::Vault) {
			vault_batch.push_back(chosen.block);
			vault_batch_bars.insert(vault_batch_bars.end(), chosen.bars.begin(), chosen.bars.end());
		} else {
			staging_batch.push_back(chosen.block);
			staging_batch_bars.insert(staging_batch_bars.end(), chosen.bars.begin(), chosen.bars.end());
		}
		++stats->output_blocks;
	}
	if (have_destination && !(status = flush_output(last_destination)).ok()) {
		RemoveCompactionTree(build_root);
		return status;
	}
	std::vector<std::string> files;
	status = StoreFiles(frequency_path, CompactionDestination::Vault, &files);
	if (!status.ok()) {
		RemoveCompactionTree(build_root);
		return status;
	}
	for (size_t i = 0; i < files.size(); ++i) {
		stats->io_bytes += FileBytes(frequency_path + "/" + files[i]);
	}
	status = StoreFiles(frequency_path, CompactionDestination::Staging, &files);
	if (!status.ok()) {
		RemoveCompactionTree(build_root);
		return status;
	}
	for (size_t i = 0; i < files.size(); ++i) {
		stats->io_bytes += FileBytes(frequency_path + "/" + files[i]);
	}
	stats->io_bytes += stats->temporary_bytes * 2;
	status = StoreFiles(vault_build, CompactionDestination::Vault, &files);
	if (!status.ok()) {
		RemoveCompactionTree(build_root);
		return status;
	}
	for (size_t i = 0; i < files.size(); ++i) {
		stats->io_bytes += FileBytes(vault_build + "/" + files[i]);
	}
	status = StoreFiles(staging_build, CompactionDestination::Staging, &files);
	if (!status.ok()) {
		RemoveCompactionTree(build_root);
		return status;
	}
	for (size_t i = 0; i < files.size(); ++i) {
		stats->io_bytes += FileBytes(staging_build + "/" + files[i]);
	}
	status = PublishCompactedStore(frequency_path, vault_build,
		frequency_path + "/vault." + std::to_string(token), CompactionDestination::Vault);
	const bool vault_published = status.ok();
	if (vault_published) {
		status = PublishCompactedStore(frequency_path, staging_build,
			frequency_path + "/staging." + std::to_string(token), CompactionDestination::Staging);
		if (!status.ok()) {
			const Status rollback_status = RestoreCompactedStore(frequency_path, vault_build,
				frequency_path + "/vault." + std::to_string(token), CompactionDestination::Vault);
			if (!rollback_status.ok()) {
				status = Status::Error(ErrorCode::IoError, "compaction publication and rollback both failed");
			}
		}
	}
	RemoveCompactionTree(build_root);
	if (!status.ok()) {
		return status;
	}
	status = market->sync();
	if (!status.ok()) {
		return status;
	}
	stats->elapsed_milliseconds = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
		std::chrono::steady_clock::now() - started).count());
	return Status::Ok();
}

}  // namespace zstfs
