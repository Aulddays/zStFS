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
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>

#include "serialization.h"
#include "zstfs/market.h"

namespace zstfs {

namespace {

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

}  // namespace

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
// History Store Lifecycle Shells
//
// ActiveStore, StagingStore and VaultStore are still lifecycle shells. Public
// History writes and reads remain unimplemented until the next storage layer
// milestones connect them to the block codec above.
// =============================================================================

ActiveStore::ActiveStore(Frequency frequency)
	: frequency_(frequency) {
}

StagingStore::StagingStore(Frequency frequency)
	: frequency_(frequency) {
}

VaultStore::VaultStore(Frequency frequency)
	: frequency_(frequency) {
}

History::History(Frequency frequency)
	: frequency_(frequency),
	  active_(new ActiveStore(frequency)),
	  staging_(new StagingStore(frequency)),
	  vault_(new VaultStore(frequency)) {
}

History::~History() {
}

Frequency History::frequency() const {
	return frequency_;
}

Status History::put(const Bar& bar) {
	(void)bar;
	return Status::Error(ErrorCode::NotImplemented, "history writes are not implemented");
}

Status History::put(const std::vector<Bar>& bars) {
	(void)bars;
	return Status::Error(ErrorCode::NotImplemented, "history writes are not implemented");
}

Status History::get(SymbolId symbol_id,
		    const std::string& local_time,
		    Bar* out) const {
	(void)symbol_id;
	(void)local_time;
	(void)out;
	return Status::Error(ErrorCode::NotImplemented, "history reads are not implemented");
}

Status History::get(SymbolId symbol_id,
		    const std::string& begin,
		    const std::string& end,
		    AdjustMode adjust_mode,
		    std::vector<Bar>* out) const {
	(void)symbol_id;
	(void)begin;
	(void)end;
	(void)adjust_mode;
	(void)out;
	return Status::Error(ErrorCode::NotImplemented, "history reads are not implemented");
}

Status History::get(const std::vector<SymbolId>& symbol_ids,
		    const std::string& begin,
		    const std::string& end,
		    AdjustMode adjust_mode,
		    std::vector<Bar>* out) const {
	(void)symbol_ids;
	(void)begin;
	(void)end;
	(void)adjust_mode;
	(void)out;
	return Status::Error(ErrorCode::NotImplemented, "history reads are not implemented");
}

Status History::flush() {
	return Status::Error(ErrorCode::NotImplemented, "history flush is not implemented");
}

Status History::seal_before(const std::string& local_time) {
	(void)local_time;
	return Status::Error(ErrorCode::NotImplemented, "history sealing is not implemented");
}

}  // namespace zstfs
