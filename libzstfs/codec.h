// codec.h
//
// Declares the private M3 frame types and codec operations. Numeric frames are
// self-describing so callers can encode or decode a short field run independently.

#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "zstfs/data.h"
#include "zstfs/status.h"
#include "calendar.h"

namespace zstfs {

// State frames use the same historical RLE wire id as numeric residual frames.
// Keeping this value named in the codec boundary prevents state orchestration
// from depending on an undocumented numeric literal.
static const uint8_t kStateRleCodecId = 1;
static const size_t kMaxFrameSamples = 32;

// MicroblockFrame is the independently decodable encoded part of a field.
// Its header carries the field, location, predictor, quantizer, and anchor.
struct MicroblockFrame {
	FieldId field;
	BlockOff first_offset;
	BlockOff sample_count;
	uint8_t codec_id;
	uint8_t predictor_id;
	uint8_t quantizer_id;
	std::vector<uint8_t> quantizer_parameters;
	int64_t anchor;
	std::vector<uint8_t> payload;
};

// FrameInput is one contiguous run of normal values for one numeric field.
struct FrameInput {
	FieldId field;
	BlockOff first_offset;
	std::vector<double> values;
};

// Precision validation is shared by OHLCV orchestration and numeric encoding.
bool ValidPrecisionProfile(const PrecisionProfile& profile);

// EncodeFrame quantizes and residual-encodes one normal field run.
Status EncodeFrame(const FrameInput& input,
			   const PrecisionProfile& profile,
			   MicroblockFrame* output);

// DecodeFrame validates a frame and reconstructs its numeric values.
Status DecodeFrame(const MicroblockFrame& frame,
			   const PrecisionProfile& profile,
			   std::vector<double>* values);

// Frame wire format is little-endian and length-delimited. The enclosing page
// or blob remains responsible for integrity checks such as checksums.
Status SerializeFrame(const MicroblockFrame& frame, std::vector<uint8_t>* bytes);
Status ParseFrame(const std::vector<uint8_t>& bytes, MicroblockFrame* frame);

}  // namespace zstfs
