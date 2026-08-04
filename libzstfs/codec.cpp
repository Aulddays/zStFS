// codec.cpp
//
// Implements the private numeric MicroblockFrame codec used by History. One
// frame contains one contiguous run of one OHLCV field:
//
//     source values -> frame-local integer levels -> anchor and residuals
//     -> smallest residual payload -> ZMF3 header, parameters, and payload
//
// Price frames select a decimal 1/2/5 step and volume frames select a logarithmic
// step. Both parameters are stored in the frame, so reconstruction does not
// need a preceding frame. History owns the surrounding block policy: state
// positions, invalid-bar handling, and OHLCV field assembly stay
// in history.cpp rather than becoming codec concerns.

#include "codec.h"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>

#include "serialization.h"

namespace zstfs {

namespace {

// =============================================================================
// Frame Format Contract and Validation
//
// Defines the identifiers carried by ZMF3 and the structural rules shared by
// frame construction and parsing. These values are persisted format contract.
// =============================================================================

// ZMF3 fixed header version. A reader rejects any version it does not know.
static const uint8_t kFrameVersion = 1;

// Codec IDs select the residual payload syntax after the frame anchor.
static const uint8_t kCodecRle = kStateRleCodecId;
static const uint8_t kCodecUleb = 2;
static const uint8_t kCodecRaw = 3;
static const uint8_t kCodecPacked = 4;
static const uint8_t kCodecGroupVarint = 5;
static const uint8_t kCodecRice = 6;

// Predictor and quantizer IDs describe how numeric anchors and residuals are
// reconstructed. Their values are persisted in every numeric frame header.
static const uint8_t kPredictPrevious = 1;
static const uint8_t kQuantizerPrice = 1;
static const uint8_t kQuantizerVolume = 2;

// The ZMF3 fixed header precedes variable quantizer parameters and payload.
static const size_t kFrameHeaderSize = 27;

static bool IsNumericField(FieldId field) {
	return field != FieldId::State &&
		field <= FieldId::Volume;
}

static bool ValidState(BarState state) {
	return state >= BarState::Normal && state <= BarState::Missing;
}

static bool ValidProfile(const PrecisionProfile& profile) {
	return std::isfinite(profile.price_relative_epsilon) &&
		profile.price_relative_epsilon > 0.0 &&
		std::isfinite(profile.volume_relative_epsilon) &&
		profile.volume_relative_epsilon > 0.0 &&
		profile.volume_relative_epsilon < 1.0;
}

static bool AddInt64(int64_t left, int64_t right, int64_t* result) {
	if ((right > 0 && left > std::numeric_limits<int64_t>::max() - right) ||
		(right < 0 && left < std::numeric_limits<int64_t>::min() - right)) {
		return false;
	}
	*result = left + right;
	return true;
}

static bool DifferenceInt64(int64_t current,
			    int64_t previous,
			    int64_t* result) {
	if (current >= 0 && previous < 0 &&
		current > std::numeric_limits<int64_t>::max() + previous) {
		return false;
	}
	if (current < 0 && previous >= 0 &&
		current < std::numeric_limits<int64_t>::min() + previous) {
		return false;
	}
	*result = current - previous;
	return true;
}

// =============================================================================
// Frame-Local Quantization
//
// Turns finite field values into deterministic integer levels. Quantizer
// parameters travel with the frame, so the decoding contract is self-contained.
// =============================================================================

// Price frames use a frame-local decimal 1/2/5 step derived from their
// smallest positive value. The step is stored as decimal parameters rather
// than as an IEEE floating-point byte sequence.
static int DecimalExponent(double value) {
	return static_cast<int>(std::floor(std::log10(value)));
}

static bool ChoosePriceStep(double minimum,
			    double relative_epsilon,
			    uint8_t* mantissa,
			    int32_t* exponent,
			    double* step) {
	double limit = 2.0 * relative_epsilon * minimum;
	if (!std::isfinite(limit) || limit <= 0.0) {
		return false;
	}
	int exponent_value = DecimalExponent(limit);
	for (;;) {
		double scale = std::pow(10.0, static_cast<double>(exponent_value));
		if (std::isfinite(scale) && scale > 0.0) {
			const uint8_t candidates[] = {5, 2, 1};
			for (size_t i = 0; i < 3; ++i) {
				double candidate = candidates[i] * scale;
				if (candidate <= limit && std::isfinite(candidate)) {
					*mantissa = candidates[i];
					*exponent = exponent_value;
					*step = candidate;
					return true;
				}
			}
		}
		--exponent_value;
		if (exponent_value < -10000) {
			return false;
		}
	}
}

static bool QuantizePrice(double value, double step, int64_t* output) {
	long double scaled = static_cast<long double>(value) /
		static_cast<long double>(step);
	if (!std::isfinite(static_cast<double>(scaled)) ||
		scaled > static_cast<long double>(std::numeric_limits<int64_t>::max()) ||
		scaled < static_cast<long double>(std::numeric_limits<int64_t>::min())) {
		return false;
	}
	long double rounded = std::floor(scaled + 0.5L);
	if (rounded > static_cast<long double>(std::numeric_limits<int64_t>::max()) ||
		rounded < static_cast<long double>(std::numeric_limits<int64_t>::min())) {
		return false;
	}
	*output = static_cast<int64_t>(rounded);
	return true;
}

static void MakePriceParameters(uint8_t mantissa,
				int32_t exponent,
				std::vector<uint8_t>* parameters) {
	parameters->clear();
	PutU8(parameters, kQuantizerPrice);
	PutU8(parameters, mantissa);
	PutI32(parameters, exponent);
}

static bool ReadPriceParameters(const std::vector<uint8_t>& parameters,
				uint8_t* mantissa,
				int32_t* exponent,
				double* step) {
	if (parameters.size() != 6 || parameters[0] != kQuantizerPrice ||
			(parameters[1] != 1 && parameters[1] != 2 && parameters[1] != 5)) {
		return false;
	}
	size_t cursor = 2;
	if (!GetI32(parameters, &cursor, exponent)) {
		return false;
	}
	*mantissa = parameters[1];
	*step = *mantissa * std::pow(10.0, static_cast<double>(*exponent));
	return std::isfinite(*step) && *step > 0.0;
}

// Volume frames reserve level zero for an exact zero volume. Positive values
// use logarithmic levels; the persisted integer log step makes each frame
// independently decodable without reading market configuration.
static bool MakeVolumeParameters(double epsilon,
				 std::vector<uint8_t>* parameters,
				 double* log_step) {
	*log_step = 2.0 * std::log1p(epsilon);
	uint64_t nanos = static_cast<uint64_t>(std::floor(*log_step * 1000000000.0 + 0.5));
	if (nanos == 0 || nanos > std::numeric_limits<uint32_t>::max()) {
		return false;
	}
	*log_step = static_cast<double>(nanos) / 1000000000.0;
	parameters->clear();
	PutU8(parameters, kQuantizerVolume);
	PutU32(parameters, static_cast<uint32_t>(nanos));
	return true;
}

static bool ReadVolumeParameters(const std::vector<uint8_t>& parameters,
				 double* log_step) {
	if (parameters.size() != 5 || parameters[0] != kQuantizerVolume) {
		return false;
	}
	size_t cursor = 1;
	uint32_t nanos = 0;
	if (!GetU32(parameters, &cursor, &nanos) || nanos == 0) {
		return false;
	}
	*log_step = static_cast<double>(nanos) / 1000000000.0;
	return true;
}

static bool QuantizeVolume(double value, double log_step, int64_t* output) {
	if (value == 0.0) {
		*output = 0;
		return true;
	}
	long double level = std::log(static_cast<long double>(value)) /
		static_cast<long double>(log_step);
	level = std::floor(level + 0.5L);
	if (level < static_cast<long double>(std::numeric_limits<int64_t>::min() + 1) ||
		level > static_cast<long double>(std::numeric_limits<int64_t>::max() - 1)) {
		return false;
	}
	*output = static_cast<int64_t>(level) + 1;
	return true;
}

// =============================================================================
// Residual Payload Codecs
//
// Stores the first integer level as an anchor and encodes later levels as
// ZigZag-mapped deltas. Each codec below defines one complete payload syntax;
// selection compares complete payload sizes and never changes reconstruction.
// =============================================================================

// Packed values begin with their common bit width and are then written
// least-significant-bit first.
static void EncodePacked(const std::vector<int64_t>& residuals,
				 uint8_t bit_width,
				 std::vector<uint8_t>* payload) {
	uint64_t bit_count = static_cast<uint64_t>(residuals.size()) * bit_width;
	payload->assign(1 + static_cast<size_t>((bit_count + 7) / 8), 0);
	(*payload)[0] = bit_width;
	uint64_t bit_offset = 0;
	for (size_t i = 0; i < residuals.size(); ++i) {
		uint64_t value = ZigZagEncode(residuals[i]);
		for (uint8_t bit = 0; bit < bit_width; ++bit) {
			if ((value & (static_cast<uint64_t>(1) << bit)) != 0) {
				size_t byte = 1 + static_cast<size_t>(bit_offset / 8);
				uint8_t mask = static_cast<uint8_t>(1u << (bit_offset % 8));
				(*payload)[byte] |= mask;
			}
			++bit_offset;
		}
	}
}

static bool DecodePacked(const std::vector<uint8_t>& payload,
				 size_t count,
				 std::vector<int64_t>* residuals) {
	if (payload.empty()) {
		return false;
	}
	uint8_t bit_width = payload[0];
	uint64_t bit_count = static_cast<uint64_t>(count) * bit_width;
	if (payload.size() != 1 + static_cast<size_t>((bit_count + 7) / 8) ||
		bit_width > 64) {
		return false;
	}
	residuals->clear();
	residuals->reserve(count);
	uint64_t bit_offset = 0;
	for (size_t i = 0; i < count; ++i) {
		uint64_t value = 0;
		for (uint8_t bit = 0; bit < bit_width; ++bit) {
			size_t byte = 1 + static_cast<size_t>(bit_offset / 8);
			if ((payload[byte] & (static_cast<uint8_t>(1u << (bit_offset % 8)))) != 0) {
				value |= static_cast<uint64_t>(1) << bit;
			}
			++bit_offset;
		}
		int64_t decoded = 0;
		if (!ZigZagDecode(value, &decoded)) {
			return false;
		}
		residuals->push_back(decoded);
	}
	return true;
}

// Group-varint stores a u16 control word for up to four values. Each three-bit
// control field contains that value's little-endian byte width minus one.
static void EncodeGroupVarint(const std::vector<int64_t>& residuals,
				       std::vector<uint8_t>* payload) {
	payload->clear();
	for (size_t begin = 0; begin < residuals.size(); begin += 4) {
		size_t count = std::min(static_cast<size_t>(4), residuals.size() - begin);
		uint16_t control = 0;
		uint64_t values[4] = {};
		uint8_t widths[4] = {};
		for (size_t i = 0; i < count; ++i) {
			values[i] = ZigZagEncode(residuals[begin + i]);
			widths[i] = 1;
			while (widths[i] < 8 && (values[i] >> (widths[i] * 8)) != 0) {
				++widths[i];
			}
			control |= static_cast<uint16_t>(widths[i] - 1) << (i * 3);
		}
		PutU16(payload, control);
		for (size_t i = 0; i < count; ++i) {
			for (uint8_t byte = 0; byte < widths[i]; ++byte) {
				PutU8(payload, static_cast<uint8_t>(values[i] >> (byte * 8)));
			}
		}
	}
}

static bool DecodeGroupVarint(const std::vector<uint8_t>& payload,
				      size_t count,
				      std::vector<int64_t>* residuals) {
	residuals->clear();
	residuals->reserve(count);
	size_t cursor = 0;
	while (residuals->size() < count) {
		uint16_t control = 0;
		size_t group_count = std::min(static_cast<size_t>(4), count - residuals->size());
		if (!GetU16(payload, &cursor, &control) ||
			(control >> (group_count * 3)) != 0) {
			return false;
		}
		for (size_t i = 0; i < group_count; ++i) {
			uint8_t width = static_cast<uint8_t>((control >> (i * 3)) & 0x7) + 1;
			uint64_t value = 0;
			if (width > payload.size() - cursor) {
				return false;
			}
			for (uint8_t byte = 0; byte < width; ++byte) {
				value |= static_cast<uint64_t>(payload[cursor++]) << (byte * 8);
			}
			int64_t decoded = 0;
			if (!ZigZagDecode(value, &decoded)) {
				return false;
			}
			residuals->push_back(decoded);
		}
	}
	return cursor == payload.size();
}

// Rice stores its parameter in the first byte, followed by unary quotients and
// fixed-width remainders. These two helpers are shared by its matching encoder
// and decoder so both use the same least-significant-bit-first layout.
static void PutBit(std::vector<uint8_t>* payload, uint64_t bit_offset, bool value) {
	if (bit_offset / 8 >= payload->size()) {
		payload->resize(static_cast<size_t>(bit_offset / 8 + 1), 0);
	}
	if (value) {
		(*payload)[static_cast<size_t>(bit_offset / 8)] |=
			static_cast<uint8_t>(1u << (bit_offset % 8));
	}
}

static bool GetBit(const std::vector<uint8_t>& payload,
			   uint64_t bit_offset,
			   bool* value) {
	if (bit_offset / 8 >= payload.size()) {
		return false;
	}
	*value = (payload[static_cast<size_t>(bit_offset / 8)] &
		static_cast<uint8_t>(1u << (bit_offset % 8))) != 0;
	return true;
}

static bool EncodeRice(const std::vector<int64_t>& residuals,
			       uint8_t parameter,
			       std::vector<uint8_t>* payload) {
	uint64_t bit_count = 0;
	for (size_t i = 0; i < residuals.size(); ++i) {
		uint64_t value = ZigZagEncode(residuals[i]);
		uint64_t quotient = value >> parameter;
		uint64_t value_bits = quotient + parameter + 1;
		if (quotient > 4096 || value_bits > 4096 ||
			value_bits > 4096 - bit_count) {
			return false;
		}
		bit_count += value_bits;
	}
	payload->assign(1 + static_cast<size_t>((bit_count + 7) / 8), 0);
	(*payload)[0] = parameter;
	uint64_t bit_offset = 8;
	for (size_t i = 0; i < residuals.size(); ++i) {
		uint64_t value = ZigZagEncode(residuals[i]);
		uint64_t quotient = value >> parameter;
		for (uint64_t zero = 0; zero < quotient; ++zero) {
			PutBit(payload, bit_offset++, false);
		}
		PutBit(payload, bit_offset++, true);
		for (uint8_t bit = 0; bit < parameter; ++bit) {
			PutBit(payload, bit_offset++, (value & (static_cast<uint64_t>(1) << bit)) != 0);
		}
	}
	return true;
}

static bool DecodeRice(const std::vector<uint8_t>& payload,
			       size_t count,
			       std::vector<int64_t>* residuals) {
	if (payload.empty() || payload[0] > 15) {
		return false;
	}
	uint8_t parameter = payload[0];
	uint64_t bit_offset = 8;
	residuals->clear();
	residuals->reserve(count);
	for (size_t index = 0; index < count; ++index) {
		uint64_t quotient = 0;
		bool bit = false;
		do {
			if (!GetBit(payload, bit_offset++, &bit) || quotient > 4096) {
				return false;
			}
			if (!bit) {
				++quotient;
			}
		} while (!bit);
		uint64_t value = quotient << parameter;
		for (uint8_t remainder_bit = 0; remainder_bit < parameter; ++remainder_bit) {
			if (!GetBit(payload, bit_offset++, &bit)) {
				return false;
			}
			if (bit) {
				value |= static_cast<uint64_t>(1) << remainder_bit;
			}
		}
		int64_t decoded = 0;
		if (!ZigZagDecode(value, &decoded)) {
			return false;
		}
		residuals->push_back(decoded);
	}
	while (bit_offset < payload.size() * 8) {
		bool bit = false;
		if (!GetBit(payload, bit_offset++, &bit) || bit) {
			return false;
		}
	}
	return true;
}

// RLE uses u16 run lengths and ULEB128 ZigZag values. The selector computes
// every supported payload and chooses the smallest one. Ties retain the order
// below, keeping encoded bytes deterministic across runs and platforms.
static bool EncodeResiduals(const std::vector<int64_t>& residuals,
				uint8_t* codec,
				std::vector<uint8_t>* payload) {
	std::vector<uint8_t> rle;
	for (size_t i = 0; i < residuals.size();) {
		size_t end = i + 1;
		while (end < residuals.size() && residuals[end] == residuals[i] &&
			end - i < std::numeric_limits<uint16_t>::max()) {
			++end;
		}
		PutU16(&rle, static_cast<uint16_t>(end - i));
		PutUleb(&rle, ZigZagEncode(residuals[i]));
		i = end;
	}

	std::vector<uint8_t> uleb;
	for (size_t i = 0; i < residuals.size(); ++i) {
		PutUleb(&uleb, ZigZagEncode(residuals[i]));
	}

	std::vector<uint8_t> raw;
	for (size_t i = 0; i < residuals.size(); ++i) {
		PutU64(&raw, static_cast<uint64_t>(residuals[i]));
	}

	uint8_t bit_width = 0;
	for (size_t i = 0; i < residuals.size(); ++i) {
		uint64_t value = ZigZagEncode(residuals[i]);
		uint8_t value_width = 0;
		while (value != 0) {
			++value_width;
			value >>= 1;
		}
		bit_width = std::max(bit_width, value_width);
	}
	std::vector<uint8_t> packed;
	EncodePacked(residuals, bit_width, &packed);
	std::vector<uint8_t> group_varint;
	EncodeGroupVarint(residuals, &group_varint);
	std::vector<uint8_t> rice;
	for (uint8_t parameter = 0; parameter <= 15; ++parameter) {
		std::vector<uint8_t> candidate;
		if (EncodeRice(residuals, parameter, &candidate) &&
			(rice.empty() || candidate.size() < rice.size())) {
			rice = candidate;
		}
	}

	payload->clear();
	*codec = kCodecRle;
	*payload = rle;
	if (packed.size() < payload->size()) {
		*codec = kCodecPacked;
		*payload = packed;
	}
	if (!rice.empty() && rice.size() < payload->size()) {
		*codec = kCodecRice;
		*payload = rice;
	}
	if (group_varint.size() < payload->size()) {
		*codec = kCodecGroupVarint;
		*payload = group_varint;
	}
	if (uleb.size() < payload->size()) {
		*codec = kCodecUleb;
		*payload = uleb;
	}
	if (raw.size() < payload->size()) {
		*codec = kCodecRaw;
		*payload = raw;
	}
	return true;
}

static bool DecodeResiduals(const MicroblockFrame& frame,
				std::vector<int64_t>* residuals) {
	if (frame.sample_count == 0 || frame.sample_count > kMaxFrameSamples) {
		return false;
	}
	const size_t count = static_cast<size_t>(frame.sample_count) - 1;
	residuals->clear();
	residuals->reserve(count);
	size_t cursor = 0;
	if (frame.codec_id == kCodecRaw) {
		if (frame.payload.size() != count * sizeof(uint64_t)) {
			return false;
		}
		for (size_t i = 0; i < count; ++i) {
			uint64_t raw = 0;
			if (!GetU64(frame.payload, &cursor, &raw)) {
				return false;
			}
			int64_t value = static_cast<int64_t>(raw);
			residuals->push_back(value);
		}
		return true;
	}
	if (frame.codec_id == kCodecPacked) {
		return DecodePacked(frame.payload, count, residuals);
	}
	if (frame.codec_id == kCodecRice) {
		return DecodeRice(frame.payload, count, residuals);
	}
	if (frame.codec_id == kCodecGroupVarint) {
		return DecodeGroupVarint(frame.payload, count, residuals);
	}
	if (frame.codec_id == kCodecUleb) {
		for (size_t i = 0; i < count; ++i) {
			uint64_t encoded = 0;
			int64_t value = 0;
			if (!GetUleb(frame.payload, &cursor, &encoded) ||
				!ZigZagDecode(encoded, &value)) {
				return false;
			}
			residuals->push_back(value);
		}
		return cursor == frame.payload.size();
	}
	if (frame.codec_id == kCodecRle) {
		while (residuals->size() < count) {
			uint16_t run = 0;
			uint64_t encoded = 0;
			int64_t value = 0;
			if (!GetU16(frame.payload, &cursor, &run) || run == 0 ||
				!GetUleb(frame.payload, &cursor, &encoded) ||
				!ZigZagDecode(encoded, &value) ||
				run > count - residuals->size()) {
				return false;
			}
			for (uint16_t i = 0; i < run; ++i) {
				residuals->push_back(value);
			}
		}
		return cursor == frame.payload.size();
	}
	return false;
}

// =============================================================================
// Numeric Frame Lifecycle and ZMF3 Wire Format
//
// Constructs and reconstructs one self-describing numeric frame, then writes
// or parses its exact length-delimited ZMF3 representation.
// =============================================================================

// State and numeric frames share a wire envelope but have intentionally
// different header contracts. State has no predictor or quantizer; numeric
// frames always use a frame-local predictor and quantizer.
static bool ValidFrameHeader(const MicroblockFrame& frame) {
	if (frame.field == FieldId::State) {
		return frame.sample_count > 0 && frame.codec_id == kCodecRle &&
			frame.predictor_id == 0 && frame.quantizer_id == 0 &&
			frame.quantizer_parameters.empty() &&
			ValidState(static_cast<BarState>(frame.anchor));
	}
	return IsNumericField(frame.field) && frame.sample_count > 0 &&
		frame.sample_count <= kMaxFrameSamples &&
		frame.predictor_id == kPredictPrevious &&
		(frame.quantizer_id == kQuantizerPrice ||
		 frame.quantizer_id == kQuantizerVolume);
}

}  // namespace

bool ValidPrecisionProfile(const PrecisionProfile& profile) {
	return ValidProfile(profile);
}

// Numeric frame encoding quantizes absolute values first, then stores deltas
// between adjacent reconstructed levels. Predictor choice therefore affects
// payload size only; it cannot accumulate quantization error across samples.
Status EncodeFrame(const FrameInput& input,
		   const PrecisionProfile& profile,
		   MicroblockFrame* output) {
	if (output == NULL || !ValidProfile(profile) ||
		!IsNumericField(input.field) || input.values.empty() ||
		input.values.size() > kMaxFrameSamples) {
		return Status::Error(ErrorCode::InvalidArgument, "invalid frame input");
	}
	if (input.values.size() > std::numeric_limits<BlockOff>::max()) {
		return Status::Error(ErrorCode::InvalidArgument, "frame sample count is too large");
	}

	bool volume = input.field == FieldId::Volume;
	double minimum = std::numeric_limits<double>::max();
	for (size_t i = 0; i < input.values.size(); ++i) {
		double value = input.values[i];
		if (!std::isfinite(value) || (!volume && value <= 0.0) ||
			(volume && value < 0.0)) {
			return Status::Error(ErrorCode::InvalidArgument, "non-finite or invalid field value");
		}
		if (!volume && value < minimum) {
			minimum = value;
		}
	}

	std::vector<int64_t> quantized(input.values.size());
	std::vector<uint8_t> parameters;
	uint8_t quantizer = 0;
	if (volume) {
		double log_step = 0.0;
		if (!MakeVolumeParameters(profile.volume_relative_epsilon,
					  &parameters, &log_step)) {
			return Status::Error(ErrorCode::InvalidArgument, "volume precision is out of range");
		}
		quantizer = kQuantizerVolume;
		for (size_t i = 0; i < input.values.size(); ++i) {
			if (!QuantizeVolume(input.values[i], log_step, &quantized[i])) {
				return Status::Error(ErrorCode::InvalidArgument, "volume cannot be quantized");
			}
		}
	} else {
		uint8_t mantissa = 0;
		int32_t exponent = 0;
		double step = 0.0;
		if (!ChoosePriceStep(minimum, profile.price_relative_epsilon,
					&mantissa, &exponent, &step)) {
			return Status::Error(ErrorCode::InvalidArgument, "price precision is out of range");
		}
		MakePriceParameters(mantissa, exponent, &parameters);
		quantizer = kQuantizerPrice;
		for (size_t i = 0; i < input.values.size(); ++i) {
			if (!QuantizePrice(input.values[i], step, &quantized[i])) {
				return Status::Error(ErrorCode::InvalidArgument, "price cannot be quantized");
			}
		}
	}

	std::vector<int64_t> residuals;
	residuals.reserve(quantized.size() - 1);
	for (size_t i = 1; i < quantized.size(); ++i) {
		int64_t residual = 0;
		if (!DifferenceInt64(quantized[i], quantized[i - 1], &residual)) {
			return Status::Error(ErrorCode::InvalidArgument, "quantized residual overflow");
		}
		residuals.push_back(residual);
	}

	output->field = input.field;
	output->first_offset = input.first_offset;
	output->sample_count = static_cast<BlockOff>(quantized.size());
	output->predictor_id = kPredictPrevious;
	output->quantizer_id = quantizer;
	output->quantizer_parameters = parameters;
	output->anchor = quantized[0];
	if (!EncodeResiduals(residuals, &output->codec_id, &output->payload)) {
		return Status::Error(ErrorCode::InvalidArgument, "cannot encode residuals");
	}
	return Status::Ok();
}

Status DecodeFrame(const MicroblockFrame& frame,
		   const PrecisionProfile& profile,
		   std::vector<double>* values) {
	if (values == NULL || !ValidProfile(profile) || !ValidFrameHeader(frame)) {
		return Status::Error(ErrorCode::CorruptData, "invalid frame header");
	}
	std::vector<int64_t> residuals;
	if (!DecodeResiduals(frame, &residuals)) {
		return Status::Error(ErrorCode::CorruptData, "invalid frame payload");
	}

	std::vector<int64_t> quantized(frame.sample_count);
	quantized[0] = frame.anchor;
	for (size_t i = 1; i < quantized.size(); ++i) {
		if (!AddInt64(quantized[i - 1], residuals[i - 1], &quantized[i])) {
			return Status::Error(ErrorCode::CorruptData, "frame predictor overflow");
		}
	}

	values->clear();
	values->reserve(quantized.size());
	if (frame.quantizer_id == kQuantizerPrice) {
		uint8_t mantissa = 0;
		int32_t exponent = 0;
		double step = 0.0;
		if (!ReadPriceParameters(frame.quantizer_parameters, &mantissa,
					 &exponent, &step)) {
			return Status::Error(ErrorCode::CorruptData, "invalid price quantizer");
		}
		for (size_t i = 0; i < quantized.size(); ++i) {
			double value = static_cast<double>(quantized[i]) * step;
			if (!std::isfinite(value) || value <= 0.0) {
				return Status::Error(ErrorCode::CorruptData, "invalid decoded price");
			}
			values->push_back(value);
		}
	} else {
		double log_step = 0.0;
		if (!ReadVolumeParameters(frame.quantizer_parameters, &log_step)) {
			return Status::Error(ErrorCode::CorruptData, "invalid volume quantizer");
		}
		for (size_t i = 0; i < quantized.size(); ++i) {
			if (quantized[i] < 0) {
				return Status::Error(ErrorCode::CorruptData, "invalid decoded volume level");
			}
			double value = quantized[i] == 0 ? 0.0 :
				std::exp(static_cast<double>(quantized[i] - 1) * log_step);
			if (!std::isfinite(value) || value < 0.0) {
				return Status::Error(ErrorCode::CorruptData, "invalid decoded volume");
			}
			values->push_back(value);
		}
	}
	return Status::Ok();
}


// ZMF3 is length-delimited: magic, version, field, offset, sample count,
// codec, predictor, quantizer, parameter length, anchor, payload length,
// parameters, then payload. This milestone has no frame checksum, so parsing
// requires an exact length match and rejects unknown format identifiers.
Status SerializeFrame(const MicroblockFrame& frame, std::vector<uint8_t>* bytes) {
	if (bytes == NULL || !ValidFrameHeader(frame) ||
		frame.quantizer_parameters.size() > std::numeric_limits<uint16_t>::max() ||
		frame.payload.size() > std::numeric_limits<uint32_t>::max()) {
		return Status::Error(ErrorCode::InvalidArgument, "invalid frame for serialization");
	}
	bytes->clear();
	bytes->reserve(kFrameHeaderSize + frame.quantizer_parameters.size() +
		frame.payload.size());
	PutU8(bytes, 'Z');
	PutU8(bytes, 'M');
	PutU8(bytes, 'F');
	PutU8(bytes, '3');
	PutU8(bytes, kFrameVersion);
	PutU8(bytes, static_cast<uint8_t>(frame.field));
	PutU16(bytes, frame.first_offset);
	PutU16(bytes, frame.sample_count);
	PutU8(bytes, frame.codec_id);
	PutU8(bytes, frame.predictor_id);
	PutU8(bytes, frame.quantizer_id);
	PutU16(bytes, static_cast<uint16_t>(frame.quantizer_parameters.size()));
	PutU64(bytes, static_cast<uint64_t>(frame.anchor));
	PutU32(bytes, static_cast<uint32_t>(frame.payload.size()));
	bytes->insert(bytes->end(), frame.quantizer_parameters.begin(),
		frame.quantizer_parameters.end());
	bytes->insert(bytes->end(), frame.payload.begin(), frame.payload.end());
	return Status::Ok();
}

Status ParseFrame(const std::vector<uint8_t>& bytes, MicroblockFrame* frame) {
	if (frame == NULL || bytes.size() < kFrameHeaderSize ||
		bytes[0] != 'Z' || bytes[1] != 'M' || bytes[2] != 'F' || bytes[3] != '3' ||
		bytes[4] != kFrameVersion) {
		return Status::Error(ErrorCode::CorruptData, "invalid frame wire header");
	}
	size_t cursor = 5;
	uint8_t field = 0;
	uint16_t first_offset = 0;
	uint16_t sample_count = 0;
	uint8_t codec = 0;
	uint8_t predictor = 0;
	uint8_t quantizer = 0;
	uint16_t parameter_size = 0;
	uint64_t anchor = 0;
	uint32_t payload_size = 0;
	if (!GetU8(bytes, &cursor, &field) || !GetU16(bytes, &cursor, &first_offset) ||
		!GetU16(bytes, &cursor, &sample_count) || !GetU8(bytes, &cursor, &codec) ||
		!GetU8(bytes, &cursor, &predictor) || !GetU8(bytes, &cursor, &quantizer) ||
		!GetU16(bytes, &cursor, &parameter_size) || !GetU64(bytes, &cursor, &anchor) ||
		!GetU32(bytes, &cursor, &payload_size)) {
		return Status::Error(ErrorCode::CorruptData, "truncated frame wire header");
	}
	if (parameter_size > bytes.size() - cursor ||
		payload_size > bytes.size() - cursor - parameter_size ||
		kFrameHeaderSize + parameter_size + payload_size != bytes.size()) {
		return Status::Error(ErrorCode::CorruptData, "invalid frame wire length");
	}
	frame->field = static_cast<FieldId>(field);
	frame->first_offset = static_cast<BlockOff>(first_offset);
	frame->sample_count = static_cast<BlockOff>(sample_count);
	frame->codec_id = codec;
	frame->predictor_id = predictor;
	frame->quantizer_id = quantizer;
	frame->anchor = static_cast<int64_t>(anchor);
	frame->quantizer_parameters.assign(bytes.begin() + cursor,
		bytes.begin() + cursor + parameter_size);
	cursor += parameter_size;
	frame->payload.assign(bytes.begin() + cursor, bytes.end());
	if (!ValidFrameHeader(*frame) ||
		(frame->codec_id != kCodecRle && frame->codec_id != kCodecUleb &&
		 frame->codec_id != kCodecRaw && frame->codec_id != kCodecPacked &&
		 frame->codec_id != kCodecGroupVarint && frame->codec_id != kCodecRice)) {
		return Status::Error(ErrorCode::CorruptData, "unsupported frame codec");
	}
	return Status::Ok();
}


}  // namespace zstfs
