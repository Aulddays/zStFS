// codec.cpp
//
// Implements the private persisted BarBlockFrame codec. A frame keeps each
// complete OHLCV sample together so range reads and relational predictors
// reconstruct a bar as one immutable storage unit.
//
#include "codec.h"
#include "calendar.h"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>

#include "serialization.h"

namespace zstfs {



// =============================================================================
// BarBlockFrame
//
// The block format keeps a complete OHLCV sample together. Its substreams are
// bit-packed independently, but the reconstruction order keeps price relations
// explicit and makes the frame the smallest immutable storage unit.
// =============================================================================

static const uint8_t kBarBlockFrameVersion = 1;
static const uint8_t kStateAllNormal = 0;
static const uint8_t kStateSparse = 1;
static const uint8_t kStateDense = 2;
static const uint8_t kBarBlockHasZeroVolume = 1;
static const uint8_t kBarBlockRawValues = 2;
static const double kBarBlockPriceEpsilon = 1e-4;
static const double kBarBlockVolumeEpsilon = 0.01;
static const double kBarBlockPriceTickDivisor = 8192.0;
static const size_t kBarBlockHeaderBytes = 26;

static bool ValidBlockBar(const BlockBar& value) {
	return std::isfinite(value.open) && std::isfinite(value.high) &&
		std::isfinite(value.low) && std::isfinite(value.close) &&
		std::isfinite(value.volume) && value.open > 0.0 && value.high > 0.0 &&
		value.low > 0.0 && value.close > 0.0 && value.volume >= 0.0 &&
		value.low <= std::min(value.open, value.close) &&
		value.high >= std::max(value.open, value.close);
}

static bool ValidBarState(BarState state) {
	return static_cast<unsigned int>(state) <= static_cast<unsigned int>(BarState::Missing);
}

static double RelativeError(double expected, double actual) {
	return expected == 0.0 ? std::abs(actual) : std::abs(actual - expected) / std::abs(expected);
}

static uint8_t BitsForCode(uint64_t maximum) {
	uint8_t bits = 0;
	while (maximum != 0) {
		++bits;
		maximum >>= 1;
	}
	return bits;
}

static uint64_t ZigZagCode(int64_t value) {
	return value >= 0 ? static_cast<uint64_t>(value) * 2 :
		static_cast<uint64_t>(-(value + 1)) * 2 + 1;
}

static int64_t DecodeZigZag(uint64_t value) {
	return value & 1 ? -static_cast<int64_t>((value + 1) / 2) :
		static_cast<int64_t>(value / 2);
}

static size_t PackedBytes(size_t values, uint8_t bits) {
	return bits == 0 ? 0 : (values * static_cast<size_t>(bits) + 7) / 8;
}

static void PutPacked(std::vector<uint8_t>* bytes, const std::vector<uint64_t>& values,
					  uint8_t bits) {
	if (bits == 0) {
		return;
	}
	const size_t start = bytes->size();
	const size_t size = PackedBytes(values.size(), bits);
	bytes->resize(start + size, 0);
	size_t bit_offset = 0;
	for (size_t i = 0; i < values.size(); ++i) {
		for (uint8_t bit = 0; bit < bits; ++bit, ++bit_offset) {
			if (values[i] & (UINT64_C(1) << bit)) {
				(*bytes)[start + bit_offset / 8] |= static_cast<uint8_t>(1U << (bit_offset & 7));
			}
		}
	}
}

static bool GetPacked(const std::vector<uint8_t>& bytes, size_t* cursor, size_t values,
					  uint8_t bits, std::vector<uint64_t>* output) {
	const size_t size = PackedBytes(values, bits);
	if (cursor == NULL || output == NULL || *cursor > bytes.size() || size > bytes.size() - *cursor) {
		return false;
	}
	output->assign(values, 0);
	for (size_t value = 0; value < values; ++value) {
		for (uint8_t bit = 0; bit < bits; ++bit) {
			const size_t offset = value * static_cast<size_t>(bits) + bit;
			if (bytes[*cursor + offset / 8] & static_cast<uint8_t>(1U << (offset & 7))) {
				(*output)[value] |= UINT64_C(1) << bit;
			}
		}
	}
	*cursor += size;
	return true;
}

static bool FitsSignedCode(double value, int64_t* code) {
	if (!std::isfinite(value) || value < static_cast<double>(std::numeric_limits<int64_t>::min()) ||
		value > static_cast<double>(std::numeric_limits<int64_t>::max())) {
		return false;
	}
	*code = static_cast<int64_t>(std::llround(value));
	return true;
}

Status EncodeBarBlockFrame(const std::vector<BlockBar>& input, BarBlockFrame* output) {
	if (output == NULL || input.empty() || input.size() > std::numeric_limits<BlockOff>::max()) {
		return Status::Error(ErrorCode::InvalidArgument, "invalid bar block frame input");
	}
	std::vector<BlockBar> positions(input);
	std::vector<size_t> normal;
	for (size_t i = 0; i < positions.size(); ++i) {
		if (!ValidBarState(positions[i].state)) {
			return Status::Error(ErrorCode::InvalidArgument, "invalid bar state");
		}
		if (positions[i].state == BarState::Normal && !ValidBlockBar(positions[i])) {
			positions[i].state = BarState::Missing;
			positions[i].open = positions[i].high = positions[i].low = positions[i].close = positions[i].volume = 0.0;
		}
		if (positions[i].state == BarState::Normal) {
			normal.push_back(i);
		}
	}

	uint8_t state_mode = kStateAllNormal;
	std::vector<uint8_t> state_bytes;
	if (normal.size() != positions.size()) {
		const size_t non_normal = positions.size() - normal.size();
		state_mode = non_normal * 4 <= positions.size() ? kStateSparse : kStateDense;
		if (state_mode == kStateSparse) {
			state_bytes.assign((positions.size() + 7) / 8, 0);
			for (size_t i = 0; i < positions.size(); ++i) {
				if (positions[i].state != BarState::Normal) {
					state_bytes[i / 8] |= static_cast<uint8_t>(1U << (i & 7));
					PutU8(&state_bytes, static_cast<uint8_t>(positions[i].state));
				}
			}
		} else {
			state_bytes.assign((positions.size() * 3 + 7) / 8, 0);
			for (size_t i = 0; i < positions.size(); ++i) {
				const uint8_t code = static_cast<uint8_t>(positions[i].state);
				for (uint8_t bit = 0; bit < 3; ++bit) {
					const size_t offset = i * 3 + bit;
					if (code & (1U << bit)) {
						state_bytes[offset / 8] |= static_cast<uint8_t>(1U << (offset & 7));
					}
				}
			}
		}
	}
	if (state_bytes.size() > std::numeric_limits<uint16_t>::max()) {
		return Status::Error(ErrorCode::InvalidArgument, "bar block state payload is too large");
	}

	float stored_anchor = 0.0f;
	double tick = 0.0;
	bool raw = false;
	if (!normal.empty()) {
		double minimum = static_cast<double>(positions[normal[0]].low);
		for (size_t i = 0; i < normal.size(); ++i) {
			const BlockBar& bar = positions[normal[i]];
			minimum = std::min(minimum, std::min(std::min(static_cast<double>(bar.open), static_cast<double>(bar.high)),
				std::min(static_cast<double>(bar.low), static_cast<double>(bar.close))));
		}
		stored_anchor = std::nextafter(static_cast<float>(minimum), -std::numeric_limits<float>::infinity());
		tick = static_cast<double>(stored_anchor) / kBarBlockPriceTickDivisor;
		raw = !std::isfinite(tick) || tick <= 0.0;
	}

	std::vector<uint64_t> close_codes;
	std::vector<uint64_t> open_codes;
	std::vector<uint64_t> high_codes;
	std::vector<uint64_t> low_codes;
	std::vector<uint64_t> volume_codes;
	std::vector<uint8_t> zero_volume;
	int32_t minimum_level = 0;
	bool has_zero = false;
	if (!raw && !normal.empty()) {
		int64_t minimum_log_level = std::numeric_limits<int64_t>::max();
		const double log_step = 2.0 * std::log1p(kBarBlockVolumeEpsilon);
		for (size_t i = 0; i < normal.size(); ++i) {
			if (positions[normal[i]].volume != 0.0) {
				const int64_t level = static_cast<int64_t>(std::llround(std::log(positions[normal[i]].volume) / log_step));
				minimum_log_level = std::min(minimum_log_level, level);
			}
		}
		if (minimum_log_level != std::numeric_limits<int64_t>::max() &&
			(minimum_log_level < std::numeric_limits<int32_t>::min() ||
			 minimum_log_level > std::numeric_limits<int32_t>::max())) {
			raw = true;
		} else if (minimum_log_level != std::numeric_limits<int64_t>::max()) {
			minimum_level = static_cast<int32_t>(minimum_log_level);
		}
		zero_volume.assign((normal.size() + 7) / 8, 0);
		for (size_t i = 0; !raw && i < normal.size(); ++i) {
			const BlockBar& source = positions[normal[i]];
			int64_t close_code = 0;
			int64_t open_code = 0;
			int64_t high_code = 0;
			int64_t low_code = 0;
			if (!FitsSignedCode((source.close - stored_anchor) / tick, &close_code) || close_code < 0) {
				raw = true;
				break;
			}
			const double close = stored_anchor + close_code * tick;
			if (!FitsSignedCode((source.open - close) / tick, &open_code)) {
				raw = true;
				break;
			}
			const double open = close + open_code * tick;
		if (RelativeError(source.close, close) > kBarBlockPriceEpsilon ||
			RelativeError(source.open, open) > kBarBlockPriceEpsilon) {
			raw = true;
			break;
		}
			if (!FitsSignedCode((source.high - std::max(open, close)) / tick, &high_code) || high_code < 0 ||
				!FitsSignedCode((std::min(open, close) - source.low) / tick, &low_code) || low_code < 0) {
				raw = true;
				break;
			}
			const double high = std::max(open, close) + high_code * tick;
		const double low = std::min(open, close) - low_code * tick;
		if (RelativeError(source.high, high) > kBarBlockPriceEpsilon ||
			RelativeError(source.low, low) > kBarBlockPriceEpsilon) {
			raw = true;
			break;
		}
		close_codes.push_back(static_cast<uint64_t>(close_code));
			open_codes.push_back(ZigZagCode(open_code));
			high_codes.push_back(ZigZagCode(high_code));
			low_codes.push_back(ZigZagCode(low_code));
			if (source.volume == 0.0) {
				has_zero = true;
				zero_volume[i / 8] |= static_cast<uint8_t>(1U << (i & 7));
				continue;
			}
			const int64_t level = static_cast<int64_t>(std::llround(std::log(source.volume) / log_step));
			if (level < minimum_level) {
				raw = true;
				break;
			}
			const double volume = std::exp(static_cast<double>(level) * log_step);
			if (RelativeError(source.volume, volume) > kBarBlockVolumeEpsilon) {
				raw = true;
				break;
			}
			volume_codes.push_back(static_cast<uint64_t>(level - minimum_level));
		}
	}

	uint8_t close_bits = 0;
	uint8_t open_bits = 0;
	uint8_t high_bits = 0;
	uint8_t low_bits = 0;
	uint8_t volume_bits = 0;
	if (!raw) {
		for (size_t i = 0; i < normal.size(); ++i) {
			close_bits = std::max(close_bits, BitsForCode(close_codes[i]));
			open_bits = std::max(open_bits, BitsForCode(open_codes[i]));
			high_bits = std::max(high_bits, BitsForCode(high_codes[i]));
			low_bits = std::max(low_bits, BitsForCode(low_codes[i]));
		}
		for (size_t i = 0; i < volume_codes.size(); ++i) {
			volume_bits = std::max(volume_bits, BitsForCode(volume_codes[i]));
		}
	}

	std::vector<uint8_t> bytes;
	bytes.reserve(kBarBlockHeaderBytes + state_bytes.size() + normal.size() * 8);
	PutU8(&bytes, 'Z'); PutU8(&bytes, 'B'); PutU8(&bytes, 'F'); PutU8(&bytes, '4');
	PutU8(&bytes, kBarBlockFrameVersion);
	PutU16(&bytes, static_cast<uint16_t>(positions.size()));
	PutU16(&bytes, static_cast<uint16_t>(normal.size()));
	PutU8(&bytes, state_mode);
	PutU8(&bytes, static_cast<uint8_t>((has_zero ? kBarBlockHasZeroVolume : 0) |
		(raw ? kBarBlockRawValues : 0)));
	PutFloat(&bytes, stored_anchor);
	PutI32(&bytes, minimum_level);
	PutU8(&bytes, close_bits); PutU8(&bytes, open_bits); PutU8(&bytes, high_bits);
	PutU8(&bytes, low_bits); PutU8(&bytes, volume_bits);
	PutU16(&bytes, static_cast<uint16_t>(state_bytes.size()));
	bytes.insert(bytes.end(), state_bytes.begin(), state_bytes.end());
	if (raw) {
		for (size_t i = 0; i < normal.size(); ++i) {
			const BlockBar& bar = positions[normal[i]];
			PutFloat(&bytes, bar.open); PutFloat(&bytes, bar.high); PutFloat(&bytes, bar.low);
			PutFloat(&bytes, bar.close); PutFloat(&bytes, bar.volume);
		}
	} else {
		PutPacked(&bytes, close_codes, close_bits);
		PutPacked(&bytes, open_codes, open_bits);
		PutPacked(&bytes, high_codes, high_bits);
		PutPacked(&bytes, low_codes, low_bits);
		if (has_zero) {
			bytes.insert(bytes.end(), zero_volume.begin(), zero_volume.end());
		}
		PutPacked(&bytes, volume_codes, volume_bits);
	}
	output->bytes.swap(bytes);
	return Status::Ok();
}

Status DecodeBarBlockFrame(const BarBlockFrame& frame, std::vector<BlockBar>* positions) {
	if (positions == NULL || frame.bytes.size() < kBarBlockHeaderBytes ||
		frame.bytes[0] != 'Z' || frame.bytes[1] != 'B' || frame.bytes[2] != 'F' ||
		frame.bytes[3] != '4' || frame.bytes[4] != kBarBlockFrameVersion) {
		return Status::Error(ErrorCode::CorruptData, "invalid bar block frame header");
	}
	size_t cursor = 5;
	uint16_t position_count = 0;
	uint16_t normal_count = 0;
	uint8_t state_mode = 0;
	uint8_t flags = 0;
	float stored_anchor = 0.0f;
	int32_t minimum_level = 0;
	uint8_t close_bits = 0, open_bits = 0, high_bits = 0, low_bits = 0, volume_bits = 0;
	uint16_t state_size = 0;
	if (!GetU16(frame.bytes, &cursor, &position_count) || !GetU16(frame.bytes, &cursor, &normal_count) ||
		!GetU8(frame.bytes, &cursor, &state_mode) || !GetU8(frame.bytes, &cursor, &flags) ||
		!GetFloat(frame.bytes, &cursor, &stored_anchor) || !std::isfinite(stored_anchor) ||
		!GetI32(frame.bytes, &cursor, &minimum_level) ||
		!GetU8(frame.bytes, &cursor, &close_bits) || !GetU8(frame.bytes, &cursor, &open_bits) ||
		!GetU8(frame.bytes, &cursor, &high_bits) || !GetU8(frame.bytes, &cursor, &low_bits) ||
		!GetU8(frame.bytes, &cursor, &volume_bits) || !GetU16(frame.bytes, &cursor, &state_size) ||
		position_count == 0 || normal_count > position_count ||
		(state_mode != kStateAllNormal && state_mode != kStateSparse && state_mode != kStateDense) ||
		(flags & ~(kBarBlockHasZeroVolume | kBarBlockRawValues)) != 0 ||
		cursor > frame.bytes.size() || state_size > frame.bytes.size() - cursor) {
		return Status::Error(ErrorCode::CorruptData, "invalid bar block frame metadata");
	}
	const size_t state_start = cursor;
	const size_t state_end = cursor + state_size;
	positions->assign(position_count, BlockBar());
	std::vector<size_t> normal;
	if (state_mode == kStateAllNormal) {
		if (state_size != 0 || normal_count != position_count) {
			return Status::Error(ErrorCode::CorruptData, "invalid all-normal state frame");
		}
		for (size_t i = 0; i < position_count; ++i) {
			(*positions)[i].state = BarState::Normal;
			normal.push_back(i);
		}
	} else if (state_mode == kStateSparse) {
		const size_t bitmap_size = (position_count + 7) / 8;
		if (state_size < bitmap_size) {
			return Status::Error(ErrorCode::CorruptData, "truncated sparse bar states");
		}
		size_t state_cursor = state_start + bitmap_size;
		for (size_t i = 0; i < position_count; ++i) {
			if ((frame.bytes[state_start + i / 8] & static_cast<uint8_t>(1U << (i & 7))) == 0) {
				(*positions)[i].state = BarState::Normal;
				normal.push_back(i);
			} else {
				uint8_t state = 0;
				if (!GetU8(frame.bytes, &state_cursor, &state) || !ValidBarState(static_cast<BarState>(state)) ||
					static_cast<BarState>(state) == BarState::Normal) {
					return Status::Error(ErrorCode::CorruptData, "invalid sparse bar state");
				}
				(*positions)[i].state = static_cast<BarState>(state);
			}
		}
		if (state_cursor != state_end) {
			return Status::Error(ErrorCode::CorruptData, "invalid sparse state length");
		}
	} else {
		if (state_size != PackedBytes(position_count, 3)) {
			return Status::Error(ErrorCode::CorruptData, "invalid dense state length");
		}
		for (size_t i = 0; i < position_count; ++i) {
			uint8_t state = 0;
			for (uint8_t bit = 0; bit < 3; ++bit) {
				const size_t offset = i * 3 + bit;
				state |= ((frame.bytes[state_start + offset / 8] >> (offset & 7)) & 1U) << bit;
			}
			if (!ValidBarState(static_cast<BarState>(state))) {
				return Status::Error(ErrorCode::CorruptData, "invalid dense bar state");
			}
			(*positions)[i].state = static_cast<BarState>(state);
			if ((*positions)[i].state == BarState::Normal) {
				normal.push_back(i);
			}
		}
	}
	if (normal.size() != normal_count) {
		return Status::Error(ErrorCode::CorruptData, "bar state count does not match frame");
	}
	cursor = state_end;
	const bool raw = (flags & kBarBlockRawValues) != 0;
	const bool has_zero = (flags & kBarBlockHasZeroVolume) != 0;
	if (raw) {
		if (close_bits != 0 || open_bits != 0 || high_bits != 0 || low_bits != 0 || volume_bits != 0 || has_zero ||
			frame.bytes.size() - cursor != normal.size() * 5 * sizeof(float)) {
			return Status::Error(ErrorCode::CorruptData, "invalid raw bar block frame");
		}
		for (size_t i = 0; i < normal.size(); ++i) {
			BlockBar& bar = (*positions)[normal[i]];
			if (!GetFloat(frame.bytes, &cursor, &bar.open) || !GetFloat(frame.bytes, &cursor, &bar.high) ||
				!GetFloat(frame.bytes, &cursor, &bar.low) || !GetFloat(frame.bytes, &cursor, &bar.close) ||
				!GetFloat(frame.bytes, &cursor, &bar.volume) || !ValidBlockBar(bar)) {
				return Status::Error(ErrorCode::CorruptData, "invalid raw bar values");
			}
		}
		return Status::Ok();
	}
	if ((normal_count != 0 && (!std::isfinite(stored_anchor) || stored_anchor <= 0.0f)) ||
		close_bits > 63 || open_bits > 64 || high_bits > 64 || low_bits > 64 || volume_bits > 64) {
		return Status::Error(ErrorCode::CorruptData, "invalid bar block quantizer");
	}
	std::vector<uint64_t> close_codes, open_codes, high_codes, low_codes, volume_codes;
	if (!GetPacked(frame.bytes, &cursor, normal.size(), close_bits, &close_codes) ||
		!GetPacked(frame.bytes, &cursor, normal.size(), open_bits, &open_codes) ||
		!GetPacked(frame.bytes, &cursor, normal.size(), high_bits, &high_codes) ||
		!GetPacked(frame.bytes, &cursor, normal.size(), low_bits, &low_codes)) {
		return Status::Error(ErrorCode::CorruptData, "truncated price bitstream");
	}
	std::vector<uint8_t> zero_volume;
	size_t non_zero_count = normal.size();
	if (has_zero) {
		const size_t zero_size = (normal.size() + 7) / 8;
		if (cursor > frame.bytes.size() || zero_size > frame.bytes.size() - cursor) {
			return Status::Error(ErrorCode::CorruptData, "truncated volume presence bitmap");
		}
		zero_volume.assign(frame.bytes.begin() + cursor, frame.bytes.begin() + cursor + zero_size);
		cursor += zero_size;
		non_zero_count = 0;
		for (size_t i = 0; i < normal.size(); ++i) {
			non_zero_count += (zero_volume[i / 8] & static_cast<uint8_t>(1U << (i & 7))) == 0;
		}
	}
	if (!GetPacked(frame.bytes, &cursor, non_zero_count, volume_bits, &volume_codes) || cursor != frame.bytes.size()) {
		return Status::Error(ErrorCode::CorruptData, "invalid volume bitstream");
	}
	const double tick = static_cast<double>(stored_anchor) / kBarBlockPriceTickDivisor;
	const double log_step = 2.0 * std::log1p(kBarBlockVolumeEpsilon);
	size_t volume_index = 0;
	for (size_t i = 0; i < normal.size(); ++i) {
		BlockBar& bar = (*positions)[normal[i]];
		const double close = static_cast<double>(stored_anchor) + close_codes[i] * tick;
		const double open = close + DecodeZigZag(open_codes[i]) * tick;
		const double high = std::max(open, close) + DecodeZigZag(high_codes[i]) * tick;
		const double low = std::min(open, close) - DecodeZigZag(low_codes[i]) * tick;
		const bool zero = has_zero && (zero_volume[i / 8] & static_cast<uint8_t>(1U << (i & 7))) != 0;
		const double volume = zero ? 0.0 : std::exp((static_cast<double>(minimum_level) +
			static_cast<double>(volume_codes[volume_index++])) * log_step);
		bar.open = static_cast<float>(open);
		bar.high = static_cast<float>(high);
		bar.low = static_cast<float>(low);
		bar.close = static_cast<float>(close);
		bar.volume = static_cast<float>(volume);
		if (!ValidBlockBar(bar)) {
			return Status::Error(ErrorCode::CorruptData, "invalid reconstructed bar");
		}
	}
	return Status::Ok();
}

}  // namespace zstfs
