// test_codec.cpp
//
// Verifies the persisted BarBlockFrame codec and shared floating-point wire
// primitives without depending on the public Market API.

#include <assert.h>

#include <cmath>
#include <cstddef>
#include <vector>

#include "../libzstfs/codec.h"
#include "../libzstfs/serialization.h"

static zstfs::BlockBar NormalBar(double close, double volume) {
	zstfs::BlockBar bar = {};
	bar.state = zstfs::BarState::Normal;
	bar.open = close - 0.2;
	bar.high = close + 0.5;
	bar.low = close - 0.4;
	bar.close = close;
	bar.volume = volume;
	return bar;
}

int main() {
	std::vector<uint8_t> double_bytes;
	const double expected_double = -123.125;
	zstfs::PutDouble(&double_bytes, expected_double);
	assert(double_bytes.size() == 8);
	size_t double_offset = 0;
	double decoded_double = 0.0;
	assert(zstfs::GetDouble(double_bytes, &double_offset, &decoded_double));
	assert(double_offset == double_bytes.size());
	assert(decoded_double == expected_double);
	double_bytes.pop_back();
	double_offset = 0;
	assert(!zstfs::GetDouble(double_bytes, &double_offset, &decoded_double));

	std::vector<uint8_t> float_bytes;
	const float expected_float = -123.125f;
	zstfs::PutFloat(&float_bytes, expected_float);
	assert(float_bytes.size() == 4);
	size_t float_offset = 0;
	float decoded_float = 0.0f;
	assert(zstfs::GetFloat(float_bytes, &float_offset, &decoded_float));
	assert(float_offset == float_bytes.size());
	assert(decoded_float == expected_float);
	float_bytes.pop_back();
	float_offset = 0;
	assert(!zstfs::GetFloat(float_bytes, &float_offset, &decoded_float));

	// A BarBlockFrame owns a complete OHLCV block. Values cross byte boundaries
	// in its substreams and states remain part of the same persisted frame.
	std::vector<zstfs::BlockBar> block;
	for (size_t i = 0; i < 64; ++i) {
		block.push_back(NormalBar(100.0 + i * 0.17,
			i % 7 == 0 ? 0.0 : 1000.0 + i * 13.0));
	}
	block[5].state = zstfs::BarState::Suspended;
	block[5].open = block[5].high = block[5].low = block[5].close = block[5].volume = 0.0;
	block[37].state = zstfs::BarState::Missing;
	block[37].open = block[37].high = block[37].low = block[37].close = block[37].volume = 0.0;
	zstfs::BarBlockFrame block_frame;
	assert(zstfs::EncodeBarBlockFrame(block, &block_frame).ok());
	std::vector<zstfs::BlockBar> decoded_block;
	assert(zstfs::DecodeBarBlockFrame(block_frame, &decoded_block).ok());
	assert(decoded_block.size() == block.size());
	for (size_t i = 0; i < block.size(); ++i) {
		assert(decoded_block[i].state == block[i].state);
		if (block[i].state == zstfs::BarState::Normal) {
			assert(std::abs(decoded_block[i].open - block[i].open) / block[i].open <= 1e-4);
			assert(std::abs(decoded_block[i].high - block[i].high) / block[i].high <= 1e-4);
			assert(std::abs(decoded_block[i].low - block[i].low) / block[i].low <= 1e-4);
			assert(std::abs(decoded_block[i].close - block[i].close) / block[i].close <= 1e-4);
			if (block[i].volume == 0.0) {
				assert(decoded_block[i].volume == 0.0);
			} else {
				assert(std::abs(decoded_block[i].volume - block[i].volume) /
					block[i].volume <= 0.01);
			}
		}
	}
	zstfs::BarBlockFrame corrupt_frame = block_frame;
	corrupt_frame.bytes[0] = 0;
	assert(!zstfs::DecodeBarBlockFrame(corrupt_frame, &decoded_block).ok());

	return 0;
}
