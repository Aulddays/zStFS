// test_codec.cpp
//
// Verifies the Milestone 3 numeric frame codec and its length-delimited wire
// representation without depending on the public Market API.

#include <assert.h>

#include <cmath>
#include <cstddef>
#include <vector>

#include "../libzstfs/codec.h"
#include "../libzstfs/history.h"
#include "../libzstfs/serialization.h"

static zstfs::PrecisionProfile Profile(double price_epsilon) {
	zstfs::PrecisionProfile profile = {};
	profile.price_relative_epsilon = price_epsilon;
	profile.volume_relative_epsilon = 0.04;
	return profile;
}

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

static size_t WireSize(const zstfs::FrameInput& input,
		       const zstfs::PrecisionProfile& profile) {
	zstfs::MicroblockFrame frame;
	std::vector<uint8_t> bytes;
	assert(zstfs::EncodeFrame(input, profile, &frame).ok());
	assert(zstfs::SerializeFrame(frame, &bytes).ok());
	return bytes.size();
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

	std::vector<double> prices;
	for (size_t i = 0; i < 32; ++i) {
		prices.push_back(100.0 + static_cast<double>(i) * 0.0737);
	}
	zstfs::FrameInput price_input = {zstfs::FieldId::Close, 7, prices};
	zstfs::PrecisionProfile precise = Profile(1e-4);
	zstfs::PrecisionProfile relaxed = Profile(5e-4);
	zstfs::MicroblockFrame price_frame;
	std::vector<double> decoded_prices;
	assert(zstfs::EncodeFrame(price_input, precise, &price_frame).ok());
	assert(zstfs::DecodeFrame(price_frame, precise, &decoded_prices).ok());
	assert(decoded_prices.size() == prices.size());
	for (size_t i = 0; i < prices.size(); ++i) {
		assert(std::fabs(decoded_prices[i] - prices[i]) / prices[i] <= 1e-4 + 1e-12);
	}

	std::vector<uint8_t> wire;
	zstfs::MicroblockFrame parsed;
	assert(zstfs::SerializeFrame(price_frame, &wire).ok());
	assert(zstfs::ParseFrame(wire, &parsed).ok());
	assert(parsed.field == price_frame.field);
	assert(parsed.first_offset == price_frame.first_offset);
	assert(parsed.sample_count == price_frame.sample_count);
	assert(parsed.codec_id == price_frame.codec_id);
	assert(parsed.quantizer_parameters == price_frame.quantizer_parameters);
	assert(parsed.payload == price_frame.payload);
	assert(zstfs::DecodeFrame(parsed, precise, &decoded_prices).ok());
	wire.pop_back();
	assert(!zstfs::ParseFrame(wire, &parsed).ok());
	assert(zstfs::SerializeFrame(price_frame, &wire).ok());
	wire[10] = 99;
	assert(!zstfs::ParseFrame(wire, &parsed).ok());

	assert(WireSize(price_input, relaxed) < WireSize(price_input, precise));

	std::vector<double> volumes;
	volumes.push_back(0.0);
	volumes.push_back(1.0);
	volumes.push_back(10.0);
	volumes.push_back(1000.0);
	volumes.push_back(1000000.0);
	zstfs::FrameInput volume_input = {zstfs::FieldId::Volume, 12, volumes};
	zstfs::MicroblockFrame volume_frame;
	std::vector<double> decoded_volumes;
	assert(zstfs::EncodeFrame(volume_input, precise, &volume_frame).ok());
	assert(zstfs::DecodeFrame(volume_frame, precise, &decoded_volumes).ok());
	assert(decoded_volumes[0] == 0.0);
	for (size_t i = 1; i < volumes.size(); ++i) {
		assert(std::fabs(decoded_volumes[i] - volumes[i]) / volumes[i] <= 0.04 + 1e-12);
	}

	std::vector<zstfs::BlockBar> state_positions;
	state_positions.push_back(NormalBar(100.0, 1000.0));
	state_positions.push_back(NormalBar(101.0, 1001.0));
	zstfs::BlockBar missing = {};
	missing.state = zstfs::BarState::Missing;
	state_positions.push_back(missing);
	state_positions.push_back(missing);
	zstfs::MicroblockFrame state_frame;
	std::vector<zstfs::BarState> decoded_states;
	assert(zstfs::EncodeStateFrame(20, state_positions, &state_frame).ok());
	assert(zstfs::DecodeStateFrame(state_frame, &decoded_states).ok());
	assert(decoded_states.size() == state_positions.size());
	assert(decoded_states[0] == zstfs::BarState::Normal);
	assert(decoded_states[2] == zstfs::BarState::Missing);
	assert(zstfs::SerializeFrame(state_frame, &wire).ok());
	assert(zstfs::ParseFrame(wire, &parsed).ok());
	assert(zstfs::DecodeStateFrame(parsed, &decoded_states).ok());

	std::vector<zstfs::BlockBar> ohlcv_positions;
	ohlcv_positions.push_back(NormalBar(100.0, 1000.0));
	ohlcv_positions.push_back(NormalBar(101.0, 1200.0));
	zstfs::BlockBar malformed = NormalBar(102.0, 1300.0);
	malformed.high = malformed.low - 1.0;
	ohlcv_positions.push_back(malformed);
	ohlcv_positions.push_back(NormalBar(103.0, 1400.0));
	std::vector<zstfs::MicroblockFrame> ohlcv_frames;
	std::vector<zstfs::BlockBar> decoded_bars;
	assert(zstfs::EncodeOhlcvFrames(40, ohlcv_positions, precise, &ohlcv_frames).ok());
	assert(zstfs::DecodeOhlcvFrames(40, 4, ohlcv_frames, precise, &decoded_bars).ok());
	assert(decoded_bars[2].state == zstfs::BarState::Missing);
	for (size_t i = 0; i < decoded_bars.size(); ++i) {
		if (decoded_bars[i].state != zstfs::BarState::Normal) {
			continue;
		}
		assert(decoded_bars[i].high >= decoded_bars[i].open);
		assert(decoded_bars[i].high >= decoded_bars[i].close);
		assert(decoded_bars[i].low <= decoded_bars[i].open);
		assert(decoded_bars[i].low <= decoded_bars[i].close);
		assert(std::fabs(decoded_bars[i].close - ohlcv_positions[i].close) /
				ohlcv_positions[i].close <= 1e-4 + 1e-12);
	}

	std::vector<zstfs::BlockBar> positions;
	for (size_t i = 0; i < 70; ++i) {
		positions.push_back(NormalBar(100.0 + i, 1000.0 + i));
	}
	zstfs::BlockBar suspended = {};
	suspended.state = zstfs::BarState::Suspended;
	positions.insert(positions.begin() + 33, suspended);
	zstfs::BlockBar invalid = NormalBar(150.0, 1000.0);
	invalid.high = invalid.low - 1.0;
	positions.insert(positions.begin() + 51, invalid);
	std::vector<zstfs::MicroblockFrame> frames;
	assert(zstfs::EncodeFieldFrames(zstfs::FieldId::Close, 0, positions,
					precise, &frames).ok());
	assert(frames.size() == 4);
	assert(frames[0].first_offset == 0 && frames[0].sample_count == 32);
	assert(frames[1].first_offset == 32 && frames[1].sample_count == 1);
	assert(frames[2].first_offset == 34 && frames[2].sample_count == 17);
	assert(frames[3].first_offset == 52 && frames[3].sample_count == 20);

	zstfs::FrameInput too_many = {zstfs::FieldId::Open, 0,
		std::vector<double>(33, 10.0)};
	assert(!zstfs::EncodeFrame(too_many, precise, &price_frame).ok());
	zstfs::FrameInput bad_price = {zstfs::FieldId::Open, 0,
		std::vector<double>(1, -1.0)};
	assert(!zstfs::EncodeFrame(bad_price, precise, &price_frame).ok());
	zstfs::FrameInput bad_field = {zstfs::FieldId::State, 0,
		std::vector<double>(1, 1.0)};
	assert(!zstfs::EncodeFrame(bad_field, precise, &price_frame).ok());

	return 0;
}
