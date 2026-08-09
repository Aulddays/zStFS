// codec.h
//
// Declares the private persisted BarBlockFrame codec. Complete OHLCV samples
// remain together because readers reconstruct every bar as one unit.

#pragma once

#include <cstdint>
#include <vector>

#include "zstfs/data.h"
#include "zstfs/status.h"

namespace zstfs {

// BlockBar is the complete state and raw OHLCV payload at one calendar position.
// BarBlockFrame keeps all five values together because an OHLC sample is always
// reconstructed as a unit by range reads and by the relational predictors.
struct BlockBar {
	BarState state;
	double open;
	double high;
	double low;
	double close;
	double volume;
};

struct BarBlockFrame {
	std::vector<uint8_t> bytes;
};

// BarBlockFrame is the current immutable block format. Its fixed quantization
// rules are part of the persisted format: price relative error is 1e-4 and
// volume relative error is 0.01. The returned frame bytes are self-describing.
Status EncodeBarBlockFrame(const std::vector<BlockBar>& positions,
					   BarBlockFrame* output);
Status DecodeBarBlockFrame(const BarBlockFrame& frame,
					   std::vector<BlockBar>* positions);

}  // namespace zstfs
