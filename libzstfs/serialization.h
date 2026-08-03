#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

namespace zstfs {

// Persistent zStFS files use little-endian fixed-width integers and
// uint16_t-length-prefixed strings. Individual formats retain ownership of
// their record layouts and validation; these primitives only encode fields.
inline void PutU8(std::vector<uint8_t>* out, uint8_t value) {
	out->push_back(value);
}

inline void PutU16(std::vector<uint8_t>* out, uint16_t value) {
	out->push_back(static_cast<uint8_t>(value));
	out->push_back(static_cast<uint8_t>(value >> 8));
}

inline void PutU32(std::vector<uint8_t>* out, uint32_t value) {
	for (int shift = 0; shift < 32; shift += 8) {
		out->push_back(static_cast<uint8_t>(value >> shift));
	}
}

inline void PutU64(std::vector<uint8_t>* out, uint64_t value) {
	for (int shift = 0; shift < 64; shift += 8) {
		out->push_back(static_cast<uint8_t>(value >> shift));
	}
}

inline bool PutString(std::vector<uint8_t>* out, const std::string& value) {
	if (value.size() > std::numeric_limits<uint16_t>::max()) {
		return false;
	}
	PutU16(out, static_cast<uint16_t>(value.size()));
	out->insert(out->end(), value.begin(), value.end());
	return true;
}

inline bool GetU8(const std::vector<uint8_t>& data, size_t* offset, uint8_t* out) {
	if (*offset >= data.size()) {
		return false;
	}
	*out = data[*offset];
	++*offset;
	return true;
}

inline bool GetU16(const std::vector<uint8_t>& data, size_t* offset, uint16_t* out) {
	if (*offset + 2 > data.size()) {
		return false;
	}
	*out = static_cast<uint16_t>(data[*offset]) |
	       static_cast<uint16_t>(data[*offset + 1]) << 8;
	*offset += 2;
	return true;
}

inline bool GetU32(const std::vector<uint8_t>& data, size_t* offset, uint32_t* out) {
	if (*offset + 4 > data.size()) {
		return false;
	}
	*out = static_cast<uint32_t>(data[*offset]) |
	       static_cast<uint32_t>(data[*offset + 1]) << 8 |
	       static_cast<uint32_t>(data[*offset + 2]) << 16 |
	       static_cast<uint32_t>(data[*offset + 3]) << 24;
	*offset += 4;
	return true;
}

inline bool GetU64(const std::vector<uint8_t>& data, size_t* offset, uint64_t* out) {
	if (*offset + 8 > data.size()) {
		return false;
	}
	*out = 0;
	for (int shift = 0; shift < 64; shift += 8) {
		*out |= static_cast<uint64_t>(data[*offset + shift / 8]) << shift;
	}
	*offset += 8;
	return true;
}

inline bool GetString(const std::vector<uint8_t>& data,
                      size_t* offset,
                      std::string* out) {
	uint16_t length = 0;
	if (!GetU16(data, offset, &length) || *offset + length > data.size()) {
		return false;
	}
	out->assign(reinterpret_cast<const char*>(&data[*offset]), length);
	*offset += length;
	return true;
}

}  // namespace zstfs

