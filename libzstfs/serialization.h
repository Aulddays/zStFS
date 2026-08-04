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

inline void PutI32(std::vector<uint8_t>* out, int32_t value) {
	PutU32(out, static_cast<uint32_t>(value));
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

inline bool GetI32(const std::vector<uint8_t>& data, size_t* offset, int32_t* out) {
	uint32_t value = 0;
	if (!GetU32(data, offset, &value)) {
		return false;
	}
	*out = static_cast<int32_t>(value);
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

// Signed integers use ZigZag before variable-length encoding so values near
// zero retain a short representation regardless of their sign.
inline uint64_t ZigZagEncode(int64_t value) {
	return (static_cast<uint64_t>(value) << 1) ^
		static_cast<uint64_t>(value >> 63);
}

inline bool ZigZagDecode(uint64_t value, int64_t* out) {
	int64_t decoded = static_cast<int64_t>((value >> 1) ^ (0 - (value & 1)));
	if (ZigZagEncode(decoded) != value) {
		return false;
	}
	*out = decoded;
	return true;
}

inline void PutUleb(std::vector<uint8_t>* out, uint64_t value) {
	do {
		uint8_t byte = static_cast<uint8_t>(value & 0x7f);
		value >>= 7;
		if (value != 0) {
			byte |= 0x80;
		}
		out->push_back(byte);
	} while (value != 0);
}

inline bool GetUleb(const std::vector<uint8_t>& data,
			    size_t* offset,
			    uint64_t* out) {
	uint64_t value = 0;
	for (size_t index = 0; index < 10; ++index) {
		if (*offset >= data.size()) {
			return false;
		}
		uint8_t byte = data[(*offset)++];
		if (index == 9 && (byte & 0x7e) != 0) {
			return false;
		}
		value |= static_cast<uint64_t>(byte & 0x7f) << (index * 7);
		if ((byte & 0x80) == 0) {
			*out = value;
			return true;
		}
	}
	return false;
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

