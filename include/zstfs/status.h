#pragma once

#include <string>

namespace zstfs {

enum class ErrorCode {
	Ok,
	InvalidArgument,
	NotFound,
	AlreadyPresent,
	Conflict,
	NotImplemented,
	CorruptData,
	IoError
};

// Status carries the result of an operation that can fail for normal data,
// conflict, or I/O conditions without requiring exception-based control flow.
class Status {
public:
	static Status Ok();
	static Status Error(ErrorCode code, const std::string& message);

	bool ok() const;
	ErrorCode code() const;
	const std::string& message() const;

private:
	Status(ErrorCode code, const std::string& message);

	ErrorCode code_;
	std::string message_;
};

}  // namespace zstfs

