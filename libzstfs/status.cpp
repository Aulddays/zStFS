#include "zstfs/status.h"

namespace zstfs {

Status Status::Ok() {
	return Status(ErrorCode::Ok, "");
}

Status Status::Error(ErrorCode code, const std::string& message) {
	return Status(code, message);
}

bool Status::ok() const {
	return code_ == ErrorCode::Ok;
}

ErrorCode Status::code() const {
	return code_;
}

const std::string& Status::message() const {
	return message_;
}

Status::Status(ErrorCode code, const std::string& message)
	: code_(code), message_(message) {
}

}  // namespace zstfs
