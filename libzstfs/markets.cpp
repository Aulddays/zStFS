#include "zstfs/market.h"

#include <fstream>

namespace zstfs {

static std::string Trim(const std::string& value) {
	std::string::size_type begin = value.find_first_not_of(" \t\r\n");
	if (begin == std::string::npos) {
		return "";
	}
	std::string::size_type end = value.find_last_not_of(" \t\r\n");
	return value.substr(begin, end - begin + 1);
}

static bool ParseMarketLine(const std::string& line,
                     std::string* name,
                     std::string* type,
                     std::string* path) {
	std::string::size_type first = line.find(',');
	if (first == std::string::npos) {
		return false;
	}
	std::string::size_type second = line.find(',', first + 1);
	if (second == std::string::npos || line.find(',', second + 1) != std::string::npos) {
		return false;
	}
	*name = Trim(line.substr(0, first));
	*type = Trim(line.substr(first + 1, second - first - 1));
	*path = Trim(line.substr(second + 1));
	return !name->empty() && !type->empty() && !path->empty();
}

Status Markets::load(const std::string& file_path) {
	std::ifstream input(file_path.c_str());
	if (!input) {
		return Status::Error(ErrorCode::IoError, "failed to open markets config");
	}

	std::map<std::string, std::unique_ptr<Market> > loaded;
	std::string line;
	unsigned int line_number = 0;
	while (std::getline(input, line)) {
		++line_number;
		line = Trim(line);
		if (line.empty() || line[0] == '#') {
			continue;
		}
		std::string name;
		std::string type;
		std::string path;
		if (!ParseMarketLine(line, &name, &type, &path)) {
			return Status::Error(ErrorCode::InvalidArgument,
			                     "invalid markets config line " +
			                     std::to_string(line_number));
		}
		if (!loaded.insert(std::make_pair(name,
				std::unique_ptr<Market>(new Market(name, path, type)))).second) {
			return Status::Error(ErrorCode::AlreadyPresent,
			                     "duplicate market name: " + name);
		}
	}
	if (!input.eof()) {
		return Status::Error(ErrorCode::IoError, "failed to read markets config");
	}
	by_name_.swap(loaded);
	return Status::Ok();
}

Status Markets::get(const std::string& name, Market** out) {
	if (out == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "market output is required");
	}
	std::map<std::string, std::unique_ptr<Market> >::iterator found =
		by_name_.find(name);
	if (found == by_name_.end()) {
		return Status::Error(ErrorCode::NotFound, "market was not found");
	}
	*out = found->second.get();
	return Status::Ok();
}

Status Markets::get(const std::string& name, const Market** out) const {
	if (out == NULL) {
		return Status::Error(ErrorCode::InvalidArgument, "market output is required");
	}
	std::map<std::string, std::unique_ptr<Market> >::const_iterator found =
		by_name_.find(name);
	if (found == by_name_.end()) {
		return Status::Error(ErrorCode::NotFound, "market was not found");
	}
	*out = found->second.get();
	return Status::Ok();
}

}  // namespace zstfs
