// market.cpp
//
// Opens one market generation and owns the ZMF8 publication protocol. The
// manifest is a best-effort crash detector without pretending to provide
// durability: complete files are closed, renamed within their directory, and
// the manifest is published last. Startup accepts only the exact files named
// by the current manifest; unreferenced temporary names are ignored.

#include "zstfs/market.h"

#include <algorithm>
#include <dirent.h>
#include <errno.h>
#include <fstream>
#include <functional>
#include <sstream>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

#include "calendar.h"
#include "serialization.h"

namespace zstfs {

namespace {

const uint16_t kManifestVersion = 1;

struct ManifestFile {
	std::string name;
	uint64_t size;
};

bool IsTemporaryName(const std::string& name) {
	return name.find(".tmp") != std::string::npos ||
		name.find(".partial") != std::string::npos;
}

bool IsLiveFile(const std::string& name) {
	if (name == "active.data" || name == "staging-index" || name == "vault-index") {
		return true;
	}
	const std::string staging_prefix = "staging-pages-";
	const std::string vault_prefix = "vault-";
	return (name.compare(0, staging_prefix.size(), staging_prefix) == 0 &&
		name.size() > staging_prefix.size() + 4 &&
		name.substr(name.size() - 4) == ".seg") ||
		(name.compare(0, vault_prefix.size(), vault_prefix) == 0 &&
		name.size() > vault_prefix.size() + 4 &&
		name.substr(name.size() - 4) == ".seg");
}

bool SameManifestName(const ManifestFile& left, const ManifestFile& right) {
	return left.name == right.name;
}

Status ReadBytes(const std::string& path, std::vector<uint8_t>* bytes) {
	std::ifstream input(path.c_str(), std::ios::binary);
	if (!input) {
		return Status::Error(ErrorCode::CorruptData, "cannot read market manifest");
	}
	*bytes = std::vector<uint8_t>((std::istreambuf_iterator<char>(input)),
		std::istreambuf_iterator<char>());
	return Status::Ok();
}

Status ListDirectory(const std::string& directory,
	const std::string& prefix,
	std::vector<ManifestFile>* files) {
	DIR* handle = opendir(directory.c_str());
	if (handle == NULL) {
		if (errno == ENOENT) {
			return Status::Ok();
		}
		return Status::Error(ErrorCode::IoError, "cannot list market directory");
	}
	struct dirent* entry = NULL;
	while ((entry = readdir(handle)) != NULL) {
		const std::string name(entry->d_name);
		if (name == "." || name == ".." || IsTemporaryName(name) || !IsLiveFile(name)) {
			continue;
		}
		const std::string path = directory + "/" + name;
		struct stat metadata = {};
		if (stat(path.c_str(), &metadata) != 0 || !S_ISREG(metadata.st_mode)) {
			closedir(handle);
			return Status::Error(ErrorCode::CorruptData, "invalid market live file");
		}
		ManifestFile file = {prefix + "/" + name,
			static_cast<uint64_t>(metadata.st_size)};
		files->push_back(file);
	}
	closedir(handle);
	return Status::Ok();
}

Status CurrentFiles(const std::string& market_path, std::vector<ManifestFile>* files) {
	files->clear();
	struct stat metadata = {};
	const std::string symbols_path = market_path + "/symbols.bin";
	if (stat(symbols_path.c_str(), &metadata) == 0) {
		if (!S_ISREG(metadata.st_mode)) {
			return Status::Error(ErrorCode::CorruptData, "symbols.bin is not a regular file");
		}
		files->push_back(ManifestFile{"symbols.bin", static_cast<uint64_t>(metadata.st_size)});
	}
	const std::string actions_path = market_path + "/actions.bin";
	if (stat(actions_path.c_str(), &metadata) == 0) {
		if (!S_ISREG(metadata.st_mode)) {
			return Status::Error(ErrorCode::CorruptData, "actions.bin is not a regular file");
		}
		files->push_back(ManifestFile{"actions.bin", static_cast<uint64_t>(metadata.st_size)});
	}
	Status status = ListDirectory(market_path + "/daily", "daily", files);
	if (!status.ok()) {
		return status;
	}
	status = ListDirectory(market_path + "/hourly", "hourly", files);
	if (!status.ok()) {
		return status;
	}
	std::sort(files->begin(), files->end(),
		[](const ManifestFile& left, const ManifestFile& right) {
			return left.name < right.name;
		});
	return Status::Ok();
}

void PutManifest(std::vector<uint8_t>* bytes, uint64_t generation,
	const std::vector<ManifestFile>& files) {
	bytes->clear();
	bytes->push_back('Z');
	bytes->push_back('M');
	bytes->push_back('F');
	bytes->push_back('8');
	PutU16(bytes, kManifestVersion);
	PutU16(bytes, 0);
	PutU64(bytes, generation);
	PutU32(bytes, static_cast<uint32_t>(files.size()));
	for (size_t i = 0; i < files.size(); ++i) {
		PutString(bytes, files[i].name);
		PutU64(bytes, files[i].size);
	}
}

Status ParseManifest(const std::vector<uint8_t>& bytes, uint64_t* generation,
	std::vector<ManifestFile>* files) {
	if (bytes.size() < 20 || bytes[0] != 'Z' || bytes[1] != 'M' ||
		bytes[2] != 'F' || bytes[3] != '8') {
		return Status::Error(ErrorCode::CorruptData, "invalid manifest magic");
	}
	size_t offset = 4;
	uint16_t version = 0;
	uint16_t reserved = 0;
	uint32_t count = 0;
	if (!GetU16(bytes, &offset, &version) || !GetU16(bytes, &offset, &reserved) ||
		!GetU64(bytes, &offset, generation) || !GetU32(bytes, &offset, &count) ||
		version != kManifestVersion || reserved != 0 || count > bytes.size()) {
		return Status::Error(ErrorCode::CorruptData, "invalid manifest header");
	}
	files->clear();
	for (uint32_t i = 0; i < count; ++i) {
		ManifestFile file = {};
		if (!GetString(bytes, &offset, &file.name) || !GetU64(bytes, &offset, &file.size) ||
			file.name.empty() || file.name[0] == '/' ||
			file.name.find("..") != std::string::npos ||
			std::find_if(files->begin(), files->end(),
				[&file](const ManifestFile& other) { return SameManifestName(file, other); }) != files->end()) {
			return Status::Error(ErrorCode::CorruptData, "invalid manifest file set");
		}
		if (file.name != "symbols.bin" && file.name != "actions.bin" &&
			file.name.compare(0, 6, "daily/") != 0 &&
			file.name.compare(0, 7, "hourly/") != 0) {
			return Status::Error(ErrorCode::CorruptData, "manifest contains an unknown file");
		}
		files->push_back(file);
	}
	if (offset != bytes.size()) {
		return Status::Error(ErrorCode::CorruptData, "manifest has trailing bytes");
	}
	return Status::Ok();
}

Status ValidateManifest(const std::string& market_path,
	uint64_t* generation) {
	std::vector<uint8_t> bytes;
	Status status = ReadBytes(market_path + "/manifest", &bytes);
	if (!status.ok()) {
		return status;
	}
	std::vector<ManifestFile> referenced;
	status = ParseManifest(bytes, generation, &referenced);
	if (!status.ok()) {
		return status;
	}
	std::vector<ManifestFile> current;
	status = CurrentFiles(market_path, &current);
	if (!status.ok()) {
		return status;
	}
	if (referenced.size() != current.size()) {
		std::ostringstream message;
		message << "manifest file count differs: expected " << referenced.size() <<
			", found " << current.size();
		return Status::Error(ErrorCode::CorruptData, message.str());
	}
	for (size_t i = 0; i < referenced.size(); ++i) {
		const bool active_log = referenced[i].name == "daily/active.data" ||
			referenced[i].name == "hourly/active.data";
		if (referenced[i].name != current[i].name ||
			(active_log ? current[i].size < referenced[i].size :
				current[i].size != referenced[i].size)) {
			std::ostringstream message;
			message << "manifest file differs at " << referenced[i].name;
			return Status::Error(ErrorCode::CorruptData, message.str());
		}
		struct stat metadata = {};
		if (stat((market_path + "/" + referenced[i].name).c_str(), &metadata) != 0 ||
			!S_ISREG(metadata.st_mode)) {
			return Status::Error(ErrorCode::CorruptData, "manifest references a missing file");
		}
	}
	return Status::Ok();
}

Status WritePublishedManifest(const std::string& market_path,
	uint64_t generation, const std::vector<ManifestFile>& files) {
	std::vector<uint8_t> bytes;
	PutManifest(&bytes, generation, files);
	std::ostringstream generation_name;
	generation_name << market_path << "/manifest-" << generation << ".bin";
	const std::string generation_temp = generation_name.str() + ".tmp-manifest";
	const std::string current_temp = market_path + "/manifest.tmp-manifest";
	std::ofstream output(generation_temp.c_str(), std::ios::binary | std::ios::trunc);
	if (!output) {
		return Status::Error(ErrorCode::IoError, "cannot create manifest generation");
	}
	output.write(reinterpret_cast<const char*>(&bytes[0]), bytes.size());
	output.close();
	if (!output || rename(generation_temp.c_str(), generation_name.str().c_str()) != 0) {
		unlink(generation_temp.c_str());
		return Status::Error(ErrorCode::IoError, "cannot publish manifest generation");
	}
	output.open(current_temp.c_str(), std::ios::binary | std::ios::trunc);
	if (!output) {
		return Status::Error(ErrorCode::IoError, "cannot create current manifest");
	}
	output.write(reinterpret_cast<const char*>(&bytes[0]), bytes.size());
	output.close();
	if (!output || rename(current_temp.c_str(), (market_path + "/manifest").c_str()) != 0) {
		unlink(current_temp.c_str());
		return Status::Error(ErrorCode::IoError, "cannot publish current manifest");
	}
	return Status::Ok();
}

bool HasLiveData(const std::string& market_path) {
	std::vector<ManifestFile> files;
	if (!CurrentFiles(market_path, &files).ok()) {
		return true;
	}
	return !files.empty();
}

}  // namespace

Market::Market(const std::string& name, const std::string& path,
	const std::string& schedule, DataFields fields)
	: name_(name), path_(path), schedule_(schedule), fields_(fields),
	calendar_(new Calendar(schedule)),
	symbols_(new Symbols()), actions_(new Actions()), daily_history_(),
	hourly_history_(), status_(Status::Ok()), manifest_generation_(0),
	manifest_loaded_(false) {
	bool symbols_created = false;
	status_ = initialize_storage();
	if (status_.ok()) {
		status_ = load_or_bootstrap_manifest();
	}
	const std::function<Status()> publish = std::bind(&Market::publish_manifest, this);
	if (status_.ok()) {
		status_ = symbols_->configure_persistence(path_ + "/symbols.bin", publish,
			&symbols_created);
	}
	if (status_.ok()) {
		status_ = actions_->configure_persistence(path_ + "/actions.bin", publish);
	}
	daily_history_.reset(new History(Frequency::Daily, *calendar_, path_, *actions_,
		publish, status_, fields_));
	hourly_history_.reset(new History(Frequency::Hourly, *calendar_, path_, *actions_,
		publish, status_, fields_));
	if (status_.ok()) {
		if (!daily_history_->status().ok()) {
			status_ = daily_history_->status();
		} else if (!hourly_history_->status().ok()) {
			status_ = hourly_history_->status();
		} else if (!manifest_loaded_ || symbols_created) {
			status_ = publish_manifest();
		}
	}
}

Market::~Market() {
}

Status Market::initialize_storage() {
	if (mkdir(path_.c_str(), 0755) != 0 && errno != EEXIST) {
		return Status::Error(ErrorCode::IoError, "cannot create market directory");
	}
	struct stat metadata = {};
	if (stat(path_.c_str(), &metadata) != 0 || !S_ISDIR(metadata.st_mode)) {
		return Status::Error(ErrorCode::IoError, "market path is not a directory");
	}
	return Status::Ok();
}

Status Market::load_or_bootstrap_manifest() {
	const std::string manifest_path = path_ + "/manifest";
	if (access(manifest_path.c_str(), F_OK) == 0) {
		manifest_loaded_ = true;
		return ValidateManifest(path_, &manifest_generation_);
	}
	if (errno != ENOENT) {
		return Status::Error(ErrorCode::IoError, "cannot inspect market manifest");
	}
	if (HasLiveData(path_)) {
		return Status::Error(ErrorCode::CorruptData, "market manifest is missing");
	}
	manifest_generation_ = 0;
	return Status::Ok();
}

Status Market::publish_manifest() {
	std::vector<ManifestFile> files;
	Status status = CurrentFiles(path_, &files);
	if (!status.ok()) {
		return status;
	}
	++manifest_generation_;
	status = WritePublishedManifest(path_, manifest_generation_, files);
	if (!status.ok()) {
		--manifest_generation_;
		return status;
	}
	return Status::Ok();
}

const std::string& Market::name() const {
	return name_;
}

const std::string& Market::path() const {
	return path_;
}

const std::string& Market::schedule() const {
	return schedule_;
}

Status Market::status() const {
	return status_;
}

Status Market::sync() {
	if (!status_.ok()) {
		return status_;
	}
	return publish_manifest();
}

Symbols& Market::symbols() {
	return *symbols_;
}

const Symbols& Market::symbols() const {
	return *symbols_;
}

Actions& Market::actions() {
	return *actions_;
}

const Actions& Market::actions() const {
	return *actions_;
}

History& Market::history(Frequency frequency) {
	return frequency == Frequency::Daily ? *daily_history_ : *hourly_history_;
}

const History& Market::history(Frequency frequency) const {
	return frequency == Frequency::Daily ? *daily_history_ : *hourly_history_;
}

}  // namespace zstfs
