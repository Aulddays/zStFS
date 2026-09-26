// daemon/server.h
//
// Defines the two-thread daemon boundary. The HTTP thread owns Asio sockets
// and protocol parsing; the worker owns the zStFS object and executes requests
// serially so the current core library is never accessed concurrently.

#pragma once

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include "config.h"
#include "zstfs/market.h"

namespace zstfsd {

struct HttpRequest {
	std::string method;
	std::string target;
	std::string body;
	std::function<void(const std::string&, int, const std::string&)> complete;
};

struct HttpResponse {
	int status_code;
	std::string content_type;
	std::string body;
};

// RequestWorker runs Markets on a dedicated thread. HTTP requests are queued
// by the HTTP thread and processed serially here so the core library is never
// accessed concurrently. The worker owns the Markets object and its lifetime.
class RequestWorker {
public:
	// Constructs a worker with the given config. Cache sizes are applied to
	// the global zStFS cache before Markets is constructed.
	explicit RequestWorker(const DaemonConfig& config);
	~RequestWorker();

	void start();
	void stop();
	void submit(const HttpRequest& request);

	// Blocks until Markets is fully constructed (data loaded)
	zstfs::Status wait_ready();

private:
	void run();
	HttpResponse handle(const HttpRequest& request);

	DaemonConfig config_;
	std::unique_ptr<zstfs::Markets> markets_;
	zstfs::Status ready_status_;
	bool ready_;
	std::mutex mutex_;
	std::condition_variable condition_;
	std::queue<HttpRequest> requests_;
	bool stopping_;
	std::thread thread_;
};

class HttpServer {
public:
	// Constructs an HTTP server that listens on config.listen_addr:listen_port
	// and dispatches requests to a RequestWorker built from config.
	explicit HttpServer(const DaemonConfig& config);
	~HttpServer();

	int run();
	void stop();

private:
	class Impl;
	std::unique_ptr<Impl> impl_;
};

}  // namespace zstfsd
