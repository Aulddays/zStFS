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

class RequestWorker {
public:
	explicit RequestWorker(const std::string& root_path);
	~RequestWorker();

	void start();
	void stop();
	void submit(const HttpRequest& request);

private:
	void run();
	HttpResponse handle(const HttpRequest& request);

	std::string root_path_;
	std::unique_ptr<zstfs::Markets> markets_;
	std::mutex mutex_;
	std::condition_variable condition_;
	std::queue<HttpRequest> requests_;
	bool stopping_;
	std::thread thread_;
};

class HttpServer {
public:
	HttpServer(const std::string& root_path, unsigned short port);
	~HttpServer();

	int run();
	void stop();

private:
	class Impl;
	std::unique_ptr<Impl> impl_;
};

}  // namespace zstfsd
