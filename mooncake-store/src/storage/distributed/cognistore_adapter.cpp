#include "storage/distributed/cognistore_adapter.h"

#include <algorithm>
#include <cerrno>
#include <climits>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <mutex>
#include <unistd.h>
#include <glog/logging.h>

namespace mooncake {
namespace {
constexpr size_t kBufferSize = 1024 * 1024;
ErrorCode Translate(int64_t rc, ErrorCode fallback) {
    if (rc == -ENOENT) return ErrorCode::FILE_NOT_FOUND;
    if (rc == -EINVAL) return ErrorCode::INVALID_PARAMS;
    if (rc == -ETIMEDOUT) return ErrorCode::DFS_NETWORK_TIMEOUT;
    return fallback;
}
cogni_sdk_bytes_v1 Bytes(const std::string& text) {
    return {reinterpret_cast<const uint8_t*>(text.data()), text.size()};
}
cogni_sdk_path_at_v1 Path(const std::string& text) {
    cogni_sdk_path_at_v1 result{};
    result.has_path = 1;
    result.path = Bytes(text);
    return result;
}
// Every request/result uses the versioned ABI's explicit size contract.
template <typename T>
T Request() {
    T result{};
    result.struct_size = sizeof(result);
    result.options.struct_size = sizeof(result.options);
    return result;
}
struct ResultOwner {
    cogni_sdk_result_v1 value{};
    ResultOwner() { value.struct_size = sizeof(value); }
    ~ResultOwner() { cogni_sdk_result_release_v1(&value); }
};
std::string String(cogni_sdk_bytes_v1 value) {
    return value.size ? std::string(reinterpret_cast<const char*>(value.data),
                                    value.size)
                      : std::string{};
}
}  // namespace

struct CogniStoreFsAdapter::Runtime {
    std::mutex mutex;
    std::string config;
    pid_t pid{::getpid()};
    cogni_sdk sdk{};
    cogni_sdk_iov iov{};
    cogni_sdk_ior read{}, write{};
    void* buffer{nullptr};
    bool poisoned{false};

    int Start(const std::string& path) {
        config = path;
        if (cogni_sdk_abi_version() != COGNI_SDK_ABI_VERSION) return -ENOTSUP;
        int rc = cogni_sdk_create(&sdk, path.c_str(), nullptr);
        if (rc) return rc;
        if (posix_memalign(&buffer, 4096, kBufferSize)) return -ENOMEM;
        // Native registration verifies the NUMA home of resident payload pages.
        std::memset(buffer, 0, kBufferSize);
        int numa = -1;
        if (const char* setting =
                std::getenv("MOONCAKE_COGNISTORE_NUMA_NODE")) {
            char* end = nullptr;
            long parsed = std::strtol(setting, &end, 10);
            if (!*setting || *end || parsed < -1 || parsed > INT_MAX)
                return -EINVAL;
            numa = static_cast<int>(parsed);
        }
        rc = cogni_sdk_iov_register(&sdk, &iov, buffer, kBufferSize,
                                    kBufferSize, numa);
        if (rc) return rc;
        rc = cogni_sdk_ior_create(&sdk, &read, 1, 1, 1, 1, 30000, 0);
        if (rc) return rc;
        return cogni_sdk_ior_create(&sdk, &write, 1, 0, 1, 1, 30000, 0);
    }
    ~Runtime() {
        // Fail closed rather than freeing buffers still owned by an unknown
        // I/O.
        if (read.private_data) CHECK_EQ(cogni_sdk_ior_destroy(&read), 0);
        if (write.private_data) CHECK_EQ(cogni_sdk_ior_destroy(&write), 0);
        if (iov.private_data) CHECK_EQ(cogni_sdk_iov_release(&iov), 0);
        if (sdk.private_data) CHECK_EQ(cogni_sdk_shutdown(&sdk), 0);
        std::free(buffer);
    }
};

CogniStoreFsAdapter::CogniStoreFsAdapter() = default;
CogniStoreFsAdapter::~CogniStoreFsAdapter() {
    CHECK(Shutdown().has_value()) << "CogniStore adapter could not drain";
}

tl::expected<void, ErrorCode> CogniStoreFsAdapter::Init(
    const std::string& root) {
    if (runtime_) return {};
    const char* config = std::getenv("MOONCAKE_COGNISTORE_CONFIG");
    if (!config || !*config || !std::filesystem::path(root).is_absolute())
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    static std::mutex registry_mutex;
    static std::weak_ptr<Runtime> process_runtime;
    {
        std::lock_guard lock(registry_mutex);
        runtime_ = process_runtime.lock();
        if (runtime_) {
            if (runtime_->pid != ::getpid() || runtime_->config != config) {
                runtime_.reset();
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
        } else {
            auto runtime = std::make_shared<Runtime>();
            int rc = runtime->Start(config);
            if (rc) {
                LOG(ERROR) << "CogniStore SDK initialization failed: " << rc;
                return tl::make_unexpected(
                    Translate(rc, ErrorCode::FILE_OPEN_FAIL));
            }
            runtime_ = runtime;
            process_runtime = runtime;
        }
    }
    int rc;
    {
        std::lock_guard lock(runtime_->mutex);
        auto request = Request<cogni_sdk_mkdirs_req_v1>();
        request.path = Path(root);
        request.permissions = 0755;
        request.recursive = 1;
        ResultOwner result;
        cogni_sdk_status_v1 status{};
        rc = cogni_sdk_mkdirs_v1(&runtime_->sdk, &request, &result.value,
                                 &status);
        if (rc == -EEXIST) {
            auto stat = Request<cogni_sdk_stat_req_v1>();
            stat.path = Path(root);
            ResultOwner existing;
            rc = cogni_sdk_stat_v1(&runtime_->sdk, &stat, &existing.value,
                                   &status);
            cogni_sdk_inode_view_v1 view{};
            view.struct_size = sizeof(view);
            if (!rc)
                rc = cogni_sdk_result_inode_v1(&existing.value, &view, &status);
            if (!rc && view.inode.type != 2) rc = -ENOTDIR;
        }
    }
    if (rc) {
        runtime_.reset();
        return tl::make_unexpected(Translate(rc, ErrorCode::FILE_WRITE_FAIL));
    }
    return {};
}

tl::expected<void, ErrorCode> CogniStoreFsAdapter::Shutdown() {
    if (!runtime_) return {};
    {
        std::lock_guard lock(runtime_->mutex);
        for (auto it = files_.begin(); it != files_.end();) {
            int rc = cogni_sdk_file_close(&it->second);
            if (rc)
                return tl::make_unexpected(
                    Translate(rc, ErrorCode::FILE_INVALID_HANDLE));
            it = files_.erase(it);
        }
    }
    runtime_.reset();
    return {};
}

tl::expected<size_t, ErrorCode> CogniStoreFsAdapter::GetFileSize(
    const std::string& path) {
    if (!runtime_) return tl::make_unexpected(ErrorCode::FILE_INVALID_HANDLE);
    std::lock_guard lock(runtime_->mutex);
    uint64_t size = 0;
    int rc = cogni_sdk_file_size(&runtime_->sdk, path.c_str(), &size);
    if (rc)
        return tl::make_unexpected(Translate(rc, ErrorCode::FILE_READ_FAIL));
    if (size > SIZE_MAX) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    return static_cast<size_t>(size);
}

tl::expected<bool, ErrorCode> CogniStoreFsAdapter::FileExists(
    const std::string& path) {
    if (!runtime_) return tl::make_unexpected(ErrorCode::FILE_INVALID_HANDLE);
    std::lock_guard lock(runtime_->mutex);
    auto request = Request<cogni_sdk_stat_req_v1>();
    request.path = Path(path);
    ResultOwner result;
    cogni_sdk_status_v1 status{};
    int rc =
        cogni_sdk_stat_v1(&runtime_->sdk, &request, &result.value, &status);
    if (!rc) return true;
    if (rc == -ENOENT) return false;
    return tl::make_unexpected(Translate(rc, ErrorCode::FILE_READ_FAIL));
}

tl::expected<std::string, ErrorCode> CogniStoreFsAdapter::CanonicalPath(
    const std::string& path) {
    if (!runtime_) return tl::make_unexpected(ErrorCode::FILE_INVALID_HANDLE);
    std::lock_guard lock(runtime_->mutex);
    auto request = Request<cogni_sdk_get_real_path_req_v1>();
    request.path = Path(path);
    request.absolute = 1;
    ResultOwner result;
    cogni_sdk_status_v1 status{};
    int rc = cogni_sdk_get_real_path_v1(&runtime_->sdk, &request, &result.value,
                                        &status);
    cogni_sdk_path_view_v1 view{};
    view.struct_size = sizeof(view);
    if (!rc) rc = cogni_sdk_result_path_v1(&result.value, &view, &status);
    if (rc)
        return tl::make_unexpected(Translate(rc, ErrorCode::FILE_OPEN_FAIL));
    return String(view.path);
}

tl::expected<std::vector<std::string>, ErrorCode>
CogniStoreFsAdapter::ListFiles(const std::string& path) {
    if (!runtime_) return tl::make_unexpected(ErrorCode::FILE_INVALID_HANDLE);
    std::lock_guard lock(runtime_->mutex);
    std::vector<std::string> names;
    std::string previous;
    for (;;) {
        auto request = Request<cogni_sdk_list_req_v1>();
        request.path = Path(path);
        request.previous = Bytes(previous);
        request.limit = 128;
        ResultOwner result;
        cogni_sdk_status_v1 status{};
        int rc =
            cogni_sdk_list_v1(&runtime_->sdk, &request, &result.value, &status);
        cogni_sdk_list_view_v1 view{};
        view.struct_size = sizeof(view);
        if (!rc) rc = cogni_sdk_result_list_v1(&result.value, &view, &status);
        if (rc)
            return tl::make_unexpected(
                Translate(rc, ErrorCode::FILE_READ_FAIL));
        for (uint64_t i = 0; i < view.entry_count; ++i) {
            // C ABI inode types: file=1, directory=2, symlink=3.
            if (view.entries[i].type != 2)
                names.push_back(String(view.entries[i].name));
        }
        if (!view.more) return names;
        if (!view.entry_count)
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        auto next = String(view.entries[view.entry_count - 1].name);
        if (next <= previous)
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        previous = std::move(next);
    }
}

tl::expected<void, ErrorCode> CogniStoreFsAdapter::DeleteFile(
    const std::string& path) {
    if (!runtime_) return tl::make_unexpected(ErrorCode::FILE_INVALID_HANDLE);
    std::lock_guard lock(runtime_->mutex);
    int rc = cogni_sdk_remove(&runtime_->sdk, path.c_str());
    if (rc)
        return tl::make_unexpected(Translate(rc, ErrorCode::FILE_WRITE_FAIL));
    return {};
}

tl::expected<int, ErrorCode> CogniStoreFsAdapter::Open(const std::string& path,
                                                       int flags) {
    if (!runtime_) return tl::make_unexpected(ErrorCode::FILE_INVALID_HANDLE);
    std::lock_guard lock(runtime_->mutex);
    if (next_fd_ == INT_MAX)
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    // Allocate the map node before acquiring the SDK resource.
    auto [it, inserted] = files_.emplace(next_fd_, cogni_sdk_file{});
    int rc = cogni_sdk_file_open(&runtime_->sdk, &it->second, path.c_str(),
                                 flags, 0644);
    if (rc) {
        files_.erase(it);
        return tl::make_unexpected(Translate(rc, ErrorCode::FILE_OPEN_FAIL));
    }
    return next_fd_++;
}
tl::expected<int, ErrorCode> CogniStoreFsAdapter::OpenFile(
    const std::string& path) {
    return Open(path, O_RDWR | O_CREAT);
}
tl::expected<int, ErrorCode> CogniStoreFsAdapter::OpenExistingFile(
    const std::string& path) {
    return Open(path, O_RDWR);
}
tl::expected<int, ErrorCode> CogniStoreFsAdapter::OpenExistingFileForRead(
    const std::string& path) {
    return Open(path, O_RDONLY);
}
tl::expected<void, ErrorCode> CogniStoreFsAdapter::CloseFile(int fd) {
    if (!runtime_) return tl::make_unexpected(ErrorCode::FILE_INVALID_HANDLE);
    std::lock_guard lock(runtime_->mutex);
    auto it = files_.find(fd);
    if (it == files_.end())
        return tl::make_unexpected(ErrorCode::FILE_INVALID_HANDLE);
    int rc = cogni_sdk_file_close(&it->second);
    if (rc)
        return tl::make_unexpected(
            Translate(rc, ErrorCode::FILE_INVALID_HANDLE));
    files_.erase(it);
    return {};
}

tl::expected<size_t, ErrorCode> CogniStoreFsAdapter::Transfer(
    int fd, const iovec* vectors, int count, int64_t offset, bool read) {
    if (!runtime_) return tl::make_unexpected(ErrorCode::FILE_INVALID_HANDLE);
    if (count < 0 || offset < 0 || (count && !vectors))
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    size_t total = 0;
    for (int i = 0; i < count; ++i) {
        if ((vectors[i].iov_len && !vectors[i].iov_base) ||
            vectors[i].iov_len > size_t(INT64_MAX - offset) - total)
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        total += vectors[i].iov_len;
    }
    std::lock_guard lock(runtime_->mutex);
    auto it = files_.find(fd);
    if (it == files_.end())
        return tl::make_unexpected(ErrorCode::FILE_INVALID_HANDLE);
    const auto failure =
        read ? ErrorCode::FILE_READ_FAIL : ErrorCode::FILE_WRITE_FAIL;
    if (runtime_->poisoned) return tl::make_unexpected(failure);
    auto* ring = read ? &runtime_->read : &runtime_->write;
    size_t done = 0;
    for (int i = 0; i < count; ++i) {
        size_t part = 0;
        while (part < vectors[i].iov_len) {
            size_t n = std::min(kBufferSize, vectors[i].iov_len - part);
            auto* user = static_cast<char*>(vectors[i].iov_base) + part;
            if (!read) std::memcpy(runtime_->buffer, user, n);
            int index = cogni_sdk_prep_io(ring, &it->second, &runtime_->iov,
                                          offset + done, 0, n, nullptr);
            if (index < 0)
                return tl::make_unexpected(Translate(index, failure));
            int rc = cogni_sdk_submit_ios(ring);
            if (rc) {
                runtime_->poisoned = true;
                return tl::make_unexpected(Translate(rc, failure));
            }
            cogni_sdk_cqe cqe{};
            rc = cogni_sdk_wait_for_ios(ring, &cqe, 1, 1, nullptr);
            if (rc != 1 || cqe.index != index) {
                runtime_->poisoned = true;
                return tl::make_unexpected(failure);
            }
            if (cqe.result < 0)
                return tl::make_unexpected(Translate(cqe.result, failure));
            if (static_cast<uint64_t>(cqe.result) > n) {
                runtime_->poisoned = true;
                return tl::make_unexpected(failure);
            }
            if (read)
                std::memcpy(user, runtime_->buffer,
                            static_cast<size_t>(cqe.result));
            done += cqe.result;
            part += cqe.result;
            if (static_cast<size_t>(cqe.result) != n) {
                if (!read) return tl::make_unexpected(failure);
                return done;
            }
        }
    }
    if (!read && done) {
        int rc = cogni_sdk_file_fsync(&it->second);
        if (rc) return tl::make_unexpected(Translate(rc, failure));
    }
    return done;
}
tl::expected<size_t, ErrorCode> CogniStoreFsAdapter::WriteAt(int fd,
                                                             const iovec* v,
                                                             int n, int64_t o) {
    return Transfer(fd, v, n, o, false);
}
tl::expected<size_t, ErrorCode> CogniStoreFsAdapter::ReadAt(int fd, iovec* v,
                                                            int n, int64_t o) {
    return Transfer(fd, v, n, o, true);
}

tl::expected<void, ErrorCode> CogniStoreFsAdapter::PreallocateFile(
    const std::string& path, uint64_t size) {
    if (!size || size > INT64_MAX)
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    auto opened = Open(path, O_RDWR | O_CREAT | O_EXCL);
    if (!opened) return tl::make_unexpected(opened.error());
    std::vector<char> zeros(std::min<uint64_t>(size, kBufferSize), 0);
    tl::expected<void, ErrorCode> result;
    for (uint64_t offset = 0; offset < size;) {
        iovec v{zeros.data(), static_cast<size_t>(std::min<uint64_t>(
                                  zeros.size(), size - offset))};
        auto written = WriteAt(*opened, &v, 1, static_cast<int64_t>(offset));
        if (!written || *written != v.iov_len) {
            result = tl::make_unexpected(written ? ErrorCode::FILE_WRITE_FAIL
                                                 : written.error());
            break;
        }
        offset += *written;
    }
    auto closed = CloseFile(*opened);
    if (!result) return result;
    if (!closed) return closed;
    auto actual = GetFileSize(path);
    if (!actual) return tl::make_unexpected(actual.error());
    if (*actual != size) return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    return {};
}
}  // namespace mooncake
