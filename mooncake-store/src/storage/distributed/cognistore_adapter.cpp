#include "storage/distributed/cognistore_adapter.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <condition_variable>
#include <chrono>
#include <sstream>
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
constexpr size_t kBatchEntries = 8;
constexpr size_t kMaxStripeBytes = 16 * 1024 * 1024;
constexpr size_t kDefaultStripeContexts = 4;
constexpr size_t kMaxWriteDepth = 16;
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
// Opt-in aggregates: times are summed across callers, not phase wall time.
// Emit once after draining the Runtime; never log in an I/O's hot loop.
struct AdapterProfile {
    enum Phase {
        PathLock,
        PathSdk,
        OpenLock,
        OpenSdk,
        CloseLock,
        CloseIdle,
        CloseSdk,
        ReadLock,
        ReadIdle,
        ReadSetup,
        ReadWait,
        ReadCopy,
        ReadReleaseLock,
        ReadTotal,
        BucketCreate,
        Count
    };
    static constexpr const char* names[Count] = {
        "path_lock",         "path_sdk",   "open_lock",    "open_sdk",
        "close_lock",        "close_idle", "close_sdk",    "read_lock",
        "read_idle",         "read_setup", "read_wait",    "read_copy",
        "read_release_lock", "read_total", "bucket_create"};
    struct Counter {
        std::atomic<uint64_t> calls{0}, ns{0};
    };
    const bool enabled = [] {
        const auto* value = std::getenv("MOONCAKE_COGNISTORE_PROFILE");
        return value && std::strcmp(value, "1") == 0;
    }();
    std::array<Counter, Count> counters;
    std::atomic<uint64_t> copied_bytes{0};
    struct Scope {
        using Clock = std::chrono::steady_clock;
        Counter* counter;
        Clock::time_point start;
        Scope(AdapterProfile& profile, Phase phase, bool read = true)
            : counter(profile.enabled && read ? &profile.counters[phase]
                                              : nullptr),
              start(counter ? Clock::now() : Clock::time_point{}) {}
        Scope(const Scope&) = delete;
        Scope& operator=(const Scope&) = delete;
        void Stop() {
            if (!counter) return;
            counter->ns.fetch_add(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    Clock::now() - start)
                    .count(),
                std::memory_order_relaxed);
            counter->calls.fetch_add(1, std::memory_order_relaxed);
            counter = nullptr;
        }
        ~Scope() { Stop(); }
    };
    void Report() const {
        if (!enabled) return;
        std::ostringstream output;
        output << "COGNISTORE_ADAPTER_PROFILE {\"pid\":" << ::getpid()
               << ",\"copied_bytes\":" << copied_bytes.load()
               << ",\"stages\":{";
        for (size_t i = 0; i < Count; ++i) {
            if (i) output << ',';
            output << '"' << names[i]
                   << "\":{\"calls\":" << counters[i].calls.load()
                   << ",\"ns\":" << counters[i].ns.load() << '}';
        }
        output << "}}";
        LOG(INFO) << output.str();
    }
};
using ProfileScope = AdapterProfile::Scope;
}  // namespace

struct CogniStoreFsAdapter::Runtime {
    std::mutex mutex;
    // Serialize shared SDK-handle operations without blocking File/IOR leases.
    std::mutex sdk_mutex;
    AdapterProfile profile;
    std::string config;
    pid_t pid{::getpid()};
    cogni_sdk sdk{};
    cogni_sdk_iov iov{};
    cogni_sdk_ior read{}, write{}, batch_read{}, batch_write{};
    void* buffer{nullptr};
    struct StripeIo {
        cogni_sdk_iov iov{};
        cogni_sdk_ior read{}, write{};
        void* buffer{nullptr};
        bool busy{false};

        int Start(cogni_sdk& sdk, int numa) {
            if (iov.private_data && read.private_data && write.private_data)
                return 0;
            // A failed registration can retain an unknown owner until shutdown.
            if (buffer) return -EIO;
            if (posix_memalign(&buffer, 4096, kMaxStripeBytes)) return -ENOMEM;
            std::memset(buffer, 0, kMaxStripeBytes);
            int rc = cogni_sdk_iov_register(&sdk, &iov, buffer, kMaxStripeBytes,
                                            kMaxStripeBytes, numa);
            if (rc) return rc;
            rc = cogni_sdk_ior_create(&sdk, &read, 1, 1, 1, 1, 30000, 0);
            if (rc) return rc;
            return cogni_sdk_ior_create(&sdk, &write, 1, 0, 1, 1, 30000, 0);
        }
        void Drain() {
            if (read.private_data) CHECK_EQ(cogni_sdk_ior_destroy(&read), 0);
            if (write.private_data) CHECK_EQ(cogni_sdk_ior_destroy(&write), 0);
            if (iov.private_data) CHECK_EQ(cogni_sdk_iov_release(&iov), 0);
        }
    };
    // Independent producers share the SDK, but never an active buffer or IOR.
    std::array<StripeIo, kMaxWriteDepth> stripes;
    size_t write_depth{kDefaultStripeContexts};
    std::condition_variable idle;
    bool coalesced_read_busy{false};
    int numa{-1};
    std::atomic<bool> poisoned{false};

    int Start(const std::string& path) {
        config = path;
        if (const char* setting =
                std::getenv("MOONCAKE_COGNISTORE_WRITE_DEPTH")) {
            char* end = nullptr;
            long parsed = std::strtol(setting, &end, 10);
            if (!*setting || *end || parsed < 1 ||
                parsed > static_cast<long>(kMaxWriteDepth))
                return -EINVAL;
            write_depth = static_cast<size_t>(parsed);
        }
        if (cogni_sdk_abi_version() != COGNI_SDK_ABI_VERSION) return -ENOTSUP;
        int rc = cogni_sdk_create(&sdk, path.c_str(), nullptr);
        if (rc) return rc;
        if (posix_memalign(&buffer, 4096, kBufferSize * kBatchEntries))
            return -ENOMEM;
        // Native registration verifies the NUMA home of resident payload pages.
        std::memset(buffer, 0, kBufferSize * kBatchEntries);
        if (const char* setting =
                std::getenv("MOONCAKE_COGNISTORE_NUMA_NODE")) {
            char* end = nullptr;
            long parsed = std::strtol(setting, &end, 10);
            if (!*setting || *end || parsed < -1 || parsed > INT_MAX)
                return -EINVAL;
            numa = static_cast<int>(parsed);
        }
        rc = cogni_sdk_iov_register(&sdk, &iov, buffer, kBufferSize * kBatchEntries,
                                    kBufferSize, numa);
        if (rc) return rc;
        rc = cogni_sdk_ior_create(&sdk, &read, 1, 1, 1, 1, 30000, 0);
        if (rc) return rc;
        rc = cogni_sdk_ior_create(&sdk, &write, 1, 0, 1, 1, 30000, 0);
        if (rc) return rc;
        // Negative depth flushes an incomplete batch after 1 ms. Positive
        // depth would leave the final short batch waiting for missing entries.
        rc = cogni_sdk_ior_create(&sdk, &batch_read, kBatchEntries, 1,
                                  -int(kBatchEntries), 1, 1, 0);
        if (rc) return rc;
        return cogni_sdk_ior_create(&sdk, &batch_write, kBatchEntries, 0,
                                    -int(kBatchEntries), 1, 1, 0);
    }
    ~Runtime() {
        // Fail closed rather than freeing buffers still owned by an unknown
        // I/O.
        if (batch_read.private_data) CHECK_EQ(cogni_sdk_ior_destroy(&batch_read), 0);
        if (batch_write.private_data) CHECK_EQ(cogni_sdk_ior_destroy(&batch_write), 0);
        if (read.private_data) CHECK_EQ(cogni_sdk_ior_destroy(&read), 0);
        if (write.private_data) CHECK_EQ(cogni_sdk_ior_destroy(&write), 0);
        for (auto& stripe : stripes) stripe.Drain();
        if (iov.private_data) CHECK_EQ(cogni_sdk_iov_release(&iov), 0);
        if (sdk.private_data) CHECK_EQ(cogni_sdk_shutdown(&sdk), 0);
        std::free(buffer);
        for (auto& stripe : stripes) std::free(stripe.buffer);
        profile.Report();
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
        std::lock_guard lock(runtime_->sdk_mutex);
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
        std::unique_lock lock(runtime_->mutex);
        for (auto& [fd, file] : files_) file.closing = true;
        runtime_->idle.notify_all();
        runtime_->idle.wait(lock, [&] {
            return std::all_of(
                files_.begin(), files_.end(),
                [](const auto& item) { return item.second.active == 0; });
        });
        for (auto it = files_.begin(); it != files_.end();) {
            int rc = cogni_sdk_file_close(&it->second.sdk);
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
    std::lock_guard lock(runtime_->sdk_mutex);
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
    std::lock_guard lock(runtime_->sdk_mutex);
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
    ProfileScope lock_wait(runtime_->profile, AdapterProfile::PathLock);
    std::lock_guard lock(runtime_->sdk_mutex);
    lock_wait.Stop();
    auto request = Request<cogni_sdk_get_real_path_req_v1>();
    request.path = Path(path);
    request.absolute = 1;
    ResultOwner result;
    cogni_sdk_status_v1 status{};
    ProfileScope sdk_time(runtime_->profile, AdapterProfile::PathSdk);
    int rc = cogni_sdk_get_real_path_v1(&runtime_->sdk, &request, &result.value,
                                        &status);
    sdk_time.Stop();
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
    std::lock_guard lock(runtime_->sdk_mutex);
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
    std::lock_guard lock(runtime_->sdk_mutex);
    int rc = cogni_sdk_remove(&runtime_->sdk, path.c_str());
    if (rc)
        return tl::make_unexpected(Translate(rc, ErrorCode::FILE_WRITE_FAIL));
    return {};
}

tl::expected<int, ErrorCode> CogniStoreFsAdapter::Open(
    const std::string& path, int flags, std::optional<uint64_t> expected_size) {
    if (!runtime_) return tl::make_unexpected(ErrorCode::FILE_INVALID_HANDLE);
    std::unique_lock lock(runtime_->mutex);
    if (next_fd_ == INT_MAX)
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    // Reserve the ID before open: a failed close after validation must retain
    // its SDK owner for Shutdown without colliding with a later open.
    const int fd = next_fd_++;
    auto [it, inserted] = files_.emplace(fd, File{});
    auto* file = &it->second;
    ++file->active;
    // The fd is private until Open returns. Pin it for Shutdown while the
    // shared SDK handle waits for metadata, without blocking unrelated I/O.
    lock.unlock();
    int rc;
    {
        ProfileScope lock_wait(runtime_->profile, AdapterProfile::OpenLock);
        std::lock_guard sdk_lock(runtime_->sdk_mutex);
        lock_wait.Stop();
        ProfileScope sdk_time(runtime_->profile, AdapterProfile::OpenSdk);
        rc = cogni_sdk_file_open(&runtime_->sdk, &file->sdk, path.c_str(),
                                 flags, 0644);
    }
    lock.lock();
    --file->active;
    runtime_->idle.notify_all();
    if (rc) {
        files_.erase(fd);
        return tl::make_unexpected(Translate(rc, ErrorCode::FILE_OPEN_FAIL));
    }
    ResultOwner metadata;
    cogni_sdk_status_v1 status{};
    cogni_sdk_inode_view_v1 view{};
    view.struct_size = sizeof(view);
    rc = cogni_sdk_file_metadata_v1(&file->sdk, &metadata.value, &status);
    if (!rc) rc = cogni_sdk_result_inode_v1(&metadata.value, &view, &status);
    if (rc || (expected_size && view.inode.size != *expected_size)) {
        const auto error = rc ? Translate(rc, ErrorCode::FILE_OPEN_FAIL)
                              : ErrorCode::INVALID_PARAMS;
        if (!cogni_sdk_file_close(&file->sdk)) files_.erase(fd);
        return tl::make_unexpected(error);
    }
    if ((flags & O_ACCMODE) != O_RDONLY) {
        // This is the just-opened File's observation, not a cached path stat.
        // Actual I/O still resolves and validates the current stripe layout.
        const auto& layout = view.inode.layout;
        const uint64_t bytes =
            uint64_t(layout.chunk_size) * layout.data_fragments;
        if (layout.parity_fragments && bytes && bytes <= kMaxStripeBytes)
            file->stripe_bytes = size_t(bytes);
    }
    return fd;
}
tl::expected<int, ErrorCode> CogniStoreFsAdapter::OpenValidatedFile(
    const std::string& path, uint64_t expected_size,
    const std::string& canonical_root, bool write) {
    auto canonical = CanonicalPath(path);
    if (!canonical) return tl::make_unexpected(canonical.error());
    const std::filesystem::path resolved(*canonical);
    if (resolved.parent_path() != std::filesystem::path(canonical_root) ||
        resolved.filename() != std::filesystem::path(path).filename())
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    return Open(*canonical, write ? O_RDWR : O_RDONLY, expected_size);
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
    ProfileScope lock_wait(runtime_->profile, AdapterProfile::CloseLock);
    std::unique_lock lock(runtime_->mutex);
    lock_wait.Stop();
    auto it = files_.find(fd);
    if (it == files_.end() || it->second.closing)
        return tl::make_unexpected(ErrorCode::FILE_INVALID_HANDLE);
    auto* file = &it->second;
    file->closing = true;
    runtime_->idle.notify_all();
    ProfileScope idle_time(runtime_->profile, AdapterProfile::CloseIdle);
    runtime_->idle.wait(lock, [&] { return file->active == 0; });
    idle_time.Stop();
    // Closing fences this File. Retain its map entry while the SDK drains it,
    // so Shutdown waits, but unrelated handles can make progress meanwhile.
    ++file->active;
    lock.unlock();
    ProfileScope sdk_time(runtime_->profile, AdapterProfile::CloseSdk);
    int rc = cogni_sdk_file_close(&file->sdk);
    sdk_time.Stop();
    lock.lock();
    --file->active;
    runtime_->idle.notify_all();
    if (rc) {
        file->closing = false;
        return tl::make_unexpected(
            Translate(rc, ErrorCode::FILE_INVALID_HANDLE));
    }
    files_.erase(fd);
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
    ProfileScope lock_wait(runtime_->profile, AdapterProfile::ReadLock, read);
    std::unique_lock lock(runtime_->mutex);
    lock_wait.Stop();
    ProfileScope idle_time(runtime_->profile, AdapterProfile::ReadIdle, read);
    runtime_->idle.wait(lock, [&] {
        auto current = files_.find(fd);
        return current == files_.end() || current->second.closing ||
               !current->second.active;
    });
    idle_time.Stop();
    auto it = files_.find(fd);
    if (it == files_.end() || it->second.closing)
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
            int index = cogni_sdk_prep_io(ring, &it->second.sdk, &runtime_->iov,
                                          offset + done, 0, n, nullptr);
            if (index < 0)
                return tl::make_unexpected(Translate(index, failure));
            int rc = cogni_sdk_submit_ios(ring);
            if (rc) {
                runtime_->poisoned = true;
                return tl::make_unexpected(Translate(rc, failure));
            }
            cogni_sdk_cqe cqe{};
            ProfileScope wait_time(runtime_->profile, AdapterProfile::ReadWait,
                                   read);
            rc = cogni_sdk_wait_for_ios(ring, &cqe, 1, 1, nullptr);
            wait_time.Stop();
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
            if (read) {
                ProfileScope copy_time(runtime_->profile,
                                       AdapterProfile::ReadCopy);
                std::memcpy(user, runtime_->buffer,
                            static_cast<size_t>(cqe.result));
                if (runtime_->profile.enabled)
                    runtime_->profile.copied_bytes += cqe.result;
            }
            done += cqe.result;
            part += cqe.result;
            if (static_cast<size_t>(cqe.result) != n) {
                if (!read) return tl::make_unexpected(failure);
                return done;
            }
        }
    }
    if (!read && done) {
        int rc = cogni_sdk_file_fsync(&it->second.sdk);
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

std::optional<std::vector<tl::expected<size_t, ErrorCode>>>
CogniStoreFsAdapter::CoalescedTransfer(
    std::span<const FdIoRequest> requests, bool read) {
    if (requests.empty() || !runtime_) return std::nullopt;
    ProfileScope lock_wait(runtime_->profile, AdapterProfile::ReadLock, read);
    std::unique_lock lock(runtime_->mutex);
    lock_wait.Stop();
    auto file = files_.find(requests.front().fd);
    if (file == files_.end() || (!read && !file->second.stripe_bytes))
        return std::nullopt;
    const auto offset = requests.front().offset;
    if (offset < 0 || (!read && uint64_t(offset) % file->second.stripe_bytes))
        return std::nullopt;
    std::vector<tl::expected<size_t, ErrorCode>> results;
    results.reserve(requests.size());
    size_t total = 0;
    void* read_destination = nullptr;
    uintptr_t next_destination = 0;
    bool contiguous_destination = true;
    for (const auto& r : requests) {
        if (r.fd != requests.front().fd || r.offset < 0 ||
            uint64_t(r.offset) != uint64_t(offset) + total || r.iovcnt <= 0 ||
            !r.iov)
            return std::nullopt;
        size_t length = 0;
        for (int i = 0; i < r.iovcnt; ++i) {
            if ((r.iov[i].iov_len && !r.iov[i].iov_base) ||
                r.iov[i].iov_len > kMaxStripeBytes - total)
                return std::nullopt;
            if (read && r.iov[i].iov_len) {
                const auto address =
                    reinterpret_cast<uintptr_t>(r.iov[i].iov_base);
                if (!read_destination)
                    read_destination = r.iov[i].iov_base;
                else if (address != next_destination)
                    contiguous_destination = false;
                next_destination = address + r.iov[i].iov_len;
            }
            length += r.iov[i].iov_len;
            total += r.iov[i].iov_len;
        }
        if (!length) return std::nullopt;
        results.emplace_back(length);
    }
    if ((!read && total % file->second.stripe_bytes) ||
        (read && total <= kBufferSize) || total > size_t(INT64_MAX - offset))
        return std::nullopt;
    const auto failure =
        read ? ErrorCode::FILE_READ_FAIL : ErrorCode::FILE_WRITE_FAIL;
    auto fail = [&](int64_t rc) {
        for (auto& result : results)
            result = tl::make_unexpected(Translate(rc, failure));
        return results;
    };
    Runtime::StripeIo* stripe = nullptr;
    const int fd = requests.front().fd;
    ProfileScope idle_time(runtime_->profile, AdapterProfile::ReadIdle, read);
    runtime_->idle.wait(lock, [&] {
        auto current = files_.find(fd);
        if (runtime_->poisoned || current == files_.end() ||
            current->second.closing)
            return true;
        // The C ABI requires external synchronization for each File handle.
        if (current->second.active || (read && runtime_->coalesced_read_busy))
            return false;
        // Write depth does not expand the read/copy buffer pool or admit
        // additional SDK reads. Both directions retain exclusive leases.
        const size_t contexts =
            read ? kDefaultStripeContexts : runtime_->write_depth;
        for (size_t i = 0; i < contexts; ++i) {
            auto& candidate = runtime_->stripes[i];
            if (!candidate.busy) {
                stripe = &candidate;
                return true;
            }
        }
        return false;
    });
    idle_time.Stop();
    if (runtime_->poisoned) return fail(-EIO);
    file = files_.find(fd);
    if (file == files_.end() || file->second.closing) return fail(-EBADF);
    // Map element addresses survive rehash. Close waits for this retained use.
    auto* retained = &file->second;
    ++retained->active;
    stripe->busy = true;
    bool owns_read_admission = read;
    if (read) runtime_->coalesced_read_busy = true;
    auto release = [&](Runtime::StripeIo* used) {
        ProfileScope release_wait(runtime_->profile,
                                  AdapterProfile::ReadReleaseLock, read);
        if (!lock.owns_lock()) lock.lock();
        release_wait.Stop();
        --retained->active;
        used->busy = false;
        if (owns_read_admission) runtime_->coalesced_read_busy = false;
        runtime_->idle.notify_all();
    };
    std::unique_ptr<Runtime::StripeIo, decltype(release)> lease(stripe,
                                                                release);
    // Let metadata operations on other handles proceed during SDK waits and
    // scatter copies. The lease retains this File, IOR and registered buffer.
    lock.unlock();
    int rc = 0;
    if (!stripe->write.private_data) {
        // Always release the state lock before acquiring the shared SDK lock.
        // The lease pins this File and StripeIo throughout lazy registration.
        std::lock_guard sdk_lock(runtime_->sdk_mutex);
        ProfileScope setup_time(runtime_->profile, AdapterProfile::ReadSetup,
                                read);
        rc = stripe->Start(runtime_->sdk, runtime_->numa);
    }
    if (rc) return fail(rc);
    if (!read) {
        size_t copied = 0;
        for (const auto& r : requests) {
            for (int i = 0; i < r.iovcnt; ++i) {
                if (r.iov[i].iov_len)
                    std::memcpy(static_cast<char*>(stripe->buffer) + copied,
                                r.iov[i].iov_base, r.iov[i].iov_len);
                copied += r.iov[i].iov_len;
            }
        }
    }
    // One range operation. Writes use only exact caller bytes, without padding.
    // Contiguous reads use one SDK request even across adjacent KV boundaries.
    auto* ring = read ? &stripe->read : &stripe->write;
    int index = cogni_sdk_prep_io(ring, &retained->sdk, &stripe->iov, offset, 0,
                                  total, nullptr);
    if (index < 0) return fail(index);
    rc = cogni_sdk_submit_ios(ring);
    if (rc) {
        runtime_->poisoned = true;
        return fail(rc);
    }
    cogni_sdk_cqe cqe{};
    ProfileScope wait_time(runtime_->profile, AdapterProfile::ReadWait, read);
    rc = cogni_sdk_wait_for_ios(ring, &cqe, 1, 1, nullptr);
    wait_time.Stop();
    if (rc != 1 || cqe.index != index || cqe.result > int64_t(total)) {
        runtime_->poisoned = true;
        return fail(-EIO);
    }
    if (cqe.result < 0) return fail(cqe.result);
    if (read) {
        // The CQE releases SDK ownership. Admit the next independent reader
        // while this caller copies, retaining this File and buffer until done.
        // The next reader must use another free StripeIo; never release its
        // admission when this older transfer's lease later goes out of scope.
        {
            ProfileScope release_wait(runtime_->profile,
                                      AdapterProfile::ReadReleaseLock);
            lock.lock();
            release_wait.Stop();
            runtime_->coalesced_read_busy = false;
            owns_read_admission = false;
            runtime_->idle.notify_all();
            lock.unlock();
        }
        ProfileScope copy_time(runtime_->profile, AdapterProfile::ReadCopy);
        if (runtime_->profile.enabled)
            runtime_->profile.copied_bytes += cqe.result;
        // Adjacent caller ranges are one destination extent. A single copy
        // lets libc choose its large-transfer path without changing EOF
        // lengths.
        if (contiguous_destination && cqe.result)
            std::memcpy(read_destination, stripe->buffer, size_t(cqe.result));
        size_t copied = 0;
        for (size_t i = 0; i < requests.size(); ++i) {
            const auto bytes = std::min(*results[i], size_t(cqe.result) - copied);
            if (contiguous_destination) {
                results[i] = bytes;
                copied += bytes;
                continue;
            }
            size_t remaining = bytes;
            for (int j = 0; j < requests[i].iovcnt && remaining; ++j) {
                const auto& v = requests[i].iov[j];
                const auto n = std::min(v.iov_len, remaining);
                if (n)
                    std::memcpy(v.iov_base,
                                static_cast<char*>(stripe->buffer) + copied, n);
                copied += n;
                remaining -= n;
            }
            // EOF affects each KV independently; leave bytes past EOF untouched.
            results[i] = bytes;
        }
        VLOG(1) << "CogniStore coalesced read bytes=" << cqe.result
                << " requests=" << requests.size() << " offset=" << offset;
        return results;
    }
    if (size_t(cqe.result) != total) return fail(-EIO);
    // This File handle remains exclusive through the fsync acknowledgement.
    rc = cogni_sdk_file_fsync(&retained->sdk);
    if (rc) return fail(rc);
    VLOG(1) << "CogniStore full-stripe write bytes=" << total
            << " requests=" << requests.size() << " offset=" << offset;
    return results;
}

std::vector<tl::expected<size_t, ErrorCode>> CogniStoreFsAdapter::BatchWriteAt(
    std::span<const FdIoRequest> requests) {
    if (auto combined = CoalescedTransfer(requests, false))
        return std::move(*combined);
    return BatchTransfer(requests, false);
}
std::vector<tl::expected<size_t, ErrorCode>> CogniStoreFsAdapter::BatchReadAt(
    std::span<const FdIoRequest> requests) {
    if (!runtime_) return BatchTransfer(requests, true);
    ProfileScope total(runtime_->profile, AdapterProfile::ReadTotal);
    if (auto combined = CoalescedTransfer(requests, true))
        return std::move(*combined);
    return BatchTransfer(requests, true);
}
std::vector<tl::expected<size_t, ErrorCode>> CogniStoreFsAdapter::BatchTransfer(
    std::span<const FdIoRequest> requests, bool read) {
    const auto failure =
        read ? ErrorCode::FILE_READ_FAIL : ErrorCode::FILE_WRITE_FAIL;
    std::vector<tl::expected<size_t, ErrorCode>> results(
        requests.size(), tl::make_unexpected(failure));
    for (size_t start = 0; start < requests.size(); start += kBatchEntries) {
        const auto wave = requests.subspan(
            start, std::min(kBatchEntries, requests.size() - start));
        std::array<size_t, kBatchEntries> lengths{};
        bool eligible = wave.size() > 1;
        for (size_t i = 0; i < wave.size() && eligible; ++i) {
            const auto& r = wave[i];
            eligible = r.offset >= 0 && r.iovcnt > 0 && r.iov;
            for (int j = 0; j < r.iovcnt && eligible; ++j) {
                eligible = (!r.iov[j].iov_len || r.iov[j].iov_base) &&
                           r.iov[j].iov_len <= kBufferSize - lengths[i];
                if (eligible) lengths[i] += r.iov[j].iov_len;
            }
            eligible = eligible && lengths[i] &&
                       lengths[i] <= size_t(INT64_MAX - r.offset);
            // Preserve the scalar ordering contract for overlapping writes.
            for (size_t j = 0; !read && j < i && eligible; ++j) {
                if (r.fd == wave[j].fd && r.offset < wave[j].offset + int64_t(lengths[j]) &&
                    wave[j].offset < r.offset + int64_t(lengths[i])) eligible = false;
            }
        }
        // Oversized, empty or overlapping requests keep the scalar semantics.
        if (!eligible || !runtime_) {
            for (size_t i = 0; i < wave.size(); ++i) {
                const auto& r = wave[i];
                results[start + i] = Transfer(r.fd, r.iov, r.iovcnt, r.offset, read);
            }
            continue;
        }
        ProfileScope lock_wait(runtime_->profile, AdapterProfile::ReadLock,
                               read);
        std::unique_lock lock(runtime_->mutex);
        lock_wait.Stop();
        ProfileScope idle_time(runtime_->profile, AdapterProfile::ReadIdle,
                               read);
        runtime_->idle.wait(lock, [&] {
            return std::all_of(
                wave.begin(), wave.end(), [&](const auto& request) {
                    auto file = files_.find(request.fd);
                    return file == files_.end() || file->second.closing ||
                           !file->second.active;
                });
        });
        idle_time.Stop();
        if (runtime_->poisoned) break;
        auto* ring = read ? &runtime_->batch_read : &runtime_->batch_write;
        std::array<int, kBatchEntries> indices;
        indices.fill(-1);
        size_t pending = 0;
        for (size_t i = 0; i < wave.size(); ++i) {
            const auto& r = wave[i];
            auto file = files_.find(r.fd);
            if (file == files_.end() || file->second.closing) {
                results[start + i] = tl::make_unexpected(ErrorCode::FILE_INVALID_HANDLE);
                continue;
            }
            auto* buffer = static_cast<char*>(runtime_->buffer) + i * kBufferSize;
            size_t offset = 0;
            if (!read) {
                for (int j = 0; j < r.iovcnt; ++j) {
                    if (r.iov[j].iov_len)
                        std::memcpy(buffer + offset, r.iov[j].iov_base, r.iov[j].iov_len);
                    offset += r.iov[j].iov_len;
                }
            }
            int index = cogni_sdk_prep_io(ring, &file->second.sdk, &runtime_->iov,
                                         r.offset, i * kBufferSize, lengths[i], nullptr);
            if (index < 0) {
                results[start + i] = tl::make_unexpected(Translate(index, failure));
            } else {
                indices[i] = index;
                ++pending;
            }
        }
        // Even if a later prep fails, drain every accepted entry before the
        // registered slots can be reused or their file handles can be closed.
        if (pending && cogni_sdk_submit_ios(ring)) runtime_->poisoned = true;
        while (pending && !runtime_->poisoned) {
            std::array<cogni_sdk_cqe, kBatchEntries> cqes{};
            ProfileScope wait_time(runtime_->profile, AdapterProfile::ReadWait,
                                   read);
            int count = cogni_sdk_wait_for_ios(ring, cqes.data(), pending, 1, nullptr);
            wait_time.Stop();
            if (count <= 0 || size_t(count) > pending) {
                runtime_->poisoned = true;
                break;
            }
            for (int j = 0; j < count; ++j) {
                auto found = std::find(indices.begin(), indices.end(), cqes[j].index);
                if (cqes[j].index < 0 || found == indices.end()) {
                    runtime_->poisoned = true;
                    break;
                }
                const auto i = size_t(found - indices.begin());
                *found = -1;
                --pending;
                const auto n = cqes[j].result;
                if (n < 0) results[start + i] = tl::make_unexpected(Translate(n, failure));
                else if (uint64_t(n) > lengths[i]) runtime_->poisoned = true;
                else if (!read && size_t(n) != lengths[i]) results[start + i] = tl::make_unexpected(failure);
                else results[start + i] = size_t(n);
            }
        }
        if (runtime_->poisoned) {
            for (size_t i = start; i < results.size(); ++i)
                results[i] = tl::make_unexpected(failure);
            break;
        }
        for (size_t i = 0; i < wave.size(); ++i) {
            if (!results[start + i]) continue;
            const auto& r = wave[i];
            if (read) {
                ProfileScope copy_time(runtime_->profile,
                                       AdapterProfile::ReadCopy);
                if (runtime_->profile.enabled)
                    runtime_->profile.copied_bytes += *results[start + i];
                size_t copied = 0;
                auto* buffer = static_cast<char*>(runtime_->buffer) + i * kBufferSize;
                for (int j = 0; j < r.iovcnt && copied < *results[start + i]; ++j) {
                    size_t n = std::min(r.iov[j].iov_len, *results[start + i] - copied);
                    if (n) std::memcpy(r.iov[j].iov_base, buffer + copied, n);
                    copied += n;
                }
            } else {
                bool synced = false;
                for (size_t j = 0; j < i; ++j) {
                    if (wave[j].fd == r.fd && results[start + j]) synced = true;
                }
                if (synced) continue;
                int rc = cogni_sdk_file_fsync(&files_.at(r.fd).sdk);
                if (rc) {
                    for (size_t j = i; j < wave.size(); ++j) {
                        if (wave[j].fd == r.fd && results[start + j])
                            results[start + j] = tl::make_unexpected(Translate(rc, failure));
                    }
                }
            }
        }
    }
    return results;
}

tl::expected<void, ErrorCode> CogniStoreFsAdapter::PreallocateFile(
    const std::string& path, uint64_t size) {
    if (!size || size > INT64_MAX)
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    if (!runtime_) return tl::make_unexpected(ErrorCode::FILE_INVALID_HANDLE);
    // Includes open, tail initialization, fsync, close and size verification.
    ProfileScope create_time(runtime_->profile, AdapterProfile::BucketCreate);
    auto opened = Open(path, O_RDWR | O_CREAT | O_EXCL);
    if (!opened) return tl::make_unexpected(opened.error());
    std::vector<char> zeros(std::min<uint64_t>(size, kBufferSize), 0);
    tl::expected<void, ErrorCode> result;
    constexpr size_t entries = kMaxStripeBytes / kBufferSize;
    // New EC buckets need logical capacity, not a second full copy of zeros.
    // Materialize the final stripe to establish EOF; earlier holes read as zero.
    // Keep the existing initialization for layouts without a verified geometry.
    uint64_t begin = 0;
    {
        std::lock_guard lock(runtime_->mutex);
        const auto stripe = files_.at(*opened).stripe_bytes;
        if (stripe) begin = ((size - 1) / stripe) * stripe;
    }
    for (uint64_t offset = begin; offset < size;) {
        std::array<iovec, entries> buffers{};
        std::vector<FdIoRequest> requests;
        requests.reserve(entries);
        for (size_t i = 0; i < entries && offset < size; ++i) {
            buffers[i] = {zeros.data(), static_cast<size_t>(std::min<uint64_t>(
                                           zeros.size(), size - offset))};
            requests.push_back(
                {*opened, &buffers[i], 1, static_cast<int64_t>(offset)});
            offset += buffers[i].iov_len;
        }
        // BatchWriteAt drains and fsyncs this file before returning. Keep the
        // bucket private until the tail write and close complete.
        auto written = BatchWriteAt(requests);
        for (size_t i = 0; i < written.size(); ++i) {
            if (!written[i] || *written[i] != buffers[i].iov_len) {
                result = tl::make_unexpected(written[i] ? ErrorCode::FILE_WRITE_FAIL
                                                        : written[i].error());
                break;
            }
        }
        if (!result) break;
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
