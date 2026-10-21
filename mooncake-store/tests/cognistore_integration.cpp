// Explicit opt-in integration driver: requires a real, ready CogniStore
// cluster.
#include <algorithm>
#include <array>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <set>
#include <stdexcept>
#include <thread>
#include <unistd.h>

#include "client_service.h"
#include "storage/distributed/cognistore_adapter.h"
#include "storage/distributed/distributed_storage_backend.h"
#include "storage/distributed/immutable_bucket_allocator.h"

using namespace mooncake;

namespace {
void Check(bool ok, const std::string& what) {
    if (!ok) throw std::runtime_error(what);
}
template <typename T>
T Take(tl::expected<T, ErrorCode> result, const std::string& what) {
    if (!result)
        throw std::runtime_error(
            what + ": " + std::to_string(static_cast<int>(result.error())));
    if constexpr (!std::is_void_v<T>) return std::move(*result);
}
std::string Pattern(size_t size, size_t seed) {
    std::string value(size, '\0');
    for (size_t i = 0; i < size; ++i)
        value[i] =
            static_cast<char>((i * 131 + i / 257 + (seed + 1) * 17) % 251);
    return value;
}
void AdapterTest(const std::string& root) {
    CogniStoreFsAdapter first, second;
    Take(first.Init(root), "adapter init");
    Take(second.Init(root), "shared runtime init");
    Check(Take(first.FileExists(root), "directory exists"), "root missing");
    Check(Take(first.CanonicalPath(root + "/."), "canonical root") == root,
          "wrong canonical root");
    const auto path = root + "/value";
    Check(!Take(first.FileExists(path), "absent exists"),
          "fixture already exists");
    auto missing = first.OpenExistingFile(path);
    Check(!missing && missing.error() == ErrorCode::FILE_NOT_FOUND,
          "missing open created a file");
    constexpr size_t capacity = 2 * 1024 * 1024 + 17;
    Take(first.PreallocateFile(path, capacity), "preallocate");
    Check(Take(first.GetFileSize(path), "size") == capacity, "wrong capacity");
    Check(!first.PreallocateFile(path, 4096),
          "preallocate overwrote existing file");
    CogniStoreFsAdapter file_root;
    Check(!file_root.Init(path), "ordinary file accepted as adapter root");
    int fd = Take(first.OpenExistingFile(path), "open");
    auto expected = Pattern(1024 * 1024 + 113, 5);
    iovec parts[]{{expected.data(), 79},
                  {expected.data() + 79, expected.size() - 79}};
    Check(Take(first.WriteAt(fd, parts, 2, 33), "write") == expected.size(),
          "short write");
    Check(!first.WriteAt(fd, parts, 2, -1), "negative offset accepted");
    Take(first.CloseFile(fd), "close writer");
    Take(first.Shutdown(), "release first adapter");
    // The remaining adapter must keep the single process runtime alive.
    fd = Take(second.OpenExistingFile(path), "reopen");
    std::string got(expected.size(), '\0');
    iovec output{got.data(), got.size()};
    Check(Take(second.ReadAt(fd, &output, 1, 33), "read") == got.size() &&
              got == expected,
          "read mismatch");
    std::array<char, 32> tail{};
    iovec tail_iov{tail.data(), tail.size()};
    Check(Take(second.ReadAt(fd, &tail_iov, 1, capacity - 7), "short EOF") == 7,
          "wrong EOF result");
    Check(std::all_of(tail.begin(), tail.begin() + 7,
                      [](char c) { return c == 0; }),
          "preallocation not zero");
    std::array<FdIoRequest, 3> batch{
        {{fd, &output, 1, 33}, {-1, &output, 1, 33}, {fd, &output, 1, 33}}};
    auto results = second.BatchReadAt(batch);
    Check(results.size() == 3 && results[0] && !results[1] && results[2],
          "batch partial-error contract");
    Check(got == expected, "batch mismatch");
    Take(second.CloseFile(fd), "close reader");
    Check(!second.ReadAt(fd, &output, 1, 0), "closed handle accepted");
    // Exercise a continuation page, not just the first directory page.
    for (int i = 0; i < 130; ++i) {
        auto p = root + "/entry_" + std::to_string(i);
        Take(second.CloseFile(Take(second.OpenFile(p), "create list entry")),
             "close list entry");
    }
    auto names = Take(second.ListFiles(root), "paginated list");
    Check(names.size() == 131 &&
              std::set<std::string>(names.begin(), names.end()).size() == 131,
          "list lost/duplicated entries");
    for (const auto& name : names)
        Take(second.DeleteFile(root + "/" + name), "delete fixture");
    Check(!Take(second.FileExists(path), "removed exists"),
          "removed file visible");
    Check(Take(second.ListFiles(root), "empty list").empty(),
          "cleanup incomplete");
    Take(second.Shutdown(), "final runtime shutdown");
    // Use the real allocator and backend to cover remote namespace guards.
    DistributedStorageConfig distributed;
    distributed.fsdir = root + "/buckets";
    distributed.fs_adapter_type = "cognistore";
    distributed.allocator_type = "bucket";
    distributed.bucket_capacity = 8192;
    distributed.alignment = 4096;
    distributed.max_bucket_count = 2;
    distributed.single_tenant = true;
    distributed.eviction_enabled = false;
    {
        ImmutableBucketAllocator allocator;
        Take(allocator.Init(distributed), "allocator init");
        auto descriptor =
            Take(allocator.Allocate("entry", 16), "allocate entry");
        ImmutableBucketAllocator restart;
        Check(!restart.Init(distributed),
              "nonempty root accepted without recovery");
        FileStorageConfig file_config;
        file_config.storage_backend_type = StorageBackendType::kDistributed;
        file_config.storage_filepath = "/tmp";
        DistributedStorageBackend backend(
            file_config, distributed, std::make_unique<CogniStoreFsAdapter>());
        Take(backend.Init(), "bucket backend init");
        auto value = Pattern(16, 123);
        auto written = backend.BatchWrite(
            {{"entry", descriptor, {{value.data(), value.size()}}}});
        Check(written.size() == 1, "bucket write result count");
        Take(written[0], "bucket write");
        std::string output(value.size(), '\0');
        auto read = backend.BatchRead(
            {{"entry", descriptor, {{output.data(), output.size()}}}});
        Check(read.size() == 1, "bucket read result count");
        Take(read[0], "bucket read");
        Check(output == value, "bucket bytes mismatch");
        CogniStoreFsAdapter remover;
        Take(remover.Init(distributed.fsdir), "remover init");
        Take(remover.DeleteFile(descriptor.file_path), "remove bucket");
        auto stale = backend.BatchRead(
            {{"entry", descriptor, {{output.data(), output.size()}}}});
        Check(stale.size() == 1 && !stale[0] &&
                  stale[0].error() == ErrorCode::FILE_NOT_FOUND,
              "stale descriptor read did not fail as missing");
        Check(!Take(remover.FileExists(descriptor.file_path),
                    "stale file exists"),
              "stale read recreated bucket");
    }
    std::cout << "COGNISTORE_ADAPTER_PASS pid=" << getpid()
              << " verified_bytes=" << expected.size() << '\n';
}
const std::array<size_t, 10> sizes{1,       4095,    4096,    4097,    262144,
                                   1048576, 2097169, 3145728, 1048581, 3145728};
void ClientTest(const std::string& mode, const std::string& master,
                const std::string& root) {
    auto created = Client::Create("127.0.0.1:0", "P2PHANDSHAKE", "tcp",
                                  std::nullopt, master);
    Check(created.has_value(), "client create failed");
    auto client = std::move(*created);
    FileStorageConfig config;
    config.storage_backend_type = StorageBackendType::kDistributed;
    config.storage_filepath = "/tmp";
    auto base = Take(CreateStorageBackend(config), "backend factory");
    Take(base->Init(), "backend init");
    auto backend = std::dynamic_pointer_cast<DistributedStorageBackend>(base);
    Check(backend != nullptr, "not distributed backend");
    client->SetDfsStorageBackend(backend);
    std::vector<ObjectKey> keys;
    std::vector<std::string> values;
    for (size_t i = 0; i < sizes.size(); ++i) {
        keys.push_back("cognistore-integration-" + std::to_string(i));
        values.push_back(Pattern(sizes[i], i));
    }
    if (mode == "put") {
        std::vector<std::vector<Slice>> slices;
        for (auto& value : values)
            slices.push_back({{value.data(), value.size()}});
        ReplicateConfig replication;
        replication.replica_num = 0;
        replication.dfs_replica_num = 1;
        auto results = client->BatchPut(keys, slices, replication);
        Check(results.size() == keys.size(), "put result count");
        for (size_t i = 0; i < results.size(); ++i)
            Take(results[i], "put " + keys[i]);
    } else if (mode == "read") {
        std::set<int> buckets;
        std::vector<QueryResult> queries;
        std::unordered_map<std::string, std::vector<Slice>> slices;
        std::vector<std::string> output(sizes.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            auto query = Take(client->Query(keys[i]), "query " + keys[i]);
            Check(query.replicas.size() == 1 &&
                      query.replicas[0].is_dfs_replica(),
                  "not DFS-only");
            const auto& descriptor = query.replicas[0].get_dfs_descriptor();
            Check(descriptor.file_path.starts_with(root + "/bucket_"),
                  "wrong remote path");
            buckets.insert(descriptor.shard_idx);
            output[i].resize(sizes[i]);
            slices[keys[i]] = {{output[i].data(), output[i].size()}};
            queries.push_back(std::move(query));
        }
        auto results = client->BatchGet(keys, queries, slices);
        Check(results.size() == keys.size(), "get result count");
        for (size_t i = 0; i < keys.size(); ++i) {
            Take(results[i], "get " + keys[i]);
            Check(output[i] == values[i], "byte mismatch " + keys[i]);
        }
        Check(buckets.size() >= 2, "bucket rollover not exercised");
        auto good = queries[0].replicas[0].get_dfs_descriptor();
        auto outside = good;
        outside.file_path =
            "/outside/" +
            std::filesystem::path(good.file_path).filename().string();
        auto wrong_id = good;
        ++wrong_id.shard_idx;
        auto bad_range = good;
        bad_range.offset = UINT64_MAX;
        std::string one(1, '\0');
        auto mixed =
            backend->BatchRead({{keys[0], outside, {{one.data(), 1}}},
                                {keys[0], wrong_id, {{one.data(), 1}}},
                                {keys[0], bad_range, {{one.data(), 1}}},
                                {keys[0], good, {{one.data(), 1}}}});
        Check(mixed.size() == 4 && !mixed[0] && !mixed[1] && !mixed[2] &&
                  mixed[3] && one == values[0],
              "descriptor validation/partial batch");
        auto deadline = queries[0].lease_timeout;
        for (const auto& q : queries)
            deadline = std::max(deadline, q.lease_timeout);
        std::this_thread::sleep_until(deadline +
                                      std::chrono::milliseconds(100));
        auto removed = client->BatchRemove(keys);
        Check(removed.size() == keys.size(), "remove count");
        for (auto& result : removed) Take(result, "remove after lease");
        for (const auto& key : keys) {
            auto q = client->Query(key);
            Check(!q && q.error() == ErrorCode::OBJECT_NOT_FOUND,
                  "deleted key visible");
        }
        std::cout << "buckets=" << buckets.size()
                  << " descriptor_rejections=3 ";
    } else {
        throw std::runtime_error("unknown client mode");
    }
    client.reset();
    backend.reset();
    base.reset();
    size_t bytes = 0;
    for (auto n : sizes) bytes += n;
    std::cout << "COGNISTORE_CLIENT_PASS mode=" << mode << " pid=" << getpid()
              << " objects=" << sizes.size() << " bytes=" << bytes << '\n';
}
}  // namespace
int main(int argc, char** argv) {
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = true;
    try {
        Check(argc >= 3,
              "usage: cognistore_integration adapter ROOT | put/read ROOT "
              "MASTER");
        std::string mode = argv[1], root = argv[2];
        if (mode == "adapter")
            AdapterTest(root);
        else {
            Check(argc == 4, "master address required");
            ClientTest(mode, argv[3], root);
        }
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "COGNISTORE_INTEGRATION_FAIL " << error.what() << '\n';
        return 1;
    }
}
