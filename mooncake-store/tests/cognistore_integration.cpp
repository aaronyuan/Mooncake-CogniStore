// Explicit opt-in integration driver: requires a real, ready CogniStore
// cluster.
#include <algorithm>
#include <array>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
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
    auto missing_read = first.OpenExistingFileForRead(path);
    Check(!missing_read && missing_read.error() == ErrorCode::FILE_NOT_FOUND,
          "missing read open created a file");
    constexpr size_t capacity = 2 * 1024 * 1024 + 17;
    Take(first.PreallocateFile(path, capacity), "preallocate");
    Check(Take(first.GetFileSize(path), "size") == capacity, "wrong capacity");
    Check(!first.PreallocateFile(path, 4096),
          "preallocate overwrote existing file");
    CogniStoreFsAdapter file_root;
    Check(!file_root.Init(path), "ordinary file accepted as adapter root");
    // Verify the whole zero-filled capacity, including a partial final slot,
    // before any KV overwrites it. Preallocation must preserve exact length.
    int zero_fd = Take(first.OpenExistingFileForRead(path), "preallocation reader");
    std::string zero_bytes(capacity, char(0xcd));
    iovec zero_read{zero_bytes.data(), zero_bytes.size()};
    Check(Take(first.ReadAt(zero_fd, &zero_read, 1, 0), "preallocation read") == capacity &&
              std::all_of(zero_bytes.begin(), zero_bytes.end(), [](char c) { return c == 0; }),
          "preallocation bytes mismatch");
    Take(first.CloseFile(zero_fd), "close preallocation reader");
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
    fd = Take(second.OpenExistingFileForRead(path), "reopen read-only");
    Check(!second.WriteAt(fd, parts, 2, 33), "read-only handle accepted write");
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
        // Two offsets in one bucket and a second bucket share a batch. The
        // backend must keep handles alive until every operation completes.
        auto same = Take(allocator.Allocate("same", 16), "allocate same bucket");
        auto other = Take(allocator.Allocate("other", 16), "allocate other bucket");
        Check(same.file_path == descriptor.file_path &&
                  other.file_path != descriptor.file_path,
              "unexpected batch bucket layout");
        auto b = Pattern(16, 124), c = Pattern(16, 125);
        auto writes = backend.BatchWrite({
            {"entry", descriptor, {{value.data(), value.size()}}},
            {"same", same, {{b.data(), b.size()}}},
            {"other", other, {{c.data(), c.size()}}}});
        Check(writes.size() == 3, "multi-bucket write result count");
        for (auto& result : writes) Take(result, "multi-bucket write");
        std::string out_b(16, '\0'), out_c(16, '\0');
        auto reads = backend.BatchRead({
            {"entry", descriptor, {{output.data(), output.size()}}},
            {"same", same, {{out_b.data(), out_b.size()}}},
            {"other", other, {{out_c.data(), out_c.size()}}}});
        Check(reads.size() == 3, "multi-bucket read result count");
        for (auto& result : reads) Take(result, "multi-bucket read");
        Check(output == value && out_b == b && out_c == c,
              "multi-bucket bytes mismatch");
        CogniStoreFsAdapter remover;
        Take(remover.Init(distributed.fsdir), "remover init");
        Take(remover.DeleteFile(descriptor.file_path), "remove bucket");
        Take(remover.DeleteFile(other.file_path), "remove other bucket");
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
// Exercise a full SDK batch plus a short tail, scatter buffers, failed prep,
// short EOF, and ordered overlapping writes against the real storage cluster.
void BatchIoTest(const std::string& root) {
    CogniStoreFsAdapter adapter;
    Take(adapter.Init(root), "batch init");
    const auto path = root + "/value";
    constexpr size_t capacity = 2 * 1024 * 1024;
    constexpr size_t count = 11, size = 65536;
    Take(adapter.PreallocateFile(path, capacity), "batch preallocate");
    int fd = Take(adapter.OpenExistingFile(path), "batch writer");
    int ro = Take(adapter.OpenExistingFileForRead(path), "batch reader");
    std::array<std::string, count> values, output;
    std::array<std::array<iovec, 2>, count> inputs, outputs;
    std::array<FdIoRequest, count> writes, reads;
    for (size_t i = 0; i < count; ++i) {
        values[i] = Pattern(size, i);
        output[i].resize(size);
        inputs[i] = {{{values[i].data(), 79}, {values[i].data() + 79, size - 79}}};
        outputs[i] = {{{output[i].data(), 113}, {output[i].data() + 113, size - 113}}};
        writes[i] = {fd, inputs[i].data(), 2, int64_t(33 + i * (size + 4096))};
        reads[i] = {ro, outputs[i].data(), 2, writes[i].offset};
    }
    auto written = adapter.BatchWriteAt(writes);
    Check(written.size() == count, "batch write count");
    for (auto& result : written) Check(Take(result, "batch write") == size, "batch short write");
    auto verify = [&] {
        auto read = adapter.BatchReadAt(reads);
        Check(read.size() == count, "batch read count");
        for (size_t i = 0; i < count; ++i)
            Check(Take(read[i], "batch read") == size && output[i] == values[i], "batch bytes mismatch");
    };
    verify();
    // The read-only handle fails during SDK prep after a valid prep. The
    // accepted entries still have to finish, including the short SDK batch.
    std::array<FdIoRequest, 3> mixed{writes[0], writes[1], writes[2]};
    mixed[1].fd = ro;
    auto partial = adapter.BatchWriteAt(mixed);
    Check(partial.size() == 3 && partial[0] && !partial[1] && partial[2], "batch failed-prep contract");
    verify();
    std::array<char, 32> tail{};
    iovec tail_iov{tail.data(), tail.size()};
    std::array<FdIoRequest, 3> eof{{{ro, &tail_iov, 1, capacity - 7}, reads[0], reads[1]}};
    eof[1].fd = -1;
    auto short_read = adapter.BatchReadAt(eof);
    Check(short_read.size() == 3 && short_read[0] && *short_read[0] == 7 &&
              !short_read[1] && short_read[2] && *short_read[2] == size && output[1] == values[1],
          "batch short EOF or per-item error");
    Check(std::all_of(tail.begin(), tail.begin() + 7, [](char c) { return c == 0; }), "batch EOF bytes");
    std::array<FdIoRequest, 2> overlap{writes[0], writes[1]};
    overlap[1].offset = overlap[0].offset;
    auto ordered = adapter.BatchWriteAt(overlap);
    Check(ordered.size() == 2 && ordered[0] && ordered[1], "batch overlap write");
    // Do not modify the backing source buffers until all I/O completes.
    values[0] = values[1];
    verify();
    Take(adapter.CloseFile(fd), "batch close writer");
    verify();
    Take(adapter.CloseFile(ro), "batch close reader");
    Take(adapter.DeleteFile(path), "batch cleanup");
    std::cout << "COGNISTORE_BATCH_IO_PASS objects=11 scatter=2 partial_prep=1 short_eof=1 overlap=1" << std::endl;
}
// A 16 MiB stripe is split across sixteen KV requests. Preserve untouched
// bytes when a short, gapped or unaligned batch cannot take the full-stripe path.
void FullStripeTest(const std::string& root) {
    CogniStoreFsAdapter adapter;
    Take(adapter.Init(root), "stripe init");
    constexpr size_t size = 1024 * 1024, count = 16, capacity = count * size + 17;
    const auto path = root + "/value";
    Take(adapter.PreallocateFile(path, capacity), "stripe preallocate");
    Check(Take(adapter.GetFileSize(path), "stripe size") == capacity,
          "stripe capacity mismatch");
    int fd = Take(adapter.OpenExistingFile(path), "stripe writer");
    int ro = Take(adapter.OpenExistingFileForRead(path), "stripe reader");
    std::string expected(capacity, '\0');
    auto verify = [&] {
        std::string output(capacity, char(0xcd));
        std::array<std::array<iovec, 2>, count> buffers;
        std::array<FdIoRequest, count> requests;
        for (size_t i = 0; i < count; ++i) {
            buffers[i] = {{{output.data() + i * size, 113},
                           {output.data() + i * size + 113, size - 113}}};
            requests[i] = {ro, buffers[i].data(), 2, int64_t(i * size)};
        }
        auto read = adapter.BatchReadAt(requests);
        Check(read.size() == count, "stripe read count");
        for (auto& result : read)
            Check(Take(result, "stripe read") == size, "stripe short read");
        iovec tail{output.data() + count * size, 17};
        Check(Take(adapter.ReadAt(ro, &tail, 1, count * size), "stripe tail") == 17 &&
                  output == expected,
              "stripe data or untouched bytes mismatch");
    };
    verify();
    std::array<std::string, count> values;
    std::array<std::array<iovec, 2>, count> buffers;
    std::array<FdIoRequest, count> requests;
    auto prepare = [&](size_t seed, int64_t offset) {
        for (size_t i = 0; i < count; ++i) {
            values[i] = Pattern(size, seed + i);
            buffers[i] = {{{values[i].data(), 113},
                           {values[i].data() + 113, size - 113}}};
            requests[i] = {fd, buffers[i].data(), 2, offset + int64_t(i * size)};
        }
    };
    auto write = [&](std::span<const FdIoRequest> batch) {
        auto results = adapter.BatchWriteAt(batch);
        Check(results.size() == batch.size(), "stripe result count");
        for (size_t i = 0; i < batch.size(); ++i) {
            Check(Take(results[i], "stripe write") == size, "short stripe write");
            size_t offset = batch[i].offset;
            for (int j = 0; j < batch[i].iovcnt; ++j) {
                const auto& part = batch[i].iov[j];
                expected.replace(offset, part.iov_len,
                                 static_cast<const char*>(part.iov_base), part.iov_len);
                offset += part.iov_len;
            }
        }
        verify();
    };
    prepare(100, 0);
    write(requests);
    prepare(200, 0);
    write(std::span<const FdIoRequest>(requests).subspan(5, 4));
    // A held read-only handle must resolve the committed version after close.
    Take(adapter.CloseFile(fd), "stripe close partial writer");
    verify();
    fd = Take(adapter.OpenExistingFile(path), "stripe reopen writer");
    prepare(300, 0);
    write(requests);
    prepare(400, 0);
    std::vector<FdIoRequest> gapped(requests.begin(), requests.end());
    gapped.erase(gapped.begin() + 8);
    write(gapped);
    prepare(500, 1);
    write(requests);
    Take(adapter.CloseFile(fd), "stripe close writer");
    verify();
    Take(adapter.CloseFile(ro), "stripe close held reader");
    ro = Take(adapter.OpenExistingFileForRead(path), "stripe fresh reader");
    verify();
    std::array<std::string, 2> eof_data{std::string(size, char(0xcd)),
                                        std::string(size, char(0xcd))};
    std::array<iovec, 2> eof_buffers{{{eof_data[0].data(), size},
                                      {eof_data[1].data(), size}}};
    std::array<FdIoRequest, 2> eof_requests{{
        {ro, &eof_buffers[0], 1, int64_t(capacity - size - 7)},
        {ro, &eof_buffers[1], 1, int64_t(capacity - 7)}}};
    auto eof = adapter.BatchReadAt(eof_requests);
    Check(eof.size() == 2 && Take(eof[0], "EOF first KV") == size &&
              Take(eof[1], "EOF second KV") == 7 &&
              eof_data[0] == expected.substr(capacity - size - 7, size) &&
              eof_data[1].substr(0, 7) == expected.substr(capacity - 7) &&
              eof_data[1].substr(7) == std::string(size - 7, char(0xcd)),
          "coalesced EOF overwrote bytes or lost per-KV lengths");
    eof_requests[0].offset = capacity;
    eof_requests[1].offset = capacity + size;
    const auto before = eof_data;
    eof = adapter.BatchReadAt(eof_requests);
    Check(eof.size() == 2 && Take(eof[0], "EOF empty first") == 0 &&
              Take(eof[1], "EOF empty second") == 0 && eof_data == before,
          "coalesced empty EOF changed buffers");
    Take(adapter.CloseFile(ro), "stripe close reader");
    Take(adapter.DeleteFile(path), "stripe cleanup");
    std::cout << "COGNISTORE_FULL_STRIPE_PASS stripe_bytes=16777216 objects=16"
                 " full=2 partial=1 gapped=1 unaligned=1 tail=17" << std::endl;
}
// Check the overwritten bytes and both retained patterned sides, then repeat
// through a new read-only handle after writer close/promotion.
void SmallIoTest(const std::string& root) {
    CogniStoreFsAdapter adapter;
    Take(adapter.Init(root), "small IO init");
    for (size_t length : {size_t(16), size_t(4096), size_t(65536), size_t(65537)}) {
        const auto path = root + "/value_" + std::to_string(length);
        Take(adapter.PreallocateFile(path, 2 * 1024 * 1024), "small IO preallocate");
        int fd = Take(adapter.OpenExistingFile(path), "small IO writer");
        auto initial = Pattern(1024 * 1024, 42);
        iovec seed{initial.data(), initial.size()};
        Check(Take(adapter.WriteAt(fd, &seed, 1, 0), "small IO seed") ==
                  initial.size(),
              "small IO short seed");
        auto value = Pattern(length, length);
        iovec input{value.data(), value.size()};
        Check(Take(adapter.WriteAt(fd, &input, 1, 33), "small IO write") == length,
              "small IO short write");
        auto verify = [&](const std::string& phase, size_t read_length) {
            auto expected = initial.substr(0, read_length);
            expected.replace(33, length, value);
            std::string got(read_length, char(0xcd));
            iovec output{got.data(), got.size()};
            Check(Take(adapter.ReadAt(fd, &output, 1, 0), "small IO read") ==
                      got.size(),
                  "small IO short read");
            Check(got == expected, "small IO mismatch " + phase + " size=" +
                                       std::to_string(length));
        };
        verify("before-close", length + 66);
        verify("before-close", 1024 * 1024);
        Take(adapter.CloseFile(fd), "small IO close writer");
        fd = Take(adapter.OpenExistingFileForRead(path), "small IO reader");
        verify("after-close", length + 66);
        verify("after-close", 1024 * 1024);
        Take(adapter.CloseFile(fd), "small IO close reader");
        Take(adapter.DeleteFile(path), "small IO cleanup");
    }
    std::cout << "COGNISTORE_SMALL_IO_PASS cases=4 phases=2 read_sizes=2"
              << std::endl;
}
// Invoked in separate processes by run-concurrent.py. Control markers live on
// local disk, never in the bucket under test. Use 4 KiB values for the TCP
// fixture's 256 KiB stripe, or 1 MiB values for Wuxi's 16 MiB stripe.
void ConcurrentBucketTest(bool writer, const std::string& root,
                          const std::filesystem::path& control, size_t value_size) {
    Check(value_size == 4096 || value_size == 1048576, "unsupported test value size");
    auto signal = [&](const std::string& name) {
        std::ofstream marker(control / name);
        Check(marker.good(), "cannot signal " + name);
    };
    auto wait = [&](const std::string& name) {
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::seconds(120);
        while (!std::filesystem::exists(control / name)) {
            Check(std::chrono::steady_clock::now() < deadline,
                  "timed out waiting for " + name);
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    };
    if (!writer) wait("writer-ready");
    DistributedStorageConfig config;
    config.fsdir = root;
    config.fs_adapter_type = "cognistore";
    config.allocator_type = "bucket";
    config.bucket_capacity = 16 * value_size;
    config.alignment = 4096;
    config.max_bucket_count = 1;
    config.single_tenant = true;
    config.eviction_enabled = false;
    CogniStoreFsAdapter adapter;
    Take(adapter.Init(root), "concurrent adapter init");
    FileStorageConfig file_config;
    DistributedStorageBackend backend(
        file_config, config, std::make_unique<CogniStoreFsAdapter>());
    Take(backend.Init(), "concurrent backend init");
    const auto path = root + "/bucket_000000.data";
    DistributedFSDescriptor stable{path, 0, value_size, value_size, 0};
    auto original = Pattern(value_size, 0);
    if (writer) {
        Take(adapter.PreallocateFile(path, config.bucket_capacity),
             "concurrent preallocate");
        Take(backend.BatchWrite(
                 {{"stable", stable, {{original.data(), original.size()}}}})[0],
             "seed stable offset");
        signal("writer-ready");
        wait("reader-ready");
    }
    int read_fd = -1;
    if (!writer) {
        read_fd = Take(adapter.OpenExistingFileForRead(path), "long-lived reader");
        auto invalid = Pattern(value_size, 99);
        iovec denied{invalid.data(), invalid.size()};
        Check(!adapter.WriteAt(read_fd, &denied, 1, 0),
              "read-only concurrent handle accepted write");
        std::string initial(value_size, '\0');
        iovec initial_read{initial.data(), initial.size()};
        Check(Take(adapter.ReadAt(read_fd, &initial_read, 1, 0), "read initial value") ==
                  initial.size() && initial == original, "initial committed value mismatch");
        signal("reader-ready");
    }
    size_t reads_during_writes = 0;
    for (int round = 1; round <= 8; ++round) {
        auto suffix = std::to_string(round);
        DistributedFSDescriptor next{path, uint64_t(round) * value_size,
                                     value_size, value_size, 0};
        auto expected = Pattern(value_size, round);
        if (writer) {
            signal("writing-" + suffix);
            wait("reading-" + suffix);
            Take(backend.BatchWrite(
                     {{suffix, next, {{expected.data(), expected.size()}}}})[0],
                 "concurrent append " + suffix);
            signal("committed-" + suffix);
            wait("verified-" + suffix);
        } else {
            wait("writing-" + suffix);
            signal("reading-" + suffix);
            do {
                std::string got(value_size, '\0');
                Take(backend.BatchRead(
                         {{"stable", stable, {{got.data(), got.size()}}}})[0],
                     "read stable offset during append");
                Check(got == original, "adjacent write corrupted stable offset");
                ++reads_during_writes;
            } while (!std::filesystem::exists(control / ("committed-" + suffix)));
            // Reuse the O_RDONLY handle across all writer closes/promotions.
            std::string got(value_size, '\0');
            iovec output{got.data(), got.size()};
            Check(Take(adapter.ReadAt(read_fd, &output, 1, next.offset),
                       "read newly committed offset") == got.size() &&
                      got == expected,
                  "long-lived reader missed committed append " + suffix);
            std::fill(got.begin(), got.end(), '\0');
            Take(backend.BatchRead({{suffix, next, {{got.data(), got.size()}}}})[0],
                 "Get newly committed offset");
            Check(got == expected, "Get mismatch " + suffix);
            signal("verified-" + suffix);
        }
    }
    if (writer) {
        wait("reader-closed");
        Take(adapter.DeleteFile(path), "concurrent cleanup");
        Check(!Take(adapter.FileExists(path), "concurrent cleanup exists"),
              "concurrent bucket still exists");
    } else {
        Take(adapter.CloseFile(read_fd), "close long-lived reader");
        signal("reader-closed");
    }
    std::cout << "COGNISTORE_CONCURRENT_PASS role=" << (writer ? "writer" : "reader")
              << " pid=" << getpid() << " rounds=8 stable_reads=" << reads_during_writes
              << " value_bytes=" << value_size
              << " bucket_bytes=" << config.bucket_capacity
              << " max_offset=" << 8 * value_size << std::endl;
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
              "usage: cognistore_integration adapter/small-io/batch-io/full-stripe ROOT | put/read ROOT "
              "MASTER | concurrent-writer/concurrent-reader ROOT CONTROL_DIR VALUE_SIZE");
        std::string mode = argv[1], root = argv[2];
        if (mode == "adapter")
            AdapterTest(root);
        else if (mode == "batch-io")
            BatchIoTest(root);
        else if (mode == "full-stripe")
            FullStripeTest(root);
        else if (mode == "small-io")
            SmallIoTest(root);
        else if (mode == "concurrent-writer" || mode == "concurrent-reader") {
            Check(argc == 5, "control directory and value size required");
            ConcurrentBucketTest(mode == "concurrent-writer", root, argv[3],
                                 std::stoul(argv[4]));
        } else {
            Check(argc == 4, "master address required");
            ClientTest(mode, argv[3], root);
        }
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "COGNISTORE_INTEGRATION_FAIL " << error.what() << '\n';
        return 1;
    }
}
