#pragma once

#include <memory>
#include <optional>
#include <unordered_map>
#include "storage/distributed/fs_adapter.h"
#include <cognistore/sdk/cogni.h>

namespace mooncake {
// Initial synchronous, copy-based bridge to the process-local SDK C ABI.
// Bucket batching reuses handles and submits bounded SDK I/O batches.
// Contiguous aligned writes coalesce into full stripes (up to 16 MiB).
// Up to four coalesced writes have independent buffers/IORs in one Runtime.
// Reads and partial-write fallback retain their serialization.
// Close drains retained transfers. Stale-fd retries remain disabled.
class CogniStoreFsAdapter final : public FileSystemAdapter {
   public:
    CogniStoreFsAdapter();
    ~CogniStoreFsAdapter() override;
    CogniStoreFsAdapter(const CogniStoreFsAdapter&) = delete;
    CogniStoreFsAdapter& operator=(const CogniStoreFsAdapter&) = delete;
    tl::expected<void, ErrorCode> Init(const std::string&) override;
    tl::expected<void, ErrorCode> Shutdown() override;
    tl::expected<void, ErrorCode> DeleteFile(const std::string&) override;
    tl::expected<bool, ErrorCode> FileExists(const std::string&) override;
    tl::expected<std::vector<std::string>, ErrorCode> ListFiles(
        const std::string&) override;
    tl::expected<size_t, ErrorCode> GetFileSize(const std::string&) override;
    tl::expected<std::string, ErrorCode> CanonicalPath(
        const std::string&) override;
    tl::expected<int, ErrorCode> OpenFile(const std::string&) override;
    tl::expected<int, ErrorCode> OpenExistingFile(const std::string&) override;
    tl::expected<int, ErrorCode> OpenExistingFileForRead(
        const std::string&) override;
    tl::expected<int, ErrorCode> OpenValidatedFile(const std::string&, uint64_t,
                                                   const std::string&,
                                                   bool) override;
    tl::expected<void, ErrorCode> CloseFile(int) override;
    tl::expected<void, ErrorCode> PreallocateFile(const std::string&,
                                                  uint64_t) override;
    tl::expected<size_t, ErrorCode> WriteAt(int, const iovec*, int,
                                            int64_t) override;
    tl::expected<size_t, ErrorCode> ReadAt(int, iovec*, int, int64_t) override;
    std::vector<tl::expected<size_t, ErrorCode>> BatchWriteAt(
        std::span<const FdIoRequest>) override;
    std::vector<tl::expected<size_t, ErrorCode>> BatchReadAt(
        std::span<const FdIoRequest>) override;
    bool SupportsBatchIo() const override { return true; }
    const char* GetName() const override { return "cognistore"; }

   private:
    std::vector<tl::expected<size_t, ErrorCode>> BatchTransfer(
        std::span<const FdIoRequest>, bool read);
    std::optional<std::vector<tl::expected<size_t, ErrorCode>>> CoalescedTransfer(
        std::span<const FdIoRequest>, bool read);
    struct File {
        size_t active{0};
        bool closing{false};
        cogni_sdk_file sdk{};
        size_t stripe_bytes{0};
    };
    struct Runtime;
    std::shared_ptr<Runtime> runtime_;
    std::unordered_map<int, File> files_;
    int next_fd_{1};
    tl::expected<int, ErrorCode> Open(const std::string&, int,
                                      std::optional<uint64_t> = std::nullopt);
    tl::expected<size_t, ErrorCode> Transfer(int, const iovec*, int, int64_t,
                                             bool);
};
}  // namespace mooncake
