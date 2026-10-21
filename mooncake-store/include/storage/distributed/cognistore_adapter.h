#pragma once

#include <memory>
#include <unordered_map>
#include "storage/distributed/fs_adapter.h"
#include <cognistore/sdk/cogni.h>

namespace mooncake {
// Initial synchronous, copy-based bridge to the process-local SDK C ABI.
// Batch and stale-fd retry capabilities intentionally remain disabled.
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
    tl::expected<void, ErrorCode> CloseFile(int) override;
    tl::expected<void, ErrorCode> PreallocateFile(const std::string&,
                                                  uint64_t) override;
    tl::expected<size_t, ErrorCode> WriteAt(int, const iovec*, int,
                                            int64_t) override;
    tl::expected<size_t, ErrorCode> ReadAt(int, iovec*, int, int64_t) override;
    const char* GetName() const override { return "cognistore"; }

   private:
    struct Runtime;
    std::shared_ptr<Runtime> runtime_;
    std::unordered_map<int, cogni_sdk_file> files_;
    int next_fd_{1};
    tl::expected<int, ErrorCode> Open(const std::string&, int);
    tl::expected<size_t, ErrorCode> Transfer(int, const iovec*, int, int64_t,
                                             bool);
};
}  // namespace mooncake
