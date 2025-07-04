#pragma once

#include <yql/essentials/providers/common/config/yql_dispatch.h>
#include <yql/essentials/providers/common/config/yql_setting.h>

#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <contrib/ydb/library/yql/providers/s3/actors_factory/yql_s3_actors_factory.h>

namespace NYql {

struct TS3Settings {
    using TConstPtr = std::shared_ptr<const TS3Settings>;
private:
    static constexpr NCommon::EConfSettingType Static = NCommon::EConfSettingType::Static;
public:
    NCommon::TConfSetting<bool, Static> SourceCoroActor;
    NCommon::TConfSetting<ui64, Static> MaxOutputObjectSize;
    NCommon::TConfSetting<ui64, Static> UniqueKeysCountLimit;
    NCommon::TConfSetting<ui64, Static> BlockSizeMemoryLimit;
    NCommon::TConfSetting<ui64, Static> SerializeMemoryLimit; // Total serialization memory limit for all current blocks for all patition keys. Reachable in case of many but small partitions.
    NCommon::TConfSetting<ui64, Static> InFlightMemoryLimit; // Maximum memory used by one sink.
    NCommon::TConfSetting<ui64, Static> JsonListSizeLimit; // Limit of elements count in json list written to S3 file. Default: 10'000. Max: 100'000.
    NCommon::TConfSetting<ui64, Static> ArrowParallelRowGroupCount; // Number of parquet row groups to read in parallel, min == 1
    NCommon::TConfSetting<bool, Static> ArrowRowGroupReordering;    // Allow to push rows from file in any order, default false, but usually it is OK
    NCommon::TConfSetting<ui64, Static> ParallelDownloadCount;      // Number of files to read in parallel, min == 1
    NCommon::TConfSetting<bool, Static> UseBlocksSource;            // Deprecated and has to be removed after config cleanup
    NCommon::TConfSetting<bool, Static> UseBlocksSink;
    NCommon::TConfSetting<bool, Static> AtomicUploadCommit;         // Commit each file independently, w/o transaction semantic over all files
    NCommon::TConfSetting<bool, Static> UseConcurrentDirectoryLister;
    NCommon::TConfSetting<ui64, Static> MaxDiscoveryFilesPerDirectory;
    NCommon::TConfSetting<bool, Static> UseRuntimeListing; // Enables runtime listing
    NCommon::TConfSetting<ui64, Static> FileQueueBatchSizeLimit; // Limits total size of files in one PathBatch from FileQueue
    NCommon::TConfSetting<ui64, Static> FileQueueBatchObjectCountLimit; // Limits count of files in one PathBatch from FileQueue
    NCommon::TConfSetting<ui64, Static> FileQueuePrefetchSize;
    NCommon::TConfSetting<bool, Static> AsyncDecoding;  // Parse and decode input data at separate mailbox/thread of TaskRunner
    NCommon::TConfSetting<bool, Static> UsePredicatePushdown;
    NCommon::TConfSetting<bool, Static> AsyncDecompressing;  // Decompression and parsing input data in different mailbox/thread
};

struct TS3ClusterSettings {
    TString Url;
};

struct TS3Configuration : public TS3Settings, public NCommon::TSettingDispatcher {
    using TPtr = TIntrusivePtr<TS3Configuration>;

    TS3Configuration();
    TS3Configuration(const TS3Configuration&) = delete;

    void Init(const TS3GatewayConfig& config, TIntrusivePtr<TTypeAnnotationContext> typeCtx);

    bool HasCluster(TStringBuf cluster) const;

    TS3Settings::TConstPtr Snapshot() const;
    THashMap<TString, TString> Tokens;
    std::unordered_map<TString, TS3ClusterSettings> Clusters;

    ui64 FileSizeLimit = 0;
    ui64 BlockFileSizeLimit = 0;
    std::unordered_map<TString, ui64> FormatSizeLimits;
    ui64 MaxFilesPerQuery = 0;
    ui64 MaxDiscoveryFilesPerQuery = 0;
    ui64 MaxDirectoriesAndFilesPerQuery = 0;
    ui64 MinDesiredDirectoriesOfFilesPerQuery = 0;
    ui64 MaxInflightListsPerQuery = 0;
    ui64 ListingCallbackThreadCount = 0;
    ui64 ListingCallbackPerThreadQueueSize = 0;
    ui64 RegexpCacheSize = 0;
    bool AllowLocalFiles = false;
    bool AllowConcurrentListings = false;
    ui64 GeneratorPathsLimit = 0;
    bool WriteThroughDqIntegration = false;
    ui64 MaxListingResultSizePerPhysicalPartition;
    bool AllowAtomicUploadCommit = true;
    NYql::NDq::TS3ReadActorFactoryConfig S3ReadActorFactoryConfig;
};

} // NYql
