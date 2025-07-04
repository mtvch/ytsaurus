#pragma once
#include "abstract_scheme.h"

#include <contrib/ydb/core/tx/columnshard/common/path_id.h>
#include <contrib/ydb/core/tx/columnshard/engines/scheme/abstract/schema_version.h>
#include <contrib/ydb/core/tx/columnshard/engines/scheme/common/cache.h>
#include <contrib/ydb/core/tx/sharding/sharding.h>

namespace NKikimr::NOlap {

class IDbWrapper;

class TGranuleShardingInfo {
private:
    YDB_READONLY_DEF(NSharding::TGranuleShardingLogicContainer, ShardingInfo);
    YDB_READONLY(TSnapshot, SinceSnapshot, TSnapshot::Zero());
    YDB_READONLY(ui64, SnapshotVersion, 0);
    YDB_READONLY_DEF(TInternalPathId, PathId);

public:
    TGranuleShardingInfo(const NSharding::TGranuleShardingLogicContainer& shardingInfo, const TSnapshot& sinceSnapshot, const ui64 version,
        const TInternalPathId pathId)
        : ShardingInfo(shardingInfo)
        , SinceSnapshot(sinceSnapshot)
        , SnapshotVersion(version)
        , PathId(pathId) {
        AFL_VERIFY(!!ShardingInfo);
    }
};

class TVersionedIndex {
private:
    THashMap<TInternalPathId, std::map<TSnapshot, TGranuleShardingInfo>> ShardingInfo;
    std::map<TSnapshot, ISnapshotSchema::TPtr> Snapshots;
    std::shared_ptr<arrow::Schema> PrimaryKey;
    std::map<ui64, ISnapshotSchema::TPtr> SnapshotByVersion;
    ui64 LastSchemaVersion = 0;
    std::optional<ui64> SchemeVersionForActualization;
    ISnapshotSchema::TPtr SchemeForActualization;

public:
    bool IsEqualTo(const TVersionedIndex& vIndex) {
        return LastSchemaVersion == vIndex.LastSchemaVersion && SnapshotByVersion.size() == vIndex.SnapshotByVersion.size() &&
               ShardingInfo.size() == vIndex.ShardingInfo.size() && SchemeVersionForActualization == vIndex.SchemeVersionForActualization;
    }

    ISnapshotSchema::TPtr GetLastCriticalSchema() const {
        return SchemeForActualization;
    }

    ISnapshotSchema::TPtr GetLastCriticalSchemaDef(const ISnapshotSchema::TPtr defaultSchema) const {
        auto result = GetLastCriticalSchema();
        return result ? result : defaultSchema;
    }

    std::optional<TGranuleShardingInfo> GetShardingInfoOptional(const TInternalPathId pathId, const TSnapshot& ss) const {
        auto it = ShardingInfo.find(pathId);
        if (it == ShardingInfo.end() || it->second.empty()) {
            return std::nullopt;
        } else {
            auto itSS = it->second.upper_bound(ss);
            if (itSS == it->second.end()) {
                return it->second.rbegin()->second;
            } else if (itSS == it->second.begin()) {
                return std::nullopt;
            } else {
                --itSS;
                return itSS->second;
            }
        }
    }

    std::optional<TGranuleShardingInfo> GetShardingInfoActual(const TInternalPathId pathId) const;

    void AddShardingInfo(const TGranuleShardingInfo& shardingInfo) {
        AFL_VERIFY(ShardingInfo[shardingInfo.GetPathId()].emplace(shardingInfo.GetSinceSnapshot(), shardingInfo).second);
    }

    TString DebugString() const {
        TStringBuilder sb;
        for (auto&& i : Snapshots) {
            sb << i.first << ":" << i.second->DebugString() << ";";
        }
        return sb;
    }

    ISnapshotSchema::TPtr GetSchemaOptional(const ui64 version) const {
        auto it = SnapshotByVersion.find(version);
        return it == SnapshotByVersion.end() ? nullptr : it->second;
    }

    ISnapshotSchema::TPtr GetSchemaVerified(const ui64 version) const {
        auto it = SnapshotByVersion.find(version);
        Y_ABORT_UNLESS(it != SnapshotByVersion.end(), "no schema for version %lu", version);
        return it->second;
    }

    ISnapshotSchema::TPtr GetSchemaVerified(const TSnapshot& version) const {
        for (auto it = Snapshots.rbegin(); it != Snapshots.rend(); ++it) {
            if (it->first <= version) {
                return it->second;
            }
        }
        Y_ABORT_UNLESS(!Snapshots.empty());
        return Snapshots.begin()->second;
    }

    ISnapshotSchema::TPtr GetLastSchemaBeforeOrEqualSnapshotOptional(const ui64 version) const {
        if (SnapshotByVersion.empty()) {
            return nullptr;
        }
        auto upperBound = SnapshotByVersion.upper_bound(version);
        if (upperBound == SnapshotByVersion.begin()) {
            return nullptr;
        }
        return std::prev(upperBound)->second;
    }

    ISnapshotSchema::TPtr GetLastSchema() const {
        Y_ABORT_UNLESS(!Snapshots.empty());
        return Snapshots.rbegin()->second;
    }

    bool IsEmpty() const {
        return Snapshots.empty();
    }

    const std::shared_ptr<arrow::Schema>& GetPrimaryKey() const noexcept {
        return PrimaryKey;
    }

    const TIndexInfo* AddIndex(const TSnapshot& snapshot, TObjectCache<TSchemaVersionId, TIndexInfo>::TEntryGuard&& indexInfo);

    bool LoadShardingInfo(IDbWrapper& db);
};
}   // namespace NKikimr::NOlap
