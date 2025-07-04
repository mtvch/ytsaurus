#pragma once

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/ytlib/hydra/public.h>

#include <yt/yt/ytlib/tablet_client/public.h>
#include <yt/yt/ytlib/tablet_client/backup.h>

#include <yt/yt/core/misc/arithmetic_formula.h>
#include <yt/yt/core/misc/public.h>

#include <library/cpp/yt/misc/enum.h>

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NTableClient::NProto {

class TRspCheckBackup;
class TTabletStatistics;
class TTableReplicaStatistics;

} // namespace NYT::NTableClient::NProto

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TTabletCellStatistics;
class TTabletResources;
class TBackupCutoffDescriptor;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

using NHydra::InvalidPeerId;
using NHydra::EPeerState;

using NTabletClient::TTabletCellBundleId;
using NTabletClient::NullTabletCellBundleId;
using NTabletClient::TTabletCellId;
using NTabletClient::NullTabletCellId;
using NTabletClient::TTabletId;
using NTabletClient::NullTabletId;
using NTabletClient::TStoreId;
using NTabletClient::THunkStorageId;
using NTabletClient::ETabletState;
using NTabletClient::ETableReplicaMode;
using NTabletClient::TypicalPeerCount;
using NTabletClient::TTableReplicaId;
using NTabletClient::TTabletActionId;
using NTabletClient::TTabletOwnerId;
using NTabletClient::TTableReplicaId;

using NTabletClient::TTabletCellOptions;
using NTabletClient::TTabletCellOptionsPtr;
using NTabletClient::TDynamicTabletCellOptions;
using NTabletClient::TDynamicTabletCellOptionsPtr;
using NTabletClient::ETabletCellHealth;
using NTabletClient::ETableReplicaState;
using NTabletClient::ETabletActionKind;
using NTabletClient::ETabletActionState;
using NTabletClient::ETableBackupState;
using NTabletClient::ETabletBackupState;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(ITabletManager)
DECLARE_REFCOUNTED_STRUCT(ITabletService)
DECLARE_REFCOUNTED_STRUCT(ITabletBalancer)
DECLARE_REFCOUNTED_STRUCT(ITabletCellDecommissioner)
DECLARE_REFCOUNTED_STRUCT(ITabletActionManager)
DECLARE_REFCOUNTED_STRUCT(IMasterReplicatedTableTracker)
DECLARE_REFCOUNTED_STRUCT(IReplicatedTableTrackerStateProvider)
DECLARE_REFCOUNTED_STRUCT(ITabletCellBalancerProvider)
DECLARE_REFCOUNTED_STRUCT(ITabletNodeTracker)
DECLARE_REFCOUNTED_STRUCT(IBackupManager)
DECLARE_REFCOUNTED_STRUCT(ITabletChunkManager)

DECLARE_REFCOUNTED_CLASS(TMountConfigStorage)

struct ITabletCellBalancer;

DECLARE_REFCOUNTED_STRUCT(TTabletBalancerMasterConfig)
DECLARE_REFCOUNTED_STRUCT(TTabletCellDecommissionerConfig)
DECLARE_REFCOUNTED_STRUCT(TTabletActionManagerMasterConfig)
DECLARE_REFCOUNTED_STRUCT(TReplicatedTableTrackerConfig)
DECLARE_REFCOUNTED_STRUCT(TDynamicTabletCellBalancerMasterConfig)
DECLARE_REFCOUNTED_STRUCT(TDynamicTabletManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TDynamicTablesMulticellGossipConfig)
DECLARE_REFCOUNTED_STRUCT(TDynamicTabletNodeTrackerConfig)
DECLARE_REFCOUNTED_STRUCT(TDynamicCellHydraPersistenceSynchronizerConfig)

class TTableReplica;

DECLARE_ENTITY_TYPE(TTabletCellBundle, TTabletCellBundleId, NObjectClient::TObjectIdEntropyHash)
DECLARE_ENTITY_TYPE(TTabletCell, TTabletCellId, NObjectClient::TObjectIdEntropyHash)
DECLARE_ENTITY_TYPE(TTablet, TTabletId, NObjectClient::TObjectIdEntropyHash)
DECLARE_ENTITY_TYPE(THunkTablet, TTabletId, NObjectClient::TObjectIdEntropyHash)
DECLARE_ENTITY_TYPE(TTabletBase, TTabletId, NObjectClient::TObjectIdEntropyHash)
DECLARE_ENTITY_TYPE(TTabletOwnerBase, TTabletOwnerId, NObjectClient::TObjectIdEntropyHash)
DECLARE_ENTITY_TYPE(TTableReplica, TTableReplicaId, NObjectClient::TObjectIdEntropyHash)
DECLARE_ENTITY_TYPE(TTabletAction, TTabletActionId, NObjectClient::TObjectIdEntropyHash)
DECLARE_ENTITY_TYPE(THunkStorageNode, THunkStorageId, NObjectClient::TObjectIdEntropyHash)

DECLARE_MASTER_OBJECT_TYPE(TTabletCellBundle)
DECLARE_MASTER_OBJECT_TYPE(TTabletCell)
DECLARE_MASTER_OBJECT_TYPE(TTablet)
DECLARE_MASTER_OBJECT_TYPE(TTabletOwnerBase)
DECLARE_MASTER_OBJECT_TYPE(THunkTablet)
DECLARE_MASTER_OBJECT_TYPE(TTabletBase)
DECLARE_MASTER_OBJECT_TYPE(TTableReplica)
DECLARE_MASTER_OBJECT_TYPE(TTabletAction)
DECLARE_MASTER_OBJECT_TYPE(THunkStorageNode)

struct TTabletStatistics;

class TTabletResources;

extern const std::string DefaultTabletCellBundleName;
extern const std::string SequoiaTabletCellBundleName;

extern const TTimeFormula DefaultTabletBalancerSchedule;

constexpr i64 EdenStoreIdsSizeLimit = 100;

constexpr auto DefaultSyncTabletActionKeepalivePeriod = TDuration::Minutes(1);

constexpr int DefaultTabletCountLimit = 1000;

constexpr int MaxStoresPerBackupMutation = 10000;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
