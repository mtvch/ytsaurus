#pragma once

#include <yt/yt/ytlib/controller_agent/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/scheduler/public.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

namespace NNode {

class TReqHeartbeat;
class TRspHeartbeat;

class TAllocationToAbort;

} // namespace NNode

class TAllocationStatus;

class TReqScheduleAllocationHeartbeat;
class TRspScheduleAllocationHeartbeat;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

// NB: Please keep the range of values small as this type
// is used as a key of TEnumIndexedArray.
DEFINE_ENUM(EAllocationState,
    ((Scheduled)  (0))
    ((Waiting)    (1))
    ((Running)    (2))
    ((Finishing)  (3))
    ((Finished)   (4))
);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESchedulerAlertType,
    ((UpdatePools)                                  ( 0))
    ((UpdateConfig)                                 ( 1))
    ((UpdateFairShare)                              ( 2))
    ((UpdateArchiveVersion)                         ( 3))
    ((SyncClusterDirectory)                         ( 4))
    ((UnrecognizedConfigOptions)                    ( 5))
    ((OperationsArchivation)                        ( 6))
    ((JobsArchivation)                              ( 7))
    ((UpdateNodesFailed)                            ( 8))
    ((NodesWithoutPoolTree)                         ( 9))
    ((SchedulerCannotConnect)                       (10))
    ((InvalidPoolTreeTemplateConfigSet)             (11))
    ((TooFewControllerAgentsAlive)                  (12))
    ((UpdateUserToDefaultPoolMap)                   (13))
    ((OperationAlertArchivation)                    (14))
    ((ManageSchedulingSegments)                     (15))
    ((UpdateSsdPriorityPreemptionMedia)             (16))
    ((FoundNodesWithUnsupportedPreemption)          (17))
    ((ArchiveIsOutdated)                            (18))
    ((ExperimentAssignmentError)                    (19))
    ((UnrecognizedPoolTreeConfigOptions)            (20))
);

DEFINE_ENUM(EOperationAlertType,
    ((UnusedTmpfsSpace)                             (0))
    ((LostIntermediateChunks)                       (1))
    ((LostInputChunks)                              (2))
    ((IntermediateDataSkew)                         (3))
    ((LongAbortedJobs)                              (4))
    ((ExcessiveDiskUsage)                           (5))
    ((ShortJobsDuration)                            (6))
    ((OperationSuspended)                           (7))
    ((ExcessiveJobSpecThrottling)                   (8))
    ((ScheduleJobTimedOut)                          (9))
    ((InvalidAcl)                                  (10))
    ((LowCpuUsage)                                 (11))
    ((HighCpuWait)                                 (30))
    ((OperationTooLong)                            (12))
    ((OperationPending)                            (13))
    ((OperationCompletedByUserRequest)             (14))
    ((OperationBannedInTentativeTree)              (15))
    ((OwnersInSpecIgnored)                         (16))
    ((OmittedInaccessibleColumnsInInputTables)     (17))
    ((LegacyLivePreviewSuppressed)                 (18))
    ((LowGpuUsage)                                 (19))
    ((LowGpuPower)                                 (29))
    ((LowGpuSMUsage)                               (35))
    ((LowGpuPowerOnWindow)                         (32))
    ((HighQueueTotalTimeEstimate)                  (20))
    ((AutoMergeDisabled)                           (21))
    ((InvalidatedJobsFound)                        (23))
    ((NoTablesWithEnabledDynamicStoreRead)         (24))
    ((UnusedMemory)                                (25))
    ((UserJobMonitoringLimited)                    (26))
    ((MemoryOverconsumption)                       (27))
    ((InvalidControllerRuntimeData)                (28))
    ((CustomStatisticsLimitExceeded)               (31))
    ((BaseLayerProbeFailed)                        (33))
    ((MtnExperimentFailed)                         (34))
    ((NewPartitionsCountIsSignificantlyLarger)     (36))
    ((UseChunkSliceStatisticsDisabled)             (37))
    ((JobIsNotDeterministic)                       (38))
    ((IncompatibleStatistics)                      (39))
    ((UnavailableNetworkBandwidthToClusters)       (40))
    ((WriteBufferMemoryOverrun)                    (41))
    ((Unknown)                                     (42))
    ((SpecifiedCpuLimitIsTooSmall)                 (43))
    ((InvalidDataWeightPerJob)                     (44)) // COMPAT(apollo1321)
);

DEFINE_ENUM_UNKNOWN_VALUE(EOperationAlertType, Unknown);

DEFINE_ENUM(EAgentToSchedulerOperationEventType,
    ((Completed)                (0))
    ((Suspended)                (1))
    ((Failed)                   (2))
    ((Aborted)                  (3))
    ((BannedInTentativeTree)    (4))
    ((InitializationFinished)   (5))
    ((PreparationFinished)      (6))
    ((MaterializationFinished)  (7))
    ((RevivalFinished)          (8))
    ((CommitFinished)           (9))
);

DEFINE_ENUM(ESchedulerToAgentOperationEventType,
    ((UpdateGroupedNeededResources) (0))
    ((UnregisterOperation)          (1))
);

DEFINE_ENUM(EControlQueue,
    (Default)
    (UserRequest)
    (MasterConnector)
    (StaticOrchid)
    (DynamicOrchid)
    (CommonPeriodicActivity)
    (OperationsPeriodicActivity)
    (SchedulerProfiling)
    (Operation)
    (AgentTracker)
    (NodeTracker)
    (OperationsCleaner)
    (FairShareStrategy)
    (EventLog)
    (Metering)
);

DEFINE_ENUM(EControllerAgentPickStrategy,
    (Random)
    (MemoryUsageBalanced)
);

DEFINE_ENUM(ENodeState,
    (Unknown)
    (Offline)
    (Online)
);

DEFINE_ENUM(ESegmentedSchedulingMode,
    (Disabled)
    (LargeGpu)
);

DEFINE_ENUM(ESchedulingSegmentModuleAssignmentHeuristic,
    (MaxRemainingCapacity)
    (MinRemainingFeasibleCapacity)
);

DEFINE_ENUM(ESchedulingSegmentModulePreemptionHeuristic,
    (Greedy)
);

DEFINE_ENUM(ESchedulingSegmentModuleType,
    (DataCenter)
    (InfinibandCluster)
);

DEFINE_ENUM(EOperationPreemptionPriorityScope,
    (OperationOnly)
    (OperationAndAncestors)
);

static constexpr int MaxNodeShardCount = 64;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EOperationManagementAction,
    (Suspend)
    (Resume)
    (Abort)
    (Complete)
    (UpdateParameters)
);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TStrategyTestingOptions)
DECLARE_REFCOUNTED_STRUCT(TOperationStuckCheckOptions)
DECLARE_REFCOUNTED_STRUCT(TFairShareStrategyConfig)
DECLARE_REFCOUNTED_STRUCT(TFairShareStrategyOperationControllerConfig)
DECLARE_REFCOUNTED_CLASS(TFairShareStrategyControllerThrottling)
DECLARE_REFCOUNTED_STRUCT(TTreeTestingOptions)
DECLARE_REFCOUNTED_STRUCT(TFairShareStrategyTreeConfig)
DECLARE_REFCOUNTED_STRUCT(TDelayConfig)
DECLARE_REFCOUNTED_STRUCT(TTestingOptions)
DECLARE_REFCOUNTED_STRUCT(TOperationsCleanerConfig)
DECLARE_REFCOUNTED_STRUCT(TControllerAgentTrackerConfig)
DECLARE_REFCOUNTED_STRUCT(TResourceMeteringConfig)
DECLARE_REFCOUNTED_STRUCT(TPoolTreesTemplateConfig)
DECLARE_REFCOUNTED_STRUCT(TSchedulerConfig)
DECLARE_REFCOUNTED_STRUCT(TSchedulerBootstrapConfig)
DECLARE_REFCOUNTED_STRUCT(TSchedulerProgramConfig)
DECLARE_REFCOUNTED_STRUCT(TSchedulerIntegralGuaranteesConfig)
DECLARE_REFCOUNTED_STRUCT(TFairShareStrategySchedulingSegmentsConfig)
DECLARE_REFCOUNTED_STRUCT(TGpuAllocationSchedulerConfig)
DECLARE_REFCOUNTED_STRUCT(TFairShareStrategySsdPriorityPreemptionConfig)
DECLARE_REFCOUNTED_STRUCT(TBatchOperationSchedulingConfig)
DECLARE_REFCOUNTED_STRUCT(TOperationOptions)

DECLARE_REFCOUNTED_STRUCT(TExperimentEffectConfig)
DECLARE_REFCOUNTED_STRUCT(TExperimentGroupConfig)
DECLARE_REFCOUNTED_STRUCT(TExperimentConfig)
DECLARE_REFCOUNTED_STRUCT(TExperimentAssignment)

DECLARE_REFCOUNTED_STRUCT(TExecNodeDescriptor)
using TExecNodeDescriptorMap = THashMap<NNodeTrackerClient::TNodeId, TExecNodeDescriptorPtr>;
DECLARE_REFCOUNTED_STRUCT(TRefCountedExecNodeDescriptorMap)

class TSchedulingTagFilter;

DECLARE_REFCOUNTED_STRUCT(TControllerScheduleAllocationResult)

struct TAllocationStartDescriptor;
struct TOperationControllerInitializeAttributes;

struct TPreemptedFor;

using TControllerEpoch = NControllerAgent::TControllerEpoch;
using TIncarnationId = NControllerAgent::TIncarnationId;
using TAgentId = NControllerAgent::TAgentId;
using TControllerAgentDescriptor = NControllerAgent::TControllerAgentDescriptor;

static constexpr TControllerEpoch InvalidControllerEpoch = TControllerEpoch(-1);

////////////////////////////////////////////////////////////////////////////////

extern const TString CommittedAttribute;

////////////////////////////////////////////////////////////////////////////////

using TNetworkPriority = i8;

////////////////////////////////////////////////////////////////////////////////

extern const TString DefaultTreeAttributeName;
extern const TString TreeConfigAttributeName;
extern const TString IdAttributeName;
extern const TString ParentIdAttributeName;
extern const TString StrategyStatePath;
extern const TString OldSegmentsStatePath;
extern const TString LastMeteringLogTimePath;

////////////////////////////////////////////////////////////////////////////////

inline const TString ProfilingPoolTreeKey{"tree"};

////////////////////////////////////////////////////////////////////////////////

inline const TString InfinibandClusterNameKey{"infiniband_cluster_tag"};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
