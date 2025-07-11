#include "chunk_pools_helpers.h"

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/controller_agent/helpers.h>
#include <yt/yt/server/controller_agent/job_size_constraints.h>
#include <yt/yt/server/controller_agent/operation_controller.h>

#include <yt/yt/server/lib/chunk_pools/unittests/chunk_pools_helpers.h>

#include <yt/yt/server/lib/chunk_pools/config.h>
#include <yt/yt/server/lib/chunk_pools/ordered_chunk_pool.h>

#include <yt/yt/ytlib/chunk_client/input_chunk_slice.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/core/misc/blob_output.h>

#include <library/cpp/iterator/zip.h>

#include <util/stream/null.h>

#include <random>

namespace NYT::NChunkPools {
namespace {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NControllerAgent;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NTableClient;

using NControllerAgent::TCompletedJobSummary;

using namespace ::testing;

////////////////////////////////////////////////////////////////////////////////

//! A unit to measure all sizes in this file.
static constexpr i32 Inf32 = std::numeric_limits<i32>::max();
static constexpr i64 Inf64 = std::numeric_limits<i64>::max();

////////////////////////////////////////////////////////////////////////////////

class TOrderedChunkPoolTestBase
    : public Test
{
protected:
    void SetUp() override
    {
        Options_.MinTeleportChunkSize = Inf64;
        Options_.MaxTotalSliceCount = Inf64;
        Options_.ShouldSliceByRowIndices = true;
        Options_.Logger = GetTestLogger();
        DataWeightPerJob_ = Inf64;
        SamplingDataWeightPerJob_ = Inf64;
        CompressedDataSizePerJob_ = Inf64;
        MaxDataSlicesPerJob_ = Inf32;
        InputSliceDataWeight_ = Inf64;
        InputSliceRowCount_ = Inf64;
        MaxDataWeightPerJob_ = Inf64;
        MaxCompressedDataSizePerJob_ = Inf64;
        CanAdjustDataSizePerJob_ = false;
    }

    void InitJobConstraints()
    {
        Options_.JobSizeConstraints = CreateExplicitJobSizeConstraints(
            CanAdjustDataSizePerJob_,
            /*isExplicitJobCount*/ static_cast<bool>(ExplicitJobCount_),
            /*jobCount*/ ExplicitJobCount_.value_or(0),
            DataWeightPerJob_,
            /*primaryDataWeightPerJob*/ Inf64,
            /*compressedDataSizePerJob*/ CompressedDataSizePerJob_,
            MaxDataSlicesPerJob_,
            /*maxDataWeightPerJob_*/ MaxDataWeightPerJob_,
            /*maxPrimaryDataWeightPerJob*/ 0,
            /*maxCompressedDataSizePerJob*/ MaxCompressedDataSizePerJob_,
            InputSliceDataWeight_,
            InputSliceRowCount_,
            BatchRowCount_,
            /*foreignSliceDataWeight*/ 0,
            SamplingRate_,
            SamplingDataWeightPerJob_);
    }

    TInputChunkPtr CreateChunk(
        int tableIndex,
        i64 dataWeight = 1_KB,
        i64 rowCount = 1000,
        std::optional<i64> compressedDataSize = std::nullopt)
    {
        if (!compressedDataSize.has_value()) {
            compressedDataSize = dataWeight;
        }
        auto inputChunk = New<TInputChunk>();
        inputChunk->SetChunkId(MakeRandomId(EObjectType::Chunk, TCellTag(0x42)));
        inputChunk->SetCompressedDataSize(*compressedDataSize);
        inputChunk->SetTotalUncompressedDataSize(dataWeight);
        inputChunk->SetTotalDataWeight(dataWeight);
        inputChunk->SetTableIndex(tableIndex);
        inputChunk->SetTableRowIndex(UnversionedTableRowCounts_[tableIndex]);
        UnversionedTableRowCounts_[tableIndex] += rowCount;
        if (!InputTables_[tableIndex].IsVersioned()) {
            CreatedUnversionedPrimaryChunks_.insert(inputChunk);
        }
        inputChunk->SetTotalRowCount(rowCount);
        return inputChunk;
    }

    void InitTables(std::vector<bool> isTeleportable, std::vector<bool> isVersioned)
    {
        YT_VERIFY(isTeleportable.size() == isVersioned.size() && isVersioned.size() > 0);
        for (int index = 0; index < std::ssize(isVersioned); ++index) {
            InputTables_.emplace_back(isTeleportable[index], /*isPrimary*/ true, isVersioned[index]);
        }
        UnversionedTableRowCounts_.resize(InputTables_.size(), 0);
        OriginalChunks_.resize(InputTables_.size());
    }

    void CreateChunkPool(bool useGenericInputStreamDirectory = false)
    {
        ChunkPool_ = CreateOrderedChunkPool(
            Options_,
            useGenericInputStreamDirectory ? IntermediateInputStreamDirectory : TInputStreamDirectory(InputTables_));
        ChunkPool_->SubscribeChunkTeleported(
            BIND([this] (TInputChunkPtr teleportChunk, std::any tag) {
                TeleportChunks_.emplace(std::any_cast<TOutputCookie>(tag), std::move(teleportChunk));
            }));
    }

    TLegacyDataSlicePtr BuildDataSliceByChunk(const TInputChunkPtr& chunk)
    {
        auto dataSlice = CreateUnversionedInputDataSlice(CreateInputChunkSlice(chunk));
        dataSlice->SetInputStreamIndex(chunk->GetTableIndex());
        dataSlice->TransformToNewKeyless();
        dataSlice->Tag = chunk->GetChunkId().Parts64[0] ^ chunk->GetChunkId().Parts64[1];
        return dataSlice;
    }

    IChunkPoolInput::TCookie AddChunk(const TInputChunkPtr& chunk)
    {
        MaxChunkRowDataWeight_ = std::max(MaxChunkRowDataWeight_, DivCeil(chunk->GetDataWeight(), chunk->GetRowCount()));

        auto dataSlice = BuildDataSliceByChunk(chunk);
        ActiveChunks_.insert(chunk->GetChunkId());
        YT_VERIFY(chunk->GetTableIndex() < std::ssize(OriginalChunks_) );
        OriginalChunks_[chunk->GetTableIndex()].push_back(chunk->GetChunkId());
        return ChunkPool_->Add(New<TChunkStripe>(dataSlice));
    }

    void SuspendChunk(IChunkPoolInput::TCookie cookie, const TInputChunkPtr& chunk)
    {
        YT_VERIFY(ActiveChunks_.erase(chunk->GetChunkId()));
        ChunkPool_->Suspend(cookie);
    }

    void ResumeChunk(IChunkPoolInput::TCookie cookie, const TInputChunkPtr& chunk)
    {
        auto dataSlice = BuildDataSliceByChunk(chunk);
        ActiveChunks_.insert(chunk->GetChunkId());
        return ChunkPool_->Resume(cookie);
    }

    void ExtractOutputCookiesWhilePossible()
    {
        while (ChunkPool_->GetJobCounter()->GetPending()) {
            ExtractCookie(TNodeId(0));
        }
    }

    IChunkPoolOutput::TCookie ExtractCookie(TNodeId nodeId)
    {
        auto cookie = ChunkPool_->Extract(nodeId);
        if (cookie != IChunkPoolOutput::NullCookie) {
            OutputCookies_.insert(cookie);
        }
        return cookie;
    }

    void PersistAndRestore()
    {
        TBlobOutput output;
        TSaveContext saveContext(&output);
        Save(saveContext, ChunkPool_);
        saveContext.Finish();
        auto blob = output.Flush();
        ChunkPool_.Reset();

        TMemoryInput input(blob.Begin(), blob.Size());
        TLoadContext loadContext(&input, RowBuffer_, GetCurrentSnapshotVersion());
        Load(loadContext, ChunkPool_);
        ChunkPool_->SubscribeChunkTeleported(
            BIND([this] (TInputChunkPtr teleportChunk, std::any tag) {
                TeleportChunks_.emplace(std::any_cast<TOutputCookie>(tag), std::move(teleportChunk));
            }));
    }

    std::vector<TChunkStripeListPtr> GetAllStripeLists()
    {
        std::vector<TChunkStripeListPtr> stripeLists;
        for (auto cookie : OutputCookies_) {
            stripeLists.emplace_back(ChunkPool_->GetStripeList(cookie));
        }
        return stripeLists;
    }

    std::vector<TChunkStripeListPtr> GetAllStripeListsByOutputOrder()
    {
        auto outputCookiesInOrder = ChunkPool_->GetOutputCookiesInOrder();
        std::vector<TChunkStripeListPtr> stripeLists;
        for (const auto& cookie : outputCookiesInOrder) {
            if (TeleportChunks_.contains(cookie)) {
                continue;
            }
            stripeLists.push_back(ChunkPool_->GetStripeList(cookie));
        }
        return stripeLists;
    }

    //! Perform all the correctness checks over the given result of ordered chunk pool invocation
    //! (without any suspends nor job interruptions).
    void CheckEverything(
        const std::vector<TChunkStripeListPtr>& stripeLists)
    {
        CheckStripeListsContainOnlyActiveChunks();
        CheckSlicesFollowInOriginalOrder(stripeLists);
    }

    void CheckStripeListsContainOnlyActiveChunks()
    {
        for (auto cookie : OutputCookies_) {
            auto stripeList = ChunkPool_->GetStripeList(cookie);
            for (const auto& stripe : stripeList->Stripes) {
                YT_VERIFY(stripe);
                for (const auto& dataSlice : stripe->DataSlices) {
                    for (const auto& chunkSlice : dataSlice->ChunkSlices) {
                        auto chunk = chunkSlice->GetInputChunk();
                        EXPECT_TRUE(chunk);
                        EXPECT_TRUE(ActiveChunks_.contains(chunk->GetChunkId()));
                    }
                }
            }
        }
    }

    void CheckSlicesFollowInOriginalOrder(const std::vector<TChunkStripeListPtr>& stripeLists)
    {
        std::vector<int> chunkIndices(InputTables_.size());
        for (const auto& stripeList : stripeLists) {
            for (const auto& stripe : stripeList->Stripes) {
                ASSERT_LE(stripe->GetTableIndex(), std::ssize(OriginalChunks_));
                const auto& chunks = OriginalChunks_[stripe->GetTableIndex()];
                int& chunkIndex = chunkIndices[stripe->GetTableIndex()];
                for (const auto& dataSlice : stripe->DataSlices) {
                    if (dataSlice->Type != EDataSourceType::UnversionedTable) {
                        continue;
                    }

                    while (
                        chunkIndex < std::ssize(chunks) &&
                        dataSlice->GetSingleUnversionedChunk()->GetChunkId() != chunks[chunkIndex])
                    {
                        ++chunkIndex;
                    }
                    EXPECT_NE(chunkIndex, std::ssize(chunks));
                }
            }
        }
    }

    void PrintCookie(TOutputCookie cookie)
    {
        if (TeleportChunks_.contains(cookie)) {
            Cerr << "T " << ToString(TeleportChunks_[cookie]->GetChunkId()) << Endl;
        } else {
            Cerr << "C ";
            auto stripeList = ChunkPool_->GetStripeList(cookie);
            for (const auto& dataSlice : stripeList->Stripes[0]->DataSlices) {
                Cerr << ToString(dataSlice->GetSingleUnversionedChunk()->GetChunkId()) << " ";
            }
            Cerr << Endl;
        }
    }

    void SplitJob(IChunkPoolOutput::TCookie cookie, int splitJobCount)
    {
        ChunkPool_->Completed(cookie, SummaryWithSplitJobCount(ChunkPool_->GetStripeList(cookie), splitJobCount));
    }

    void ExpectCookieIsTeleportChunk(TOutputCookie cookie, TInputChunkPtr chunk)
    {
        ASSERT_TRUE(TeleportChunks_.contains(cookie));
        EXPECT_EQ(TeleportChunks_[cookie]->GetChunkId(), chunk->GetChunkId());
    }

    void ExpectCookieIsRegularJob(TOutputCookie cookie, std::vector<TInputChunkPtr> chunks)
    {
        ASSERT_FALSE(TeleportChunks_.contains(cookie));
        auto stripeList = ChunkPool_->GetStripeList(cookie);
        EXPECT_EQ(stripeList->Stripes.size(), 1u);
        EXPECT_EQ(stripeList->Stripes[0]->DataSlices.size(), chunks.size());
        for (int index = 0; index < std::ssize(stripeList->Stripes[0]->DataSlices); ++index) {
            EXPECT_EQ(stripeList->Stripes[0]->DataSlices[index]->GetSingleUnversionedChunk()->GetChunkId(), chunks[index]->GetChunkId());
        }
    }

    void CheckEntryForBatchRowCountAcceptability(TOutputCookie cookie)
    {
        EXPECT_FALSE(TeleportChunks_.contains(cookie));

        auto stripeList = ChunkPool_->GetStripeList(cookie);
        EXPECT_TRUE(stripeList->TotalRowCount % *BatchRowCount_ == 0);
        EXPECT_LE(std::abs(stripeList->TotalDataWeight - DataWeightPerJob_), *BatchRowCount_ * MaxChunkRowDataWeight_);
    }

    void CheckBatchRowCount()
    {
        auto cookies = ChunkPool_->GetOutputCookiesInOrder();
        for (int index = 0; index + 1 < std::ssize(cookies); ++index) {
            CheckEntryForBatchRowCountAcceptability(cookies[index]);
        }
    }

    void CheckExplicitRowCounts(std::vector<i64> rowCounts)
    {
        auto cookies = ChunkPool_->GetOutputCookiesInOrder();
        EXPECT_EQ(std::ssize(cookies), std::ssize(rowCounts));

        for (const auto& [cookie, rowCount] : Zip(cookies, rowCounts)) {
            if (TeleportChunks_.contains(cookie)) {
                EXPECT_EQ(TeleportChunks_[cookie]->GetRowCount(), rowCount);
            } else {
                EXPECT_EQ(ChunkPool_->GetStripeList(cookie)->TotalRowCount, rowCount);
            }
        }
    }

    std::vector<std::vector<TChunkId>> OriginalChunks_;

    IOrderedChunkPoolPtr ChunkPool_;

    //! Set containing all unversioned primary input chunks that have ever been created.
    THashSet<TInputChunkPtr> CreatedUnversionedPrimaryChunks_;
    //! Set containing all chunks that are added to the pool without being suspended.
    THashSet<TChunkId> ActiveChunks_;

    i64 MaxChunkRowDataWeight_ = 0;

    std::vector<TInputStreamDescriptor> InputTables_;

    TRowBufferPtr RowBuffer_ = New<TRowBuffer>();

    THashSet<IChunkPoolOutput::TCookie> OutputCookies_;

    std::vector<int> UnversionedTableRowCounts_;

    TOrderedChunkPoolOptions Options_;

    i64 DataWeightPerJob_;

    i64 SamplingDataWeightPerJob_;

    i32 MaxDataSlicesPerJob_;

    i64 MaxDataWeightPerJob_;

    i64 MaxCompressedDataSizePerJob_;

    i64 CompressedDataSizePerJob_;

    i64 InputSliceDataWeight_;

    i64 InputSliceRowCount_;

    bool CanAdjustDataSizePerJob_;

    std::optional<i64> BatchRowCount_;

    std::optional<i32> ExplicitJobCount_;

    std::optional<double> SamplingRate_;

    std::mt19937 Gen_;

    THashMap<TOutputCookie, TInputChunkPtr> TeleportChunks_;
};

// COMPAT(apollo1321): Remove in 25.2 release.
class TOrderedChunkPoolTest
    : public TOrderedChunkPoolTestBase
    , public WithParamInterface<bool>
{
    void SetUp() override
    {
        TOrderedChunkPoolTestBase::SetUp();
        Options_.UseNewSlicingImplementation = GetParam();
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_P(TOrderedChunkPoolTest, OrderedMergeSimple)
{
    InitTables(
        /*isTeleportable*/ {true, true, true},
        /*isVersioned*/ {false, false, false});

    DataWeightPerJob_ = 2_KB;

    InitJobConstraints();

    auto chunkA1 = CreateChunk(0);
    auto chunkA2 = CreateChunk(0);
    auto chunkB = CreateChunk(1);
    auto chunkC = CreateChunk(2);

    CreateChunkPool();

    AddChunk(chunkA1);
    AddChunk(chunkA2);
    AddChunk(chunkB);
    AddChunk(chunkC);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_EQ(2u, stripeLists.size());

    CheckEverything(stripeLists);
}

////////////////////////////////////////////////////////////////////////////////

TEST_P(TOrderedChunkPoolTest, LargeChunksPreciseSlicing)
{
    InitTables(
        /*isTeleportable*/ {true, true, true},
        /*isVersioned*/ {false, false, false});

    DataWeightPerJob_ = 2_KB;

    InitJobConstraints();

    auto chunkA1 = CreateChunk(0, 10_KB);
    auto chunkA2 = CreateChunk(0, 22_KB);
    auto chunkB = CreateChunk(1, 23_KB);
    auto chunkC = CreateChunk(2, 3_KB);

    CreateChunkPool();

    AddChunk(chunkA1);
    AddChunk(chunkA2);
    AddChunk(chunkB);
    AddChunk(chunkC);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_EQ(29u, stripeLists.size());

    CheckEverything(stripeLists);
}

////////////////////////////////////////////////////////////////////////////////

TEST_P(TOrderedChunkPoolTest, BatchRowCountBasic)
{
    InitTables(
        /*isTeleportable*/ {true, true, true},
        /*isVersioned*/ {false, false, false});

    // This should have no effect!
    Options_.MinTeleportChunkSize = 2_KB;

    // Nor this!
    BatchRowCount_ = 42;
    DataWeightPerJob_ = 2_KB;

    InitJobConstraints();

    auto chunkA1 = CreateChunk(0, 10_KB, 1234);
    auto chunkA2 = CreateChunk(0, 22_KB, 2435);
    auto chunkB = CreateChunk(1, 23_KB, 3434);
    auto chunkC = CreateChunk(2, 3_KB, 333);

    CreateChunkPool();

    AddChunk(chunkA1);
    AddChunk(chunkA2);
    AddChunk(chunkB);
    AddChunk(chunkC);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();

    CheckBatchRowCount();
    CheckEverything(GetAllStripeLists());
}

////////////////////////////////////////////////////////////////////////////////

TEST_P(TOrderedChunkPoolTest, BatchRowCountDoesNotFailWithVersionedChunks)
{
    InitTables(
        /*isTeleportable*/ {true, true, true},
        /*isVersioned*/ {false, true, false});

    // This should have no effect!
    Options_.MinTeleportChunkSize = 2_KB;
    // Nor this!
    BatchRowCount_ = 42;
    DataWeightPerJob_ = 2_KB;

    InitJobConstraints();

    auto chunkA1 = CreateChunk(0, 10_KB, 1234);
    auto chunkA2 = CreateChunk(0, 22_KB, 2435);
    auto chunkB = CreateChunk(1, 23_KB, 3434);
    auto chunkC = CreateChunk(2, 3_KB, 333);

    CreateChunkPool();

    AddChunk(chunkA1);
    AddChunk(chunkA2);
    AddChunk(chunkB);
    AddChunk(chunkC);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();

    CheckEverything(GetAllStripeLists());
}

////////////////////////////////////////////////////////////////////////////////

TEST_P(TOrderedChunkPoolTest, BatchRowCountBigBatchesSmallDataSizePerJob)
{
    InitTables(
        /*isTeleportable*/ {true, true, true},
        /*isVersioned*/ {false, false, false});

    // This should have no effect!
    Options_.MinTeleportChunkSize = 2_KB;
    BatchRowCount_ = 20;
    DataWeightPerJob_ = 2_KB;

    InitJobConstraints();

    auto chunkA1 = CreateChunk(0, 10_KB, 10);
    auto chunkA2 = CreateChunk(0, 30_KB, 5);
    auto chunkB = CreateChunk(1, 60_KB, 30);
    auto chunkC = CreateChunk(2, 3_KB, 6);

    CreateChunkPool();

    AddChunk(chunkA1);
    AddChunk(chunkA2);
    AddChunk(chunkB);
    AddChunk(chunkC);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();

    CheckBatchRowCount();

    auto stripeLists = GetAllStripeLists();
    EXPECT_EQ(std::ssize(stripeLists), 3);
    CheckExplicitRowCounts({20, 20, 11});

    CheckEverything(GetAllStripeLists());
}

////////////////////////////////////////////////////////////////////////////////

TEST_P(TOrderedChunkPoolTest, OrderedMergeOrderedOutput)
{
    InitTables(
        /*isTeleportable*/ {true, true, true},
        /*isVersioned*/ {false, false, false});

    Options_.MinTeleportChunkSize = 2_KB;
    DataWeightPerJob_ = 2_KB;

    InitJobConstraints();

    std::vector<TInputChunkPtr> chunks = {
        CreateChunk(0, 1_KB),
        CreateChunk(0, 10_KB),
        CreateChunk(0, 10_KB),
        CreateChunk(0, 1_KB),
        CreateChunk(0, 1_KB),
        CreateChunk(0, 1_KB),
        CreateChunk(0, 1_KB),
        CreateChunk(0, 1_KB),
        CreateChunk(0, 10_KB),
        CreateChunk(0, 1_KB),
    };

    CreateChunkPool();

    for (const auto& chunk : chunks) {
        AddChunk(chunk);
    }

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();

    PersistAndRestore();

    auto cookiesInOrder = ChunkPool_->GetOutputCookiesInOrder();

    ASSERT_EQ(cookiesInOrder.size(), 8u);
    ExpectCookieIsRegularJob(cookiesInOrder[0], {chunks[0]});
    ExpectCookieIsTeleportChunk(cookiesInOrder[1], chunks[1]);
    ExpectCookieIsTeleportChunk(cookiesInOrder[2], chunks[2]);
    ExpectCookieIsRegularJob(cookiesInOrder[3], {chunks[3], chunks[4]});
    ExpectCookieIsRegularJob(cookiesInOrder[4], {chunks[5], chunks[6]});
    ExpectCookieIsRegularJob(cookiesInOrder[5], {chunks[7]});
    ExpectCookieIsTeleportChunk(cookiesInOrder[6], chunks[8]);
    ExpectCookieIsRegularJob(cookiesInOrder[7], {chunks[9]});

    auto originalCookies = cookiesInOrder;

    SplitJob(originalCookies[4], 10);
    cookiesInOrder = ChunkPool_->GetOutputCookiesInOrder();
    ASSERT_EQ(cookiesInOrder.size(), 10u);

    SplitJob(originalCookies[0], 10);
    cookiesInOrder = ChunkPool_->GetOutputCookiesInOrder();
    ASSERT_EQ(cookiesInOrder.size(), 11u);

    SplitJob(originalCookies[7], 10);
    cookiesInOrder = ChunkPool_->GetOutputCookiesInOrder();
    ASSERT_EQ(cookiesInOrder.size(), 12u);

    ExtractOutputCookiesWhilePossible();

    // entries[0] is now invalidated.
    ExpectCookieIsRegularJob(cookiesInOrder[1], {chunks[0]});
    ExpectCookieIsTeleportChunk(cookiesInOrder[2], chunks[1]);
    ExpectCookieIsTeleportChunk(cookiesInOrder[3], chunks[2]);
    ExpectCookieIsRegularJob(cookiesInOrder[4], {chunks[3], chunks[4]});
    // entries[5] is now invalidated.
    ExpectCookieIsRegularJob(cookiesInOrder[6], {chunks[5]});
    ExpectCookieIsRegularJob(cookiesInOrder[7], {chunks[6]});
    ExpectCookieIsRegularJob(cookiesInOrder[8], {chunks[7]});
    ExpectCookieIsTeleportChunk(cookiesInOrder[9], chunks[8]);
    // entries[10] is now invalidated.
    ExpectCookieIsRegularJob(cookiesInOrder[11], {chunks[9]});
}

////////////////////////////////////////////////////////////////////////////////

TEST_P(TOrderedChunkPoolTest, OrderedMergeSliceLargeChunks)
{
    InitTables(
        /*isTeleportable*/ {false},
        /*isVersioned*/ {false});

    DataWeightPerJob_ = 2_KB;
    InputSliceDataWeight_ = 2_KB;
    InputSliceRowCount_ = 100;

    InitJobConstraints();

    auto chunkA = CreateChunk(0, 20_KB, /*rowCount*/ 1000);

    CreateChunkPool();

    AddChunk(chunkA);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_LE(9u, stripeLists.size());
    EXPECT_LE(stripeLists.size(), 11u);

    CheckEverything(stripeLists);
}

////////////////////////////////////////////////////////////////////////////////

TEST_P(TOrderedChunkPoolTest, ExplicitSingleJob)
{
    InitTables(
        /*isTeleportable*/ {true},
        /*isVersioned*/ {false});

    ExplicitJobCount_ = 1;
    DataWeightPerJob_ = 1_KB;
    MaxDataSlicesPerJob_ = 1;
    InputSliceDataWeight_ = 2_KB;
    InputSliceRowCount_ = 100;

    InitJobConstraints();

    // We have many data slices, large data weight and teleportable chunks.
    // So many reasons to create two jobs.
    auto chunkA = CreateChunk(0, 10_KB, /*rowCount*/ 1000);
    auto chunkB = CreateChunk(0, 10_KB, /*rowCount*/ 1000);

    CreateChunkPool();

    AddChunk(chunkA);
    AddChunk(chunkB);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_EQ(stripeLists.size(), 1u);

    CheckEverything(stripeLists);
}

TEST_P(TOrderedChunkPoolTest, UnsuccessfulSplitMarksJobUnsplittable)
{
    InitTables(
        /*isTeleportable*/ {false},
        /*isVersioned*/ {false});

    DataWeightPerJob_ = 2_KB;
    InitJobConstraints();

    auto chunk = CreateChunk(
        0,
        /*dataWeight*/ 1_KB,
        /*rowCount*/ 1);

    CreateChunkPool();

    AddChunk(chunk);

    ChunkPool_->Finish();

    CheckUnsuccessfulSplitMarksJobUnsplittable(ChunkPool_);
}

TEST_P(TOrderedChunkPoolTest, EnlargementAfterSampling)
{
    // This test verifies that jobs are initially sliced by sampling data
    // weight per job, some jobs are omitted, and the remaining jobs are
    // enlarged to meet the target data weight per job.
    InitTables(
        /*isTeleportable*/ {false},
        /*isVersioned*/ {false});

    DataWeightPerJob_ = 5_KB;
    SamplingDataWeightPerJob_ = 1_KB;
    SamplingRate_ = 0.5;

    InitJobConstraints();
    CreateChunkPool();
    std::map<TChunkId, int> chunkIdToIndex;
    for (int i = 0; i < 100; ++i) {
        auto chunk = CreateChunk(
            0,
            /*dataWeight*/ 1_KB,
            /*rowCount*/ 1);

        chunkIdToIndex[chunk->GetChunkId()] = i;
        AddChunk(chunk);
    }

    ChunkPool_->Finish();
    auto stripeLists = GetAllStripeLists();
    CheckEverything(stripeLists);

    for (const auto& stripeList : stripeLists) {
        EXPECT_NEAR(stripeList->GetAggregateStatistics().DataWeight, 5_KB, 1_KB);
    }

    auto allCookies = ChunkPool_->GetOutputCookiesInOrder();

    int chunkCountAfterSampling = 0;
    int previousChunkIndex = -1;
    int numberOfSmallHoles = 0;

    for (const auto& cookie : allCookies) {
        const auto& stripeList = ChunkPool_->GetStripeList(cookie);
        for (const auto& stripe : stripeList->Stripes) {
            for (const auto& slice : stripe->DataSlices) {
                ++chunkCountAfterSampling;
                int currentChunkIndex = chunkIdToIndex[slice->GetSingleUnversionedChunk()->GetChunkId()];
                ASSERT_LT(previousChunkIndex, currentChunkIndex);
                if (currentChunkIndex != previousChunkIndex + 1) {
                    if (currentChunkIndex - previousChunkIndex - 1 < 5) {
                        ++numberOfSmallHoles;
                    }
                }
                previousChunkIndex = currentChunkIndex;
            }
        }
    }

    EXPECT_NEAR(chunkCountAfterSampling, 50, 10);
    EXPECT_GT(numberOfSmallHoles, 5);
}

TEST_P(TOrderedChunkPoolTest, SliceByDataWeightFirstChunkOnly)
{
    if (!Options_.UseNewSlicingImplementation) {
        GTEST_SKIP_("Compressed data size per job is not supported");
    }

    // +-------+---------------+
    // |       |               |
    // | Data  |               |
    // | Weight|               |
    // |       | #  _  _  _  _ | <- 250_MB
    // +-------+---------------+
    // |       | #  #  #  #  # | <- 1_GB
    // | Compr.| #  #  #  #  # |
    // | Data  | #  #  #  #  # |
    // | Size  | #  #  #  #  # |
    // +-------+---------------+
    // | Chunk | 1  2  3  4  5 |
    // +-------+---------------+
    //
    // Expected slicing:
    //  - Row count: |2|2|2|2|2|   400   |
    //  - Chunk id:  |1|1|1|1|1| 2,3,4,5 |

    InitTables(
        /*isTeleportable*/ {false},
        /*isVersioned*/ {false});

    CompressedDataSizePerJob_ = 6_GB;
    DataWeightPerJob_ = 50_MB;
    InitJobConstraints();

    CreateChunkPool();

    AddChunk(CreateChunk(
        0,
        /*weight*/ 250_MB,
        /*rowCount*/ 10,
        /*compressedSize*/ 1_GB));
    PersistAndRestore();

    for (int i = 0; i < 4; ++i) {
        AddChunk(CreateChunk(
            0,
            /*weight*/ 10_KB,
            /*rowCount*/ 100,
            /*compressedSize*/ 1_GB));
        PersistAndRestore();
    }

    ChunkPool_->Finish();

    PersistAndRestore();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_TRUE(TeleportChunks_.empty());

    EXPECT_EQ(stripeLists.size(), 6u);

    CheckEverything(stripeLists);
}

TEST_P(TOrderedChunkPoolTest, SliceByCompressedDataSizeAndThenByDataWeight)
{
    if (!Options_.UseNewSlicingImplementation) {
        GTEST_SKIP_("Compressed data size per job is not supported");
    }

    // +-------+---------------+
    // |       |          #  # | <- 1_GB
    // | Data  |          #  # |
    // | Weight|    #  #  #  # | <- data_weight_per_job = 500_MB
    // |       | #  #  #  #  # |
    // +-------+---------------+
    // | Compr.| #  #  #       | <- 1_GB
    // | Data  | #  #  #       |
    // | Size  | #  #  #       | <- compressed_data_size_per_job = 500_MB
    // |       | #  #  #  #  # | <- 250_MB
    // +-------+---------------+
    // | Chunk | 1  2  3  4  5 |
    // +-------+---------------+

    InitTables(
        /*isTeleportable*/ {false},
        /*isVersioned*/ {false});

    CompressedDataSizePerJob_ = 500_MB;
    DataWeightPerJob_ = 500_MB;
    InitJobConstraints();

    CreateChunkPool();

    AddChunk(CreateChunk(
        0,
        /*weight*/ 250_MB,
        /*rowCount*/ 4,
        /*compressedSize*/ 1_GB));
    PersistAndRestore();

    for (int i = 0; i < 2; ++i) {
        AddChunk(CreateChunk(
            0,
            /*weight*/ 500_MB,
            /*rowCount*/ 4,
            /*compressedSize*/ 1_GB));
        PersistAndRestore();
    }

    for (int i = 0; i < 2; ++i) {
        AddChunk(CreateChunk(
            0,
            /*weight*/ 1_GB,
            /*rowCount*/ 4,
            /*compressedSize*/ 250_MB));
        PersistAndRestore();
    }

    ChunkPool_->Finish();

    PersistAndRestore();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_TRUE(TeleportChunks_.empty());

    EXPECT_EQ(stripeLists.size(), 10u);

    CheckEverything(stripeLists);
}

INSTANTIATE_TEST_SUITE_P(BasicTests,
    TOrderedChunkPoolTest,
    Values(/*useNewSlicingImplementation*/ false, /*useNewSlicingImplementation*/ true));

////////////////////////////////////////////////////////////////////////////////

class TOrderedChunkPoolTestJobSizeAdjuster
    : public TOrderedChunkPoolTest
{
protected:
    void InitPoolAndFeedChunks()
    {
        InitTables(
            /*isTeleportable*/ {false},
            /*isVersioned*/ {false});

        DataWeightPerJob_ = 1_KB;
        CanAdjustDataSizePerJob_ = true;
        InitJobConstraints();
        Options_.JobSizeAdjusterConfig = New<TJobSizeAdjusterConfig>();
        CreateChunkPool();

        for (int index = 0; index < 10; ++index) {
            auto chunk = CreateChunk(
                0,
                /*dataWeight*/ 1_KB,
                /*rowCount*/ 1);

            AddChunk(chunk);
        }
        ChunkPool_->Finish();
    }

    static TCompletedJobSummary GetJobSummary()
    {
        TCompletedJobSummary jobSummary;
        jobSummary.TotalInputDataStatistics.emplace();
        jobSummary.TotalInputDataStatistics->set_data_weight(1_KB),
        jobSummary.TimeStatistics.PrepareDuration = TDuration::Seconds(100);
        jobSummary.TimeStatistics.ExecDuration = TDuration::Seconds(1);
        return jobSummary;
    }
};

TEST_P(TOrderedChunkPoolTestJobSizeAdjuster, EnlargeSimple)
{
    InitPoolAndFeedChunks();
    EXPECT_EQ(ChunkPool_->GetJobCounter()->GetPending(), 10);

    {
        auto cookie = ExtractCookie(TNodeId(0));
        ASSERT_EQ(cookie, 0);
        ChunkPool_->Completed(cookie, GetJobSummary());
    }

    EXPECT_EQ(ChunkPool_->GetJobCounter()->GetPending(), 5);

    {
        auto cookie = ExtractCookie(TNodeId(0));
        ASSERT_GE(cookie, 10); // Assert enlargement happened.
        ChunkPool_->Completed(cookie, GetJobSummary());
    }

    EXPECT_EQ(ChunkPool_->GetJobCounter()->GetPending(), 2);

    ExtractOutputCookiesWhilePossible();

    auto stripeLists = GetAllStripeListsByOutputOrder();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_EQ(stripeLists.size(), 4u);

    CheckEverything(stripeLists);
}

TEST_P(TOrderedChunkPoolTestJobSizeAdjuster, RemoveFromBeginning)
{
    InitTables(
            /*isTeleportable*/ {false},
            /*isVersioned*/ {false});

    DataWeightPerJob_ = 1_KB;
    CanAdjustDataSizePerJob_ = true;
    InitJobConstraints();
    Options_.JobSizeAdjusterConfig = New<TJobSizeAdjusterConfig>();
    CreateChunkPool();

    for (int index = 0; index < 11; ++index) {
        auto chunk = CreateChunk(
            0,
            /*dataWeight*/ 1_KB + index + 1,
            /*rowCount*/ 1);

        AddChunk(chunk);
    }
    ChunkPool_->Finish();

    EXPECT_EQ(ChunkPool_->GetJobCounter()->GetPending(), 11);

    {
        auto cookie = ExtractCookie(TNodeId(0));
        ASSERT_EQ(cookie, 10);
        ChunkPool_->Completed(cookie, GetJobSummary());
    }

    // No enlargement, because 2_KB is less than the sum of any two jobs.
    EXPECT_EQ(ChunkPool_->GetJobCounter()->GetPending(), 10);

    {
        auto cookie = ExtractCookie(TNodeId(0));
        ASSERT_EQ(cookie, 9);
        ChunkPool_->Completed(cookie, GetJobSummary());
    }

    // Enlargement happened and grouped jobs by three.
    EXPECT_EQ(ChunkPool_->GetJobCounter()->GetPending(), 3);

    {
        auto cookie = ExtractCookie(TNodeId(0));
        ASSERT_EQ(cookie, 13);
        ChunkPool_->Completed(cookie, GetJobSummary());
    }

    EXPECT_EQ(ChunkPool_->GetJobCounter()->GetPending(), 1);

    {
        auto cookie = ExtractCookie(TNodeId(0));
        ASSERT_EQ(cookie, 14);
        ChunkPool_->Completed(cookie, GetJobSummary());
    }

    EXPECT_EQ(ChunkPool_->GetJobCounter()->GetPending(), 0);

    auto stripeLists = GetAllStripeListsByOutputOrder();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_EQ(stripeLists.size(), 4u);

    CheckEverything(stripeLists);
}

TEST_P(TOrderedChunkPoolTestJobSizeAdjuster, EnlargeMaxDataWeight)
{
    MaxDataWeightPerJob_ = 2_KB;
    InitPoolAndFeedChunks();

    EXPECT_EQ(ChunkPool_->GetJobCounter()->GetPending(), 10);

    {
        auto cookie = ExtractCookie(TNodeId(0));
        ASSERT_EQ(cookie, 0);
        ChunkPool_->Completed(cookie, GetJobSummary());
    }

    EXPECT_EQ(ChunkPool_->GetJobCounter()->GetPending(), 5);

    {
        auto cookie = ExtractCookie(TNodeId(0));
        ASSERT_GE(cookie, 10); // Assert enlargement happened.
        ChunkPool_->Completed(cookie, GetJobSummary());
    }

    EXPECT_EQ(ChunkPool_->GetJobCounter()->GetPending(), 4);

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeListsByOutputOrder();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_EQ(stripeLists.size(), 6u);

    CheckEverything(stripeLists);
}

TEST_P(TOrderedChunkPoolTestJobSizeAdjuster, EnlargeMaxCompressedDataSize)
{
    MaxCompressedDataSizePerJob_ = 2_KB;
    InitPoolAndFeedChunks();

    EXPECT_EQ(ChunkPool_->GetJobCounter()->GetPending(), 10);

    {
        auto cookie = ExtractCookie(TNodeId(0));
        ASSERT_EQ(cookie, 0);
        ChunkPool_->Completed(cookie, GetJobSummary());
    }

    EXPECT_EQ(ChunkPool_->GetJobCounter()->GetPending(), 5);

    {
        auto cookie = ExtractCookie(TNodeId(0));
        ASSERT_GE(cookie, 10); // Assert enlargement happened.
        ChunkPool_->Completed(cookie, GetJobSummary());
    }

    EXPECT_EQ(ChunkPool_->GetJobCounter()->GetPending(), 4);

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeListsByOutputOrder();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_EQ(stripeLists.size(), 6u);

    CheckEverything(stripeLists);
}

TEST_P(TOrderedChunkPoolTestJobSizeAdjuster, EnlargeMaxDataSlices)
{
    MaxDataSlicesPerJob_ = 2;
    InitPoolAndFeedChunks();

    EXPECT_EQ(ChunkPool_->GetJobCounter()->GetPending(), 10);

    {
        auto cookie = ExtractCookie(TNodeId(0));
        ASSERT_EQ(cookie, 0);
        ChunkPool_->Completed(cookie, GetJobSummary());
    }

    EXPECT_EQ(ChunkPool_->GetJobCounter()->GetPending(), 5);

    {
        auto cookie = ExtractCookie(TNodeId(0));
        ASSERT_GE(cookie, 10); // Assert enlargement happened.
        ChunkPool_->Completed(cookie, GetJobSummary());
    }

    EXPECT_EQ(ChunkPool_->GetJobCounter()->GetPending(), 4);

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeListsByOutputOrder();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_EQ(stripeLists.size(), 6u);

    CheckEverything(stripeLists);
}

INSTANTIATE_TEST_SUITE_P(OrderedChunkPoolJobSizeAdjuster,
    TOrderedChunkPoolTestJobSizeAdjuster,
    Values(/*useNewSlicingImplementation*/ false, /*useNewSlicingImplementation*/ true));

////////////////////////////////////////////////////////////////////////////////

class TOrderedChunkPoolTestRandomized
    : public WithParamInterface<std::tuple<int, bool>>
    , public TOrderedChunkPoolTestBase
{
public:
    void SetUp() final
    {
        TOrderedChunkPoolTestBase::SetUp();
        Gen_.seed(std::get<0>(GetParam()));
        Options_.UseNewSlicingImplementation = std::get<1>(GetParam());
    }
};

static constexpr int NumberOfRepeats = 15;

TEST_P(TOrderedChunkPoolTestRandomized, VariousOperationsWithPoolTest)
{
    InitTables(
        /*isTeleportable*/ {false},
        /*isVersioned*/ {false});
    DataWeightPerJob_ = 1_KB;
    InitJobConstraints();

    constexpr int chunkCount = 25;
    constexpr int maxJobLosts = 50;

    for (int index = 0; index < chunkCount; ++index) {
        auto chunk = CreateChunk(0);
    }

    CreateChunkPool();

    std::uniform_int_distribution<> dice(0, 99);

    auto chooseRandomElement = [&] (auto container) -> std::optional<typename decltype(container)::value_type> {
        if (container.empty()) {
            return std::nullopt;
        } else {
            auto it = container.begin();
            std::advance(it, std::uniform_int_distribution<>(0, container.size() - 1)(Gen_));
            return std::make_optional(*it);
        }
    };

    // All chunks from the IPersistentChunkPoolInput point of view.
    THashMap<TChunkId, IChunkPoolInput::TCookie> chunkIdToInputCookie;
    THashSet<TChunkId> suspendedChunks;
    THashSet<TChunkId> resumedChunks;
    // All chunks from the IPersistentChunkPoolOutput point of view.
    THashMap<TChunkId, IChunkPoolOutput::TCookie> chunkIdToOutputCookie;
    THashSet<TChunkId> pendingChunks;
    THashSet<TChunkId> startedChunks;
    THashSet<TChunkId> completedChunks;
    THashSet<TChunkId> lostChunks;
    THashMap<TChunkId, TInputChunkPtr> chunkIdToChunk;

    for (const auto& chunk : CreatedUnversionedPrimaryChunks_) {
        auto chunkId = chunk->GetChunkId();
        chunkIdToInputCookie[chunkId] = AddChunk(chunk);
        chunkIdToChunk[chunkId] = chunk;
        resumedChunks.insert(chunkId);
        pendingChunks.insert(chunkId);
    }

    ChunkPool_->Finish();

    ASSERT_EQ(ChunkPool_->GetJobCounter()->GetPending(), chunkCount);

    // Set this to true when debugging locally. It helps a lot to understand what happens.
    constexpr bool EnableDebugOutput = false;
    IOutputStream& Cdebug = EnableDebugOutput ? Cerr : Cnull;

    int jobLosts = 0;

    while (completedChunks.size() < chunkCount) {
        EXPECT_FALSE(ChunkPool_->IsCompleted());

        // 0..0 - pool is persisted and restored;
        // 1..29 - chunk is suspended;
        // 30..54 - chunk is resumed;
        // 55..59 - chunk is extracted;
        // 60..69 - chunk is completed;
        // 70..79 - chunk is failed;
        // 80..89 - chunk is lost.
        // 90..99 - chunk is aborted.
        int eventType = dice(Gen_);
        if (eventType <= 0) {
            Cdebug << "Persisting and restoring the pool" << Endl;
            PersistAndRestore();
        } else if (eventType <= 29) {
            if (auto randomElement = chooseRandomElement(resumedChunks)) {
                auto chunkId = *randomElement;
                Cdebug << Format("Suspending chunk %v", chunkId) << Endl;
                ASSERT_TRUE(resumedChunks.erase(chunkId));
                ASSERT_TRUE(suspendedChunks.insert(chunkId).second);
                auto inputCookie = chunkIdToInputCookie.at(chunkId);
                auto chunk = chunkIdToChunk.at(chunkId);
                SuspendChunk(inputCookie, chunk);
            }
        } else if (eventType <= 54) {
            if (auto randomElement = chooseRandomElement(suspendedChunks)) {
                auto chunkId = *randomElement;
                Cdebug << Format("Resuming chunk %v", chunkId) << Endl;
                ASSERT_TRUE(suspendedChunks.erase(chunkId));
                ASSERT_TRUE(resumedChunks.insert(chunkId).second);
                auto inputCookie = chunkIdToInputCookie.at(chunkId);
                auto chunk = chunkIdToChunk.at(chunkId);
                ResumeChunk(inputCookie, chunk);
            }
        } else if (eventType <= 59) {
            if (ChunkPool_->GetJobCounter()->GetPending()) {
                auto outputCookie = ExtractCookie(TNodeId(0));
                Cdebug << Format("Extracted cookie %v...", outputCookie);
                // TODO(max42): why the following line leads to the linkage error?
                // ASSERT_NE(outputCookie, IChunkPoolOutput::NullCookie);
                // error: undefined reference to 'NYT::NScheduler::IChunkPoolOutput::NullCookie'
                auto stripeList = ChunkPool_->GetStripeList(outputCookie);
                ASSERT_TRUE(stripeList->Stripes[0]);
                const auto& stripe = stripeList->Stripes[0];
                const auto& dataSlice = stripe->DataSlices.front();
                const auto& chunk = dataSlice->GetSingleUnversionedChunk();
                auto chunkId = chunk->GetChunkId();
                Cdebug << Format(" that corresponds to a chunk %v", chunkId) << Endl;
                ASSERT_TRUE(resumedChunks.contains(chunkId));
                ASSERT_TRUE(!suspendedChunks.contains(chunkId));
                if (chunkIdToOutputCookie.contains(chunkId)) {
                    ASSERT_EQ(chunkIdToOutputCookie.at(chunkId), outputCookie);
                } else {
                    ASSERT_TRUE(chunkIdToOutputCookie.emplace(chunkId, outputCookie).second);
                }
                if (lostChunks.contains(chunkId)) {
                    ASSERT_TRUE(lostChunks.erase(chunkId));
                } else {
                    ASSERT_TRUE(pendingChunks.erase(chunkId));
                }
                ASSERT_TRUE(startedChunks.insert(chunkId).second);
            }
        } else if (eventType <= 69) {
            if (auto randomElement = chooseRandomElement(startedChunks)) {
                auto chunkId = *randomElement;
                Cdebug << Format("Completed chunk %v", chunkId) << Endl;
                auto outputCookie = chunkIdToOutputCookie.at(chunkId);
                ASSERT_TRUE(startedChunks.erase(chunkId));
                ASSERT_TRUE(completedChunks.insert(chunkId).second);
                ChunkPool_->Completed(outputCookie, TCompletedJobSummary());
            }
        } else if (eventType <= 79) {
            if (auto randomElement = chooseRandomElement(startedChunks)) {
                auto chunkId = *randomElement;
                Cdebug << Format("Aborted chunk %v", chunkId) << Endl;
                auto outputCookie = chunkIdToOutputCookie.at(chunkId);
                ASSERT_TRUE(startedChunks.erase(chunkId));
                ASSERT_TRUE(pendingChunks.insert(chunkId).second);
                ChunkPool_->Aborted(outputCookie, EAbortReason::Unknown);
            }
        } else if (eventType <= 89) {
            if (jobLosts >= maxJobLosts) {
                continue;
            }
            if (auto randomElement = chooseRandomElement(completedChunks)) {
                auto chunkId = *randomElement;
                Cdebug << Format("Lost chunk %v", chunkId) << Endl;
                auto outputCookie = chunkIdToOutputCookie.at(chunkId);
                ASSERT_TRUE(completedChunks.erase(chunkId));
                ASSERT_TRUE(lostChunks.insert(chunkId).second);
                ChunkPool_->Lost(outputCookie);
                ++jobLosts;
            }
        } else { // if (eventType <= 99)
            if (auto randomElement = chooseRandomElement(startedChunks)) {
                auto chunkId = *randomElement;
                Cdebug << Format("Failed chunk %v", chunkId) << Endl;
                auto outputCookie = chunkIdToOutputCookie.at(chunkId);
                ASSERT_TRUE(startedChunks.erase(chunkId));
                ASSERT_TRUE(pendingChunks.insert(chunkId).second);
                ChunkPool_->Failed(outputCookie);
            }
        }
    }
    ASSERT_TRUE(ChunkPool_->IsCompleted());
    ASSERT_EQ(ChunkPool_->GetJobCounter()->GetPending(), 0);
    ASSERT_EQ(std::ssize(completedChunks), chunkCount);
    ASSERT_EQ(std::ssize(pendingChunks), 0);
    ASSERT_EQ(std::ssize(startedChunks), 0);
    ASSERT_EQ(std::ssize(lostChunks), 0);
    ASSERT_EQ(std::ssize(resumedChunks) + std::ssize(suspendedChunks), chunkCount);
}

INSTANTIATE_TEST_SUITE_P(VariousOperationsWithPoolInstantiation,
    TOrderedChunkPoolTestRandomized,
    Combine(Range(0, NumberOfRepeats), Values(/*useNewSlicingImplementation*/ false, /*useNewSlicingImplementation*/ true)));

////////////////////////////////////////////////////////////////////////////////

class TOrderedChunkPoolJobSizesTestRandomized
    : public WithParamInterface<int>
    , public TOrderedChunkPoolTestBase
{
public:
    void SetUp() final
    {
        TOrderedChunkPoolTestBase::SetUp();
        Gen_.seed(GetParam());
    }
};

TEST_P(TOrderedChunkPoolJobSizesTestRandomized, BuildJobsInputByCompressedDataSizeAndDataWeight)
{
    int tableCount = std::uniform_int_distribution<int>(1, 3)(Gen_);

    bool useJobSizeAdjuster = std::uniform_int_distribution<int>(0, 1)(Gen_);
    if (useJobSizeAdjuster) {
        Options_.JobSizeAdjusterConfig = New<TJobSizeAdjusterConfig>();
    }

    std::vector<bool> isTeleportable(tableCount);
    for (bool& value : isTeleportable) {
        value = std::uniform_int_distribution<int>(0, 1)(Gen_);
    }

    InitTables(
        /*isTeleportable*/ isTeleportable,
        /*isVersioned*/ std::vector<bool>(tableCount));

    auto generateSize = [&] () -> i64 {
        auto baseSizes = std::to_array({1_KB, 1_MB, 100_MB, 1_GB, 512_GB, 1_TB, 10_TB});
        i64 baseSize = baseSizes[std::uniform_int_distribution<i64>(0, std::ssize(baseSizes) - 1)(Gen_)];
        return std::lognormal_distribution<double>()(Gen_) * baseSize;
    };

    auto generateRowCount = [&] () -> i64 {
        auto baseCounts = std::to_array<i64>({100, 1'000, 1'000'000, 1'000'000'000, 1'000'000'000'000});
        i64 baseCount = baseCounts[std::uniform_int_distribution<i64>(0, std::ssize(baseCounts) - 1)(Gen_)];
        return std::lognormal_distribution<double>()(Gen_) * baseCount;
    };

    auto generateTableIndex = [&] {
        return std::uniform_int_distribution(0, tableCount - 1)(Gen_);
    };

    auto generateDuration = [&] {
        return TDuration::Seconds(std::uniform_int_distribution(1, 1000)(Gen_));
    };

    InputSliceRowCount_ = generateRowCount();
    InputSliceDataWeight_ = generateSize();
    if (std::uniform_int_distribution(0, 1)(Gen_) == 0) {
        BatchRowCount_ = std::uniform_int_distribution(10, 1000)(Gen_);
    }
    Options_.MinTeleportChunkSize = generateSize();
    MaxDataSlicesPerJob_ = std::uniform_int_distribution(1, 1000)(Gen_);

    std::vector<TInputChunkPtr> chunks;

    int chunkCount = std::uniform_int_distribution(1, 100)(Gen_);
    i64 totalDataWeight = 0;
    i64 totalCompressedDataSize = 0;
    for (int index = 0; index < chunkCount; ++index) {
        auto chunk = CreateChunk(
            /*tableIndex*/ generateTableIndex(),
            /*dataWeight*/ generateSize(),
            /*tableIndex*/ generateRowCount());
        totalDataWeight += chunk->GetDataWeight();
        totalCompressedDataSize += chunk->GetCompressedDataSize();
        chunks.push_back(std::move(chunk));
    };

    // Don't build too many jobs.
    auto normalizeValue = [&] (i64 value, i64 nominator, i64 maxValue) {
        while (nominator / value > maxValue) {
            value *= 10;
            value += std::uniform_int_distribution<int>(0, 9)(Gen_);
        }
        return value;
    };

    constexpr int approximateMaxJobCount = 50;

    DataWeightPerJob_ = normalizeValue(std::max<i64>(generateSize(), 1), totalDataWeight, approximateMaxJobCount);
    MaxCompressedDataSizePerJob_ = normalizeValue(std::max<i64>(generateSize(), 1), totalCompressedDataSize, approximateMaxJobCount);
    CompressedDataSizePerJob_ = normalizeValue(std::max<i64>(generateSize(), 1), totalCompressedDataSize, approximateMaxJobCount);

    CanAdjustDataSizePerJob_ = true;

    InitJobConstraints();

    CreateChunkPool();

    for (auto& chunk : chunks) {
        AddChunk(std::move(chunk));
    }

    ChunkPool_->Finish();

    while (ChunkPool_->GetJobCounter()->GetPending()) {
        auto cookie = ExtractCookie(TNodeId(0));
        TCompletedJobSummary jobSummary;
        jobSummary.TotalInputDataStatistics.emplace();
        jobSummary.TotalInputDataStatistics->set_data_weight(generateSize()),
        jobSummary.TimeStatistics.PrepareDuration = generateDuration();
        jobSummary.TimeStatistics.ExecDuration = generateDuration();
        ChunkPool_->Completed(cookie, jobSummary);
    }

    CheckEverything(GetAllStripeListsByOutputOrder());

    for (auto cookie : ChunkPool_->GetOutputCookiesInOrder()) {
        if (TeleportChunks_.contains(cookie)) {
            ASSERT_GE(TeleportChunks_[cookie]->GetDataWeight(), Options_.MinTeleportChunkSize);
            continue;
        }

        const auto& stripeList = ChunkPool_->GetStripeList(cookie);

        i64 sliceCount = 0;
        for (const auto& stripe : stripeList->Stripes) {
            sliceCount += std::ssize(stripe->DataSlices);
        }

        if (sliceCount > 1) {
            ASSERT_LE(stripeList->GetAggregateStatistics().CompressedDataSize, MaxCompressedDataSizePerJob_);
        }

        ASSERT_LE(sliceCount, MaxDataSlicesPerJob_);

        if (!BatchRowCount_.has_value()) {
            if (!useJobSizeAdjuster && sliceCount > 1)  {
                i64 maxDataWeightSlice = 0;
                for (const auto& stripe : stripeList->Stripes) {
                    for (const auto& slice : stripe->DataSlices) {
                        maxDataWeightSlice = std::max(maxDataWeightSlice, slice->GetDataWeight());
                    }
                }

                EXPECT_LT(
                    stripeList->TotalDataWeight,
                    DataWeightPerJob_ + maxDataWeightSlice);
            }
        } else if (tableCount == 1 && sliceCount < MaxDataSlicesPerJob_ && !useJobSizeAdjuster) {
            // Stripes are sorted by table index internally, so we can't check data
            // weight guarantee when batch row count is set and table count is more than 1.
            // Also, this whole check does not work with job size adjuster.

            int rowsLeft = *BatchRowCount_;
            i64 lastRowBatchDataWeight = 0;
            int stripeIndex = std::ssize(stripeList->Stripes) - 1;
            while (stripeIndex >= 0 && rowsLeft > 0) {
                int sliceIndex = std::ssize(stripeList->Stripes[stripeIndex]->DataSlices) - 1;
                while (sliceIndex >= 0 && rowsLeft > 0) {
                    const auto& slice = stripeList->Stripes[stripeIndex]->DataSlices[sliceIndex];
                    i64 currentSliceRowCount = std::min<i64>(slice->GetRowCount(), rowsLeft);

                    lastRowBatchDataWeight += std::ceil(static_cast<double>(slice->GetDataWeight()) / slice->GetRowCount()) * currentSliceRowCount;

                    rowsLeft -= currentSliceRowCount;
                    --sliceIndex;
                }
                --stripeIndex;
            }

            EXPECT_LE(stripeList->TotalDataWeight, DataWeightPerJob_ + lastRowBatchDataWeight);
        }
    }
}

INSTANTIATE_TEST_SUITE_P(BuildJobsInputByCompressedDataSizeAndDataWeight,
    TOrderedChunkPoolJobSizesTestRandomized,
    Range(/*start*/ 0, /*end*/ 10000));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkPools
