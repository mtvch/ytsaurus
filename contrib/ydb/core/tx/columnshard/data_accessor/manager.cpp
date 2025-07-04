#include "manager.h"

#include <contrib/ydb/core/tx/columnshard/hooks/abstract/abstract.h>

#include <contrib/ydb/library/accessor/positive_integer.h>

namespace NKikimr::NOlap::NDataAccessorControl {

void TLocalManager::DrainQueue() {
    std::optional<TInternalPathId> lastPathId;
    IGranuleDataAccessor* lastDataAccessor = nullptr;
    TPositiveControlInteger countToFlight;
    const ui32 inFlightLimit = NYDBTest::TControllers::GetColumnShardController()->GetLimitForPortionsMetadataAsk();
    while (PortionsAskInFlight + countToFlight < inFlightLimit && PortionsAsk.size()) {
        THashMap<TInternalPathId, TPortionsByConsumer> portionsToAsk;
        ui32 packPortionsCount = 0;
        while (PortionsAskInFlight + countToFlight < inFlightLimit && packPortionsCount < std::min<ui32>(inFlightLimit, 1000) &&
               PortionsAsk.size()) {
            auto p = PortionsAsk.front().ExtractPortion();
            const TString consumerId = PortionsAsk.front().GetConsumerId();
            PortionsAsk.pop_front();
            if (!lastPathId || *lastPathId != p->GetPathId()) {
                lastPathId = p->GetPathId();
                auto it = Managers.find(p->GetPathId());
                if (it == Managers.end()) {
                    lastDataAccessor = nullptr;
                } else {
                    lastDataAccessor = it->second.get();
                }
            }
            auto it = RequestsByPortion.find(p->GetPortionId());
            if (it == RequestsByPortion.end()) {
                continue;
            }
            if (!lastDataAccessor) {
                for (auto&& i : it->second) {
                    if (!i->IsFetched() && !i->IsAborted()) {
                        i->AddError(p->GetPathId(), "path id absent");
                    }
                }
                RequestsByPortion.erase(it);
            } else {
                bool toAsk = false;
                for (auto&& i : it->second) {
                    if (!i->IsFetched() && !i->IsAborted()) {
                        toAsk = true;
                    }
                }
                if (!toAsk) {
                    RequestsByPortion.erase(it);
                } else {
                    portionsToAsk[p->GetPathId()].UpsertConsumer(consumerId).AddPortion(p);
                    ++packPortionsCount;
                    ++countToFlight;
                }
            }
        }
        for (auto&& i : portionsToAsk) {
            auto it = Managers.find(i.first);
            AFL_VERIFY(it != Managers.end());
            auto dataAnalyzed = it->second->AnalyzeData(i.second);
            for (auto&& accessor : dataAnalyzed.GetCachedAccessors()) {
                auto it = RequestsByPortion.find(accessor.GetPortionInfo().GetPortionId());
                AFL_VERIFY(it != RequestsByPortion.end());
                for (auto&& i : it->second) {
                    Counters.ResultFromCache->Add(1);
                    if (!i->IsFetched() && !i->IsAborted()) {
                        i->AddAccessor(accessor);
                    }
                }
                RequestsByPortion.erase(it);
                --countToFlight;
            }
            if (!dataAnalyzed.GetPortionsToAsk().IsEmpty()) {
                THashMap<TInternalPathId, TPortionsByConsumer> portionsToAskImpl;
                Counters.ResultAskDirectly->Add(dataAnalyzed.GetPortionsToAsk().GetPortionsCount());
                portionsToAskImpl.emplace(i.first, dataAnalyzed.DetachPortionsToAsk());
                it->second->AskData(std::move(portionsToAskImpl), AccessorCallback);
            }
        }
    }
    PortionsAskInFlight.Add(countToFlight);
    Counters.FetchingCount->Set(PortionsAskInFlight);
    Counters.QueueSize->Set(PortionsAsk.size());
}

void TLocalManager::DoAskData(const std::shared_ptr<TDataAccessorsRequest>& request) {
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "ask_data")("request", request->DebugString());
    for (auto&& pathId : request->GetPathIds()) {
        auto portions = request->StartFetching(pathId);
        for (auto&& [_, i] : portions) {
            auto itRequest = RequestsByPortion.find(i->GetPortionId());
            if (itRequest == RequestsByPortion.end()) {
                AFL_VERIFY(RequestsByPortion.emplace(i->GetPortionId(), std::vector<std::shared_ptr<TDataAccessorsRequest>>({request})).second);
                PortionsAsk.emplace_back(i, request->GetAbortionFlag(), request->GetConsumer());
                Counters.AskNew->Add(1);
            } else {
                itRequest->second.emplace_back(request);
                Counters.AskDuplication->Add(1);
            }
        }
    }
    DrainQueue();
}

void TLocalManager::DoRegisterController(std::unique_ptr<IGranuleDataAccessor>&& controller, const bool update) {
    if (update) {
        auto it = Managers.find(controller->GetPathId());
        if (it != Managers.end()) {
            it->second = std::move(controller);
        }
    } else {
        AFL_VERIFY(Managers.emplace(controller->GetPathId(), std::move(controller)).second);
    }
}

void TLocalManager::DoAddPortion(const TPortionDataAccessor& accessor) {
    {
        auto it = Managers.find(accessor.GetPortionInfo().GetPathId());
        AFL_VERIFY(it != Managers.end());
        it->second->ModifyPortions({ accessor }, {});
    }
    {
        auto it = RequestsByPortion.find(accessor.GetPortionInfo().GetPortionId());
        if (it != RequestsByPortion.end()) {
            for (auto&& i : it->second) {
                i->AddAccessor(accessor);
            }
            --PortionsAskInFlight;
        }
        RequestsByPortion.erase(it);
    }
    DrainQueue();
}

}   // namespace NKikimr::NOlap::NDataAccessorControl
