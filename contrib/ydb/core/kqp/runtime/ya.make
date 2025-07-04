LIBRARY()

SRCS(
    kqp_arrow_memory_pool.cpp
    kqp_compute.cpp
    kqp_effects.cpp
    kqp_output_stream.cpp
    kqp_program_builder.cpp
    kqp_read_actor.cpp
    kqp_read_iterator_common.cpp
    kqp_read_table.cpp
    kqp_runtime_impl.h
    kqp_scan_data.cpp
    kqp_sequencer_actor.cpp
    kqp_sequencer_factory.cpp
    kqp_scan_data_meta.cpp
    kqp_stream_lookup_actor.cpp
    kqp_stream_lookup_actor.h
    kqp_stream_lookup_factory.cpp
    kqp_stream_lookup_factory.h
    kqp_stream_lookup_worker.cpp
    kqp_stream_lookup_worker.h
    kqp_tasks_runner.cpp
    kqp_transport.cpp
    kqp_write_actor_settings.cpp
    kqp_write_actor.cpp
    kqp_write_table.cpp

    scheduler/new/kqp_compute_scheduler_service.cpp
    scheduler/new/kqp_schedulable_actor.cpp
    scheduler/new/tree/dynamic.cpp
    scheduler/new/tree/snapshot.cpp
    scheduler/old/kqp_compute_scheduler.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    library/cpp/threading/hot_swap
    contrib/ydb/core/actorlib_impl
    contrib/ydb/core/base
    contrib/ydb/core/engine
    contrib/ydb/core/engine/minikql
    contrib/ydb/core/formats
    contrib/ydb/core/kqp/common
    contrib/ydb/core/kqp/common/buffer
    contrib/ydb/core/protos
    contrib/ydb/core/scheme
    contrib/ydb/core/ydb_convert
    contrib/ydb/library/yql/dq/actors/protos
    contrib/ydb/library/yql/dq/actors/spilling
    contrib/ydb/library/yql/dq/common
    contrib/ydb/library/yql/dq/runtime
    yql/essentials/minikql/computation/llvm16
    yql/essentials/minikql/comp_nodes
    yql/essentials/utils
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
