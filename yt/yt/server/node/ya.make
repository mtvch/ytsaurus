LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ADDINCL(
    contrib/libs/sparsehash/src
)

PROTO_NAMESPACE(yt)

SRCS(
    cluster_node/proxying_chunk_service.cpp
    cluster_node/dynamic_config_manager.cpp
    cluster_node/node_resource_manager.cpp
    cluster_node/master_connector.cpp
    cluster_node/master_heartbeat_reporter_base.cpp
    cluster_node/program.cpp
    cluster_node/bootstrap.cpp
    cluster_node/config.cpp

    data_node/ally_replica_manager.cpp
    data_node/artifact.cpp
    data_node/artifact.proto
    data_node/blob_chunk.cpp
    data_node/blob_reader_cache.cpp
    data_node/blob_session.cpp
    data_node/nbd_session.cpp
    data_node/nbd_chunk_handler.cpp
    data_node/bootstrap.cpp
    data_node/chunk.cpp
    data_node/chunk_detail.cpp
    data_node/chunk_meta_manager.cpp
    data_node/chunk_registry.cpp
    data_node/chunk_store.cpp
    data_node/chunk_reader_sweeper.cpp
    data_node/config.cpp
    data_node/data_node_service.cpp
    data_node/data_node_nbd_service.cpp
    data_node/disk_location.cpp
    data_node/job.cpp
    data_node/job_controller.cpp
    data_node/job_info.cpp
    data_node/journal_chunk.cpp
    data_node/journal_dispatcher.cpp
    data_node/journal_manager.cpp
    data_node/journal_session.cpp
    data_node/io_throughput_meter.cpp
    data_node/local_chunk_reader.cpp
    data_node/location.cpp
    data_node/location_manager.cpp
    data_node/master_connector.cpp
    data_node/medium_directory_manager.cpp
    data_node/medium_updater.cpp
    data_node/network_statistics.cpp
    data_node/offloaded_chunk_read_session.cpp
    data_node/orchid.cpp
    data_node/p2p.cpp
    data_node/session_detail.cpp
    data_node/session_manager.cpp
    data_node/skynet_http_handler.cpp
    data_node/table_schema_cache.cpp
    data_node/ytree_integration.cpp

    exec_node/allocation.cpp
    exec_node/bootstrap.cpp
    exec_node/controller_agent_connector.cpp
    exec_node/chunk_cache.cpp
    exec_node/cache_location.cpp
    exec_node/exec_node_admin_service.cpp
    exec_node/helpers.cpp
    exec_node/gpu_manager.cpp
    exec_node/job_environment.cpp
    exec_node/job.cpp
    exec_node/job_info.cpp
    exec_node/job_directory_manager.cpp
    exec_node/job_gpu_checker.cpp
    exec_node/job_controller.cpp
    exec_node/job_prober_service.cpp
    exec_node/job_proxy_log_manager.cpp
    exec_node/job_workspace_builder.cpp
    exec_node/master_connector.cpp
    exec_node/job_input_cache.cpp
    exec_node/proxying_data_node_service.cpp
    exec_node/orchid.cpp
    exec_node/public.cpp
    exec_node/scheduler_connector.cpp
    exec_node/slot.cpp
    exec_node/slot_location.cpp
    exec_node/slot_manager.cpp
    exec_node/supervisor_service.cpp
    exec_node/throttler_manager.cpp
    exec_node/volume.proto
    exec_node/volume_manager.cpp

    job_agent/job_resource_manager.cpp

    query_agent/config.cpp
    query_agent/helpers.cpp
    query_agent/multiread_request_queue_provider.cpp
    query_agent/query_executor.cpp
    query_agent/query_service.cpp
    query_agent/replication_log_batch_reader.cpp
    query_agent/session.cpp
    query_agent/session_manager.cpp
    query_agent/tablet_replication_log_reader.cpp

    tablet_node/alien_cluster_client_cache.cpp
    tablet_node/alien_cluster_client_cache_base.cpp
    tablet_node/automaton.cpp
    tablet_node/background_activity_orchid.cpp
    tablet_node/backing_store_cleaner.cpp
    tablet_node/backup_manager.cpp
    tablet_node/bootstrap.cpp
    tablet_node/cached_row.cpp
    tablet_node/chaos_agent.cpp
    tablet_node/chunk_replica_cache_pinger.cpp
    tablet_node/chunk_view_size_fetcher.cpp
    tablet_node/compaction_hint_fetcher.cpp
    tablet_node/compression_dictionary_builder.cpp
    tablet_node/compression_dictionary_manager.cpp
    tablet_node/config.cpp
    tablet_node/distributed_throttler_manager.cpp
    tablet_node/dynamic_store_bits.cpp
    tablet_node/error_manager.cpp
    tablet_node/failing_on_rotation_reader.cpp
    tablet_node/hedging_manager_registry.cpp
    tablet_node/hint_manager.cpp
    tablet_node/hunk_chunk.cpp
    tablet_node/hunk_chunk_sweeper.cpp
    tablet_node/hunk_lock_manager.cpp
    tablet_node/hunk_store.cpp
    tablet_node/hunk_tablet.cpp
    tablet_node/hunk_tablet_scanner.cpp
    tablet_node/hunk_tablet_manager.cpp
    tablet_node/hunks_serialization.cpp
    tablet_node/in_memory_manager.cpp
    tablet_node/in_memory_service.cpp
    tablet_node/in_memory_service.proto
    tablet_node/lock_manager.cpp
    tablet_node/locking_state.cpp
    tablet_node/lookup.cpp
    tablet_node/lsm_interop.cpp
    tablet_node/master_connector.cpp
    tablet_node/mutation_forwarder.cpp
    tablet_node/mutation_forwarder_thunk.cpp
    tablet_node/object_detail.cpp
    tablet_node/ordered_chunk_store.cpp
    tablet_node/ordered_dynamic_store.cpp
    tablet_node/ordered_store_manager.cpp
    tablet_node/partition.cpp
    tablet_node/partition_balancer.cpp
    tablet_node/relative_replication_throttler.cpp
    tablet_node/replicated_store_manager.cpp
    tablet_node/replication_log.cpp
    tablet_node/revision_provider.cpp
    tablet_node/row_cache.cpp
    tablet_node/row_digest_fetcher.cpp
    tablet_node/security_manager.cpp
    tablet_node/serialize.cpp
    tablet_node/slot_provider.cpp
    tablet_node/slot_manager.cpp
    tablet_node/smooth_movement_tracker.cpp
    tablet_node/sorted_chunk_store.cpp
    tablet_node/sorted_dynamic_comparer.cpp
    tablet_node/sorted_dynamic_store.cpp
    tablet_node/sorted_store_manager.cpp
    tablet_node/statistics_reporter.cpp
    tablet_node/store.cpp
    tablet_node/store_compactor.cpp
    tablet_node/store_detail.cpp
    tablet_node/store_flusher.cpp
    tablet_node/store_manager_detail.cpp
    tablet_node/store_rotator.cpp
    tablet_node/store_trimmer.cpp
    tablet_node/structured_logger.cpp
    tablet_node/table_config_manager.cpp
    tablet_node/table_replicator.cpp
    tablet_node/table_puller.cpp
    tablet_node/tablet.cpp
    tablet_node/tablet_cell_service.cpp
    tablet_node/tablet_cell_snapshot_validator.cpp
    tablet_node/tablet_manager.cpp
    tablet_node/tablet_memory_statistics.cpp
    tablet_node/tablet_cell_write_manager.cpp
    tablet_node/tablet_profiling.cpp
    tablet_node/tablet_reader.cpp
    tablet_node/tablet_service.cpp
    tablet_node/tablet_slot.cpp
    tablet_node/tablet_snapshot_store.cpp
    tablet_node/tablet_write_manager.cpp
    tablet_node/transaction.cpp
    tablet_node/transaction_manager.cpp
    tablet_node/transaction_manager.proto
    tablet_node/versioned_chunk_meta_manager.cpp
    tablet_node/write_commands.cpp
    tablet_node/write_log.cpp
)

PEERDIR(
    yt/yt/core/service_discovery/yp

    yt/yt/library/query/engine
    yt/yt/library/query/row_comparer
    yt/yt/library/dns_over_rpc/server
    yt/yt/library/re2
    yt/yt/library/containers
    yt/yt/library/containers/cri
    yt/yt/library/gpu
    yt/yt/library/tracing/jaeger
    yt/yt/library/tcmalloc
    yt/yt/library/monitoring
    yt/yt/library/server_program

    yt/yt/ytlib/distributed_throttler

    yt/yt/server/node/cellar_node
    yt/yt/server/node/chaos_node
    yt/yt/server/tools

    yt/yt/server/lib
    yt/yt/server/lib/io
    yt/yt/server/lib/cellar_agent
    yt/yt/server/lib/chaos_node
    yt/yt/server/lib/chunk_server
    yt/yt/server/lib/exec_node
    yt/yt/server/lib/tablet_server
    yt/yt/server/lib/hydra
    yt/yt/server/lib/hydra/dry_run
    yt/yt/server/lib/lsm
    yt/yt/server/lib/misc
    yt/yt/server/lib/nbd
    yt/yt/server/lib/rpc
    yt/yt/server/lib/distributed_chunk_session_server

    # TODO(max42): Eliminate.
    yt/yt/server/lib/controller_agent

    library/cpp/getopt
    library/cpp/containers/bitset

    library/cpp/yt/phdr_cache

    contrib/libs/tcmalloc/malloc_extension
)

END()

RECURSE_FOR_TESTS(
    chaos_node/unittests
    data_node/unittests
    query_agent/unittests
    tablet_node/unittests
)
