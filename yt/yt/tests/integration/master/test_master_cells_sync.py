from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, wait,
    exists, get, set, ls, remove, create_account, remove_account, make_ace, create_rack,
    create_user, remove_user, add_member, remove_member, create_group, remove_group,
    create_tablet_cell, create_tablet_cell_bundle, remove_tablet_cell_bundle, create_area, wait_for_cells,
    get_driver, disable_tablet_cells_on_node)

import pytest
from flaky import flaky

from yt_helpers import profiler_factory
from yt.common import YtError

##################################################################


@pytest.mark.enabled_multidaemon
class TestMasterCellsSync(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    ENABLE_SECONDARY_CELLS_CLEANUP = False
    NUM_SECONDARY_MASTER_CELLS = 2
    NUM_NODES = 3

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "tablet_manager": {
            "leader_reassignment_timeout": 2000,
            "peer_revocation_timeout": 3000,
        },
    }

    @classmethod
    def setup_class(cls, delayed_secondary_cells_start=False):
        super(TestMasterCellsSync, cls).setup_class()
        cls.delayed_secondary_cells_start = delayed_secondary_cells_start

    def _check_true_for_secondary(self, check):
        if self.delayed_secondary_cells_start:
            self.Env.start_secondary_master_cells(set_config=False)
        try:

            def _check():
                for i in range(self.Env.yt_config.secondary_cell_count):
                    if not check(get_driver(i + 1)):
                        return False
                return True

            timeout = 120 if self.delayed_secondary_cells_start else 60
            wait(_check, timeout=timeout, sleep_backoff=1.0)

        finally:
            if self.delayed_secondary_cells_start:
                for cell_index in range(self.Env.yt_config.secondary_cell_count):
                    self.Env.kill_masters_at_cells(cell_indexes=[cell_index + 1])

    def teardown_method(self, method):
        if self.delayed_secondary_cells_start:
            for cell_index in range(self.Env.yt_config.secondary_cell_count):
                self.Env.start_master_cell(cell_index + 1)
        super(TestMasterCellsSync, self).teardown_method(method)

    @authors("asaitgalin")
    def test_users_sync(self):
        create_user("tester", sync=False)

        for i in range(10):
            set("//sys/users/tester/@custom{0}".format(i), "value")
        self._check_true_for_secondary(
            lambda driver: all(
                [get("//sys/users/tester/@custom{0}".format(i), driver=driver) == "value" for i in range(10)]
            )
        )
        self._check_true_for_secondary(lambda driver: "tester" in ls("//sys/users", driver=driver))

        remove_user("tester", sync=False)
        self._check_true_for_secondary(lambda driver: "tester" not in ls("//sys/users", driver=driver))

    @authors("asaitgalin")
    def test_groups_sync(self):
        create_user("tester", sync=False)
        create_group("sudoers")
        add_member("tester", "sudoers")

        self._check_true_for_secondary(lambda driver: "sudoers" in ls("//sys/groups", driver=driver))
        self._check_true_for_secondary(lambda driver: "tester" in get("//sys/groups/sudoers/@members", driver=driver))
        self._check_true_for_secondary(lambda driver: "sudoers" in get("//sys/users/tester/@member_of", driver=driver))

        for i in range(10):
            set("//sys/groups/sudoers/@attr{0}".format(i), "value")
        remove_member("tester", "sudoers")

        check_attributes = lambda driver: all(  # noqa
            [get("//sys/groups/sudoers/@attr{0}".format(i), driver=driver) == "value" for i in range(10)]
        )
        check_membership = lambda driver: "tester" not in get("//sys/groups/sudoers/@members", driver=driver)  # noqa

        self._check_true_for_secondary(lambda driver: check_attributes(driver) and check_membership(driver))
        remove_group("sudoers")
        self._check_true_for_secondary(lambda driver: "sudoers" not in ls("//sys/groups", driver=driver))

    @authors("asaitgalin")
    def test_accounts_sync(self):
        create_account("tst", sync=False)

        for i in range(10):
            set("//sys/accounts/tst/@attr{0}".format(i), "value")
        self._check_true_for_secondary(
            lambda driver: all(
                [get("//sys/accounts/tst/@attr{0}".format(i), driver=driver) == "value" for i in range(10)]
            )
        )

        remove_account("tst", sync=False)
        self._check_true_for_secondary(lambda driver: "tst" not in ls("//sys/accounts", driver=driver))

    @authors("asaitgalin")
    def test_schemas_sync(self):
        create_group("testers")

        for subj in ["user", "account", "table"]:
            set(
                "//sys/schemas/{0}/@acl/end".format(subj),
                make_ace("allow", "testers", "create"),
            )

        def check(driver):
            ok = True
            for subj in ["user", "account"]:
                found = False
                for acl in get("//sys/schemas/{0}/@acl".format(subj), driver=driver):
                    if "testers" in acl["subjects"]:
                        found = True
                ok = ok and found
            return ok

        self._check_true_for_secondary(lambda driver: check(driver))

    @authors("babenko")
    def test_acl_sync(self):
        create_group("jupiter")
        create_account("jupiter", sync=False)
        set("//sys/accounts/jupiter/@acl", [make_ace("allow", "jupiter", "use")])

        def check(driver):
            return len(get("//sys/accounts/jupiter/@acl", driver=driver)) == 1

        self._check_true_for_secondary(lambda driver: check(driver))

    @authors("babenko")
    def test_rack_sync(self):
        create_rack("r")

        def check(driver):
            return exists("//sys/racks/r")

        self._check_true_for_secondary(lambda driver: check(driver))

    @authors("savrus")
    def test_tablet_cell_bundle_sync(self):
        create_tablet_cell_bundle("b")

        for i in range(10):
            set("//sys/tablet_cell_bundles/b/@custom{0}".format(i), "value")
        self._check_true_for_secondary(
            lambda driver: all(
                [
                    get(
                        "//sys/tablet_cell_bundles/b/@custom{0}".format(i),
                        driver=driver,
                    )
                    == "value"
                    for i in range(10)
                ]
            )
        )

        self._check_true_for_secondary(lambda driver: "b" in ls("//sys/tablet_cell_bundles", driver=driver))

        remove_tablet_cell_bundle("b")
        self._check_true_for_secondary(lambda driver: "b" not in ls("//sys/tablet_cell_bundles", driver=driver))

    @authors("savrus")
    def test_area_sync(self):
        create_tablet_cell_bundle("custom")
        set("//sys/tablet_cell_bundles/custom/@node_tag_filter", "default")
        custom_bundle_id = get("//sys/tablet_cell_bundles/custom/@id")
        custom_area_id = create_area("custom", cell_bundle_id=custom_bundle_id, node_tag_filter="custom")
        default_area_id = get("//sys/tablet_cell_bundles/custom/@areas/default/id")

        def _check1(driver):
            try:
                assert get("//sys/tablet_cell_bundles/custom/@areas/default/node_tag_filter", driver=driver) == "default"
                assert get("//sys/tablet_cell_bundles/custom/@areas/custom/node_tag_filter", driver=driver) == "custom"
                assert get("#{0}/@cell_bundle_id".format(default_area_id), driver=driver) == custom_bundle_id
                assert get("#{0}/@cell_bundle_id".format(custom_area_id), driver=driver) == custom_bundle_id
                assert str(default_area_id) in get("//sys/areas", driver=driver)
                assert str(custom_area_id) in get("//sys/areas", driver=driver)
                return True
            except (AssertionError, YtError):
                return False

        self._check_true_for_secondary(_check1)

        remove_tablet_cell_bundle("custom")

        def _check2(driver):
            try:
                assert str(default_area_id) not in get("//sys/areas", driver=driver)
                assert str(custom_area_id) not in get("//sys/areas", driver=driver)
                return True
            except (AssertionError, YtError):
                return False

        self._check_true_for_secondary(_check2)

    @authors("savrus")
    @flaky(max_runs=5)
    def test_tablet_cell_sync(self):
        create_tablet_cell_bundle("b")
        set(
            "//sys/tablet_cell_bundles/b/@dynamic_options/suppress_tablet_cell_decommission",
            True,
        )
        cell_id = create_tablet_cell(attributes={"tablet_cell_bundle": "b"})
        wait_for_cells()

        def _get_peer_address(cell_id):
            return get("#{0}/@peers/0/address".format(cell_id), default=None)

        peer = _get_peer_address(cell_id)
        disable_tablet_cells_on_node(peer, "test tablet cell sync")
        wait(lambda: _get_peer_address(cell_id) != peer)

        remove("#{0}".format(cell_id))

        config_version = get("#{0}/@config_version".format(cell_id), read_from="leader")
        wait_for_cells()
        assert config_version > 2

        def check(driver):
            return (
                get(
                    "//sys/tablet_cells/{0}/@tablet_cell_bundle".format(cell_id),
                    driver=driver,
                )
                == "b"
                and get(
                    "#{0}/@config_version".format(cell_id),
                    driver=driver,
                    read_from="leader",
                )
                == config_version
                and get(
                    "#{0}/@tablet_cell_life_stage".format(cell_id),
                    driver=driver,
                    read_from="leader",
                )
                == "decommissioned"
            )

        self._check_true_for_secondary(lambda driver: check(driver))

    @authors("savrus")
    def test_cell_area_sync(self):
        custom_area_id = create_area("custom", cellar_type="tablet", cell_bundle="default")
        cell_id = create_tablet_cell(attributes={"area": "custom"})
        self._check_true_for_secondary(lambda driver: get("//sys/tablet_cell_bundles/default/@areas/custom/id", driver=driver) == custom_area_id)
        self._check_true_for_secondary(lambda driver: get("#{0}/@area".format(cell_id), driver=driver) == "custom")
        self._check_true_for_secondary(lambda driver: get("#{0}/@area_id".format(cell_id), driver=driver) == custom_area_id)

    @authors("asaitgalin", "savrus")
    def test_safe_mode_sync(self):
        set("//sys/@config/enable_safe_mode", True)

        def check(driver, value):
            return get("//sys/@config/enable_safe_mode", driver=driver) == value

        self._check_true_for_secondary(lambda driver: check(driver, True))
        set("//sys/@config", {})
        self._check_true_for_secondary(lambda driver: check(driver, False))

    # NB: Think twice before ignoring flap of this test!
    # This test relies on a fact that master dynamic config
    # does not have unrecognized config options, so it is
    # usually broken after adding such options.
    @authors("gritukan")
    def test_master_alerts_sync(self):
        def check(alert_count):
            wait(lambda: len(get("//sys/@master_alerts")) == alert_count)
            # Alerts are not replicated to secondary masters.
            self._check_true_for_secondary(
                lambda driver: len(get("//sys/@master_alerts", driver=driver)) == 0)

        check(0)
        set("//sys/@config/foo", "bar")
        check(1)
        remove("//sys/@config/foo")
        check(0)


##################################################################


class TestMasterCellsSyncDelayed(TestMasterCellsSync):
    ENABLE_MULTIDAEMON = False  # There are defer components starts.
    DEFER_SECONDARY_CELL_START = True
    NUM_TEST_PARTITIONS = 2

    DELTA_NODE_CONFIG = {
        "data_node": {
            "sync_directories_on_connect": False
        }
    }

    @classmethod
    def setup_class(cls):
        super(TestMasterCellsSyncDelayed, cls).setup_class(delayed_secondary_cells_start=True)


##################################################################

@pytest.mark.enabled_multidaemon
class TestMasterHiveProfiling(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_SECONDARY_MASTER_CELLS = 2
    NUM_MASTERS = 1

    @authors("h0pless")
    def test_master_hive_sync(self):
        primary_cell_id = get("//sys/@primary_cell_id")
        secondary_cell_id = get("//sys/@cluster_connection/secondary_masters/0/cell_id")

        secondary_cell_ids = ls("//sys/secondary_masters")
        master_address = ls("//sys/secondary_masters/" + secondary_cell_ids[0])[0]
        profiler = profiler_factory().at_secondary_master(secondary_cell_ids[0], master_address)
        value_counter = profiler.counter("hive/cell_sync_time", tags={"cell_id": secondary_cell_id, "source_cell_id": primary_cell_id})

        # Some syncs always happen, so no need to do anything.
        wait(lambda: value_counter.get() > 0, ignore_exceptions=True)

    @authors("koloshmet")
    def test_master_hive_queues(self):
        set("//sys/@config/cell_master/hive_profiling_period", 500)

        primary_cell_id = get("//sys/@primary_cell_id")
        secondary_cell_id = get("//sys/@cluster_connection/secondary_masters/0/cell_id")

        primary_master_address = ls("//sys/primary_masters")[0]

        secondary_cell_ids = ls("//sys/secondary_masters")
        secondary_master_address = ls("//sys/secondary_masters/" + secondary_cell_ids[0])[0]

        profiler_primary = profiler_factory().at_primary_master(primary_master_address)
        primary_value_counter = profiler_primary.counter(
            "hive/outcoming_messages_queue_size",
            {"cell_id": primary_cell_id, "target_cell_id": secondary_cell_id})

        profiler_secondary = profiler_factory().at_secondary_master(secondary_cell_ids[0], secondary_master_address)
        secondary_value_counter = profiler_secondary.counter(
            "hive/outcoming_messages_queue_size",
            {"cell_id": secondary_cell_id, "target_cell_id": primary_cell_id})

        wait(lambda: primary_value_counter.get() > 0, ignore_exceptions=True)
        wait(lambda: secondary_value_counter.get() > 0, ignore_exceptions=True)


##################################################################
