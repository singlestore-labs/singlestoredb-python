#!/usr/bin/env python
# type: ignore
"""SingleStoreDB Fusion testing."""
import os
import random
import secrets
import tempfile
import time
import unittest
from typing import Any
from typing import List
from typing import Tuple

import pytest

import singlestoredb as s2
from singlestoredb.management import timing
from singlestoredb.tests import utils


class TestFusion(unittest.TestCase):

    dbname: str = ''
    dbexisted: bool = False

    @classmethod
    def setUpClass(cls):
        sql_file = os.path.join(os.path.dirname(__file__), 'test.sql')
        cls.dbname, cls.dbexisted = utils.load_sql(sql_file)

    @classmethod
    def tearDownClass(cls):
        if not cls.dbexisted:
            utils.drop_database(cls.dbname)

    def setUp(self):
        self.enabled = os.environ.get('SINGLESTOREDB_FUSION_ENABLED')
        os.environ['SINGLESTOREDB_FUSION_ENABLED'] = '1'
        self.conn = s2.connect(database=type(self).dbname, local_infile=True)
        self.cur = self.conn.cursor()

    def tearDown(self):
        if self.enabled:
            os.environ['SINGLESTOREDB_FUSION_ENABLED'] = self.enabled
        else:
            del os.environ['SINGLESTOREDB_FUSION_ENABLED']

        try:
            if self.cur is not None:
                self.cur.close()
        except Exception:
            # traceback.print_exc()
            pass

        try:
            if self.conn is not None:
                self.conn.close()
        except Exception:
            # traceback.print_exc()
            pass

    def test_env_var(self):
        os.environ['SINGLESTOREDB_FUSION_ENABLED'] = '0'

        with self.assertRaises(s2.ProgrammingError):
            self.cur.execute('show fusion commands')

        del os.environ['SINGLESTOREDB_FUSION_ENABLED']

        with self.assertRaises(s2.ProgrammingError):
            self.cur.execute('show fusion commands')

        os.environ['SINGLESTOREDB_FUSION_ENABLED'] = 'yes'

        self.cur.execute('show fusion commands')
        assert list(self.cur)

    def test_show_commands(self):
        self.cur.execute('show fusion commands')
        cmds = [x[0] for x in self.cur.fetchall()]
        assert cmds
        assert [x for x in cmds if x.strip().startswith('SHOW FUSION GRAMMAR')], cmds

        self.cur.execute('show fusion commands like "create%"')
        cmds = [x[0] for x in self.cur.fetchall()]
        assert cmds
        assert [x for x in cmds if x.strip().startswith('CREATE')] == cmds, cmds

    def test_show_grammar(self):
        self.cur.execute('show fusion grammar for "create workspace"')
        cmds = [x[0] for x in self.cur.fetchall()]
        assert cmds
        assert [x for x in cmds if x.strip().startswith('CREATE WORKSPACE')], cmds

    def test_cluster_commands_registered(self):
        from singlestoredb.fusion import registry

        want = {
            'SHOW CLUSTERS', 'SHOW CLUSTER REGIONS', 'SHOW PROJECTS',
            'CREATE CLUSTER', 'DROP CLUSTER', 'SUSPEND CLUSTER',
            'RESUME CLUSTER', 'USE CLUSTER', 'SHOW STARTER CLUSTERS',
            'CREATE STARTER CLUSTER', 'DROP STARTER CLUSTER',
        }
        missing = want - set(registry._handlers)
        assert not missing, missing

    def test_show_cluster_status_is_not_shadowed(self):
        """
        ``SHOW CLUSTER STATUS`` must reach the engine, not Fusion.

        The registry matches the longest key first, so registering a bare
        two-word ``SHOW CLUSTER`` would swallow the engine's own
        ``SHOW CLUSTER STATUS``. That is why the region command is spelled
        ``SHOW CLUSTER REGIONS``.
        """
        from singlestoredb.fusion import registry

        assert registry.get_handler('SHOW CLUSTER STATUS') is None
        assert registry.get_handler('SHOW CLUSTERS') is not None
        assert registry.get_handler('SHOW CLUSTER REGIONS') is not None

    def test_create_cluster_grammar(self):
        from singlestoredb.fusion import registry

        self.cur.execute('show fusion grammar for "create cluster"')
        cmds = [x[0] for x in self.cur.fetchall()]
        assert cmds
        assert [x for x in cmds if x.strip().startswith('CREATE CLUSTER')], cmds

        # Assert against the rendered clause list rather than the output of
        # SHOW FUSION GRAMMAR, which also carries the prose remarks -- and
        # those *mention* the absent clauses in order to explain the absence.
        handler = registry._handlers['CREATE CLUSTER']
        handler.compile()
        syntax = handler.syntax

        # v2 assigns no region IDs, so there is no ID alternate to offer.
        assert '<region-id>' not in syntax, syntax
        assert '<region-name>' in syntax, syntax

        # Dropped at v2 (audit item 14). A clause for any of these would
        # parse, be sent, and be silently discarded.
        assert 'KMS' not in syntax.upper(), syntax
        assert 'SMART DR' not in syntax.upper(), syntax
        assert 'PASSWORD' not in syntax.upper(), syntax

    def test_create_workspace_group_grammar_still_has_region_id(self):
        """The v1 command keeps its region-ID alternate; v2 never had one."""
        from singlestoredb.fusion import registry

        handler = registry._handlers['CREATE WORKSPACE GROUP']
        handler.compile()
        syntax = handler.syntax
        assert '<region-id>' in syntax, syntax
        assert 'KMS' in syntax.upper(), syntax

    def test_v1_workspace_commands_are_deprecated(self):
        """
        Every v1 WORKSPACE command points at its v2 CLUSTER replacement.

        ``SHOW REGIONS`` is the sole exception -- v2 assigns no region IDs, so
        ``SHOW CLUSTER REGIONS`` cannot report the ``ID`` column and is not a
        drop-in. Asserted so that adding a v1 command without a pointer, or
        quietly deprecating ``SHOW REGIONS``, fails here.
        """
        from singlestoredb.fusion import registry

        undeprecated = set()
        for key, handler in registry._handlers.items():
            if not handler.__module__.endswith('.workspace'):
                continue
            if handler._deprecated_by:
                # The replacement must be a real command, not a typo.
                assert handler._deprecated_by in registry._handlers, \
                    (key, handler._deprecated_by)
            else:
                undeprecated.add(key)

        assert undeprecated == {'SHOW REGIONS'}, undeprecated

    def test_v2_cluster_commands_are_not_deprecated(self):
        """The replacements must not themselves warn."""
        from singlestoredb.fusion import registry

        for key, handler in registry._handlers.items():
            if handler.__module__.endswith('.cluster'):
                assert not handler._deprecated_by, key

    def test_deprecation_warning_fires_on_execute(self):
        """
        ``_deprecated_by`` warns, names the command, and still runs.

        Driven through a probe handler rather than a real ``WORKSPACE`` command
        so the assertion needs no management API token: what is under test is
        the mechanism in ``SQLHandler.execute``, not any one command's body.
        """
        from singlestoredb.fusion.handler import SQLHandler
        from singlestoredb.warnings import DeprecatedFeatureWarning

        class _DeprecatedProbeHandler(SQLHandler):
            """
            SHOW FUSION DEPRECATION PROBE;

            """

            _deprecated_by = 'SHOW CLUSTERS'

            def run(self, params):
                return None

        # Deliberately not registered -- execute() only needs the class.
        handler = _DeprecatedProbeHandler(self.conn)

        with self.assertWarns(DeprecatedFeatureWarning) as caught:
            res = handler.execute('SHOW FUSION DEPRECATION PROBE')

        msg = str(caught.warning)
        assert 'SHOW FUSION DEPRECATION PROBE' in msg, msg
        assert 'SHOW CLUSTERS' in msg, msg
        # Deprecated, not removed: the command still returns a result.
        assert res is not None

    def test_no_deprecation_warning_by_default(self):
        """A command without ``_deprecated_by`` stays silent."""
        import warnings

        from singlestoredb.warnings import DeprecatedFeatureWarning

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            self.cur.execute('show fusion commands')
            self.cur.fetchall()

        assert not [
            x for x in caught
            if issubclass(x.category, DeprecatedFeatureWarning)
        ], [str(x.message) for x in caught]

    def test_maximal_create_cluster_parses(self):
        from singlestoredb.fusion import registry

        handler = registry._handlers['CREATE CLUSTER']
        handler.compile()

        sql = (
            "CREATE CLUSTER IF NOT EXISTS 'fusion-parse-test' "
            "IN REGION 'us-east-1' WITH PROVIDER 'AWS' "
            "IN PROJECT 'Some Project' "
            "WITH SIZE 'S-00' WITH SCALE FACTOR 1 "
            'AUTO SUSPEND AFTER 30 MINUTES WITH TYPE IDLE '
            'ENABLE KAI WITH CACHE CONFIG 2 '
            "WITH FIREWALL RANGES '0.0.0.0/0' ALLOW ALL TRAFFIC "
            "WITH UPDATE WINDOW '3:5' EXPIRES AT '1h' "
            'WAIT ON ACTIVE'
        )

        inst = handler.__new__(handler)
        inst.connection = None
        inst._handled = set()
        params = inst.visit(handler.grammar.parse(sql))
        for key, value in list(params.items()):
            params[key] = inst.validate_rule(key, value)

        assert params['cluster_name'] == 'fusion-parse-test'
        assert params['in_region'] == {'region_name': 'us-east-1'}
        assert params['with_provider'] == 'AWS'
        assert params['in_project'] == {'project_name': 'Some Project'}
        # <number> must accept a bare integer, not only 1.0
        assert params['with_scale_factor'] == 1.0
        # The clause is one flat dict, not a list of one dict per sub-rule.
        assert params['auto_suspend'] == dict(
            suspend_after_value=30,
            suspend_after_units='MINUTES',
            suspend_type='IDLE',
        )
        assert params['with_update_window'] == '3:5'
        assert params['wait_on_active'] is True

    def test_create_cluster_has_no_v2_only_clauses(self):
        """
        The grammar stops at what the v1 pair exposes.

        ``deploymentType`` and ``multiAZ`` have no ``CREATE WORKSPACE`` or
        ``CREATE WORKSPACE GROUP`` counterpart, so they are reachable only
        through ``ClusterManager.create_cluster``. Asserted rather than left
        implicit: re-adding a clause is a deliberate widening of the SQL
        surface, not a detail of the handler.
        """
        from singlestoredb.fusion import registry

        handler = registry._handlers['CREATE CLUSTER']
        handler.compile()
        syntax = handler.syntax.upper()
        assert 'DEPLOYMENT TYPE' not in syntax, syntax
        assert 'MULTI AZ' not in syntax, syntax
        # ... while the clauses the v1 commands do have are still here.
        for clause in ('ENABLE KAI', 'UPDATE WINDOW', 'CACHE CONFIG'):
            assert clause in syntax, (clause, syntax)

    def test_create_cluster_rejects_region_id(self):
        from singlestoredb.fusion import registry

        handler = registry._handlers['CREATE CLUSTER']
        handler.compile()
        inst = handler.__new__(handler)
        inst.connection = None
        inst._handled = set()

        with self.assertRaises(Exception):
            inst.visit(
                handler.grammar.parse(
                    "CREATE CLUSTER 'c' IN REGION ID 'some-region-id'",
                ),
            )

    def test_stage_handlers_accept_in_cluster(self):
        """All six Stage handlers take IN CLUSTER, IN GROUP and a bare IN."""
        from singlestoredb.fusion import registry
        from singlestoredb.fusion.handler import SQLHandler
        from singlestoredb.fusion.handlers import stage

        handlers = [
            x for x in vars(stage).values()
            if isinstance(x, type)
            and issubclass(x, SQLHandler) and x is not SQLHandler
        ]
        assert len(handlers) == 6, [x.__name__ for x in handlers]

        for cls in handlers:
            cls.compile()
            grammar = cls._grammar
            assert 'in_cluster = IN CLUSTER' in grammar, cls.__name__
            assert 'in_group = IN GROUP' in grammar, cls.__name__
            # in_cluster must precede the bare in_deployment in the
            # alternation, or IN would win before CLUSTER is considered.
            alternation = 'in = { in_cluster | in_group | in_deployment }'
            assert alternation in grammar, cls.__name__

        # SHOW STAGE FILES is representative; the clause is identical on all six.
        cls = registry._handlers['SHOW STAGE FILES']
        cls.compile()
        for sql, key in [
            ("SHOW STAGE FILES IN CLUSTER 'c1'", 'in_cluster'),
            ("SHOW STAGE FILES IN CLUSTER ID 'abc'", 'in_cluster'),
            ("SHOW STAGE FILES IN GROUP 'g1'", 'in_group'),
            ("SHOW STAGE FILES IN 'd1'", 'in_deployment'),
        ]:
            inst = cls.__new__(cls)
            inst.connection = None
            inst._handled = set()
            params = inst.visit(cls.grammar.parse(sql))
            assert key in params['in'], (sql, params['in'])

    def test_fusion_managers_are_version_pinned(self):
        """
        Each Fusion manager names its version rather than following the option.

        The option is an org-wide preference; a handler that *is* one version's
        vocabulary has nothing to learn from it.
        """
        import inspect

        from singlestoredb.fusion.handlers import utils

        assert '_manage_workspaces_v1()' in inspect.getsource(
            utils.get_workspace_manager,
        )
        for func in (utils.get_cluster_manager, utils.get_files_manager):
            assert "version='v2'" in inspect.getsource(func), func.__name__

    def test_get_deployment_resolves_against_v2(self):
        import inspect

        from singlestoredb.fusion.handlers import utils

        src = inspect.getsource(utils.get_deployment)
        assert 'workspace_groups' not in src
        assert 'clusters' in src

    def test_job_commands_use_the_cluster_manager(self):
        """
        JOB commands are not v1 vocabulary.

        A job runs against a deployment, and at v2 a deployment is a cluster,
        so routing them through the v1 manager gave every scheduled job a v1
        ``targetType``.
        """
        import inspect

        from singlestoredb.fusion.handlers import job

        src = inspect.getsource(job)
        assert 'get_workspace_manager' not in src
        assert src.count('get_cluster_manager().organizations.current.jobs') == 8

    def _fusion_env(self, **values):
        """Run with only the deployment variables in ``values`` set."""
        from unittest.mock import patch

        ctx = patch.dict(os.environ)
        ctx.start()
        self.addCleanup(ctx.stop)
        for name in (
            'SINGLESTOREDB_WORKSPACE',
            'SINGLESTOREDB_WORKSPACE_GROUP',
            'SINGLESTOREDB_PROJECT',
        ):
            os.environ.pop(name, None)
        os.environ.update(values)

    def test_project_falls_back_to_the_environment(self):
        """
        ``IN PROJECT`` is optional when the environment names a project.

        The notebook environment publishes ``SINGLESTOREDB_PROJECT``, which may
        hold either a name or an ID, so both spellings have to resolve.
        """
        from unittest.mock import MagicMock
        from unittest.mock import patch

        from singlestoredb.fusion.handlers import utils

        project_id = '11111111-1111-4111-8111-111111111111'
        by_name = MagicMock()
        by_name.name = 'My Project'
        manager = MagicMock()
        manager.projects = [by_name]

        with patch.object(utils, 'get_cluster_manager', return_value=manager):
            self._fusion_env(SINGLESTOREDB_PROJECT=project_id)
            assert utils.get_project({}) is manager.get_project.return_value
            manager.get_project.assert_called_once_with(project_id)

            self._fusion_env(SINGLESTOREDB_PROJECT='My Project')
            assert utils.get_project({}) is by_name

            # A clause still wins over the environment.
            self._fusion_env(SINGLESTOREDB_PROJECT='My Project')
            assert utils.get_project(
                dict(in_project=dict(project_id=project_id)),
            ) is manager.get_project.return_value

            self._fusion_env()
            assert utils.get_project({}) is None

    def test_deployment_refuses_the_group_environment_variable(self):
        """
        ``SINGLESTOREDB_WORKSPACE_GROUP`` holds a group ID, not a cluster ID.

        v2 reports the group only as ``Cluster.group`` and has no route to
        look it up, so guessing which cluster was meant could target the wrong
        deployment.
        """
        from unittest.mock import MagicMock
        from unittest.mock import patch

        from singlestoredb.fusion.handlers import utils

        with patch.object(utils, 'get_cluster_manager', return_value=MagicMock()):
            self._fusion_env(
                SINGLESTOREDB_WORKSPACE_GROUP='11111111-1111-4111-8111-111111111111',
            )
            with self.assertRaises(KeyError) as cm:
                utils.get_deployment({})

        msg = str(cm.exception)
        assert 'SINGLESTOREDB_WORKSPACE_GROUP' in msg
        assert 'SINGLESTOREDB_WORKSPACE' in msg


@pytest.mark.management
@pytest.mark.management_v1
class TestWorkspaceFusion(unittest.TestCase):
    """
    The WORKSPACE and WORKSPACE GROUP grammar, which is the v1 vocabulary.

    Marked ``management_v1`` so it switches off with the rest of the v1
    coverage; ``TestClusterFusion*`` is the v2 replacement. The grammar
    itself, and therefore this suite, goes away with ``management/v1/``.
    """

    id: str = secrets.token_hex(8)
    dbname: str = ''
    dbexisted: bool = False
    workspace_groups: List[Any] = []

    @classmethod
    def setUpClass(cls):
        sql_file = os.path.join(os.path.dirname(__file__), 'test.sql')
        cls.dbname, cls.dbexisted = utils.load_sql(sql_file)
        # Pinned: manage_workspaces() follows the management.version
        # option, and Fusion is v1-only.
        mgr = s2.manage_workspaces(version='v1')
        # US-only: no test here asserts anything about these groups' regions,
        # and creation in some non-US regions fails with a control-plane 500.
        us_regions = [x for x in mgr.regions if x.name.startswith('US')]
        wg = mgr.create_workspace_group(
            f'A Fusion Testing {cls.id}',
            region=random.choice(us_regions),
            firewall_ranges=[],
        )
        cls.workspace_groups.append(wg)
        wg = mgr.create_workspace_group(
            f'B Fusion Testing {cls.id}',
            region=random.choice(us_regions),
            firewall_ranges=[],
        )
        cls.workspace_groups.append(wg)
        wg = mgr.create_workspace_group(
            f'C Fusion Testing {cls.id}',
            region=random.choice(us_regions),
            firewall_ranges=[],
        )
        cls.workspace_groups.append(wg)

    @classmethod
    def tearDownClass(cls):
        if not cls.dbexisted:
            utils.drop_database(cls.dbname)
        while cls.workspace_groups:
            cls.workspace_groups.pop().terminate(force=True)

    def setUp(self):
        self.enabled = os.environ.get('SINGLESTOREDB_FUSION_ENABLED')
        os.environ['SINGLESTOREDB_FUSION_ENABLED'] = '1'
        self.conn = s2.connect(database=type(self).dbname, local_infile=True)
        self.cur = self.conn.cursor()

    def tearDown(self):
        if self.enabled:
            os.environ['SINGLESTOREDB_FUSION_ENABLED'] = self.enabled
        else:
            del os.environ['SINGLESTOREDB_FUSION_ENABLED']

        try:
            if self.cur is not None:
                self.cur.close()
        except Exception:
            # traceback.print_exc()
            pass

        try:
            if self.conn is not None:
                self.conn.close()
        except Exception:
            # traceback.print_exc()
            pass

    def test_show_regions(self):
        self.cur.execute('show regions')
        regs = list(self.cur)
        desc = self.cur.description

        us_regs = [x for x in regs if x[0].startswith('US')]

        assert len(desc) == 3
        assert len(regs) > 5
        assert len(us_regs) > 5

        # LIKE
        self.cur.execute('show regions like "US%"')
        regs = list(self.cur)
        assert regs == us_regs

        # LIMIT
        self.cur.execute('show regions like "US%" limit 3')
        regs = list(self.cur)
        assert len(regs) == 3

        # ORDER BY
        self.cur.execute('show regions like "US%" limit 3 order by name')
        regs = list(self.cur)
        assert len(regs) == 3
        assert regs == list(sorted(regs, key=lambda x: x[0]))

        # Wrong column
        with self.assertRaises(KeyError):
            self.cur.execute('show regions like "US%" limit 3 order by foo')

    def test_show_workspace_groups(self):
        self.cur.execute('show workspace groups')
        wgs = list(self.cur)
        desc = self.cur.description

        assert len(desc) == 4
        assert desc[0].name == 'Name'
        assert desc[1].name == 'ID'
        assert desc[2].name == 'Region'
        assert desc[3].name == 'FirewallRanges'
        assert len(wgs) >= 3

        names = [x[0] for x in wgs]
        assert f'A Fusion Testing {self.id}' in names
        assert f'B Fusion Testing {self.id}' in names
        assert f'C Fusion Testing {self.id}' in names

        # LIKE clause
        self.cur.execute(f'show workspace groups like "A%sion Testing {self.id}"')
        wgs = list(self.cur)

        names = [x[0] for x in wgs]
        assert f'A Fusion Testing {self.id}' in names
        assert f'B Fusion Testing {self.id}' not in names
        assert f'C Fusion Testing {self.id}' not in names

        # LIMIT clause
        self.cur.execute('show workspace groups limit 2')
        wgs = list(self.cur)
        assert len(wgs) == 2

        # EXTENDED attributes
        self.cur.execute('show workspace groups extended')
        wgs = list(self.cur)
        desc = self.cur.description

        assert len(desc) == 6
        assert desc[4].name == 'CreatedAt'
        assert desc[5].name == 'TerminatedAt'

        # ORDER BY
        self.cur.execute(
            f'show workspace groups like "% Fusion Testing {self.id}" order by name desc',
        )
        wgs = list(self.cur)

        names = [x[0] for x in wgs]
        assert names == [
            f'C Fusion Testing {self.id}',
            f'B Fusion Testing {self.id}',
            f'A Fusion Testing {self.id}',
        ]

        # All options
        self.cur.execute(
            f'show workspace groups like "% Fusion Testing {self.id}" '
            'extended order by name desc limit 2',
        )
        wgs = list(self.cur)
        desc = self.cur.description
        names = [x[0] for x in wgs]

        assert len(desc) == 6
        assert names == [f'C Fusion Testing {self.id}', f'B Fusion Testing {self.id}']

    def test_show_workspaces(self):
        mgr = s2.manage_workspaces(version='v1')
        wg = mgr.workspace_groups[f'B Fusion Testing {self.id}']

        self.cur.execute(
            'create workspace show-ws-1 in group '
            f'"B Fusion Testing {self.id}" with size S-00',
        )
        self.cur.execute(
            'create workspace show-ws-2 in group '
            f'"B Fusion Testing {self.id}" with size S-00',
        )
        self.cur.execute(
            'create workspace show-ws-3 in group '
            f'"B Fusion Testing {self.id}" with size S-00',
        )

        # Wait for the three to be listed, not for them to be ACTIVE. Nothing
        # below asserts a state value -- 'State' is checked as a column name,
        # never for its contents -- so all this test needs is that SHOW
        # WORKSPACES can see them. Requiring ACTIVE cost around 450 seconds a
        # run for no assertion, and at a 30 second interval most of that was
        # overshoot. Polled through timing.sleep so a traced run accounts for
        # it; a bare time.sleep here was invisible to the tracer and landed in
        # the unlabelled 'other' bucket.
        wanted = ('show-ws-1', 'show-ws-2', 'show-ws-3')
        deadline = time.time() + 600
        while True:
            listed = [x.name for x in wg.workspaces if x.name in wanted]
            if len(listed) == 3:
                break
            if time.time() >= deadline:
                raise RuntimeError(
                    'timed out waiting for workspaces to be listed; '
                    f'saw {sorted(listed)}',
                )
            timing.sleep(5, 'workspace listed')

        # SHOW
        self.cur.execute(f'show workspaces in group "B Fusion Testing {self.id}"')
        desc = self.cur.description
        out = list(self.cur)
        names = [x[0] for x in out]
        assert len(desc) == 4
        assert [x[0] for x in desc] == ['Name', 'ID', 'Size', 'State']
        assert len(out) >= 3
        assert 'show-ws-1' in names
        assert 'show-ws-2' in names
        assert 'show-ws-3' in names

        # SHOW ID
        self.cur.execute(f'show workspaces in group id {wg.id}')
        desc = self.cur.description
        out = list(self.cur)
        names = [x[0] for x in out]
        assert len(desc) == 4
        assert [x[0] for x in desc] == ['Name', 'ID', 'Size', 'State']
        assert len(out) >= 3
        assert 'show-ws-1' in names
        assert 'show-ws-2' in names
        assert 'show-ws-3' in names

        # LIKE clause
        self.cur.execute(
            'show workspaces in group '
            f'"B Fusion Testing {self.id}" like "%2"',
        )
        out = list(self.cur)
        names = [x[0] for x in out]
        assert len(out) >= 1
        assert [x for x in names if x.endswith('2')]
        assert 'show-ws-1' not in names
        assert 'show-ws-2' in names
        assert 'show-ws-3' not in names

        # Extended attributes
        self.cur.execute(
            'show workspaces in group '
            f'"B Fusion Testing {self.id}" extended',
        )
        desc = self.cur.description
        out = list(self.cur)
        assert len(desc) == 7
        assert [x[0] for x in desc] == [
            'Name', 'ID', 'Size', 'State',
            'Endpoint', 'CreatedAt', 'TerminatedAt',
        ]

        # ORDER BY
        self.cur.execute(
            'show workspaces in group '
            f'"B Fusion Testing {self.id}" order by name desc',
        )
        out = list(self.cur)
        desc = self.cur.description
        assert len(desc) == 4
        names = [x[0] for x in out]
        assert names == ['show-ws-3', 'show-ws-2', 'show-ws-1']

        # LIMIT clause
        self.cur.execute(
            'show workspaces in group '
            f'"B Fusion Testing {self.id}" order by name desc limit 2',
        )
        out = list(self.cur)
        desc = self.cur.description
        assert len(desc) == 4
        names = [x[0] for x in out]
        assert names == ['show-ws-3', 'show-ws-2']

        # All options
        self.cur.execute(
            f'show workspaces in group "B Fusion Testing {self.id}" '
            'like "show-ws%" extended order by name desc limit 2',
        )
        out = list(self.cur)
        desc = self.cur.description
        assert len(desc) == 7
        names = [x[0] for x in out]
        assert names == ['show-ws-3', 'show-ws-2']

    def test_create_drop_workspace(self):
        mgr = s2.manage_workspaces(version='v1')
        wg = mgr.workspace_groups[f'A Fusion Testing {self.id}']

        self.cur.execute(
            f'create workspace foobar-1 in group "A Fusion Testing {self.id}" '
            'with size S-00 wait on active',
        )
        foobar_1 = [x for x in wg.workspaces if x.name == 'foobar-1']
        assert len(foobar_1) == 1

        self.cur.execute(
            f'create workspace foobar-2 in group "A Fusion Testing {self.id}" '
            'with size S-00 wait on active',
        )
        foobar_2 = [x for x in wg.workspaces if x.name == 'foobar-2']
        assert len(foobar_2) == 1

        # Drop by name
        self.cur.execute(
            f'drop workspace "foobar-1" in group "A Fusion Testing {self.id}" '
            'wait on terminated',
        )
        foobar_1 = [x for x in wg.workspaces if x.name == 'foobar-1']
        assert len(foobar_1) == 0

        # Drop by ID
        foobar_2_id = foobar_2[0].id
        self.cur.execute(
            f'drop workspace id {foobar_2_id} in group '
            f'"A Fusion Testing {self.id}" wait on terminated',
        )
        foobar_2 = [x for x in wg.workspaces if x.name == 'foobar-2']
        assert len(foobar_2) == 0

        # Drop non-existent by ID
        with self.assertRaises(KeyError):
            self.cur.execute(
                f'drop workspace id {foobar_2_id} '
                f'in group "A Fusion Testing {self.id}"',
            )

        # Drop non-existent by ID with IF EXISTS
        self.cur.execute(
            f'drop workspace IF EXISTS id {foobar_2_id} '
            f'in group "A Fusion Testing {self.id}"',
        )

    def _wait_workspace_group_gone(self, mgr, wg_name, timeout=60, interval=2):
        # WAIT ON TERMINATED polls /workspaceGroups/{id}; the LIST endpoint
        # /workspaceGroups can lag briefly behind it, so poll until the listed
        # record is either absent or shows terminated_at.
        deadline = time.time() + timeout
        while True:
            wg = [x for x in mgr.workspace_groups if x.name == wg_name]
            if not wg or all(x.terminated_at is not None for x in wg):
                return
            if time.time() >= deadline:
                self.fail(
                    f'workspace group {wg_name!r} still active in list endpoint '
                    f'after {timeout}s: {wg!r}',
                )
            time.sleep(interval)

    def test_create_drop_workspace_group(self):
        mgr = s2.manage_workspaces(version='v1')

        reg = [x for x in mgr.regions if x.name.startswith('US')][0]
        wg_name = f'Create WG Test {id(self)}'

        try:
            self.cur.execute(
                f'create workspace group "{wg_name}" '
                f'in region "{reg.name}"',
            )
            wg = [
                x for x in mgr.workspace_groups
                if x.name == wg_name and x.terminated_at is None
            ]
            assert len(wg) == 1

            # Drop it by name
            self.cur.execute(
                f'drop workspace group "{wg_name}" '
                'wait on terminated',
            )
            self._wait_workspace_group_gone(mgr, wg_name)

            # Create it again
            self.cur.execute(
                f'create workspace group "{wg_name}" in region "{reg.name}"',
            )
            wg = [
                x for x in mgr.workspace_groups
                if x.name == wg_name and x.terminated_at is None
            ]
            assert len(wg) == 1

            # Drop it by ID
            wg_id = wg[0].id
            self.cur.execute(f'drop workspace group id {wg_id} wait on terminated')
            self._wait_workspace_group_gone(mgr, wg_name)

            # Drop non-existent
            with self.assertRaises(KeyError):
                self.cur.execute(f'drop workspace group id {wg_id}')

            # Drop non-existent with IF EXISTS
            self.cur.execute(f'drop workspace group if exists id {wg_id}')

        finally:
            try:
                mgr.workspace_groups[wg_name].terminate(force=True)
            except Exception:
                pass


class _ClusterFusionMixin:
    """
    Plumbing shared by the CLUSTER fusion suites.

    These are the v2 mirror of :class:`TestWorkspaceFusion`, flat rather than
    nested. A cluster is created in one statement where a workspace needed
    two, so there is no group fixture and no ``IN GROUP`` clause anywhere.
    Names are lowercase and hyphenated because ``POST /v2/clusters`` enforces
    ``[a-z0-9]([a-z0-9-]*[a-z0-9])?`` at 1-32 characters (audit item 7) --
    the spaced names the v1 suite uses are rejected.

    This was one class deploying three clusters in ``setUpClass``, which every
    test then waited out whether or not it touched a cluster: the two
    lifecycle tests deploy their own and the region, project and grammar
    tests need none at all, yet all of them paid for three. The classes below
    declare what they need in :attr:`fixture_prefixes` instead, so the
    cluster-less ones start immediately and no class deploys more than it
    reads.

    Not a ``TestCase``, and named with a leading underscore: pytest collects
    any ``Test``-prefixed ``TestCase`` subclass it can reach, so a base that
    was either would run every inherited test a second time under a fixture
    of its own.
    """

    #: Prefixes of the shared clusters to deploy before this class's tests,
    #: named ``<prefix>-fusion-cluster-<id>``. Empty means the class needs no
    #: deployment, which is true of most of them.
    fixture_prefixes: Tuple[str, ...] = ()

    #: Set per class in setUpClass rather than once for the module, so the
    #: ``LIKE`` patterns in a class can only ever match clusters that class
    #: created. The exact-count assertion in ``test_show_clusters_like`` used
    #: to rely on the lifecycle tests sorting alphabetically after it and
    #: their clusters leaving the list endpoint in time; with a per-class id
    #: it holds whatever else is running.
    id: str = ''
    dbname: str = ''
    dbexisted: bool = False
    clusters: List[Any] = []
    manager: Any = None
    project_id: str = ''
    us_regions: List[Any] = []

    @classmethod
    def _project_id(cls, mgr):
        """Pick the project to deploy into, or skip. POST requires one."""
        from_env = os.environ.get('SINGLESTOREDB_PROJECT')
        if from_env:
            return from_env
        standard = [x for x in mgr.projects if x.edition == 'STANDARD']
        if not standard:
            raise unittest.SkipTest(
                'No STANDARD project in this organization; set '
                'SINGLESTOREDB_PROJECT to the project to deploy into',
            )
        return standard[0].id

    @classmethod
    def setUpClass(cls):
        cls.id = secrets.token_hex(4)
        # Rebound per class: a list on the mixin would be one object shared by
        # every subclass, so one class's teardown would pop another's clusters.
        cls.clusters = []

        sql_file = os.path.join(os.path.dirname(__file__), 'test.sql')
        cls.dbname, cls.dbexisted = utils.load_sql(sql_file)

        # Pinned: the CLUSTER commands are the v2 vocabulary, so the fixture
        # must not follow the management.version option out of v2 either.
        mgr = s2.manage_clusters(version='v2')
        cls.manager = mgr

        cls.us_regions = [
            x for x in mgr.regions
            if 'US' in x.name or 'us-' in (x.region_name or '')
        ]
        if not cls.us_regions:
            raise unittest.SkipTest('No US regions reported by the v2 API')

        cls.project_id = cls._project_id(mgr)

        for prefix in cls.fixture_prefixes:
            region = random.choice(cls.us_regions)
            cls.clusters.append(
                mgr.create_cluster(
                    f'{prefix}-fusion-cluster-{cls.id}',
                    region=region,
                    size='S-00',
                    project=cls.project_id,
                    wait_on_active=True,
                    wait_timeout=1200,
                ),
            )

    @classmethod
    def tearDownClass(cls):
        if not cls.dbexisted:
            utils.drop_database(cls.dbname)
        while cls.clusters:
            cluster = cls.clusters.pop()
            try:
                # No wait_on_terminated: teardown only needs the DELETE to
                # land, and waiting each cluster out serially costs minutes
                # that assert nothing. Anything the DELETE fails to remove is
                # swept by utils.cleanup_tracked. The one place termination has
                # to be observed is test_create_drop_cluster, which polls the
                # listing itself through _wait_cluster_gone.
                cluster.terminate(force=True)
            except Exception:
                pass

    def setUp(self):
        self.enabled = os.environ.get('SINGLESTOREDB_FUSION_ENABLED')
        os.environ['SINGLESTOREDB_FUSION_ENABLED'] = '1'
        self.conn = s2.connect(database=type(self).dbname, local_infile=True)
        self.cur = self.conn.cursor()

    def tearDown(self):
        if self.enabled:
            os.environ['SINGLESTOREDB_FUSION_ENABLED'] = self.enabled
        else:
            del os.environ['SINGLESTOREDB_FUSION_ENABLED']

        try:
            if self.cur is not None:
                self.cur.close()
        except Exception:
            pass

        try:
            if self.conn is not None:
                self.conn.close()
        except Exception:
            pass


@pytest.mark.management
class TestClusterFusion(_ClusterFusionMixin, unittest.TestCase):
    """
    ``SHOW CLUSTERS`` against three deployed clusters.

    Three of them so the ``LIKE``/``ORDER BY``/``LIMIT`` assertions have
    something to sort. Nothing here mutates a cluster, which is what makes the
    fixture shareable -- ``SUSPEND``/``RESUME`` cannot share it and deploys its
    own in :class:`TestClusterFusionSuspendResume`.
    """

    fixture_prefixes = ('a', 'b', 'c')

    def test_show_clusters(self):
        self.cur.execute('show clusters')
        names = [x[0] for x in self.cur.fetchall()]
        assert self.cur.description[0][0] == 'Name'
        for prefix in ('a', 'b', 'c'):
            assert f'{prefix}-fusion-cluster-{self.id}' in names, names

    def test_show_clusters_columns(self):
        self.cur.execute('show clusters')
        cols = [x[0] for x in self.cur.description]
        assert cols == ['Name', 'ID', 'Region', 'Size', 'State'], cols

        self.cur.execute('show clusters extended')
        cols = [x[0] for x in self.cur.description]
        assert cols == [
            'Name', 'ID', 'Region', 'Size', 'State', 'Provider', 'Endpoint',
            'DeploymentType', 'FirewallRanges', 'ProjectID', 'CreatedAt',
            'TerminatedAt',
        ], cols

        rows = {x[0]: x for x in self.cur.fetchall()}
        row = rows[f'a-fusion-cluster-{self.id}']
        # Region is the provider slug; Cluster has no region object at v2.
        assert row[2], row
        assert row[5], row
        assert row[9] == type(self).project_id, row

    def test_show_clusters_like(self):
        self.cur.execute(f'show clusters like "a-fusion-cluster-{self.id}"')
        names = [x[0] for x in self.cur.fetchall()]
        assert names == [f'a-fusion-cluster-{self.id}'], names

        self.cur.execute(f'show clusters like "%-fusion-cluster-{self.id}"')
        names = [x[0] for x in self.cur.fetchall()]
        assert len(names) == 3, names

    def test_show_clusters_order_by_and_limit(self):
        self.cur.execute(
            f'show clusters like "%-fusion-cluster-{self.id}" order by name',
        )
        names = [x[0] for x in self.cur.fetchall()]
        assert names == sorted(names), names

        self.cur.execute(
            f'show clusters like "%-fusion-cluster-{self.id}" '
            'order by name desc',
        )
        names = [x[0] for x in self.cur.fetchall()]
        assert names == sorted(names, reverse=True), names

        self.cur.execute(
            f'show clusters like "%-fusion-cluster-{self.id}" '
            'order by name limit 2',
        )
        names = [x[0] for x in self.cur.fetchall()]
        assert len(names) == 2, names


@pytest.mark.management
class TestClusterFusionReadOnly(_ClusterFusionMixin, unittest.TestCase):
    """
    The handlers that read something the organization already has.

    Projects, regions and starter clusters are all pre-existing, so this class
    deploys nothing -- these assertions were waiting on three clusters they
    never looked at. Nothing here asserts a row count over a listing, so other
    suites deploying at the same time cannot disturb them.
    """

    def test_show_projects(self):
        self.cur.execute('show projects')
        cols = [x[0] for x in self.cur.description]
        assert cols == ['Name', 'ID', 'Edition', 'CreatedAt'], cols
        ids = [x[1] for x in self.cur.fetchall()]
        assert type(self).project_id in ids, ids

    def test_show_cluster_regions(self):
        self.cur.execute('show cluster regions')
        cols = [x[0] for x in self.cur.description]
        # No ID column: v2 assigns no region IDs. This doubles as the live
        # check that the region shape is what the wrappers assume.
        assert cols == ['Name', 'Provider', 'RegionName'], cols

        rows = self.cur.fetchall()
        assert rows
        for name, provider, region_name in rows:
            assert name, rows
            assert provider, rows
            assert region_name, rows
            # The display name and the provider slug are different senses on
            # this route; if they were equal the wrappers would be reading
            # the wrong field.
            assert region_name != name or ' ' not in name

    def test_show_cluster_regions_like(self):
        self.cur.execute('show cluster regions like "US%" order by name')
        names = [x[0] for x in self.cur.fetchall()]
        assert names, names
        assert all(x.startswith('US') for x in names), names
        assert names == sorted(names), names

    def test_show_starter_clusters(self):
        self.cur.execute('show starter clusters')
        cols = [x[0] for x in self.cur.description]
        assert cols == ['Name', 'ID', 'DatabaseName'], cols

        self.cur.execute('show starter clusters extended')
        cols = [x[0] for x in self.cur.description]
        assert cols == [
            'Name', 'ID', 'DatabaseName', 'Endpoint', 'ProjectID',
        ], cols

    def test_drop_starter_cluster_if_exists(self):
        """IF EXISTS must swallow the miss; the bare form must not."""
        with self.assertRaises(KeyError):
            self.cur.execute('drop starter cluster "no-such-starter-xyz"')
        self.cur.execute('drop starter cluster if exists "no-such-starter-xyz"')


@pytest.mark.management
class TestClusterFusionCreateDrop(_ClusterFusionMixin, unittest.TestCase):
    """
    ``CREATE CLUSTER`` and ``DROP CLUSTER`` end to end.

    Deploys nothing up front: the test creates, drops and recreates a cluster
    of its own, so the three shared fixtures it used to inherit were pure cost.
    Alone in its class because it is the longest test in the repo -- most of
    twenty minutes, nearly all of it provisioning -- and anything sharing the
    class would queue behind it.
    """

    def _wait_cluster_gone(self, name, timeout=180, interval=5):
        """
        Poll until the LIST endpoint agrees the cluster is gone.

        The mirror of ``_wait_workspace_group_gone``: ``WAIT ON TERMINATED``
        polls ``GET /v2/clusters/{id}``, and ``GET /v2/clusters`` can lag
        behind it, so a create-drop-create sequence sees a stale record.
        """
        mgr = type(self).manager
        deadline = time.time() + timeout
        while True:
            found = [x for x in mgr.clusters if x.name == name]
            if not found or all(x.terminated_at is not None for x in found):
                return
            if time.time() >= deadline:
                self.fail(
                    f'cluster {name!r} still active in the list endpoint '
                    f'after {timeout}s: {found!r}',
                )
            time.sleep(interval)

    def test_create_drop_cluster(self):
        mgr = type(self).manager
        name = f'd-fusion-cluster-{self.id}'
        region = type(self).us_regions[0]

        try:
            self.cur.execute(
                f'create cluster "{name}" in region "{region.region_name}" '
                f'with provider "{region.provider}" '
                f'in project id "{type(self).project_id}" '
                'with size "S-00" wait on active',
            )

            # Unlike CREATE WORKSPACE GROUP, this returns a row -- the
            # generated password appears in the create response and nowhere
            # else, so a caller who cannot see it has no admin access.
            row = self.cur.fetchall()
            cols = [x[0] for x in self.cur.description]
            assert cols == ['Name', 'ID', 'Endpoint', 'AdminPassword'], cols
            assert len(row) == 1, row
            assert row[0][0] == name, row
            assert row[0][1], row

            live = [
                x for x in mgr.clusters
                if x.name == name and x.terminated_at is None
            ]
            assert len(live) == 1, live
            cluster_id = live[0].id

            # IF NOT EXISTS on a live cluster is a no-op
            self.cur.execute(
                f'create cluster if not exists "{name}" '
                f'in region "{region.region_name}" '
                f'in project id "{type(self).project_id}"',
            )
            live = [
                x for x in mgr.clusters
                if x.name == name and x.terminated_at is None
            ]
            assert len(live) == 1, live

            # Drop by name
            self.cur.execute(f'drop cluster "{name}" wait on terminated')
            self._wait_cluster_gone(name)

            # Create again, drop by ID
            self.cur.execute(
                f'create cluster "{name}" in region "{region.region_name}" '
                f'in project id "{type(self).project_id}" wait on active',
            )
            live = [
                x for x in mgr.clusters
                if x.name == name and x.terminated_at is None
            ]
            assert len(live) == 1, live
            cluster_id = live[0].id

            self.cur.execute(f'drop cluster id "{cluster_id}" wait on terminated')
            self._wait_cluster_gone(name)

            # Drop non-existent by ID
            with self.assertRaises(KeyError):
                self.cur.execute(f'drop cluster id "{cluster_id}"')

            # ... and with IF EXISTS
            self.cur.execute(f'drop cluster if exists id "{cluster_id}"')

            # Drop non-existent by name, both ways
            with self.assertRaises(KeyError):
                self.cur.execute('drop cluster "no-such-cluster-xyz"')
            self.cur.execute('drop cluster if exists "no-such-cluster-xyz"')

        finally:
            for cluster in mgr.clusters:
                if cluster.name == name and cluster.terminated_at is None:
                    try:
                        cluster.terminate()
                    except Exception:
                        pass


@pytest.mark.management
class TestClusterFusionSuspendResume(_ClusterFusionMixin, unittest.TestCase):
    """
    ``SUSPEND CLUSTER`` and ``RESUME CLUSTER``.

    Deploys one cluster rather than sharing :class:`TestClusterFusion`'s three,
    for two reasons: it needs exactly one, and it is the only test here that
    changes a fixture's state, so sharing would leave the ``SHOW`` assertions
    reading a cluster mid-suspend.
    """

    fixture_prefixes = ('a',)

    def test_suspend_resume_cluster(self):
        name = f'a-fusion-cluster-{self.id}'
        mgr = type(self).manager

        self.cur.execute(f'suspend cluster "{name}" wait on suspended')
        state = [x for x in mgr.clusters if x.name == name][0].state
        assert state.upper() == 'SUSPENDED', state

        self.cur.execute(f'resume cluster "{name}" wait on resumed')
        state = [x for x in mgr.clusters if x.name == name][0].state
        assert state.upper() == 'ACTIVE', state


@pytest.mark.management
class TestClusterFusionProject(_ClusterFusionMixin, unittest.TestCase):
    """
    How ``IN PROJECT`` resolves, and which spellings must not parse.

    Deploys nothing: the one test here that creates a cluster deliberately does
    not wait it out, and the rest assert a rejection.
    """

    def test_create_cluster_without_project(self):
        """
        Omitting IN PROJECT is only valid in a single-project organization.

        ``POST /v2/clusters`` requires ``projectID``, so the handler falls
        through to ``_resolve_project_id()``, which picks the only project or
        raises naming the candidates. Either outcome is correct; silently
        choosing one of several would not be.
        """
        mgr = type(self).manager
        name = f'e-fusion-cluster-{self.id}'
        region = type(self).us_regions[0]

        if len(mgr.projects) == 1:
            raise unittest.SkipTest(
                'single-project organization; the ambiguous path is what '
                'this test is for',
            )

        with self.assertRaises(Exception):
            self.cur.execute(
                f'create cluster "{name}" in region "{region.region_name}"',
            )

        # Nothing should have been created
        live = [
            x for x in mgr.clusters
            if x.name == name and x.terminated_at is None
        ]
        assert not live, live

    def test_create_cluster_named_project(self):
        """
        ``IN PROJECT "<name>"`` resolves the name to the project's ID.

        Deliberately no ``WAIT ON ACTIVE``. The ``projectID`` is settled by the
        time ``POST /v2/clusters`` answers -- the create response carries the
        cluster ID, and ``GET /v2/clusters/{id}`` reports the project straight
        away -- so provisioning the cluster the rest of the way would add
        minutes of waiting and assert nothing this does not already prove.
        """
        mgr = type(self).manager
        project = [
            x for x in mgr.projects if x.id == type(self).project_id
        ][0]
        name = f'f-fusion-cluster-{self.id}'
        region = type(self).us_regions[0]

        cluster_id = None
        try:
            self.cur.execute(
                f'create cluster "{name}" in region "{region.region_name}" '
                f'in project "{project.name}" with size "S-00"',
            )
            row = self.cur.fetchall()
            assert len(row) == 1, row
            assert row[0][0] == name, row
            cluster_id = row[0][1]
            assert cluster_id, row

            # Read back through GET /v2/clusters/{id} rather than the listing:
            # the create is not waited out, and the LIST endpoint can lag
            # behind a cluster it has only just been told about.
            assert mgr.get_cluster(cluster_id).project.id == project.id

        finally:
            # force=True: the cluster is still PENDING, having never been
            # waited out, and a termination request is refused otherwise.
            if cluster_id is not None:
                try:
                    mgr.get_cluster(cluster_id).terminate(force=True)
                except Exception:
                    pass
            else:
                for cluster in mgr.clusters:
                    if cluster.name == name and cluster.terminated_at is None:
                        try:
                            cluster.terminate(force=True)
                        except Exception:
                            pass

    def test_region_id_does_not_parse(self):
        """v2 has no region IDs, so the v1 spelling must be rejected."""
        with self.assertRaises(Exception):
            self.cur.execute(
                'create cluster "g-fusion-cluster" in region id "abc"',
            )

    def test_unknown_project_raises(self):
        with self.assertRaises(KeyError):
            self.cur.execute(
                'create cluster "h-fusion-cluster" in region "us-east-1" '
                'in project "no such project xyz"',
            )


@pytest.mark.management
@pytest.mark.xdist_group(utils.SHARED_CLUSTER_JOBS_GROUP)
class TestJobsFusion(unittest.TestCase):

    notebook_name: str = 'Scheduling Test.ipynb'
    dbname: str = ''
    dbexisted: bool = False
    manager: None
    cluster: None
    job_ids = []

    @classmethod
    def setUpClass(cls):
        sql_file = os.path.join(os.path.dirname(__file__), 'test.sql')
        cls.dbname, cls.dbexisted = utils.load_sql(sql_file)

        # Switched to v2 along with the JOB handlers. A job runs against a
        # deployment, and at v2 a deployment is a cluster -- one create call
        # rather than a group plus a workspace. This is the only live exercise
        # of the Cluster/VirtualCluster targetType vocabulary.
        cls.manager = s2.manage_clusters(version='v2')

        # A shared cluster: a job needs a live deployment to target, and every
        # listing here is filtered by job id, so nothing this class asserts can
        # see another class's jobs.
        cls.cluster = utils.shared_clusters(1)[0]

        os.environ['SINGLESTOREDB_DEFAULT_DATABASE'] = cls.dbname
        # SINGLESTOREDB_WORKSPACE is the only deployment variable the notebook
        # environment publishes -- there is no SINGLESTOREDB_CLUSTER -- and at
        # v2 its value is a cluster ID.
        os.environ['SINGLESTOREDB_WORKSPACE'] = cls.cluster.id

    @classmethod
    def tearDownClass(cls):
        for job_id in cls.job_ids:
            try:
                cls.manager.organizations.current.jobs.delete(job_id)
            except Exception:
                pass
        # The cluster is the pool's; see TestStageFusion.tearDownClass.
        cls.manager = None
        cls.cluster = None
        for envvar in (
            'SINGLESTOREDB_WORKSPACE',
            'SINGLESTOREDB_DEFAULT_DATABASE',
        ):
            os.environ.pop(envvar, None)

    def setUp(self):
        self.enabled = os.environ.get('SINGLESTOREDB_FUSION_ENABLED')
        os.environ['SINGLESTOREDB_FUSION_ENABLED'] = '1'
        self.conn = s2.connect(database=type(self).dbname, local_infile=True)
        self.cur = self.conn.cursor()

    def tearDown(self):
        if self.enabled:
            os.environ['SINGLESTOREDB_FUSION_ENABLED'] = self.enabled
        else:
            del os.environ['SINGLESTOREDB_FUSION_ENABLED']

        try:
            if self.cur is not None:
                self.cur.close()
        except Exception:
            # traceback.print_exc()
            pass

        try:
            if self.conn is not None:
                self.conn.close()
        except Exception:
            # traceback.print_exc()
            pass

    def test_schedule_drop_job(self):
        # schedule recurring job
        self.cur.execute(
                f'schedule job using notebook "{self.notebook_name}" '
                'with mode "recurring" '
                'execute every 5 minutes '
                'with name "recurring-job" '
                'create snapshot '
                'resume target '
                'with runtime "notebooks-cpu-small" '
                'with parameters '
                '{"strParam": "string", "intParam": 1, '
                '"floatParam": 1.0, "boolParam": true}',
        )
        out = list(self.cur)
        job_id = out[0][0]
        self.job_ids.append(job_id)
        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0][0] == 'JobID'
        assert len(out) == 1
        assert out[0][0] == job_id

        # drop job
        self.cur.execute(f'drop jobs {job_id}')
        out = list(self.cur)
        desc = self.cur.description
        assert len(desc) == 2
        assert [x[0] for x in desc] == [
            'JobID', 'Success',
        ]
        assert len(out) == 1
        res = out[0]
        assert res[0] == job_id
        assert res[1] == 1

    def test_run_wait_drop_job(self):
        # run job
        self.cur.execute(
            f'run job using notebook "{self.notebook_name}" '
            'with runtime "notebooks-cpu-small" '
            'with parameters '
            '{"strParam": "string", "intParam": 1, '
            '"floatParam": 1.0, "boolParam": true}',
        )
        out = list(self.cur)
        job_id = out[0][0]
        self.job_ids.append(job_id)
        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0][0] == 'JobID'
        assert len(out) == 1
        assert out[0][0] == job_id

        # wait on job
        self.cur.execute(f'wait on jobs {job_id}')
        out = list(self.cur)
        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0][0] == 'Success'
        assert out[0][0] == 1

        # drop job
        self.cur.execute(f'drop jobs {job_id}')
        out = list(self.cur)
        desc = self.cur.description
        assert len(desc) == 2
        assert [x[0] for x in desc] == [
            'JobID', 'Success',
        ]
        assert len(out) == 1
        res = out[0]
        assert res[0] == job_id
        assert res[1] == 1

    def test_show_jobs_and_executions(self):
        # schedule recurring job
        self.cur.execute(
                f'schedule job using notebook "{self.notebook_name}" '
                'with mode "recurring" '
                'execute every 5 minutes '
                'with name "show-job" '
                'with runtime "notebooks-cpu-small" '
                'with parameters '
                '{"strParam": "string", "intParam": 1, '
                '"floatParam": 1.0, "boolParam": true}',
        )
        out = list(self.cur)
        job_id = out[0][0]
        self.job_ids.append(job_id)
        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0][0] == 'JobID'
        assert len(out) == 1
        assert out[0][0] == job_id

        # show jobs with name like "show-job"
        self.cur.execute(f'show jobs {job_id} like "show-job"')
        out = list(self.cur)
        desc = self.cur.description
        assert len(desc) == 9
        assert [x[0] for x in desc] == [
            'JobID', 'Name', 'CreatedAt', 'EnqueuedBy',
            'CompletedExecutions', 'NotebookPath', 'DatabaseName', 'TargetID',
            'TargetType',
        ]
        assert len(out) == 1
        job = out[0]
        assert job[0] == job_id
        assert job[1] == 'show-job'
        assert job[5] == self.notebook_name
        assert job[6] == self.dbname
        assert job[7] == self.cluster.id
        # targetType is 'Cluster' at v2 where v1 reported 'Workspace';
        # this is the assertion that proves the manager really moved.
        assert job[8] == 'Cluster'

        # show jobs with name like "show-job" extended
        self.cur.execute(f'show jobs {job_id} like "show-job" extended')
        out = list(self.cur)
        desc = self.cur.description
        assert len(desc) == 17
        assert [x[0] for x in desc] == [
            'JobID', 'Name', 'CreatedAt', 'EnqueuedBy',
            'CompletedExecutions', 'NotebookPath', 'DatabaseName', 'TargetID',
            'TargetType', 'Description', 'TerminatedAt', 'CreateSnapshot',
            'MaxDurationInMins', 'ExecutionIntervalInMins', 'Mode', 'StartAt',
            'ResumeTarget',
        ]
        assert len(out) == 1
        job = out[0]
        assert job[0] == job_id
        assert job[1] == 'show-job'
        assert job[5] == self.notebook_name
        assert job[6] == self.dbname
        assert job[7] == self.cluster.id
        # targetType is 'Cluster' at v2 where v1 reported 'Workspace';
        # this is the assertion that proves the manager really moved.
        assert job[8] == 'Cluster'
        assert not job[11]
        assert job[13] == 5
        assert job[14] == 'Recurring'
        assert not job[16]

        # show executions for job with id job_id from 1 to 5
        self.cur.execute(f'show job executions for {job_id} from 1 to 5')
        out = list(self.cur)
        desc = self.cur.description
        assert len(desc) == 7
        assert [x[0] for x in desc] == [
            'ExecutionID', 'ExecutionNumber', 'JobID',
            'Status', 'ScheduledStartTime', 'StartedAt', 'FinishedAt',
        ]
        exec_job_ids = [x[2] for x in out]
        for x in exec_job_ids:
            assert x == job_id

        # show executions for job with id job_id from 1 to 5 extended
        self.cur.execute(f'show job executions for {job_id} from 1 to 5 extended')
        out = list(self.cur)
        desc = self.cur.description
        assert len(desc) == 8
        assert [x[0] for x in desc] == [
            'ExecutionID', 'ExecutionNumber', 'JobID',
            'Status', 'ScheduledStartTime', 'StartedAt', 'FinishedAt',
            'SnapshotNotebookPath',
        ]
        exec_job_ids = [x[2] for x in out]
        for x in exec_job_ids:
            assert x == job_id

        # drop job
        self.cur.execute(f'drop jobs {job_id}')
        out = list(self.cur)
        desc = self.cur.description
        assert len(desc) == 2
        assert [x[0] for x in desc] == [
            'JobID', 'Success',
        ]
        assert len(out) == 1
        res = out[0]
        assert res[0] == job_id
        assert res[1] == 1


@pytest.mark.management
@pytest.mark.xdist_group(utils.SHARED_CLUSTER_STAGE_GROUP)
class TestStageFusion(unittest.TestCase):

    dbname: str = 'information_schema'
    manager: None
    cluster: None
    cluster_2: None

    @classmethod
    def setUpClass(cls):
        # Switched to v2: get_deployment() resolves against clusters, so the
        # fixtures must be clusters. The v1 stage path is still covered by
        # test_management_v1.py -- switched rather than duplicated, because
        # duplicating doubles a suite that already runs for tens of minutes.
        cls.manager = s2.manage_clusters(version='v2')

        # Two clusters from the shared pool rather than two of this class's
        # own. Nothing here mutates a cluster, and the second one exists only
        # so IN GROUP can name a deployment other than the default. Deploying
        # them was 891s of the run; see
        # docs/shared-deployment-pool-plan.md.
        cls.cluster, cls.cluster_2 = utils.shared_clusters(2)

        # The stage paths below are fixed rather than namespaced, and the
        # listings are asserted by exact contents, so both stages have to start
        # empty: a pool cluster carries whatever the class before it left
        # there. tearDown clears them again after every test.
        for cluster in (cls.cluster, cls.cluster_2):
            utils.clear_stage(cluster)

        os.environ['SINGLESTOREDB_DEFAULT_DATABASE'] = 'information_schema'
        # SINGLESTOREDB_WORKSPACE_GROUP would raise at v2: its value is a
        # group ID, which v2 reports only as Cluster.group and cannot look
        # up, so get_deployment() refuses to guess which cluster was meant
        # rather than target the wrong one.
        os.environ['SINGLESTOREDB_WORKSPACE'] = cls.cluster.id

    @classmethod
    def tearDownClass(cls):
        # The clusters are the pool's, not this class's: they stay live for the
        # classes that follow and are terminated once, at the end of the
        # session, by utils.cleanup_tracked.
        cls.manager = None
        cls.cluster = None
        cls.cluster_2 = None
        for envvar in (
            'SINGLESTOREDB_WORKSPACE',
            'SINGLESTOREDB_WORKSPACE_GROUP',
            'SINGLESTOREDB_DEFAULT_DATABASE',
        ):
            os.environ.pop(envvar, None)

    def setUp(self):
        self.enabled = os.environ.get('SINGLESTOREDB_FUSION_ENABLED')
        os.environ['SINGLESTOREDB_FUSION_ENABLED'] = '1'
        self.conn = s2.connect(database=type(self).dbname, local_infile=True)
        self.cur = self.conn.cursor()

    def tearDown(self):
        self._clear_stage()

        if self.enabled:
            os.environ['SINGLESTOREDB_FUSION_ENABLED'] = self.enabled
        else:
            del os.environ['SINGLESTOREDB_FUSION_ENABLED']

        try:
            if self.cur is not None:
                self.cur.close()
        except Exception:
            # traceback.print_exc()
            pass

        try:
            if self.conn is not None:
                self.conn.close()
        except Exception:
            # traceback.print_exc()
            pass

    def _clear_stage(self):
        if self.cluster is not None:
            self.cur.execute(f'''
                show stage files
                    in group id '{self.cluster.id}' recursive
            ''')
            files = list(self.cur)
            folders = []
            for file in files:
                if file[0].endswith('/'):
                    folders.append(file)
                    continue
                self.cur.execute(f'''
                    drop stage file '{file[0]}'
                        in group id '{self.cluster.id}'
                ''')
            for folder in folders:
                self.cur.execute(f'''
                    drop stage folder '{folder[0]}'
                        in group id '{self.cluster.id}'
                ''')

        if self.cluster_2 is not None:
            self.cur.execute(f'''
                show stage files
                    in group id '{self.cluster_2.id}' recursive
            ''')
            files = list(self.cur)
            folders = []
            for file in files:
                if file[0].endswith('/'):
                    folders.append(file)
                    continue
                self.cur.execute(f'''
                    drop stage file '{file[0]}'
                        in group id '{self.cluster_2.id}'
                ''')
            for folder in folders:
                self.cur.execute(f'''
                    drop stage folder '{folder[0]}'
                        in group id '{self.cluster_2.id}'
                ''')

    def test_show_stage(self):
        test2_sql = os.path.join(os.path.dirname(__file__), 'test2.sql')

        # Should be empty
        self.cur.execute('''
            show stage files
        ''')
        files = list(self.cur)
        assert len(files) == 0

        # Copy files to stage
        self.cur.execute(
            f'upload file to stage "new_test_1.sql" from "{test2_sql}"',
        )
        self.cur.execute('create stage folder "subdir1"')
        self.cur.execute(
            f'upload file to stage "subdir1/new_test_2.sql" from "{test2_sql}"',
        )
        self.cur.execute(
            f'upload file to stage "subdir1/new_test_3.sql" from "{test2_sql}"',
        )
        self.cur.execute('create stage folder "subdir2"')
        self.cur.execute(
            f'upload file to stage "subdir2/new_test_4.sql" from "{test2_sql}"',
        )
        self.cur.execute(
            f'upload file to stage "subdir2/new_test_5.sql" from "{test2_sql}"',
        )

        # Make sure files are there
        self.cur.execute('''
            show stage files recursive
        ''')
        files = list(self.cur)
        assert len(files) == 7
        assert list(sorted(x[0] for x in files)) == [
            'new_test_1.sql',
            'subdir1/',
            'subdir1/new_test_2.sql',
            'subdir1/new_test_3.sql',
            'subdir2/',
            'subdir2/new_test_4.sql',
            'subdir2/new_test_5.sql',
        ]

        # Do non-recursive listing
        self.cur.execute('''
            show stage files
        ''')
        files = list(self.cur)
        assert len(files) == 3
        assert list(sorted(x[0] for x in files)) == [
            'new_test_1.sql',
            'subdir1/',
            'subdir2/',
        ]

        # List files in a specific deployment. All four spellings address the
        # same cluster: IN CLUSTER is the v2-native one, IN GROUP is kept as a
        # synonym so existing scripts keep working, and the bare IN was always
        # version-neutral.
        expected = [
            'new_test_1.sql',
            'subdir1/',
            'subdir2/',
        ]
        for clause in [
            f"in cluster id '{self.cluster.id}'",
            f"in cluster '{self.cluster.name}'",
            f"in group id '{self.cluster.id}'",
            f"in group '{self.cluster.name}'",
            f"in id '{self.cluster.id}'",
            f"in '{self.cluster.name}'",
        ]:
            self.cur.execute(f'show stage files {clause}')
            files = list(self.cur)
            assert len(files) == 3, (clause, files)
            assert list(sorted(x[0] for x in files)) == expected, clause

        # Check the other cluster, by both spellings
        for clause in [
            f"in cluster '{self.cluster_2.name}'",
            f"in group '{self.cluster_2.name}'",
        ]:
            self.cur.execute(f'show stage files {clause}')
            assert len(list(self.cur)) == 0, clause

        # Limit results
        self.cur.execute('''
            show stage files recursive limit 5
        ''')
        files = list(self.cur)
        assert len(files) == 5
        assert list(sorted(x[0] for x in files)) == [
            'new_test_1.sql',
            'subdir1/',
            'subdir1/new_test_2.sql',
            'subdir1/new_test_3.sql',
            'subdir2/',
        ]

        # Order by type and name
        self.cur.execute('''
            show stage files order by type, name recursive extended
        ''')
        files = list(self.cur)
        assert len(files) == 7
        assert list(x[0] for x in files) == [
            'subdir1/',
            'subdir2/',
            'new_test_1.sql',
            'subdir1/new_test_2.sql',
            'subdir1/new_test_3.sql',
            'subdir2/new_test_4.sql',
            'subdir2/new_test_5.sql',
        ]

        # Order by type and name descending
        self.cur.execute('''
            show stage files order by type desc, name desc recursive extended
        ''')
        files = list(self.cur)
        assert len(files) == 7
        assert list(x[0] for x in files) == [
            'subdir2/new_test_5.sql',
            'subdir2/new_test_4.sql',
            'subdir1/new_test_3.sql',
            'subdir1/new_test_2.sql',
            'new_test_1.sql',
            'subdir2/',
            'subdir1/',
        ]

        # List at specific path
        self.cur.execute('''
            show stage files at 'subdir2/' recursive
        ''')
        files = list(self.cur)
        assert len(files) == 2
        assert list(sorted(x[0] for x in files)) == [
            'new_test_4.sql',
            'new_test_5.sql',
        ]

        # LIKE clause
        self.cur.execute('''
            show stage files like '%_4.%' recursive
        ''')
        files = list(self.cur)
        assert len(files) == 1
        assert list(sorted(x[0] for x in files)) == [
            'subdir2/new_test_4.sql',
        ]

    def test_download_stage(self):
        test2_sql = os.path.join(os.path.dirname(__file__), 'test2.sql')

        # Should be empty
        self.cur.execute('''
            show stage files
        ''')
        files = list(self.cur)
        assert len(files) == 0

        # Copy file to stage 1
        self.cur.execute(f'''
            upload file to stage 'dl_test.sql' from '{test2_sql}'
        ''')

        self.cur.execute('''
            show stage files
        ''')
        files = list(self.cur)
        assert len(files) == 1
        assert list(sorted(x[0] for x in files)) == ['dl_test.sql']

        # Copy file to stage 2
        self.cur.execute(f'''
            upload file to stage 'dl_test2.sql'
                in group '{self.cluster_2.name}'
                from '{test2_sql}'
        ''')

        # Make sure only one file in stage 2
        self.cur.execute(f'''
            show stage files in group '{self.cluster_2.name}'
        ''')
        files = list(self.cur)
        assert len(files) == 1
        assert list(sorted(x[0] for x in files)) == ['dl_test2.sql']

        # Download file from stage 1
        with tempfile.TemporaryDirectory() as tmpdir:
            self.cur.execute(f'''
                download stage file 'dl_test.sql' to '{tmpdir}/dl_test.sql'
            ''')
            with open(os.path.join(tmpdir, 'dl_test.sql'), 'r') as dl_file:
                assert dl_file.read() == open(test2_sql, 'r').read()

        # Download file from stage 2
        with tempfile.TemporaryDirectory() as tmpdir:
            self.cur.execute(f'''
                download stage file 'dl_test2.sql'
                    in group '{self.cluster_2.name}'
                    to '{tmpdir}/dl_test2.sql'
            ''')
            with open(os.path.join(tmpdir, 'dl_test2.sql'), 'r') as dl_file:
                assert dl_file.read() == open(test2_sql, 'r').read()

    def test_stage_multi_wg_operations(self):
        test_sql = os.path.join(os.path.dirname(__file__), 'test.sql')
        test2_sql = os.path.join(os.path.dirname(__file__), 'test2.sql')

        # Should be empty
        self.cur.execute('''
            show stage files
        ''')
        files = list(self.cur)
        assert len(files) == 0

        # Copy file to stage 1
        self.cur.execute(f'''
            upload file to stage 'new_test.sql' from '{test_sql}'
        ''')

        self.cur.execute('''
            show stage files
        ''')
        files = list(self.cur)
        assert len(files) == 1

        # Copy file to stage 2
        self.cur.execute(f'''
            upload file to stage 'new_test2.sql'
                in group '{self.cluster_2.name}'
                from '{test2_sql}'
        ''')

        # Make sure only one file in stage 1
        self.cur.execute('''
            show stage files
        ''')
        files = list(self.cur)
        assert len(files) == 1
        assert files[0][0] == 'new_test.sql'

        # Make sure only one file in stage 2
        self.cur.execute(f'''
            show stage files in group '{self.cluster_2.name}' recursive
        ''')
        files = list(self.cur)
        assert len(files) == 1
        assert list(sorted(x[0] for x in files)) == ['new_test2.sql']

        # Make sure only one file in stage 2 (using IN)
        self.cur.execute(f'''
            show stage files in '{self.cluster_2.name}' recursive
        ''')
        files = list(self.cur)
        assert len(files) == 1
        assert list(sorted(x[0] for x in files)) == ['new_test2.sql']

        # Make subdir
        self.cur.execute(f'''
            create stage folder 'data' in group '{self.cluster_2.name}'
        ''')

        # Upload file using workspace ID
        self.cur.execute(f'''
            upload file to stage 'data/new_test2_sub.sql'
                in group id '{self.cluster_2.id}'
                from '{test2_sql}'
        ''')

        # Make sure only one file in stage 1
        self.cur.execute('''
            show stage files
        ''')
        files = list(self.cur)
        assert len(files) == 1
        assert files[0][0] == 'new_test.sql'

        # Make sure two files in stage 2
        self.cur.execute(f'''
            show stage files in group id '{self.cluster_2.id}' recursive
        ''')
        files = list(self.cur)
        assert len(files) == 3
        assert list(sorted(x[0] for x in files)) == \
            ['data/', 'data/new_test2_sub.sql', 'new_test2.sql']

        # Test overwrite
        with self.assertRaises(OSError):
            self.cur.execute(f'''
                upload file to stage 'data/new_test2_sub.sql'
                    in group id '{self.cluster_2.id}'
                    from '{test2_sql}'
            ''')

        self.cur.execute(f'''
            upload file to stage 'data/new_test2_sub.sql'
                in group id '{self.cluster_2.id}'
                from '{test2_sql}' overwrite
        ''')

        # Make sure two files in stage 2
        self.cur.execute(f'''
            show stage files in group id '{self.cluster_2.id}' recursive
        ''')
        files = list(self.cur)
        assert len(files) == 3
        assert list(sorted(x[0] for x in files)) == \
            ['data/', 'data/new_test2_sub.sql', 'new_test2.sql']

        # Test LIKE clause
        self.cur.execute(f'''
            show stage files
                in group id '{self.cluster_2.id}'
                like '%_sub%' recursive
        ''')
        files = list(self.cur)
        assert len(files) == 1
        assert list(sorted(x[0] for x in files)) == ['data/new_test2_sub.sql']

        # Drop file from default stage
        self.cur.execute('''
            drop stage file 'new_test.sql'
        ''')

        # Make sure no files in stage 1
        self.cur.execute('''
            show stage files
        ''')
        files = list(self.cur)
        assert len(files) == 0

        # Make sure two files in stage 2
        self.cur.execute(f'''
            show stage files in group id '{self.cluster_2.id}' recursive
        ''')
        files = list(self.cur)
        assert len(files) == 3
        assert list(sorted(x[0] for x in files)) == \
            ['data/', 'data/new_test2_sub.sql', 'new_test2.sql']

        # Attempt to drop directory from stage 2
        with self.assertRaises(OSError):
            self.cur.execute(f'''
                drop stage folder 'data'
                    in group id '{self.cluster_2.id}'
            ''')

        self.cur.execute(f'''
            drop stage file 'data/new_test2_sub.sql'
                in group id '{self.cluster_2.id}'
        ''')

        # Make sure one file and one directory in stage 2
        self.cur.execute(f'''
            show stage files in group id '{self.cluster_2.id}' recursive
        ''')
        files = list(self.cur)
        assert len(files) == 2
        assert list(sorted(x[0] for x in files)) == ['data/', 'new_test2.sql']

        # Drop stage folder from stage 2
        self.cur.execute(f'''
            drop stage folder 'data'
                in group id '{self.cluster_2.id}'
        ''')

        # Make sure one file in stage 2
        self.cur.execute(f'''
            show stage files in group id '{self.cluster_2.id}' recursive
        ''')
        files = list(self.cur)
        assert len(files) == 1
        assert list(sorted(x[0] for x in files)) == ['new_test2.sql']

        # Drop last file
        self.cur.execute(f'''
            drop stage file 'new_test2.sql'
                in group id '{self.cluster_2.id}'
        ''')

        # Make sure no files in stage 2
        self.cur.execute(f'''
            show stage files in group id '{self.cluster_2.id}' recursive
        ''')
        files = list(self.cur)
        assert len(files) == 0


@pytest.mark.management
class TestFilesFusion(unittest.TestCase):

    id: str = secrets.token_hex(8)
    dbname: str = 'information_schema'
    manager: None

    @classmethod
    def setUpClass(cls):
        # Switched to v2 along with get_files_manager(). No deployment
        # fixture: the personal, shared and models spaces are org-scoped, and
        # none of the tests below ever referenced the workspace group this
        # method used to create -- it was a billable resource created for
        # nothing. Dropped rather than converted to a cluster.
        cls.manager = s2.manage_clusters(version='v2')
        os.environ['SINGLESTOREDB_DEFAULT_DATABASE'] = 'information_schema'

    @classmethod
    def tearDownClass(cls):
        cls.manager = None
        os.environ.pop('SINGLESTOREDB_DEFAULT_DATABASE', None)

    def setUp(self):
        self.enabled = os.environ.get('SINGLESTOREDB_FUSION_ENABLED')
        os.environ['SINGLESTOREDB_FUSION_ENABLED'] = '1'
        self.conn = s2.connect(database=type(self).dbname, local_infile=True)
        self.cur = self.conn.cursor()

    def tearDown(self):
        self._clear_files()

        if self.enabled:
            os.environ['SINGLESTOREDB_FUSION_ENABLED'] = self.enabled
        else:
            del os.environ['SINGLESTOREDB_FUSION_ENABLED']

        try:
            if self.cur is not None:
                self.cur.close()
        except Exception:
            # traceback.print_exc()
            pass

        try:
            if self.conn is not None:
                self.conn.close()
        except Exception:
            # traceback.print_exc()
            pass

    def _clear_files(self):
        cls = type(self)
        for prefix in ['show', 'dl', 'drop']:
            for i in range(1, 6):
                try:
                    self.cur.execute(
                        f'''drop personal file "{prefix}_test_{i}_{cls.id}.ipynb"''',
                    )
                except (OSError, s2.ManagementError):
                    pass
            for i in range(1, 6):
                try:
                    self.cur.execute(
                        f'''drop shared file "{prefix}_test_{i}_{cls.id}.ipynb"''',
                    )
                except (OSError, s2.ManagementError):
                    pass

    def test_show_personal_files(self):
        return self._test_show_files('personal')

    def test_show_shared_files(self):
        return self._test_show_files('shared')

    def _test_show_files(self, ftype):
        cls = type(self)
        nb = os.path.join(os.path.dirname(__file__), 'test.ipynb')

        # Should be empty
        self.cur.execute(f'''
            show {ftype} files like 'show_%{cls.id}%'
        ''')
        files = list(self.cur)
        assert len(files) == 0

        # Upload files
        self.cur.execute(
            f'upload {ftype} file to "show_test_1_{cls.id}.ipynb" from "{nb}"',
        )
        self.cur.execute(
            f'upload {ftype} file to "show_test_2_{cls.id}.ipynb" from "{nb}"',
        )
        self.cur.execute(
            f'upload {ftype} file to "show_test_3_{cls.id}.ipynb" from "{nb}"',
        )
        self.cur.execute(
            f'upload {ftype} file to "show_test_4_{cls.id}.ipynb" from "{nb}"',
        )
        self.cur.execute(
            f'upload {ftype} file to "show_test_5_{cls.id}.ipynb" from "{nb}"',
        )

        # Make sure files are there
        self.cur.execute(f'''
            show {ftype} files like 'show_%{cls.id}%'
        ''')
        files = list(self.cur)
        assert len(files) == 5
        assert list(sorted(x[0] for x in files)) == [
            f'show_test_1_{cls.id}.ipynb',
            f'show_test_2_{cls.id}.ipynb',
            f'show_test_3_{cls.id}.ipynb',
            f'show_test_4_{cls.id}.ipynb',
            f'show_test_5_{cls.id}.ipynb',
        ]

        # Test ORDER BY
        self.cur.execute(f'''
            show {ftype} files like 'show_%{cls.id}%' order by name desc
        ''')
        files = list(self.cur)
        assert len(files) == 5
        assert list(x[0] for x in files) == [
            f'show_test_5_{cls.id}.ipynb',
            f'show_test_4_{cls.id}.ipynb',
            f'show_test_3_{cls.id}.ipynb',
            f'show_test_2_{cls.id}.ipynb',
            f'show_test_1_{cls.id}.ipynb',
        ]

        # Test LIMIT
        self.cur.execute(f'''
            show {ftype} files like 'show_%{cls.id}%' order by name desc limit 3
        ''')
        files = list(self.cur)
        assert len(files) == 3
        assert list(x[0] for x in files) == [
            f'show_test_5_{cls.id}.ipynb',
            f'show_test_4_{cls.id}.ipynb',
            f'show_test_3_{cls.id}.ipynb',
        ]

        # Test EXTENDED
        self.cur.execute(f'''
            show {ftype} files like 'show_%{cls.id}%' extended
        ''')
        assert [x[0] for x in self.cur.description] == \
            ['Name', 'Type', 'Size', 'Writable', 'CreatedAt', 'LastModifiedAt']

    def test_download_personal_files(self):
        return self._test_download_files('personal')

    def test_download_shared_files(self):
        return self._test_download_files('shared')

    def _test_download_files(self, ftype):
        cls = type(self)
        nb = os.path.join(os.path.dirname(__file__), 'test.ipynb')

        # Should be empty
        self.cur.execute(f'''
            show {ftype} files like 'dl_%{cls.id}%'
        ''')
        files = list(self.cur)
        assert len(files) == 0

        # Upload files
        self.cur.execute(f'upload {ftype} file to "dl_test_1_{cls.id}.ipynb" from "{nb}"')
        self.cur.execute(f'upload {ftype} file to "dl_test_2_{cls.id}.ipynb" from "{nb}"')

        # Make sure files are there
        self.cur.execute(f'''
            show {ftype} files like 'dl_%{cls.id}%'
        ''')
        files = list(self.cur)
        assert len(files) == 2
        assert list(sorted(x[0] for x in files)) == [
            f'dl_test_1_{cls.id}.ipynb',
            f'dl_test_2_{cls.id}.ipynb',
        ]

        # Download files
        with tempfile.TemporaryDirectory() as tmpdir:
            self.cur.execute(f'''
                download {ftype} file 'dl_test_1_{cls.id}.ipynb'
                    to '{tmpdir}/dl_test_1.ipynb'
            ''')
            with open(os.path.join(tmpdir, 'dl_test_1.ipynb'), 'r') as dl_file:
                assert dl_file.read() == open(nb, 'r').read()

            self.cur.execute(f'''
                download {ftype} file 'dl_test_2_{cls.id}.ipynb'
                    to '{tmpdir}/dl_test_2.ipynb'
            ''')
            with open(os.path.join(tmpdir, 'dl_test_2.ipynb'), 'r') as dl_file:
                assert dl_file.read() == open(nb, 'r').read()

    def test_drop_personal_files(self):
        return self._test_drop_files('personal')

    def test_drop_shared_files(self):
        return self._test_drop_files('shared')

    def _test_drop_files(self, ftype):
        cls = type(self)
        nb = os.path.join(os.path.dirname(__file__), 'test.ipynb')

        # Should be empty
        self.cur.execute(f'''
            show {ftype} files like 'drop_%{cls.id}%'
        ''')
        files = list(self.cur)
        assert len(files) == 0

        # Upload files
        self.cur.execute(
            f'upload {ftype} file to "drop_test_1_{cls.id}.ipynb" from "{nb}"',
        )
        self.cur.execute(
            f'upload {ftype} file to "drop_test_2_{cls.id}.ipynb" from "{nb}"',
        )

        # Make sure files are there
        self.cur.execute(f'''
            show {ftype} files like 'drop_%{cls.id}%'
        ''')
        files = list(self.cur)
        assert len(files) == 2
        assert list(sorted(x[0] for x in files)) == [
            f'drop_test_1_{cls.id}.ipynb',
            f'drop_test_2_{cls.id}.ipynb',
        ]

        # Drop 1 file
        self.cur.execute(f'''
            drop {ftype} file 'drop_test_1_{cls.id}.ipynb'
        ''')

        # Make sure 1 file is there
        self.cur.execute(f'''
            show {ftype} files like 'drop_%{cls.id}%'
        ''')
        files = list(self.cur)
        assert len(files) == 1
        assert list(x[0] for x in files) == [f'drop_test_2_{cls.id}.ipynb']

        # Drop 2nd file
        self.cur.execute(f'''
            drop {ftype} file 'drop_test_2_{cls.id}.ipynb'
        ''')

        # Make sure no files are there
        self.cur.execute(f'''
            show {ftype} files like 'drop_%{cls.id}%'
        ''')
        files = list(self.cur)
        assert len(files) == 0
