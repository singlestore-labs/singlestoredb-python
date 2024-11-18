#!/usr/bin/env python
# type: ignore
"""SingleStoreDB Fusion testing."""
import os
import random
import secrets
import time
import unittest
from typing import Any
from typing import List

import pytest

import singlestoredb as s2
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


@pytest.mark.management
class TestWorkspaceFusion(unittest.TestCase):

    id: str = secrets.token_hex(8)
    dbname: str = ''
    dbexisted: bool = False
    workspace_groups: List[Any] = []

    @classmethod
    def setUpClass(cls):
        sql_file = os.path.join(os.path.dirname(__file__), 'test.sql')
        cls.dbname, cls.dbexisted = utils.load_sql(sql_file)
        mgr = s2.manage_workspaces()
        us_regions = [x for x in mgr.regions if x.name.startswith('US')]
        non_us_regions = [x for x in mgr.regions if not x.name.startswith('US')]
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
            region=random.choice(non_us_regions),
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
        mgr = s2.manage_workspaces()
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

        time.sleep(30)
        iterations = 20
        while True:
            wgs = wg.workspaces
            states = [
                x.state for x in wgs
                if x.name in ('show-ws-1', 'show-ws-2', 'show-ws-3')
            ]
            if len(states) == 3 and states.count('ACTIVE') == 3:
                break
            iterations -= 1
            if not iterations:
                raise RuntimeError('timed out waiting for workspaces to start')
            time.sleep(30)

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
        mgr = s2.manage_workspaces()
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

    def test_create_drop_workspace_group(self):
        mgr = s2.manage_workspaces()

        reg = [x for x in mgr.regions if x.name.startswith('US')][0]
        wg_name = f'Create WG Test {id(self)}'

        try:
            self.cur.execute(
                f'create workspace group "{wg_name}" '
                f'in region "{reg.name}"',
            )
            wg = [x for x in mgr.workspace_groups if x.name == wg_name]
            assert len(wg) == 1

            # Drop it by name
            self.cur.execute(
                f'drop workspace group "{wg_name}" '
                'wait on terminated',
            )
            wg = [x for x in mgr.workspace_groups if x.name == wg_name]
            assert len(wg) == 0

            # Create it again
            self.cur.execute(
                f'create workspace group "{wg_name}" in region "{reg.name}"',
            )
            wg = [x for x in mgr.workspace_groups if x.name == wg_name]
            assert len(wg) == 1

            # Drop it by ID
            wg_id = wg[0].id
            self.cur.execute(f'drop workspace group id {wg_id} wait on terminated')
            wg = [x for x in mgr.workspace_groups if x.name == wg_name]
            assert len(wg) == 0

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


@pytest.mark.management
class TestJobsFusion(unittest.TestCase):

    id: str = secrets.token_hex(8)
    notebook_name: str = 'Scheduling Test.ipynb'
    dbname: str = ''
    dbexisted: bool = False
    manager: None
    workspace_group: None
    workspace: None
    job_ids = []

    @classmethod
    def setUpClass(cls):
        sql_file = os.path.join(os.path.dirname(__file__), 'test.sql')
        cls.dbname, cls.dbexisted = utils.load_sql(sql_file)
        cls.manager = s2.manage_workspaces()
        us_regions = [x for x in cls.manager.regions if x.name.startswith('US')]
        cls.workspace_group = cls.manager.create_workspace_group(
            f'Jobs Fusion Testing {cls.id}',
            region=random.choice(us_regions),
            firewall_ranges=[],
        )
        cls.workspace = cls.workspace_group.create_workspace(
                f'jobs-test-{cls.id}',
                wait_on_active=True,
        )
        os.environ['SINGLESTOREDB_DEFAULT_DATABASE'] = cls.dbname
        os.environ['SINGLESTOREDB_WORKSPACE'] = cls.workspace.id

    @classmethod
    def tearDownClass(cls):
        for job_id in cls.job_ids:
            cls.manager.organizations.current.jobs.delete(job_id)
        if cls.workspace_group is not None:
            cls.workspace_group.terminate(force=True)
        cls.manager = None
        cls.workspace_group = None
        cls.workspace = None
        if os.environ.get('SINGLESTOREDB_WORKSPACE', None) is not None:
            del os.environ['SINGLESTOREDB_WORKSPACE']
        if os.environ.get('SINGLESTOREDB_DEFAULT_DATABASE', None) is not None:
            del os.environ['SINGLESTOREDB_DEFAULT_DATABASE']

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
        assert job[7] == self.workspace.id
        assert job[8] == 'Workspace'

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
        assert job[7] == self.workspace.id
        assert job[8] == 'Workspace'
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
