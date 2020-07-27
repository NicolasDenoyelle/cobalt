###############################################################################
# Copyright 2020 UChicago Argonne, LLC.
# (c.f. AUTHORS, LICENSE)
# For more info, see https://xgitlab.cels.anl.gov/argo/cobalt-python-wrapper
# SPDX-License-Identifier: BSD-3-Clause
##############################################################################

import os
import re
import stat
from getpass import getuser
from subprocess import check_output
from math import ceil
from tempfile import mkstemp
from datetime import timedelta, datetime

user = getuser()

def getoutput(cmd):
    return check_output(cmd.split(), universal_newlines=True)

class Cobalt:
    """
    User abstraction of cobalt scheduler.
    Contains a Queue abstraction (see Cobalt.Queue) and job Abstraction (see Cobalt.Job).
    """
    
    class Job:
        """
        Abstract representation of a submitted job.
        Attributes:
        jobid (int): Job identifier.
        queue (int): The queue abstraction where job has been submitted (See below).
        name (str): Job name
        users ([str]): List of users allowed to interract with the job.
        walltime (timedelta): Maximum job lifetime.
        runtime (timdelta): Time elapsed since job started running.
        start_time (timdelta): Time job started. 
        queued_time (timdelta): Time elapsed since job was queued.
        remaining_time (timdelta): Time left before job is killed.
        nodecount (int): Number of nodes used by the job.
        proccount (int): Number of processes in the job.
        location [(str)]: Machines hostname where job is run.
        state (str): "running", "queued" ...
        user_hold (bool): Whether the job is held by user. It won't run until released. 
        envs (dict): Set of environment variables set for the job.
        attrs (dict): Job attributes (usually set to configure a queue)
        dependencies (list(int)): List of jobids on which this job depends.
        kwargs: Additional keyword arguments depending on how the Job is instanciated.
        """

        jobid_re = re.compile('.*JobID\s*:\s*(?P<jobid>\d+).*', re.DOTALL)
        user_re = re.compile('.*User\s*:\s*(?P<jobid>\w+).*', re.DOTALL)
        users_re = re.compile('.*user_list\s*:\s*(?P<users>\w+(:\w+)*).*',
                              re.DOTALL)
        name_re = re.compile(
            '.*JobName\s*:\s*(?P<name>[a-zA-Z0-9<>\'\"\+\=\-_/\\\|\%\$\#\@\.\:,\t \r\!\?\(\)\[\]\{\}]*)\n.*',
            re.DOTALL)
        walltime_re = re.compile(
            '.*WallTime\s*:\s*(?P<walltime>\d+:\d+:\d+).*', re.DOTALL)
        runtime_re = re.compile('.*RunTime\s*:\s*(?P<runtime>\d+:\d+:\d+).*',
                                re.DOTALL)
        starttime_re = re.compile(
            '.*StartTime\s*:\s*(?P<starttime>\d+:\d+:\d+).*', re.DOTALL)
        queuedtime_re = re.compile(
            '.*QueuedTime\s*:\s*(?P<queuedtime>\d+:\d+:\d+).*', re.DOTALL)
        remainingtime_re = re.compile(
            '.*TimeRemaining\s*:\s*(?P<remainingtime>\d+:\d+:\d+).*',
            re.DOTALL)
        nodecount_re = re.compile('.*Nodes\s*:\s*(?P<nodes>\d+).*', re.DOTALL)
        proccount_re = re.compile('.*Procs\s*:\s*(?P<procs>\d+).*', re.DOTALL)
        location_re = re.compile(
            '.*Location\s*:\s*(?P<location>(([a-zA-Z0-9-_.]+)(\[\d+-\d+\])?,?)+).*', re.DOTALL)
        queue_re = re.compile('.*Queue\s*:\s*(?P<queue>[a-zA-Z0-9-_.]+).*',
                              re.DOTALL)
        state_re = re.compile('.*State\s*:\s*(?P<state>\w+).*', re.DOTALL)
        userhold_re = re.compile(
            '.*UserHold\s*:\s*(?P<userhold>(True|False)).*', re.DOTALL)
        attrs_re = re.compile(
            '.*attrs\s*:\s*(?P<attrs>{[a-zA-Z0-9-_.:,\'\"]*}).*', re.DOTALL)
        envs_re = re.compile(
            '.*Envs\s*:\s*(?P<env>[a-zA-Z0-9-_.]+=[a-zA-Z0-9-_.]+(:[a-zA-Z0-9-_.]+=[a-zA-Z0-9-_.]+)*).*',
            re.DOTALL)
        dependencies_re = re.compile(
            '.*Dependencies\s*:\s*(?P<dep>\d+(:\d+)*).*', re.DOTALL)

        def __init__(self,
                     jobid,
                     queue,
                     user = "unknown",
                     name='',
                     users=[],
                     walltime=None,
                     runtime=None,
                     start_time=None,
                     queued_time=None,
                     remaining_time=None,
                     nodecount=None,
                     proccount=None,
                     location=[],
                     state="unknown",
                     user_hold=None,
                     envs={},
                     attrs={},
                     dependencies=[],
                     **kwargs):

            self.jobid = jobid
            self.queue = queue
            self.user = user
            self.name = name
            self.users = users
            self.walltime = walltime
            self.runtime = runtime
            self.start_time = start_time
            self.queued_time = queued_time
            self.remaining_time = remaining_time
            self.nodecount = nodecount
            self.proccount = proccount
            self.location = location
            self.state = state
            self.user_hold = user_hold
            self.envs = envs
            self.attrs = attrs
            for k, v in kwargs.items():
                setattr(self, k, v)

        def __repr__(self):
            return str(self.jobid)

        def __str__(self):
            if self.state == 'running' and hasattr(self, 'runtime'):
                time = str(self.runtime)
            elif self.state != 'running' and hasattr(self, 'queued_time'):
                time = str(self.queued_time)
            else:
                time = 'unknown'
            location = repr(self.queue)
            if self.location is not None and len(self.location) > 1:
                location += '[{}]'.format(','.join([re.match('[a-zA-Z]+(?P<i>\d+)', l).group(1) for l in self.location]))
            return '{:8d} {:20s} {:24s} {:16s} {:8s} {:10s}'.format(
                self.jobid, self.name, location, self.user, self.state,
                time)

        def __eq__(self, other):
            if isinstance(other, Job):
                return self.jobid == other.jobid
            if isinstance(other, str):
                return self.jobid == int(other)
            if isinstance(other, int):
                return self.jobid == other

        @staticmethod
        def from_string(s):
            jobid = int(Cobalt.Job.jobid_re.match(s).group(1))
            queue = Cobalt.Job.queue_re.match(s).group(1)
            user = None
            name = None
            users = []
            walltime = None
            runtime = None
            start_time = None
            queued_time = None
            remaining_time = None
            nodecount = None
            proccount = None
            location = []
            state = "unknown"
            user_hold = None
            envs = {}
            attrs = {}
            dependencies = []

            user = Cobalt.Job.user_re.match(s)
            if user is not None:
                user = user.group(1)
            name = Cobalt.Job.name_re.match(s)
            if name is not None:
                name = name.group(1)
            users = Cobalt.Job.users_re.match(s)
            if users is not None:
                users = users.group(1).split(':')
            walltime = Cobalt.Job.walltime_re.match(s)
            if walltime is not None:
                hours, minutes, seconds = [
                    int(i) for i in walltime.group(1).split(':')
                ]
                walltime = timedelta(hours=hours,
                                     minutes=minutes,
                                     seconds=seconds)
            runtime = Cobalt.Job.runtime_re.match(s)
            if runtime is not None:
                hours, minutes, seconds = [
                    int(i) for i in runtime.group(1).split(':')
                ]
                runtime = timedelta(hours=hours,
                                    minutes=minutes,
                                    seconds=seconds)
            starttime = Cobalt.Job.starttime_re.match(s)
            if starttime is not None:
                hours, minutes, seconds = [
                    int(i) for i in starttime.group(1).split(':')
                ]
                start_time = timedelta(hours=hours,
                                       minutes=minutes,
                                       seconds=seconds)
            queuedtime = Cobalt.Job.queuedtime_re.match(s)
            if queuedtime is not None:
                hours, minutes, seconds = [
                    int(i) for i in queuedtime.group(1).split(':')
                ]
                queued_time = timedelta(hours=hours,
                                        minutes=minutes,
                                        seconds=seconds)
            remainingtime = Cobalt.Job.remainingtime_re.match(s)
            if remainingtime is not None:
                hours, minutes, seconds = [
                    int(i) for i in remainingtime.group(1).split(':')
                ]
                remaining_time = timedelta(hours=hours,
                                           minutes=minutes,
                                           seconds=seconds)
            nodecount = Cobalt.Job.nodecount_re.match(s)
            if nodecount is not None:
                nodecount = int(nodecount.group(1))
            proccount = Cobalt.Job.proccount_re.match(s)
            if proccount is not None:
                proccount = int(proccount.group(1))

            locations = Cobalt.Job.location_re.match(s)
            if locations is not None:
                locations = locations.group(1).split(',')
                regex = re.compile('(?P<name>[a-zA-Z_\-.]+)(?P<n>\d+)?(\[(?P<s>\d+)-(?P<e>\d+)\])?')
                for l in locations:
                    match = regex.match(l).groupdict()
                    if match['n'] is not None or match['e'] is None or match['s'] is None:
                        location.append(l)
                    else:
                        location += [ '{}{}'.format(match['name'], i) for i in range(int(match['s']), int(match['e'])+1) ]
                        
            state = Cobalt.Job.state_re.match(s)
            if state is not None:
                state = state.group(1)
            user_hold = Cobalt.Job.userhold_re.match(s)
            if user_hold is not None:
                user_hold = False if user_hold.group(1) == 'False' else True
            envs = Cobalt.Job.envs_re.match(s)
            if envs is not None:
                envs = {
                    kv.split('=')[0]: kv.split('=')[1]
                    for kv in envs.group(1).split(':')
                }
            attrs = Cobalt.Job.attrs_re.match(s)
            if attrs is not None:
                attrs = eval(attrs.group(1))
            dependencies = Cobalt.Job.dependencies_re.match(s)
            if dependencies is not None:
                dependencies = [
                    int(i) for i in dependencies.group(1).split(':') if i != ''
                ]

            return Cobalt.Job(jobid,
                              queue,
                              user=user,
                              name=name,
                              users=users,
                              walltime=walltime,
                              runtime=runtime,
                              start_time=start_time,
                              queued_time=queued_time,
                              remaining_time=remaining_time,
                              nodecount=nodecount,
                              proccount=proccount,
                              location=location,
                              state=state,
                              user_hold=user_hold,
                              envs=envs,
                              attrs=attrs,
                              dependencies=dependencies)

        def cancel(self):
            """
            Stop and delete this job
            """

            cmd = 'qdel {}'.format(self.jobid)
            print(cmd)
            print(getoutput(cmd))

        def hold(self):
            """
            Put this job on hold
            """

            cmd = 'qhold {}'.format(self.jobid)
            print(cmd)
            print(getoutput(cmd))

        def release(self):
            """
            Release this job from hold state
            """
            cmd = 'qrls {}'.format(self.jobid)
            print(cmd)
            print(getoutput(cmd))

    class Queue:
        """
        Abstract representation of cobal queue.
        Attributes:
        name: Queue name
        users: Users currently using this queue.
        groups: Groups allowed on this queue.
        mintime: Minimum time scheduling time. Usually set to 10 minutes
        maxtime: Maximum time allowed for scheduling. Usually set to 4 hours.
        maxrunning: Maximum allowed number of jobs on this queue.
        maxqueued: Queue size.
        maxusernodes: aximum allowed number of nodes allocation per user.
        totalnodes: Number of nodes in this queue. (Unreliable)
        state: A queue state string: running, queued, exiting
        """

        name_re = re.compile('.*Name:\s*(?P<name>[a-zA-Z0-9-_.]+)\s*?',
                             re.DOTALL)
        users_re = re.compile('.*Users\s*:\s*(?P<users>[a-zA-Z0-9-_.:]+).*',
                              re.DOTALL)
        groups_re = re.compile('.*Groups\s*:\s*(?P<groups>[a-zA-Z0-9-_.:]+).*',
                               re.DOTALL)
        mintime_re = re.compile('.*MinTime\s*:\s*(?P<mintime>\d+:\d+:\d+).*',
                                re.DOTALL)
        maxtime_re = re.compile('.*MaxTime\s*:\s*(?P<maxtime>\d+:\d+:\d+).*',
                                re.DOTALL)
        maxrunning_re = re.compile('.*MaxRunning\s*:\s*(?P<maxrunning>\d+).*',
                                   re.DOTALL)
        maxqueued_re = re.compile('.*MaxQueued\s*:\s*(?P<maxqueued>\d+).*',
                                  re.DOTALL)
        maxusernodes_re = re.compile(
            '.*MaxUserNodes\s*:\s*(?P<maxusernodes>\d+).*', re.DOTALL)
        maxnodehours_re = re.compile(
            '.*MaxNodeHours\s*:\s*(?P<maxnodehours>\d+).*', re.DOTALL)
        totalnodes_re = re.compile('.*TotalNodes\s*:\s*(?P<totalnodes>\d+).*',
                                   re.DOTALL)
        state_re = re.compile('.*State\s*:\s*(?P<state>\w+).*', re.DOTALL)

        # When this information is not available from cobalt, we lookup this
        # table filled from jlse wiki when info is available or 0 if not.
        # This alters queues attributes of the same name.
        defaults = {
            'R.sc_workshop': {
                'maxusernodes': 0,
                'totalnodes': 0
            },
            'atos': {
                'maxusernodes': 1,
                'totalnodes': 1
            },
            'cl': {
                'maxusernodes': 0,
                'totalnodes': 0
            },
            'comanche_B1': {
                'maxusernodes': 0,
                'totalnodes': 0
            },
            'comanche_B1_smt2_noturbo': {
                'maxusernodes': 0,
                'totalnodes': 0
            },
            'comanche_B1_smt2_turbo': {
                'maxusernodes': 0,
                'totalnodes': 0
            },
            'comanche_B1_smt4_noturbo': {
                'maxusernodes': 0,
                'totalnodes': 0
            },
            'comanche_B1_smt4_turbo': {
                'maxusernodes': 0,
                'totalnodes': 0
            },
            'dgx': {
                'maxusernodes': 1,
                'totalnodes': 1
            },
            'epyc_7601': {
                'maxusernodes': 2,
                'totalnodes': 2
            },
            'firestones': {
                'maxusernodes': 1,
                'totalnodes': 1
            },
            'fpga_385a': {
                'maxusernodes': 1,
                'totalnodes': 1
            },
            'gomez': {
                'maxusernodes': 4,
                'totalnodes': 4
            },
            'gomez_dual_hca': {
                'maxusernodes': 1,
                'totalnodes': 1
            },
            'gpu_mules': {
                'maxusernodes': 0,
                'totalnodes': 0
            },
            'gpu_power9_v100_smx2': {
                'maxusernodes': 1,
                'totalnodes': 1
            },
            'gpu_rtx8000': {
                'maxusernodes': 1,
                'totalnodes': 1
            },
            'gpu_v100_smx2': {
                'maxusernodes': 3,
                'totalnodes': 3
            },
            'iris': {
                'maxusernodes': 0,
                'totalnodes': 0
            },
            'iris_debug': {
                'maxusernodes': 0,
                'totalnodes': 0
            },
            'it': {
                'maxusernodes': 13,
                'totalnodes': 13
            },
            'k20x': {
                'maxusernodes': 1,
                'totalnodes': 1
            },
            'knl_7210': {
                'maxusernodes': 10,
                'totalnodes': 10
            },
            'knl_7250': {
                'maxusernodes': 2,
                'totalnodes': 2
            },
            'mustangs': {
                'maxusernodes': 2,
                'totalnodes': 2
            },
            'nurburg': {
                'maxusernodes': 0,
                'totalnodes': 0
            },
            'skylake_8176': {
                'maxusernodes': 1,
                'totalnodes': 1
            },
            'skylake_8180': {
                'maxusernodes': 12,
                'totalnodes': 12
            },
            'skylake_8180_skylake12': {
                'maxusernodes': 1,
                'totalnodes': 1
            },
            'skylake_p': {
                'maxusernodes': 0,
                'totalnodes': 0
            },
            'thing': {
                'maxusernodes': 8,
                'totalnodes': 8
            }
        }

        def __init__(self, qstat_queue_output):
            """
            Queue instanciation
            qstat_queue_output: A blob output from `qstat -Q -l` representing this queue.
            """

            self.jobs = []
            match = Cobalt.Queue.name_re.match(qstat_queue_output)
            if match is None:

                raise ValueError(
                    'Invalid queue initializer: {}'.format(qstat_queue_output))
            self.name = match.group(1)

            match = Cobalt.Queue.users_re.match(qstat_queue_output)
            if match is None:
                self.users = []
            else:
                self.users = match.group(1).split(':')

            match = Cobalt.Queue.groups_re.match(qstat_queue_output)
            if match is None:
                self.groups = []
            else:
                self.groups = match.group(1).split(':')

            match = Cobalt.Queue.mintime_re.match(qstat_queue_output)
            if match is None:
                self.mintime = timedelta(minutes=10)  # At least 10 seconds
            else:
                hours, minutes, seconds = [
                    int(i) for i in match.group(1).split(':')
                ]
                self.mintime = timedelta(hours=hours,
                                         minutes=minutes,
                                         seconds=sedonds)

            match = Cobalt.Queue.maxtime_re.match(qstat_queue_output)
            if match is None:
                self.maxtime = timedelta(hours=1)
            else:
                hours, minutes, seconds = [
                    int(i) for i in match.group(1).split(':')
                ]
                self.maxtime = timedelta(hours=hours,
                                         minutes=minutes,
                                         seconds=seconds)

            match = Cobalt.Queue.maxrunning_re.match(qstat_queue_output)
            if match is None:
                self.maxrunning = 0
            else:
                self.maxrunning = int(match.group(1))

            match = Cobalt.Queue.maxqueued_re.match(qstat_queue_output)
            if match is None:
                self.maxqueued = 0
            else:
                self.maxqueued = int(match.group(1))

            match = Cobalt.Queue.maxusernodes_re.match(qstat_queue_output)
            if match is None:
                try:
                    self.maxusernodes = Cobalt.Queue.defaults[
                        self.name]['maxusernodes']
                except KeyError:
                    self.maxusernodes = 0
            else:
                self.maxusernodes = int(match.group(1))

            match = Cobalt.Queue.maxnodehours_re.match(qstat_queue_output)
            if match is None:
                self.maxnodehours = 0
            else:
                self.maxnodehours = int(match.group(1))

            match = Cobalt.Queue.totalnodes_re.match(qstat_queue_output)
            if match is None:
                try:
                    self.totalnodes = Cobalt.Queue.defaults[
                        self.name]['totalnodes']
                except KeyError:
                    self.totalnodes = 0
            else:
                self.totalnodes = int(match.group(1))

            match = Cobalt.Queue.state_re.match(qstat_queue_output)
            if match is None:
                self.state = 'unknown'
            else:
                self.state = match.group(1)

        def __eq__(self, other):
            if isinstance(other, Cobalt.Queue):
                return self.name == other.name
            if isinstance(other, str):
                return self.name == other
            raise ValueError('Expected Queue or str.')

        def __repr__(self):
            return self.name

        def __str__(self):
            s = self.name + ':\n'
            s += '\tqueued: {}\n'.format(
                len([j for j in self.jobs if j.state == 'queued']))
            s += '\trunning: {}\n'.format(
                len([j for j in self.jobs if j.state == 'running']))
            s += '\tusers: {}\n'.format(', '.join(self.users))
            s += '\tgroups: {}\n'.format(', '.join(self.groups))
            s += '\tmintime: {!s}\n'.format(self.mintime)
            s += '\tmaxtime: {!s}\n'.format(self.maxtime)
            s += '\tmaxrunning: {}\n'.format(self.maxrunning)
            s += '\tmaxqueued: {}\n'.format(self.maxqueued)
            s += '\tmaxusernodes: {}\n'.format(self.maxusernodes)
            s += '\ttotalnodes: {}\n'.format(self.totalnodes)
            s += '\tstate: {}\n'.format(self.state)
            return s

        def submit(self,
                   cmd,
                   nodecount=1,
                   proccount=1,
                   time=None,
                   jobname=None,
                   cwd=None,
                   stderr=None,
                   stdout=None,
                   output_prefix=None,
                   users=[getuser()],
                   project=None,
                   attrs={},
                   dependencies=[],
                   geometry=[],
                   env={},
                   hold=False,
                   input_file=None,
                   email=None,
                   umask=None,
                   no_oversuscribe=True):
            """
            Submit a job on this queue.
            Options: (see man qsub).
            cmd (str): The command line to submit.
            nodecount (int): Specifies the node count for a job
            proccount (int): Specify the number of processes to start.
            time (timedelta): Specify  the  runtime  for a job. If the job runs over this limit, it will be killed.
            If time is None, queue attribute 'maxtime' will be used to set time.
            jobname (str): Sets Jobname.
            cwd (str): Tell the job to use the specified directory as the default current working directory while running the job.
            stderr (str): Send job stderr to file specified.
            stdout (str): Send job stdout to file specified.
            output_prefix (str): Use  the  specified  prefix for both .output, .error and debuglog files.
            users ([str]): Sets a list of users for the job being submitted. All users in  this list will be able to execute 
            cobalt commands to control the job. The submitting user is always able to run commands on a submitted job.
            project (str): Associate  the  job  with the allocation for project project. This is used to properly account for machine usage.
            attrs ([str]): Set a list of attributes for a job that must be fulfilled for a job  to  run.
            dependencies ([int] or [Job]): Set  the  dependencies for the job being submitted.  This job won't run until all jobs in the dependency list 
            have finished and exited with a status of 0.
            geometry ([str]): Sets  the  geometry of the block that the jobs should run on: [A,B,C,D,E].  This is in the form of AxBxCxDxE. 
            This must be the geometry of the compute block. This may not be a fraction of a block.
            env ({str: str}): Set environment variables that will be passed into the job's environment.
            hold (bool): Submit job and immediately place it in the user hold state
            input_file (str): Send file to job's stdin.
            email (str): Send an email notification at the start and stop of the job to the  specified email address.
            umask (str): set umask: octal number default(022)
            no_oversuscribe (bool): If queue has at least as many jobs queued as its capacity (totalnodes) submission will be cancelled
            and an exception StopIteration will be raised. Set to False to disable.
            """


            # Enforce user policy
            if hasattr(self, 'jobs'):
                num_used = len([j for j in self.jobs if j.user == user])
                if hasattr(self, 'maxusernodes'
                           ) and num_used + nodecount > self.maxusernodes:
                    raise StopIteration(
                        'Submition on {} cancelled due to user policy.'.format(
                            self.name))
                num_queued = len(self.jobs)
                if no_oversuscribe and hasattr(
                        self, 'totalnodes'
                ) and num_queued + nodecount > self.totalnodes:
                    raise StopIteration(
                        'Submition on {} cancelled because queue is busy.'.
                        format(self.name))
            time = time if time is not None else self.maxtime

            fd, filename = mkstemp(suffix='.sh')
            file = os.fdopen(fd, 'w')
            
            file.write('#!/bin/bash\n\n')
            file.write('#COBALT --user_list {}\n'.format(':'.join(users)))
            if proccount > 1:
                file.write('#COBALT --proccount {}\n'.format(proccount))
            if cwd is not None:
                file.write('#COBALT --cwd {}\n'.format(cwd))
            if stderr is not None:
                file.write('#COBALT --error {}\n'.format(stderr))
            if stdout is not None:
                file.write('#COBALT --output {}\n'.format(stdout))
            if output_prefix is not None:
                file.write('#COBALT --outputprefix {}\n'.format(output_prefix))
            if project is not None:
                file.write('#COBALT --run_project {}\n'.format(':'.join(project)))
            if len(attrs) > 0:
                file.write('#COBALT --attrs {}\n'.format(':'.join(
                        ['{!s}={!s}'.format(k, v) for k, v in attrs.items()])))
            if len(dependencies) > 0:
                file.write('#COBALT --dependencies {}\n'.format(':'.join([
                    d.jobid if isinstance(d, Cobalt.Job) else int(d)
                    for d in dependencies ])))
            if len(geometry) > 0:
                file.write('#COBALT --geometry {}\n'.format('x'.join(geometry)))
            if len(env) > 0:
                file.write('#COBALT --env {}\n'.format(':'.join(['{!s}={!s}'.format(k, v) for k, v in env.items()])))
            if hold:
                file.write('#COBALT --held\n')
            if input_file is not None:
                file.write('#COBALT --input_file {}\n'.format(input_file))
            if email is not None:
                file.write('#COBALT --notify {}\n'.format(email))
            if umask is not None:
                file.write('#COBALT --umask {}\n'.format(umask))

            file.write('\n')
            if cwd is not None:
                file.write('cd {}\n'.format(cwd))
            file.write('{}\n'.format(cmd))
            os.chmod(filename, stat.S_IRUSR | stat.S_IXUSR | stat.S_IROTH | stat.S_IXOTH)


            command = 'qsub --queue {} -n {} -t {}'.format(self.name, nodecount, time)
            if jobname is not None:
                command += ' --jobname {}'.format(jobname)
            command += ' ' + filename
            jobid = int(getoutput(command))
            del(file)

            self.maxusernodes -= 1
            return Cobalt.Job(jobid=jobid,
                              queue=self.name,
                              user=user,
                              name=jobname,
                              users=users,
                              project=project,
                              user_hold=hold,
                              nodecount=nodecount,
                              proccount=proccount,
                              runtime=timedelta(0),
                              walltime=time,
                              queued_time=timedelta(0),
                              remaining_time=time,
                              submit_time=datetime.now(),
                              start_time=None,
                              state='queued',
                              dependencies=dependencies,
                              envs=env,
                              attrs=attrs,
                              notify=email)

    @staticmethod
    def get_queues_jobs():
        """
        Return a list of available queues and a list of available jobs.
        (2 values to unpack)
        """

        queues = [
            Cobalt.Queue(l) for l in str(getoutput('qstat -Q -l')).split('\n\n')
            if Cobalt.Queue.name_re.match(l)
        ]
        jobs = [
            Cobalt.Job.from_string(l) for l in str(getoutput('qstat -f -l')).split('\n\n')
            if Cobalt.Job.jobid_re.match(l)
        ]
        # Connect jobs and queues.
        for j in jobs:
            q = next((q for q in queues if q.name == j.queue), None)
            if q is not None:
                j.queue = q
                q.jobs.append(j)
                if j.user == user:
                    q.maxusernodes -= 1
        return queues, jobs

    @staticmethod
    def get_queues():
        """
        Get a list of all queues on this system.
        """

        queues, _ = Cobalt.get_queues_jobs()
        return queues

    @staticmethod
    def get_jobs():
        """
        Get a list of all jobs.
        """

        _, jobs = Cobalt.get_queues_jobs()
        return jobs

    @staticmethod
    def get_myjobs():
        """
        Get a list of current user jobs
        """

        return [j for j in Cobalt.get_jobs() if j.user == user]


class UserPolicy():
    """
    Abstraction to filter available queues according 
    to defined behaviour.
    """
    def __init__(self, office_day_start, office_day_stop, office_max_occupancy,
                 max_occupancy, office_maxtime, maxtime):
        """
        Create a new policy filter
        Parameters:
        office_day_start (timedelta): A time when collaborators are mostlikely to start day.
        office_day_stop (timedelta): A time when collaborators are mostlikely to finish their day.
        max_occupancy (float): A number between 0 and 1 defining the maximum proportion of available nodes we are allowed to use.
        office_max_occupancy (float): A number between 0 and 1 defining the maximum proportion of available nodes we are 
        allowed to use during office ours.
        maxtime: The maximum submission time we are allowing for jobs.
        office_maxtime: The maximum submission time we are allowing for jobs during office hours.
        See effect on get_available_queues().
        """

        if max_occupancy <= 0 or max_occupancy > 1:
            raise ValueError('Policy nodes occupancy is a float in ]0, 1].')
        self.office_day_start = office_day_start
        self.office_day_stop = office_day_stop
        self.office_max_occupancy = office_max_occupancy
        self.max_occupancy = max_occupancy
        self.office_maxtime = office_maxtime
        self.maxtime = maxtime

    def get_queues(self):
        """
        Get available submissions queues according to this user policy.
        According to the current time (office hours or not) we compute the amount
        of available nodes we are allowed to us. The amount of allowed nodes for submission
        is overwritten in queue attribute 'maxusernodes'.
        This amount is the total usable nodes according to policy minus the amount of nodes in use by us.
        If total is > 0, the queue will be in the returned list.
        q.maxtime is set to policy time such that q.submit() will use the policy time if it is not manually set.
        """

        queues = Cobalt.get_queues()
        max_occupancy = self.max_occupancy
        maxtime = self.maxtime
        now = datetime.now()
        dt = timedelta(hours=now.hour, minutes=now.minute, seconds=now.second)

        if now.weekday(
        ) < 5 or dt < self.office_day_start or dt > self.office_day_stop:
            max_occupancy = self.office_max_occupancy
            maxtime = self.office_maxtime

        for q in queues:
            num_used = len([j for j in q.jobs if j.user == user])
            maxusernodes = max(1, ceil(q.totalnodes * max_occupancy))
            q.maxusernodes = max(1 if num_used == 0 else 0,
                                 maxusernodes - num_used)
            q.maxtime = min(maxtime, q.maxtime)
        queues = [q for q in queues if q.maxusernodes > 0]
        return queues


__all__ = ['Cobalt', 'UserPolicy']
