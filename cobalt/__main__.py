#!/bin/python3

###############################################################################
# Copyright 2020 UChicago Argonne, LLC.
# (c.f. AUTHORS, LICENSE)
# For more info, see https://xgitlab.cels.anl.gov/argo/cobalt-python-wrapper
# SPDX-License-Identifier: BSD-3-Clause
##############################################################################

from getpass import getuser
from datetime import timedelta
import argparse
import re
from cobalt import Cobalt, UserPolicy

# Local user
user = getuser()

"""
Predefined submition policy that will let most nodes available
during office hours and still half of the nodes outside office hours.

"""
nice_user = UserPolicy(office_day_start=timedelta(hours=7),
                       office_day_stop=timedelta(hours=21),
                       office_max_occupancy=0.25,
                       max_occupancy=0.5,
                       office_maxtime=timedelta(minutes=30),
                       maxtime=timedelta(hours=2))

queues, jobs = Cobalt.get_queues_jobs()

parser = argparse.ArgumentParser()    
parser.add_argument("action",
                    choices = ['del', 'jlist', 'qlist', 'hold', 'rls', 'jstat'],
                    default = 'jlist',
                    help='''
jlist: List jobs id.
qlist: List queues.
del: Delete a list of jobs.
hold: Hold a list of jobs.
rls: Unhold a list of jobs.
jstat: Get node reservation infos for a job. If not verbose, remaining time is printed''')
parser.add_argument('-a', '--all', help="Do not restrict the list of jobs to my jobs, use them all.", action='store_true')
parser.add_argument('-v', '--verbose', help="Make output verbose", action='store_true')
parser.add_argument('-q', '--queue', help="Restrict the list of jobs/queues to queues containing this string in their name.")
parser.add_argument('-l', '--location', help="Restrict the list of jobs/queues to a specific location.")
parser.add_argument('-u', '--user', help="Filter jobs to only show this user jobs.")
parser.add_argument('-j', '--jobname', help="Restrict the list of jobs to jobs containing this string.")
parser.add_argument('--restrict', help="Restrict the list of queues according to nice_user policy.", action='store_true')
args = parser.parse_args()

# Fitering
if args.restrict:
    queues = nice_user.get_queues()
    jobs = [ j for j in jobs if j.queue in queues ]
if args.queue:
    queues = [ q for q in queues if args.queue in q.name ]
    jobs = [ j for j in jobs if args.queue in j.queue.name ]
if args.location:
    queues = [ q for q in queues if q.name in args.location ]
    jobs = [ j for j in jobs if hasattr(j, 'location') and args.location in j.location ]
if not args.all and not args.user:
    jobs = [ j for j in jobs if j.user == user ]
if args.jobname:
    jobs = [ j for j in jobs if args.jobname in j.name ]
if args.user:
    jobs = [ j for j in jobs if args.user in j.user ]

# Action
if args.action == 'del':
    for j in jobs:
        j.cancel()
if args.action == 'hold':
    for j in jobs:
        j.hold()
if args.action == 'rls':
    for j in jobs:
        j.release()
if args.action == 'jlist':
    if not args.verbose:
        print('\n'.join([ str(j.jobid) for j in jobs ]))
    else:
        for j in jobs:
            print(j)
if args.action == 'qlist':
    if not args.verbose:
        print('\n'.join([ str(q.name) for q in queues ]))
    else:
        for q in queues:
            print(q)
if args.action == 'jstat':
    if not args.verbose:
        print('\n').join([ str(j.remaining_time) for j in jobs ])
    else:
        for j in jobs:
            if len(j.users) > 1:
                user = str(j.users)
            else:
                user = j.user
            location = repr(j.queue)                
            if j.location is not None and len(j.location) > 1:                
                location += '[{}]'.format(','.join([re.match('[a-zA-Z]+(?P<i>\d+)', l).group(1) for l in j.location]))
            print('{} {}'.format(user, location))
            print('\tqueued_time: {}'.format(j.queued_time))
            print('\tstart_time: {}'.format(j.start_time))
            print('\truntime: {}'.format(j.runtime))
            print('\twalltime: {}'.format(j.walltime))
            print('\tremaining_time: {}'.format(j.remaining_time))
    
