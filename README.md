# Cobalt Python Wrapper

Userspace Python module to manage jlse jobs.

Some machine properties such as the maximum number of nodes
have been hardcoded from: 
https://wiki.jlse.anl.gov/display/JLSEdocs/JLSE+Hardware

See cobalt repository for more info:
https://github.com/ido/cobalt

## Install

`python setup.py install --user`

Here `python` means `python2.7`. The scripts work with `python3` as well.
I am not able to install with it because I don't have `setuptools` installed with `python3`.

## Examples

### Command line tool:

* Getting help:

`python -m cobalt --help`

* Listing my jobs

`python -m cobalt jlist`
```
  134873
```
`python -m cobalt jlist --verbose`
```
  134874 mappings-gomez       gomez                    ndenoyelle       running  0:00:20
```

* Listing all jobs on skylake queues

`python -m cobalt jlist --verbose --all --queue skylake`
```
  134760 N/A                  skylake_8180             ########         running  5:44:02   
  134787 N/A                  skylake_8180             ######           running  4:45:15   
  130367 N/A                  skylake_8180             #######          queued   7 days, 0:14:03
  130366 N/A                  skylake_8180             #######          queued   7 days, 0:14:09
```

* Listing all knl queues

`python -m cobalt qlist --queue knl`
```
knl_7210
knl_7250
```

* Deleting my jobs containing string 'toto' in their name on knl queues:

`python -m cobalt del -q knl -n toto`

### Module:

#### Jobs

* Getting a list of all jobs:
```
from cobalt import Cobalt

jobs = Cobalt.get_jobs()
```

* Putting my jobs on hold, releasing them, then deleting them:

```
from getpass import getuser

me = getuser()
myjobs = [ j for j in jobs if j.user == me ]

for j in myjobs:
	j.hold()
for j in myjobs:
	j.release()
for j in myjobs:
	j.delete()
```

Jobs have most attributes parsed from `qstat -l -f` command.

For instance: `jobid`, `queue`, `user`, `walltime`, `runtime`, `start_time`, `remaining_time`, 
`nodecount`, `proccount`, `location`, `state`, `envs`, `attrs`, `dependencies`.

Some attributes may not be set if the output field is not present or not set.

`queue` attribute is a queue instance (see below).

#### Queues

* Getting a list of all queues:

```
from cobalt import Cobalt

queues = Cobalt.get_queues()
```

* Getting skylake_8180 queue:

```
skylake_q = next(q for q in queues if q.name == 'skylake_8180')
```

* Querying jobs on this queue:

```
print(skylake_q.jobs)
```

Jobs are full Job instances.

* Submitting a job on this queue:

```
job = skylake_q.submit('myscript.sh', cwd='path_to_my_script')
```

Many options are available to customize.

```
help(skylake_q.submit)
```

#### UserPolicy

User policy is a class to filter queues according to their business inside and outside office hours.
User policy instances set the queues occupancy ratio, maximum submit time according to office hours.

* Create a nice user policy:

```
nice_user = UserPolicy(office_day_start=timedelta(hours=7),
                       office_day_stop=timedelta(hours=21),
                       office_max_occupancy=0.25,
                       max_occupancy=0.5,
                       office_maxtime=timedelta(minutes=30),
                       maxtime=timedelta(hours=2))
```

* Get a list of skylake_8180 nodes available so that we don't bother other users.

```
skylake_q = next(q for q in nice_user.get_queues() if q.name == 'skylake_8180')
```

If we are already using too much of the machine, above line will raise StopIteration because the queue list does not contain 
any available skylake queue.

Queues maximum number of nodes `totalnodes` is hardcoded from jlse wiki as long as `qstat -Q -l` does not set this attribute.
