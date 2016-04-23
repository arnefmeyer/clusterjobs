#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Author: Arne F. Meyer <arne.f.meyer@gmail.com>
# License: GPLv2

"""

Some helper classes to run (embarassingly) parallel jobs on an HPC cluster.

TODO:
    - remove some obsolete variables
    - replace 'shell' by 'executable'
    - use 'somepattern'.format(...) instead of old 'somepattern' % (...) style
    - make it work under windows

"""

import os
from os import path
import subprocess
from shutil import copyfile
import itertools
import copy
from time import gmtime, strftime, time
import numpy as np
import socket
import multiprocessing as mp
import pickle


def _detect_backend(order=['sbatch', 'qsub']):

    infos = {'sbatch': 'SLURM', 'qsub': 'GridEngine'}
    for cmd in order:
        res = subprocess.Popen("command -v %s" % cmd, shell=True,
                               stdout=subprocess.PIPE).stdout.read()

        if cmd in res:
            return infos[cmd]

    return None


def _submit_job_parallel(job):

    job.submit()


class ClusterJob():
    """A simple job class wrapping command line arguments like qsub

    Parameters
    ----------
    script : string
        Path of the script to be submitted. A copy will be made when submitting
        the job.
    arguments : list
        a list of script arguments
    queue : string
        name of the queue for job submission
    name : string
        name of the job
    temp_dir : string
        directory for temporary files
    mem_request : int
        memory request for the job in MB
    time_request : int
        time request in seconds
    init_bashrc : bool
        If set to true .bashrc will be initialized before running the job
        to set additional paths and variables
    call_func : string
        Call the function "call_func" from python script with given arguments.
        The arguments can be either a dict or a pickle file containing
    is_click_command : bool
        Set this to True if call_func is a click command (see click package)

    """

    def __init__(self, script, arguments='', queue='', name='', email=None,
                 tempdir='', copy=True, mem_request=None, time_request=None,
                 stdout='', stderr='', verbose=False, shell='python',
                 account=None, n_workers=1, compute_local=False, backend=None,
                 init_bashrc=True, call_func=None, is_click_command=False):

        self.script = script
        self.arguments = arguments
        self.queue = queue
        self.name = name
        self.email = email
        self.tempdir = tempdir
        self.copy = copy
        self.mem_request = mem_request
        self.time_request = time_request
        self.stdout = stdout
        self.stderr = stderr
        self.verbose = verbose
        self.n_workers = n_workers
        self.compute_local = compute_local
        self.backend = backend
        self.account = account
        self.init_bashrc = init_bashrc
        self.shell = shell
        self.call_func = call_func
        self.is_click_command = is_click_command

    def submit(self):
        """submit job using qsub or slurm backend"""

        sdir, sfile = path.split(self.script)
        if self.tempdir == '':
            self.tempdir = sdir
        elif not path.exists(self.tempdir):
            os.makedirs(self.tempdir)

        script_file = self.script
        if self.copy:
            name, ext = path.splitext(sfile)
            while True:
                tt = strftime('%Y_%m_%d_%H_%M_%S', gmtime())
                script_file = path.join(self.tempdir, '%s_copy_%s%s' %
                                        (name, tt, ext))
                if not path.exists(script_file):
                    break
            copyfile(self.script, script_file)

        hostname = socket.gethostname()
        backend = self.backend

        if backend is None:
            backend = _detect_backend()

        if self.compute_local or backend is None:
            print "Could not detect HPC backend." \
                  "Computing on local computer instead!"

            if self.call_func is None:
                subprocess.call(['/bin/bash', '-i', '-c',
                                 '%s %s %s' % (self.shell,
                                               self.script,
                                               self.arguments)])
            else:
                cmd = self.get_call_with_func_command()
                subprocess.call(['/bin/bash', '-i', '-c',
                                 '%s -c "%s"' % (self.shell, cmd)])

        else:

            if backend == 'GRIDENGINE':
                batch_file = path.join(self.tempdir, self.name + '.sge')
                self._to_sge_file(batch_file, script_file, 'hero' in hostname)
                cmd = 'qsub %s' % batch_file

            elif backend == 'SLURM':
                batch_file = path.join(self.tempdir, self.name + '.slurm')
                self._to_slurm_file(batch_file, script_file)
                cmd = 'sbatch %s' % batch_file

            if self.verbose:
                print 'Submitting job'
                print '  backend:', backend
                print '  temporary directory: %s' % self.tempdir
                print '  script file: %s' % script_file
                print '  arguments:', self.arguments

            os.system(cmd)

    def get_call_with_func_command(self):

        script_dir, script_f = path.split(self.script)
        script_name = path.splitext(script_f)[0]
        cmd = "import sys; sys.path.append('%s');" % script_dir

        func_name = self.call_func
        func_call = self.call_func
        if self.is_click_command:
            func_call += '.callback'

        # unpack arguments from file?
        args = self.arguments
        if args.endswith('.pickle') or args.endswith('.pkl'):
            cmd += "import pickle;"
            cmd += "dd=pickle.load(open('%s', 'r'));" % args
            cmd += "from %s import %s; %s(**dd);" % (script_name,
                                                     func_name,
                                                     func_call)
        else:
            cmd += "from %s import %s; %s('%s');" % (script_name,
                                                     func_name,
                                                     func_call,
                                                     args)

        return cmd

    def _to_sge_file(self, sge_file, script_file, is_hero):
        """create sge file with job parameters"""

        with open(sge_file, "w") as f:
            f.write("#$ -S /bin/bash\n")
            if self.name != '':
                f.write("#$ -N %s\n" % self.name)
            if self.queue != '':
                if not is_hero:
                    f.write("#$ -q %s\n" % self.queue)
            if self.mem_request is not None:
                if is_hero:
                    f.write("#$ -l h_vmem=%gM\n" % self.mem_request)
                    if self.mem_request > 22*1024:
                        f.write("#$ -l bignode=true\n")
                else:
                    f.write("#$ -l vf=%gM\n" % self.mem_request)
            if self.time_request is not None:
                if is_hero:
                    f.write("#$ -l h_rt=%02d:%02d:%02d\n" %
                            reduce(lambda ll, b: divmod(ll[0], b) + ll[1:],
                                   [(self.time_request,), 60, 60]))
            if self.stdout != "":
                f.write("#$ -o %s\n" % self.stdout)
            if self.stderr != "":
                f.write("#$ -e %s\n" % self.stderr)
            if is_hero:
                # Number of workers
                if self.n_workers is None or self.n_workers <= 0:
                    f.write("#$ -pe smp %s\n" % 1)
                else:
                    f.write("#$ -pe smp %s\n" % self.n_workers)
            f.write("#$ -wd %s\n" % os.getcwd())

            # Command to be executed
            f.write("export TERM=xterm;\n")
            f.write("%s %s %s\n" % (self.shell, script_file, self.arguments))
            f.write("exit\n")

    def _toqsubstring(self):
        """convert job parameters to qsub string"""

        opts = ''
        if self.name != '':
            opts += ' -N %s' % self.name
        if self.queue != '':
            opts += ' -q %s' % self.queue
        if self.mem_request is not None:
            opts += ' -l vf=%dM' % self.mem_request
        if self.time_request is not None:
            opts += ' -l h_rt=%02d:%02d:%02d' % \
                reduce(lambda ll, b: divmod(ll[0], b) + ll[1:],
                       [(self.time_request,), 60, 60])
        if self.stdout != "":
            opts += ' -o %s' % self.stdout
        if self.stderr != "":
            opts += ' -e %s' % self.stderr
        return opts

    def _to_slurm_file(self, slurm_file, script_file):

        with open(slurm_file, "w") as f:

            f.write("#!/bin/bash\n")

            if self.name != '':
                f.write("#SBATCH -J %s\n" % self.name)

            partition = self.queue
            if partition == '':
                partition = 'compute'
            f.write("#SBATCH -p %s\n" % partition)

            f.write("#SBATCH -N %d\n" % self.n_workers)

            if self.account is not None:
                f.write('#SBATCH -A %s' % self.account)

            if self.mem_request is not None:
                if self.n_workers > 1:
                    f.write("#SBTACH --mem-per-cpu=%d\n" % self.mem_request)
                else:
                    f.write("#SBTACH --mem=%d\n" % self.mem_request)

            if self.time_request is not None:
                f.write("#SBATCH --time=%02d:%02d:%02d\n" %
                        reduce(lambda ll, b: divmod(ll[0], b) + ll[1:],
                               [(self.time_request,), 60, 60]))
            if self.stdout != '':
                f.write("#SBATCH -o %s\n" % self.stdout)
            if self.stderr != '':
                f.write("#SBATCH -e %s\n" % self.stderr)

            if self.email is not None:
                f.write("#SBATCH --mail-user %s\n" % self.email)
                f.write("#SBATCH --mail-type=ALL\n")

            if self.init_bashrc is not None:
                f.write("source %s\n" % path.join(path.expanduser('~'),
                                                  '.bashrc'))

            f.write("export TERM=xterm;\n")

            if self.call_func is None:
                # pass arguments to script file
                f.write("%s %s %s\n" % (self.shell,
                                        script_file,
                                        self.arguments))

            else:
                # call function from inside script
                cmd = self.get_call_with_func_command()
                f.write('%s -c "%s"\n' % (self.shell, cmd))

            f.write("exit\n")


class ClusterBatch():
    """Automatically create a batch of cluster jobs"""

    def __init__(self, script, parameters, tempdir, template_job=None,
                 verbose=True, compute_local=False, n_parallel=-1,
                 format='pickle', **kwargs):

        self.script = script
        self.parameters = parameters
        self.tempdir = tempdir
        self.template_job = template_job
        self.verbose = verbose
        self.compute_local = compute_local
        self.n_parallel = n_parallel
        self.format = format
        self.job_args = kwargs

    def submit(self):
        """dispatch all jobs for this batch"""

        # Create unique directory
        while True:
            tmpdir = path.join(self.tempdir,
                               strftime('%Y_%m_%d_%H_%M_%S', gmtime()))
            if not path.exists(tmpdir):
                os.makedirs(tmpdir)
                logdir = path.join(tmpdir, 'log')
                os.makedirs(logdir)
                break

        if self.verbose:
            print "temp. dir: %s" % tmpdir
            print "log dir: %s" % logdir

        # Create combinations of all parameters
        paramkeys = self.parameters.keys()
        values = []
        for k in paramkeys:
            values.append(self.parameters[k])
        params = list(itertools.product(*values))

        template_job = self.template_job
        if template_job is None:
            template_job = ClusterJob(self.script, **self.job_args)

        # Create one job for each parameter combination
        jobs = []
        n_jobs = len(params)
        if self.verbose:
            print "creating %d jobs ..." % n_jobs,
            t0 = time()

        for i in range(n_jobs):

            job_name = '%s_%d' % (template_job.name, i+1)

            # Copy script to temporary folder
            script_dir, script_file = path.split(self.script)
            script_name, script_ext = path.splitext(script_file)
            copy_name = '%s_%d' % (script_name, i+1)
            script_copy = path.join(tmpdir, copy_name + script_ext)
            copyfile(self.script, script_copy)

            # Save parameters with keys as numpy or pickle file
            param_dict = dict()
            for k, key in enumerate(paramkeys):
                param_dict.update({key: params[i][k]})

            param_file = path.join(tmpdir, copy_name)
            if self.format in ['npz', 'numpy']:

                param_file += '.npz'
                np.savez(param_file, **param_dict)

            elif self.format in ['pkl', 'pickle']:

                param_file += '.pickle'
                with open(param_file, 'w') as f:
                    pickle.dump(param_dict, f)

            # Standard output and error log
            stdout = path.join(logdir, copy_name + '.out')
            stderr = path.join(logdir, copy_name + '.err')

            # Use template job as base
            job = copy.deepcopy(template_job)
            job.name = job_name
            job.arguments = param_file
            job.tempdir = tmpdir
            job.copy = False
            job.script = script_copy
            job.stdout = stdout
            job.stderr = stderr
            job.compute_local = self.compute_local

            jobs.append(job)

        if self.verbose:
            print "done in %0.2f seconds" % (time() - t0)
            print "submitting jobs ..."
            t0 = time()

        if self.compute_local and self.n_parallel != 1:
            if self.n_parallel < 1:
                n_parallel = mp.cpu_count()
            else:
                n_parallel = self.n_parallel

            pool = mp.Pool(processes=n_parallel)
            pool.map(_submit_job_parallel, jobs)

        else:
            for job in jobs:
                job.submit()

        if self.verbose:
            print "done in %0.2f seconds" % (time() - t0)


if __name__ == '__main__':

    import sys

    if len(sys.argv) > 1:
        data = np.load(sys.argv[1])
        a = data['a'].item()
        print "starting job", a

        X = np.random.randn(1000, 1000)
        whatever = np.linalg.svd(X)
        print "finished job", a

    else:
        user_dir = path.expanduser('~')
        test_dir = path.join(user_dir, 'job_test')

        if not path.exists(test_dir):
            os.makedirs(test_dir)

        script_file = __file__
        params = {'a': range(10)}
        batch = ClusterBatch(script_file, params, test_dir, verbose=True,
                             compute_local=False, n_parallel=-1, queue='test',
                             mem_request=1000)
        batch.submit()
