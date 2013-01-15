# Copyright 2006-2012 Mark Diekhans
"""
Scheduling of threads to run tasks (normally rules).  A Task object is a
thread, the number running concurrently is controlled by the Sched object.
Since tasks usually run one external pipeline at a time, the number of
concurrent tasks controls the number of process executing on a host.

A scheduling group is associated with a host.  While all threads are on the
local host, groups are used for tasks that start threads on remote hosts.  The
motivation is to be able to run multiple cluster batches concurrently.  This
allows rules to create batches naturally, and still maximize the number of
jobs running in parallel.

Tasks are not preemptive, once started, they only pause when being switch
between scheduling groups.  A task is not equivalent to a thread, they
are something run by a thread.
"""
from __future__ import with_statement
import sys, os, threading, Queue, traceback
from pycbio.sys import PycbioException

# Notes:
#   - This module has no dependencies on the rest of the package
#   - Group is an component of Sched, all locking is in the Sched
#     object, although Group is public
#   - Messaging is used between Sched/Group objects and Tasks

# FIXME:
#  - should be able to addTask to group directly?

groupLocal = "localhost"

class SchedException(PycbioException):
    "various Sched exceptions"
    pass

class TaskTerminateException(PycbioException):
    "exception used to prematurely terminate a task"
    def __init__(self):
        PycbioException.__init__(self, "task terminated")

class TaskRunMsg(object):
    "message to tell a task to run"
    pass

class TaskEndMsg(object):
    "message to tell a task to exit prematurely"
    pass

class SchedTaskEndMsg(object):
    "message to scheduler that a task ended"
    __slots__ = ("task",)
    def __init__(self, task):
        self.task = task

class SchedTaskMoveMsg(object):
    "message to scheduler to move a task to another group."
    __slots__ = ("task", "newGroup")
    def __init__(self, task, newGroup):
        self.task = task
        self.newGroup = newGroup

class Task(threading.Thread):
    """A task, which is something to execute. A tasks can be moved between groups.
    Moving between groups is use for remote execution.  The runFunc takes this
    Task object as a single argument.  It must handle all errors, if it raises
    an exception, all processing will be stopped.
    """
    def __init__(self, runFunc, pri):
        self.runFunc = runFunc
        self.pri = pri  # smaller is higher
        self.group = None
        self.msgQ = Queue.Queue()
        threading.Thread.__init__(self)
        self.start()

    def __receive(self):
        "receive a message to run or terminate"
        msg = self.msgQ.get()
        if isinstance(msg, TaskEndMsg):
            raise TaskTerminateException()
        elif not isinstance(msg, TaskRunMsg):
            raise SchedException("invalid message received by Task: " + str(msg))

    def __send(self, msg):
        "send a message to the scheduler"
        self.group.sched.msgQ.put(msg)

    def run(self):
        "run the task"
        try:
            try:
                self.__receive()
                self.runFunc(self)
            except Exception, e:
                sys.stderr.write("Fatal error: unhandled exception in task:\n")
                traceback.print_exc(sys.stderr)
                os._exit(1)
        finally:
            self.__send(SchedTaskEndMsg(self))

    def moveGroup(self, newGroup):
        """move this group to another task and wait to be scheduled.
        Either Group or group name maybe specified for newGroup"""
        if isinstance(newGroup, str):
            newGroup = self.group.sched.obtainGroup(newGroup)
        self.__send(SchedTaskMoveMsg(self, newGroup))
        self.__receive()

class Group(object):
    """Scheduling group, normally associated with a host,
    contains a list of tasks. Assumes locking is done by Sched"""

    def __init__(self, name, maxConcurrent, sched):
        self.name = name
        self.sched = sched
        self.maxConcurrent = maxConcurrent
        self.running = []   # running Tasks
        self.ready = []     # ready to run Tasks

    def _addTask(self, task):
        task.group = self
        self.ready.append(task)

    def _removeTask(self, task):
        "remove a task from this group"
        if task in self.running:
            self.running.remove(task)
        elif task in self.ready:
            self.ready.remove(task)
        else:
            raise SchedException("task not in group")

    def __startNext(self):
        "start next ready task"
        task = self.ready.pop(0)
        self.running.append(task)
        task.msgQ.put(TaskRunMsg())

    def _startTasks(self):
        "start pending tasks up to maximum"
        # re-order by priority
        self.ready.sort(cmp=lambda a,b: b.pri-a.pri) 
        while (len(self.running) < self.maxConcurrent) and (len(self.ready) > 0):
            self.__startNext()

class Sched(object):
    "object that schedules threads to tasks"

    def __init__(self):
        self.lock = threading.RLock()
        self.msgQ = Queue.Queue()
        self.numTasks = 0
        self.groups = {}

    def obtainGroup(self, grpName, maxConcurrent=1):
        """get the group of the given name, create a new group if needed.
        Raise concurrency level to maxConcurrent if needed.
        """
        with self.lock:
            grp = self.groups.get(grpName)
            if grp == None:
                grp = self.groups[grpName] = Group(grpName, maxConcurrent, self)
            elif maxConcurrent > grp.maxConcurrent:
                grp.maxConcurrent = maxConcurrent
            return grp

    def obtainLocalGroup(self, maxConcurrent=1):
        """get the local host group"""
        return self.obtainGroup(groupLocal, maxConcurrent)

    def addTask(self, runFunc, group, pri=10):
        "add a new task, group can be object or group name"
        with self.lock:
            if isinstance(group, str):
                group = self.obtainGroup(group)
            group._addTask(Task(runFunc, pri))
            self.numTasks += 1

    def __startTasks(self):
        "start tasks ready to run"
        for grp in self.groups.itervalues():
            grp._startTasks()

    def __taskEnd(self, task):
        task.group._removeTask(task)
        self.numTasks -= 1
        task.group._startTasks()

    def __taskMove(self, task, newGroup):
        oldGroup = task.group
        oldGroup._removeTask(task)
        newGroup._addTask(task)
        oldGroup._startTasks()
        newGroup._startTasks()

    def __processMsg(self, msg):
        "process a message to the scheduler"
        if isinstance(msg, SchedTaskEndMsg):
            self.__taskEnd(msg.task)
        elif isinstance(msg, SchedTaskMoveMsg):
            self.__taskMove(msg.task, msg.newGroup)
        else:
            raise SchedException("invalid message received by Sched: " + str(msg))

    def run(self):
        """run until all tasks are complete or chill is set.  This runs in
        the main thread and handles creating and displaching threads to
        process tasks.  New tasks maybe added by threads.
        """
        with self.lock:
            self.__startTasks()
        while True:
            with self.lock:
                if self.numTasks == 0:
                    break  # all done!
            msg = self.msgQ.get()
            with self.lock:
                self.__processMsg(msg)
