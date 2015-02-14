import os, sqlite3
from cPickle import loads, dumps
from time import sleep
try:
    from thread import get_ident
except ImportError:
    from dummy_thread import get_ident

# awesome compact code from http://flask.pocoo.org/snippets/88/

class JobQueue(object):

    _create = (
            'CREATE TABLE IF NOT EXISTS queue ' 
            '('
            '  id INTEGER PRIMARY KEY AUTOINCREMENT,'
            '  item BLOB,'
            '  batchId text'
            ')'
            )
    _create_batch = (
            'CREATE TABLE IF NOT EXISTS batch ' 
            '('
            '  batchId TEXT PRIMARY KEY,'
            '  jobCount int DEFAULT 0,'
            '  hasFailed int DEFAULT 0,'
            '  jobDoneCount int DEFAULT 0,'
            '  concatStarted int DEFAULT 0,'
            '  concatDone int DEFAULT 0'
            ')'
            )
    _queue_index = ( 'CREATE INDEX IF NOT EXISTS q_idx ON queue (batchId);' )
    _batch_index = ( 'CREATE UNIQUE INDEX IF NOT EXISTS b_idx ON batch (batchId);' )
    _count = 'SELECT COUNT(*) FROM queue'
    _count_batch = 'SELECT COUNT(*) FROM queue where batchId=?'
    _concat_started= 'SELECT concatStarted FROM batch where batchId=?'
    _iterate = 'SELECT id, item FROM queue'
    _append = 'INSERT INTO queue (item, batchId) VALUES (?, ?)'
    _append_batch = 'INSERT INTO batch (batchId, jobCount) VALUES (?, ?)'
    _del_batch = 'DELETE FROM batch WHERE batchId=?'
    _write_lock = 'BEGIN IMMEDIATE'
    _popleft_get = (
            'SELECT id, item, batchId FROM queue '
            'ORDER BY id LIMIT 1'
            )
    _popleft_del = 'DELETE FROM queue WHERE id = ?'
    _peek = (
            'SELECT item FROM queue '
            'ORDER BY id LIMIT 1'
            )
    _fail_batch = (
            'UPDATE batch SET hasFailed=1 WHERE batchId = ?'
        )
    _del_all_jobs = (
            'DELETE FROM queue WHERE batchId = ?'
        )
    _batch_inc_count = (
            'UPDATE batch SET jobDoneCount=jobDoneCount+1 WHERE batchId = ?'
        )
    _batch_inc_concat = (
            'UPDATE batch SET concatStarted=concatStarted+1 WHERE batchId = ?'
        )
    _batch_concat_done = (
            'UPDATE batch SET concatDone=1 WHERE batchId = ?'
        )
    _batch_status = (
            'SELECT hasFailed, jobCount, jobDoneCount, concatStarted, concatDone from batch WHERE batchId=?'
        )

    def __init__(self, path):
        self.path = os.path.abspath(path)
        self._connection_cache = {}
        with self._get_conn() as conn:
            conn.execute(self._create)
            conn.execute(self._create_batch)
            conn.execute(self._queue_index)
            conn.execute(self._batch_index)

    def __len__(self):
        with self._get_conn() as conn:
            l = conn.execute(self._count).next()[0]
        return l

    def __iter__(self):
        with self._get_conn() as conn:
            for id, obj_buffer in conn.execute(self._iterate):
                yield loads(str(obj_buffer))

    def _get_conn(self):
        id = get_ident()
        if id not in self._connection_cache:
            self._connection_cache[id] = sqlite3.Connection(self.path, 
                    timeout=60)
        return self._connection_cache[id]

    def appendJobs(self, objList, batchId):
        with self._get_conn() as conn:
            conn.execute(self._append_batch, (batchId,len(objList)))
            for obj in objList:
                obj_buffer = buffer(dumps(obj, 2))
                conn.execute(self._append, (obj_buffer,batchId))

    def popleft(self, sleep_wait=True):
        keep_pooling = True
        wait = 0.1
        max_wait = 2
        tries = 0
        with self._get_conn() as conn:
            id = None
            while keep_pooling:
                conn.execute(self._write_lock)
                cursor = conn.execute(self._popleft_get)
                try:
                    id, obj_buffer, batchId = cursor.next()
                    keep_pooling = False
                except StopIteration:
                    conn.commit() # unlock the database
                    if not sleep_wait:
                        keep_pooling = False
                        continue
                    tries += 1
                    sleep(wait)
                    wait = min(max_wait, tries/10 + wait)
            if id:
                conn.execute(self._popleft_del, (id,))
                return loads(str(obj_buffer)), batchId
        return None

    def peek(self):
        with self._get_conn() as conn:
            cursor = conn.execute(self._peek)
            try:
                return loads(str(cursor.next()[0]))
            except StopIteration:
                return None

    def batchFailed(self, batchId):
        with self._get_conn() as conn:
            conn.execute(self._fail_batch, (batchId,))
            conn.execute(self._del_all_jobs, (batchId,))

    def batchIncreaseCount(self, batchId):
        with self._get_conn() as conn:
            conn.execute(self._batch_inc_count, (batchId,))

    def noJobsLeft(self, batchId):
        with self._get_conn() as conn:
            l = conn.execute(self._count_batch, (batchId,)).next()[0]
        return l==0
         
    def activeConcats(self, batchId):
        with self._get_conn() as conn:
            l = conn.execute(self._concat_started, (batchId,)).next()[0]
        return l
         
    def setConcatStarted(self, batchId):
        with self._get_conn() as conn:
            conn.execute(self._batch_inc_concat, (batchId,))

    def concatFinished(self, batchId):
        with self._get_conn() as conn:
            conn.execute(self._batch_concat_done, (batchId,))

    def deleteBatch(self, batchId):
        with self._get_conn() as conn:
            conn.execute(self._del_batch, (batchId,))
        
    def getStatus(self, batchId):
        with self._get_conn() as conn:
            cursor = conn.execute(self._batch_status, (batchId,))
            try:
                res = cursor.next()
                #if res==0:
                    #return "batch does not exist"
                hasFailed, jobCount, jobDoneCount, concatStarted, concatDone = res
                if hasFailed!=0:
                    return "error, after %d of %d jobs completed" % (jobDoneCount, jobCount)
                elif concatDone!=0:
                    return "all complete"
                elif concatStarted!=0:
                    return "jobs complete, concatting results"
                elif jobCount!=jobDoneCount:
                    return "running, %d of %d jobs completed" % (jobDoneCount, jobCount)
                else:
                    return "Somewhere between job completion and concat stage. Error?"

            except StopIteration:
                return None

def test():

    q = JobQueue('test.db')
    q.appendJobs([1,2,3], "mybatch")
    q.appendJobs([1,2,3], "batch2")
    q.batchIncreaseCount("batch2")
    q.batchIncreaseCount("batch2")

    e = q.popleft()
    print e
    assert(e==(1, "mybatch"))

    e = q.popleft()
    print e
    assert(e==(2, "mybatch"))

    e = q.popleft()
    print e
    assert(e==(3, "mybatch"))

    assert(1== q.peek())
    assert( q.noJobsLeft("mybatch")==True)
    assert( q.activeConcats("mybatch")==0)
    q.setConcatStarted("mybatch")
    assert( q.activeConcats("mybatch")==1)
    q.batchIncreaseCount("mybatch")
    q.batchIncreaseCount("mybatch")
    print q.getStatus("batch2")
    q.deleteBatch("mybatch")

if __name__=="__main__":
    test()
