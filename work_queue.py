"""
Manually handled threadpool with work queue and separate loader thread.

Checklist
+ ctrl-c
+ exception
- option to fail on first exception
"""
import queue
import threading
from typing import Iterable, Set, List
import logging

from load import LoadItem, LoadError, LoadIssuesError, LoadIssue, Loader, run_slow

logger = logging.getLogger(__name__)

class _WorkQueueOrchestrator:
    def __init__(self, num_threads: int=5):
        self.work: queue.Queue = queue.Queue()
        self.load_issues: List[LoadIssue] = []
    
    def add_load_item(self, w: LoadItem):
        self.work.put(w)

    def register_load_issue(self, load_issue: LoadIssue):
        self.load_issues.append(load_issue)


def load_all(roots, loader: Loader, num_threads: int = 5):
    output: queue.SimpleQueue = queue.SimpleQueue()
    orch = _WorkQueueOrchestrator()
    for r in roots:
        orch.add_load_item(r)
 
    def load_worker():
        # safe to che unfinished_tasks since any children will be registered directly
        # from parent thread before parent completes.
        logger.debug(f"Load worker thread {threading.get_ident()} starting")
        while orch.work.unfinished_tasks > 0:  
            try:
                # wait for work, but wake up periodically to check if we are done
                # note that timeout can be longish as this is a daemon thread which
                # will not delay program exit.
                item: LoadItem = orch.work.get(timeout=0.300)
            except queue.Empty:
                continue
            try:
                for b in loader.load(item, orchestrator=orch):
                    output.put(b)
            except Exception as e:
                orch.register_load_issue(LoadIssue(load_item=item, issue=e))
            finally:
                orch.work.task_done()
        logger.debug(f"Load worker thread {threading.get_ident()} exiting")

    def worker_dispatch():
        for _ in range(num_threads):
            threading.Thread(target=load_worker, daemon=True).start()

        orch.work.join()
        output.put(None)  # `None` is end marker

    threading.Thread(target=worker_dispatch, daemon=True).start()

    while True:
        w = output.get(block=True)
        if w is None:  # `None` is end marker
            break
        yield w
    
    if orch.load_issues:
        raise LoadIssuesError(issues=orch.load_issues)


if __name__=="__main__":
    run_slow(load_all)

