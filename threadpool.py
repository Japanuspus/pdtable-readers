"""
Threadpool with executor on separate thread

Checklist
% Ctrl-C (shuts down output thread, but does not cancel workers)
+ Exception forwarding 
"""

import queue
from typing import Iterable, Set, List, Any
import logging
from dataclasses import dataclass, field
from concurrent import futures
import threading

from load import LoadIssuesError, LoadItem, LoadError, LoadIssue, Loader, run_slow

logger = logging.getLogger(__name__)


@dataclass
class _LoadContext:
    executor: futures.Executor
    loader: Loader
    load_issues: List[Any] = field(default_factory=list)
    output: queue.SimpleQueue = field(default_factory=queue.SimpleQueue)


@dataclass
class _ConcurrentOrchestrator:
    context: _LoadContext
    child_futures: List[futures.Future] = field(default_factory=list)

    def add_load_item(self, w: LoadItem):
        load_future = self.context.executor.submit(self.load_to_output, w)
        self.child_futures.append(load_future)

    def register_load_issue(self, load_issue: LoadIssue):
        self.context.load_issues.append(load_issue)

    def load_to_output(self, load_item):
        logger.debug(f"Starting load for {load_item}")
        child_orch = _ConcurrentOrchestrator(context=self.context)
        try: 
            for b in self.context.loader.load(load_item, child_orch):
                self.context.output.put(b)
        except Exception as e:
            self.register_load_issue(LoadIssue(load_item, issue=e))
        logger.debug(f"Waiting for children of {load_item}")
        child_orch.wait_for_children()

    def wait_for_children(self):
        futures.wait(self.child_futures)
        

def load_all(roots, loader, num_threads: int=5):
    """
    Load using executor from separate thread to allow processing while loading -- adds complexity
    """
    output: queue.SimpleQueue = queue.SimpleQueue()
    issues: List[LoadIssue] = []

    def run_load_workers():
        with futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            context = _LoadContext(executor, loader=loader, output=output, load_issues=issues)
            orch = _ConcurrentOrchestrator(context=context)
            for r in roots:
                orch.add_load_item(r)
            print(f"Orch children: {orch.child_futures}")
            orch.wait_for_children()
            output.put(None)

    threading.Thread(target=run_load_workers, daemon=True).start()

    while True:
        b = output.get(timeout=10000)
        if b is None:
            break
        yield b

    if issues:
        raise LoadIssuesError(issues=issues)

if __name__=="__main__":
    run_slow(load_all)