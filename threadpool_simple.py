"""
Threadpool on main thread, loading into list.

Checklist:
+ : Forwards exceptions
? : Fail on first exception. Attempted but not working
% : Respect Ctrl-C
"""
import queue
from typing import Iterable, Set, List, Any
import logging
from dataclasses import dataclass, field
from concurrent import futures

from load import LoadItem, LoadIssuesError, LoadIssue, Loader, run_slow

logger = logging.getLogger(__name__)

@dataclass
class _OrchestratorContext:
    executor: futures.Executor
    loader: Loader
    load_issues: List[Any] = field(default_factory=list)
    fail_on_first_issue: bool = True


class _ConcurrentOrchestrator:
    def __init__(self, context: _OrchestratorContext):
        self._context = context
        self._child_futures: List[futures.Future] = []

    def add_load_item(self, w: LoadItem):
        self._child_futures.append(self._context.executor.submit(self._load_worker, w))

    def register_load_issue(self, load_issue: LoadIssue):
        if self._context.fail_on_first_issue:
            raise LoadIssuesError(issues=[load_issue])
        else:
            self._context.load_issues.append(load_issue)

    def _child_results(self) -> List[Any]:      
        return_when = "FIRST_EXCEPTION" if self._context.fail_on_first_issue else "ALL_COMPLETE"
        futures.wait(self._child_futures, return_when=return_when)
        return [b 
            for bg in futures.as_completed(self._child_futures, timeout=0.0)
            for b in bg.result()]
       
    def _load_worker(self, load_item) -> List[Any]:
        logger.debug(f"Starting load for {load_item}")
        child_orch = _ConcurrentOrchestrator(context=self._context)
        try:
            own_results = list(self._context.loader.load(load_item, child_orch))
        except Exception as e:
            self.register_load_issue(LoadIssue(load_item, issue=e))
            own_results = []
        return own_results + child_orch._child_results()      


def load_all(roots, loader, num_threads: int=5, fail_on_first_issue: bool=True):
    with futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        context = _OrchestratorContext(executor, loader=loader, fail_on_first_issue=fail_on_first_issue)
        orch = _ConcurrentOrchestrator(context=context)
        for r in roots:
            orch.add_load_item(r)
        yield from orch._child_results()
        if context.load_issues:
            raise LoadIssuesError(issues=context.load_issues)


if __name__=="__main__":
    run_slow(load_all)