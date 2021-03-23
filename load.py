from typing import Protocol, Iterator, Any, NamedTuple, Iterable, Optional, Set
from abc import abstractmethod, abstractstaticmethod
import logging, time
from dataclasses import dataclass, field


class LoadItem(NamedTuple):
    specification: str


class LoadIssuesError(Exception):
    def __init__(self, issues):
        self.issues = issues

class LoadError(Exception):
    pass

# Todo: integrate properly with origin
class LoadIssue(NamedTuple):
    load_item: Optional[LoadItem] = None
    location_file: Any = None
    location: Any = None
    issue: Any = None

class LoadOrchestrator(Protocol):
    @abstractmethod
    def add_load_item(self, w: LoadItem):
        pass

    @abstractmethod
    def register_load_issue(self, load_issue: LoadIssue):
        pass

class Loader(Protocol):
    @abstractstaticmethod
    def load(sel, load_item: LoadItem, orchestrator: LoadOrchestrator) -> Iterator[Any]:
        pass

## Debug/test helpers
logger = logging.getLogger(__name__)


class TestError(Exception):

    pass
@dataclass
class SlowLoader:
    """
    A test loader that includes children. No real content
    """
    max_depth: int = 3
    line_count: int = 5
    line_delay: float = 0.1
    child_count: int = 3
    fail_on: Set[str] = field(default_factory=set)

    def load(self, load_item: LoadItem, orchestrator: LoadOrchestrator):
        name = load_item.specification
        if name in self.fail_on:
            logger.debug(f"(Test) Failing {load_item}")
            raise TestError(f"Failing for {name}")

        levels = [int(v) for v in name.split('_')]
        logger.debug(f"(Test) Processing {load_item}")
        if len(levels)<self.max_depth:
            time.sleep(self.line_delay)
            for cc in range(self.child_count):
                orchestrator.add_load_item(LoadItem(f"{name}_{cc}"))
        for ln in range(self.line_count):
            time.sleep(self.line_delay)
            yield f"{name}> Line {ln}"
        logger.debug(f"(Test) Processing {load_item} complete")


def run_slow(fn, *args, **kwargs):
    logging.basicConfig(level="DEBUG")
    roots = [LoadItem("0")]

    print("\n\n\n**** Testing slow load: please verify ctrl-c behaviour")
    lns = fn(roots, *args, loader=SlowLoader(), **kwargs)
    for ln in lns:
        print(ln)
    time.sleep(0.5)

    print("\n\n\n**** Testing exception propagation ****")
    bad_loader = SlowLoader(line_delay=0.01, fail_on={"0_2_0"})
    try:
        res = list(fn(roots, bad_loader))
    except LoadIssuesError as e:
        print(f"PASS: Reader error forwarded as LoadIssuesError: {e.issues}")
    except Exception as e:
        print(f"MAYBE: Reader error forwarded as {e}")
    else:
        print("FAIL. Reader error not propagated")
