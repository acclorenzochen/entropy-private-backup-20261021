from pathlib import Path

from entropy_arb.process_guard import ProcessGuard, ProcessGuardError


def test_process_guard_rejects_second_owner_and_releases(tmp_path: Path):
    path = tmp_path / "entropy-arb.lock"
    first = ProcessGuard(path).acquire()
    try:
        try:
            ProcessGuard(path).acquire()
        except ProcessGuardError:
            pass
        else:
            raise AssertionError("second live process guard was accepted")
    finally:
        first.release()
    with ProcessGuard(path):
        pass
