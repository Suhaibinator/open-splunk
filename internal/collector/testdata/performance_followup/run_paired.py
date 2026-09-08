import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time

before, after, output = sys.argv[1:]
root = Path(output)
root.mkdir(exist_ok=False)
(root / "excluded").mkdir()
env = dict(os.environ, GOMAXPROCS="1")
pattern = os.environ.get("PERF_BENCH_PATTERN", "^(BenchmarkNDJSONDecoder|BenchmarkNativeDecoder|BenchmarkNativeMixedInputs|BenchmarkCollectorAllocationCases|BenchmarkCollectorPatternAllocations)$")
expected_cases = int(os.environ.get("PERF_EXPECTED_CASES", "25"))
bench_time = os.environ.get("PERF_BENCH_TIME", "200ms")
log = (root / "workloads.jsonl").open("w")

def record(event, **data):
    log.write(json.dumps(dict(event=event, time=time.time(), **data)) + "\n")
    log.flush()

def jobs(exclude=0):
    found = []
    for row in subprocess.check_output(["ps", "-axo", "pid=,comm=,args="], text=True).splitlines():
        parts = row.strip().split(None, 2)
        if len(parts) != 3: continue
        pid, command, args = parts
        name = Path(command).name
        if int(pid) == exclude: continue
        if name in ("compile", "link", "golangci-lint") or name.endswith(".test") or (name == "go" and any(s in args for s in (" test ", " build ", " vet "))) or (name in ("node", "npm") and any(s in args for s in ("next build", "tsc", "eslint", "playwright test", "vitest"))):
            found.append(dict(pid=int(pid), command=command))
    return found

def quiet():
    while True:
        competing = jobs()
        if not competing:
            time.sleep(1)
            competing = jobs()
            if not competing: return
        record("wait", jobs=competing)
        time.sleep(2)

def run(binary):
    with tempfile.TemporaryFile(mode="w+") as stream:
        process = subprocess.Popen(["taskpolicy", "-a", "-l", "0", "-t", "0", binary, "-test.run=^$", "-test.bench=" + pattern, "-test.benchmem", "-test.benchtime=" + bench_time, "-test.count=1"], env=env, stdout=stream, stderr=subprocess.STDOUT)
        observed = []
        while process.poll() is None:
            observed.extend(jobs(process.pid))
            time.sleep(.25)
        stream.seek(0)
        text = stream.read()
        if process.returncode: raise RuntimeError(text)
        rows = [line for line in text.splitlines() if line.startswith("Benchmark")]
        if len(rows) != expected_cases: raise RuntimeError(f"Expected all {expected_cases} benchmark cases: " + text)
        return text, observed

accepted = attempted = 0
while accepted < 20:
    quiet()
    attempted += 1
    samples = {}
    overlapping = []
    commands = [("before", before), ("after", after)]
    if accepted % 2: commands.reverse()
    for label, binary in commands:
        text, observed = run(binary)
        samples[label] = text
        overlapping.extend(observed)
    if overlapping:
        for label, text in samples.items():
            (root / "excluded" / (str(attempted) + "-" + label + ".txt")).write_text(text)
        record("excluded", attempt=attempted, jobs=overlapping)
        print("Excluded pair", attempted, "for observed competing workload", flush=True)
        continue
    for label, text in samples.items():
        with (root / (label + ".txt")).open("a") as stream: stream.write(text)
    accepted += 1
    record("accepted", attempt=attempted, sample=accepted)
    print("Accepted", accepted, "of 20 complete pairs", flush=True)
print(f"Completed 20 samples for all {expected_cases} cases", flush=True)
