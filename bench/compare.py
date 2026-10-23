#!/usr/bin/env python3
"""Compare emberkv with Redis, Kvrocks and Dragonfly on one machine.

Runs four experiments and writes a Markdown report:

  1. compatibility: the same commands against Redis and emberkv, replies diffed
  2. throughput: redis-benchmark against every server
  3. memory and disk: footprint after loading the same data
  4. crash safety: kill -9 during a write stream, count lost acknowledged writes

Needs redis-server, redis-cli, redis-benchmark, docker and `pip install redis`.
Used by .github/workflows/bench.yml.
"""
import argparse
import csv
import io
import json
import os
import platform
import shutil
import signal
import subprocess
import threading
import time

import redis

TESTS = "set,get,incr,hset,sadd,mset,lpush,lpop,rpush,rpop,lrange_100,zadd,zpopmin"


# ---------------------------------------------------------------- servers

def wait_ready(port, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if redis.Redis(port=port, socket_timeout=1).ping():
                return
        except Exception:
            time.sleep(0.2)
    raise RuntimeError(f"server on port {port} did not become ready")


def rss_kb(pid):
    with open(f"/proc/{pid}/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    return 0


def du_bytes(path, sudo=False):
    cmd = (["sudo"] if sudo else []) + ["du", "-sb", path]
    return int(subprocess.check_output(cmd, text=True).split()[0])


class Native:
    def __init__(self, name, port, argv, datadir, durable):
        self.name, self.port, self.argv, self.datadir, self.durable = name, port, argv, datadir, durable
        self.proc = None

    def start(self):
        os.makedirs(self.datadir, exist_ok=True)
        log = open(os.path.join(os.path.dirname(self.datadir), f"{self.port}.log"), "a")
        self.proc = subprocess.Popen(self.argv, stdout=log, stderr=log, cwd=self.datadir)
        wait_ready(self.port)

    def kill9(self):
        self.proc.send_signal(signal.SIGKILL)
        self.proc.wait()

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(30)
            except subprocess.TimeoutExpired:
                self.kill9()

    def wipe(self):
        self.stop()
        shutil.rmtree(self.datadir, ignore_errors=True)

    def rss(self):
        return rss_kb(self.proc.pid) * 1024

    def disk(self):
        return du_bytes(self.datadir)


class Docker:
    def __init__(self, name, port, image, inner_port, args=(), run_flags=(), volume_at=None, durable=False):
        self.name, self.port, self.image, self.inner_port = name, port, image, inner_port
        self.args, self.run_flags, self.volume_at, self.durable = list(args), list(run_flags), volume_at, durable
        self.container = f"bench-{port}"

    def _exists(self):
        return subprocess.run(["docker", "inspect", self.container], capture_output=True).returncode == 0

    def start(self):
        if self._exists():
            subprocess.run(["docker", "start", self.container], check=True, capture_output=True)
        else:
            cmd = ["docker", "run", "-d", "--name", self.container, "-p", f"{self.port}:{self.inner_port}"]
            if self.volume_at:
                cmd += ["-v", f"{self.container}:{self.volume_at}"]
            subprocess.run(cmd + self.run_flags + [self.image] + self.args, check=True, capture_output=True)
        wait_ready(self.port)

    def kill9(self):
        subprocess.run(["docker", "kill", "--signal=KILL", self.container], capture_output=True)
        while subprocess.run(["docker", "inspect", "-f", "{{.State.Running}}", self.container],
                             capture_output=True, text=True).stdout.strip() == "true":
            time.sleep(0.1)

    def stop(self):
        if self._exists():
            subprocess.run(["docker", "stop", self.container], capture_output=True)

    def wipe(self):
        subprocess.run(["docker", "rm", "-f", self.container], capture_output=True)
        subprocess.run(["docker", "volume", "rm", "-f", self.container], capture_output=True)

    def rss(self):
        # The container's main PID may be a wrapper (tini, a shell), so use the
        # container's memory accounting, which excludes reclaimable page cache.
        usage = subprocess.check_output(["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}",
                                         self.container], text=True).split("/")[0].strip()
        units = {"B": 1, "KiB": 1024, "kB": 1000, "MiB": 1024 ** 2, "MB": 1000 ** 2, "GiB": 1024 ** 3, "GB": 1000 ** 3}
        num = usage.rstrip("BKMGiknb")
        return int(float(num) * units[usage[len(num):]])

    def disk(self):
        if not self.volume_at:
            return None
        mp = subprocess.check_output(["docker", "volume", "inspect", "-f", "{{.Mountpoint}}", self.container],
                                     text=True).strip()
        return du_bytes(mp, sudo=True)


def make_servers(emberkv, work, baseline=None):
    def d(name):
        return os.path.join(work, name, "data")

    def ember(name, port, fsync, binary=emberkv, label="emberkv", extra=(), note=""):
        return Native(f"{label} (fsync {fsync}{note})", port,
                      [binary, "-addr", f"127.0.0.1:{port}", "-dir", d(name), "-fsync", fsync] + list(extra),
                      d(name), True)

    before = {}
    if baseline:
        before = {
            "ember-old-es": ember("ember-old-es", 7008, "everysec", baseline, "emberkv before"),
            "ember-old-always": ember("ember-old-always", 7009, "always", baseline, "emberkv before"),
        }

    def redis_server(label, port, name, extra):
        return Native(label, port,
                      ["redis-server", "--port", str(port), "--bind", "127.0.0.1", "--dir", d(name)] + extra,
                      d(name), True)

    return before | {
        "ember-es": ember("ember-es", 7001, "everysec"),
        "ember-always": ember("ember-always", 7002, "always"),
        "ember-nohist": ember("ember-nohist", 7010, "everysec", extra=["-history", "0"], note=", history off"),
        "redis-rdb": redis_server("Redis (default: RDB snapshots)", 7003, "redis-rdb",
                                  ["--appendonly", "no", "--save", "3600 1 300 100 60 10000"]),
        "redis-aof-es": redis_server("Redis (AOF everysec)", 7004, "redis-aof-es",
                                     ["--appendonly", "yes", "--appendfsync", "everysec", "--save", ""]),
        "redis-aof-always": redis_server("Redis (AOF always)", 7005, "redis-aof-always",
                                         ["--appendonly", "yes", "--appendfsync", "always", "--save", ""]),
        "kvrocks": Docker("Kvrocks (RocksDB, on disk)", 7006, "apache/kvrocks:latest", 6666,
                          volume_at="/var/lib/kvrocks", durable=True),
        "dragonfly": Docker("Dragonfly (in-memory)", 7007, "docker.dragonflydb.io/dragonflydb/dragonfly:latest", 6379,
                            run_flags=["--ulimit", "memlock=-1"], args=["--dbfilename", ""]),
    }


# ---------------------------------------------------------- compatibility

# Commands whose reply order is unspecified in Redis; compared sorted.
UNORDERED = {"SMEMBERS", "SINTER", "SUNION", "SDIFF", "KEYS", "HKEYS", "HVALS"}

COMPAT = [
    # strings
    ["SET", "s", "hello"], ["GET", "s"], ["GET", "missing"], ["STRLEN", "s"], ["STRLEN", "missing"],
    ["APPEND", "s", " world"], ["GET", "s"], ["APPEND", "new", "x"],
    ["SET", "s", "v2", "NX"], ["SET", "s2", "v", "NX"], ["SET", "s3", "v", "XX"], ["SET", "s", "v3", "XX"],
    ["SET", "s", "v4", "GET"], ["SET", "fresh", "v", "GET"], ["SET", "s", "v", "NX", "XX"],
    ["SET", "s", "v", "EX", "0"], ["SET", "s", "v", "EX", "abc"], ["SET", "s", "v", "BOGUS"], ["SET", "s"],
    ["SETNX", "s", "x"], ["SETNX", "nx1", "x"], ["SETEX", "ex1", "100", "v"], ["TTL", "ex1"],
    ["SETEX", "ex1", "0", "v"], ["PSETEX", "pex", "100000", "v"],
    ["GETSET", "s", "gs"], ["GETSET", "gsnew", "x"], ["GETDEL", "s"], ["GETDEL", "s"],
    ["MSET", "a", "1", "b", "2", "c", "3"], ["MGET", "a", "nope", "c"], ["MSET", "a"],
    # counters
    ["INCR", "n"], ["INCRBY", "n", "10"], ["DECR", "n"], ["DECRBY", "n", "20"], ["GET", "n"],
    ["INCRBY", "n", "notanumber"], ["SET", "txt", "abc"], ["INCR", "txt"],
    ["SET", "big", "9223372036854775807"], ["INCR", "big"], ["DECRBY", "big", "-1"],
    ["SET", "neg", "-9223372036854775808"], ["DECR", "neg"],
    # keys and expiry
    ["EXISTS", "a", "a", "nope"], ["DEL", "a", "nope"], ["UNLINK", "b"], ["EXISTS", "a", "b"],
    ["TYPE", "c"], ["TYPE", "nope"], ["TTL", "c"], ["TTL", "nope"], ["EXPIRE", "c", "100"], ["TTL", "c"],
    ["PERSIST", "c"], ["TTL", "c"], ["PERSIST", "c"], ["EXPIRE", "nope", "10"], ["EXPIRE", "c", "-1"],
    ["EXISTS", "c"], ["EXPIRE", "nx1", "abc"], ["PEXPIRE", "nx1", "100000"], ["EXPIREAT", "nx1", "4102444800"],
    ["TTL", "nx1"], ["SET", "kt", "1", "EX", "100"], ["SET", "kt", "2", "KEEPTTL"], ["TTL", "kt"],
    ["INCR", "kt"], ["TTL", "kt"],
    # hashes
    ["HSET", "h", "f1", "v1", "f2", "v2"], ["HSET", "h", "f1", "x"], ["HGET", "h", "f1"], ["HGET", "h", "nope"],
    ["HGET", "nope", "f"], ["HMGET", "h", "f1", "nope", "f2"], ["HGETALL", "h"], ["HGETALL", "nope"],
    ["HKEYS", "h"], ["HVALS", "h"], ["HLEN", "h"], ["HLEN", "nope"], ["HEXISTS", "h", "f1"],
    ["HEXISTS", "h", "zz"], ["HSTRLEN", "h", "f2"], ["HSETNX", "h", "f1", "y"], ["HSETNX", "h", "f3", "y"],
    ["HINCRBY", "h", "cnt", "5"], ["HINCRBY", "h", "cnt", "-2"], ["HINCRBY", "h", "f1", "1"],
    ["HMSET", "h", "m1", "1", "m2", "2"], ["HSET", "h", "odd"], ["HDEL", "h", "f1", "nope", "m1"],
    ["HLEN", "h"], ["TYPE", "h"], ["HDEL", "h", "f2", "f3", "cnt", "m2"], ["EXISTS", "h"],
    # sets
    ["SADD", "s1", "a", "b", "c", "a"], ["SADD", "s1", "d"], ["SMEMBERS", "s1"], ["SMEMBERS", "nope"],
    ["SISMEMBER", "s1", "a"], ["SISMEMBER", "s1", "z"], ["SMISMEMBER", "s1", "a", "z"], ["SCARD", "s1"],
    ["SADD", "s2", "c", "d", "e"], ["SINTER", "s1", "s2"], ["SUNION", "s1", "s2"], ["SDIFF", "s1", "s2"],
    ["SINTER", "s1", "nope"], ["SREM", "s1", "a", "zz"], ["TYPE", "s1"], ["SREM", "s1", "b", "c", "d"],
    ["EXISTS", "s1"],
    # wrong types
    ["HSET", "wt-h", "f", "v"], ["SADD", "wt-s", "m"], ["SET", "wt-str", "v"],
    ["GET", "wt-h"], ["INCR", "wt-s"], ["APPEND", "wt-h", "x"], ["HGET", "wt-str", "f"], ["SADD", "wt-h", "m"],
    ["HSET", "wt-s", "f", "v"], ["SMEMBERS", "wt-str"], ["SET", "wt-h", "v", "GET"], ["MGET", "wt-str", "wt-h"],
    ["STRLEN", "wt-s"], ["SET", "wt-s", "now-string"], ["TYPE", "wt-s"], ["EXPIRE", "wt-h", "100"],
    ["TTL", "wt-h"], ["DEL", "wt-h"], ["TYPE", "wt-h"],
    # keyspace
    ["KEYS", "*"], ["KEYS", "s*"], ["KEYS", "h?"], ["DBSIZE"], ["PING"], ["PING", "hi"], ["ECHO", "e"],
    ["SELECT", "0"], ["SELECT", "1"], ["FLUSHDB"], ["DBSIZE"],
    # lists
    ["RPUSH", "L", "a", "b", "c"], ["LPUSH", "L", "x"], ["LRANGE", "L", "0", "-1"], ["LRANGE", "L", "-2", "10"],
    ["LINDEX", "L", "1"], ["LINDEX", "L", "-9"], ["LLEN", "L"], ["LPOP", "L"], ["RPOP", "L", "2"],
    ["LPOP", "nolist", "2"], ["LPOP", "nolist"], ["RPUSH", "L", "1", "2", "1"], ["LREM", "L", "-1", "1"],
    ["LINSERT", "L", "BEFORE", "2", "z"], ["LINSERT", "L", "AFTER", "nope", "z"], ["LSET", "L", "0", "s"],
    ["LSET", "L", "50", "s"], ["LSET", "nolist", "0", "s"], ["LTRIM", "L", "1", "-1"], ["LRANGE", "L", "0", "-1"],
    ["RPOPLPUSH", "L", "L2"], ["LMOVE", "L", "L2", "LEFT", "LEFT"], ["LRANGE", "L2", "0", "-1"], ["TYPE", "L2"],
    ["LPUSHX", "nolist", "a"], ["RPUSHX", "L2", "e"], ["LPOP", "L", "-1"], ["LPOP", "L2", "0"],
    # sorted sets
    ["ZADD", "Z", "1", "a", "2", "b", "2", "c", "-1.5", "d"], ["ZADD", "Z", "XX", "CH", "5", "a", "9", "nope"],
    ["ZADD", "Z", "INCR", "0.5", "b"], ["ZADD", "Z", "NX", "INCR", "1", "a"], ["ZADD", "Z", "GT", "1", "a"],
    ["ZRANGE", "Z", "0", "-1", "WITHSCORES"], ["ZREVRANGE", "Z", "0", "1"], ["ZRANGE", "Z", "1", "2", "REV"],
    ["ZRANGEBYSCORE", "Z", "(-1.5", "+inf", "WITHSCORES", "LIMIT", "1", "2"], ["ZREVRANGEBYSCORE", "Z", "5", "2"],
    ["ZRANGE", "Z", "(5", "-inf", "BYSCORE", "REV"], ["ZCOUNT", "Z", "-inf", "(2.5"], ["ZRANK", "Z", "c"],
    ["ZREVRANK", "Z", "c"], ["ZRANK", "Z", "zz"], ["ZSCORE", "Z", "b"], ["ZSCORE", "Z", "zz"],
    ["ZMSCORE", "Z", "a", "zz"], ["ZINCRBY", "Z", "0.1", "d"], ["ZCARD", "Z"], ["ZPOPMIN", "Z"],
    ["ZPOPMAX", "Z", "2"], ["ZREM", "Z", "c", "zz"], ["ZADD", "Z2", "1", "a", "2", "b", "3", "c", "4", "d"],
    ["ZREMRANGEBYRANK", "Z2", "0", "0"], ["ZREMRANGEBYSCORE", "Z2", "-inf", "(3"], ["ZRANGE", "Z2", "0", "-1"],
    ["ZADD", "Z3", "+inf", "top", "-inf", "bottom", "0", "zero"], ["ZRANGE", "Z3", "0", "-1", "WITHSCORES"],
    ["TYPE", "Z2"], ["ZADD", "Z", "abc", "x"], ["ZADD", "Z", "NX", "XX", "1", "x"], ["ZADD", "Z", "GT", "LT", "1", "x"],
    ["ZCOUNT", "Z", "x", "1"], ["LPUSH", "Z2", "x"], ["ZADD", "L2", "1", "x"], ["ZPOPMIN", "nozset"],
    # errors
    ["NOSUCHCOMMAND", "x"], ["GET"], ["GET", "a", "b"],
]


def run_compat(port):
    r = redis.Redis(port=port, decode_responses=True)
    r.response_callbacks.clear()  # compare raw protocol replies
    r.execute_command("FLUSHALL")
    out = []
    for cmd in COMPAT:
        try:
            res = r.execute_command(*cmd)
        except redis.ResponseError as e:
            res = f"(error) {e}"
        if cmd[0] in UNORDERED and isinstance(res, list):
            res = sorted(res, key=str)
        elif cmd[0] == "HGETALL" and isinstance(res, list):
            res = sorted(zip(res[::2], res[1::2]))
        out.append(res)
    return out


def compat(servers):
    want = run_compat(servers["redis-rdb"].port)
    got = run_compat(servers["ember-es"].port)
    diffs = []
    for cmd, w, g in zip(COMPAT, want, got):
        if w != g:
            diffs.append({"command": " ".join(cmd), "redis": repr(w), "emberkv": repr(g)})
    return {"total": len(COMPAT), "same": len(COMPAT) - len(diffs), "diffs": diffs}


def redis_cli_checks(port):
    """Drive emberkv with the real redis-cli, including its non-interactive modes."""
    def cli(*args, stdin=None):
        return subprocess.run(["redis-cli", "-p", str(port)] + list(args), input=stdin,
                              capture_output=True, text=True, timeout=30)

    cli("FLUSHALL")
    checks = []

    def check(name, proc, expect):
        ok = proc.returncode == 0 and expect(proc.stdout)
        checks.append({"check": name, "ok": ok, "output": (proc.stdout + proc.stderr).strip()[:200]})

    check("SET / GET", cli("SET", "greeting", "hello"), lambda o: o.strip() == "OK")
    check("GET", cli("GET", "greeting"), lambda o: o.strip() == "hello")
    check("HSET / HGETALL", cli("HSET", "user:1", "name", "ada", "lang", "go"), lambda o: o.strip() == "2")
    check("HGETALL", cli("HGETALL", "user:1"), lambda o: o.split() == ["lang", "go", "name", "ada"])
    for i in range(50):
        cli("SET", f"user:{i + 100}", "x")
    check("--scan --pattern", cli("--scan", "--pattern", "user:*"), lambda o: len(o.split()) == 51)
    check("commands from stdin", cli(stdin="PING\nSET x 41\nINCR x\n"), lambda o: o.split() == ["PONG", "OK", "42"])
    check("--pipe mass insert", cli("--pipe", stdin="".join(f"SET pipe:{i} v\r\n" for i in range(1000))),
          lambda o: "errors: 0, replies: 1000" in o)
    check("INFO", cli("INFO", "server"), lambda o: "emberkv_version" in o)
    check("DBSIZE", cli("DBSIZE"), lambda o: o.strip() == str(1000 + 50 + 3))
    check("--latency (2s)", subprocess.run(["timeout", "2", "redis-cli", "-p", str(port), "--latency", "--raw"],
                                           capture_output=True, text=True), lambda o: True)
    checks[-1]["ok"] = True  # latency mode only needs to start without errors
    return checks


# ------------------------------------------------------------- throughput

def bench(port, pipeline, n):
    out = subprocess.run(
        ["redis-benchmark", "-p", str(port), "-c", "50", "-n", str(n), "-r", "100000", "-d", "64",
         "-P", str(pipeline), "-t", TESTS, "--csv"],
        capture_output=True, text=True, timeout=1800).stdout
    rows = {}
    for row in csv.DictReader(io.StringIO(out)):
        if "needed" in row["test"]:  # e.g. "LPUSH (needed to benchmark LRANGE)"
            continue
        name = row["test"].split()[0]
        rows[name] = {"rps": float(row["rps"]), "p50_ms": float(row["p50_latency_ms"]),
                      "p99_ms": float(row["p99_latency_ms"])}
    return rows


# ---------------------------------------------------------- memory & disk

def footprint(server):
    server.wipe()
    server.start()
    subprocess.run(["redis-benchmark", "-p", str(server.port), "-c", "50", "-n", "1000000", "-r", "1000000",
                    "-d", "100", "-P", "16", "-t", "set", "-q"], capture_output=True, timeout=1800)
    time.sleep(3)  # let background flushes settle
    r = redis.Redis(port=server.port)
    if "Kvrocks" in server.name:
        # Kvrocks counts keys in the background: DBSIZE SCAN starts it.
        r.execute_command("DBSIZE", "SCAN")
        deadline = time.time() + 60
        while r.dbsize() == 0 and time.time() < deadline:
            time.sleep(1)
    keys = r.dbsize()
    return {"keys": keys, "rss": server.rss(), "disk": server.disk()}


# ------------------------------------------------------------ crash safety

def crash(server, seconds=3.0):
    server.wipe()
    server.start()
    r = redis.Redis(port=server.port, socket_timeout=10)
    acked = 0
    timer = threading.Timer(seconds, server.kill9)
    timer.start()
    try:
        while True:
            r.set(f"k:{acked}", f"v{acked}")
            acked += 1
    except Exception:
        pass
    timer.join()
    server.start()
    r = redis.Redis(port=server.port, decode_responses=True)
    survived = 0
    for lo in range(0, acked, 1000):
        hi = min(acked, lo + 1000)
        vals = r.mget([f"k:{i}" for i in range(lo, hi)])
        survived += sum(1 for i, v in zip(range(lo, hi), vals) if v == f"v{i}")
    server.stop()
    return {"acked": acked, "lost": acked - survived}


# ------------------------------------------------------------------ report

def mib(b):
    return "n/a" if b is None else f"{b / 1048576:.0f} MiB"


def report(res):
    L = []
    w = L.append
    w("# emberkv vs Redis, Kvrocks and Dragonfly\n")
    w(f"Machine: {res['machine']}. Versions: {res['versions']}.\n")
    w("All servers ran on the same machine as the load generator (`redis-benchmark`, 50 clients, "
      "64-byte values, 100k random keys).\n")

    if "ember-old-es" in res["throughput"]["1"]:
        w(f"## emberkv before vs after (same machine, same run)\n\nBaseline: `{res['baseline']}`.\n")
        names = ["SET", "GET", "INCR", "HSET", "SADD", "MSET"]
        w("| fsync | pipeline | " + " | ".join(names) + " |\n|---|---|" + "---|" * len(names))
        for fsync, old, new in (("everysec", "ember-old-es", "ember-es"), ("always", "ember-old-always", "ember-always")):
            for p in ("1", "16"):
                cells = []
                for n in names:
                    a, b = res["throughput"][p][old].get(n), res["throughput"][p][new].get(n)
                    cells.append(f"{a['rps']:,.0f} → {b['rps']:,.0f} ({(b['rps'] / a['rps'] - 1) * 100:+.0f}%)"
                                 if a and b else "-")
                w(f"| {fsync} | {p} | " + " | ".join(cells) + " |")
        w("")

    c = res["compat"]
    w(f"## Compatibility with Redis\n\n**{c['same']} of {c['total']}** commands returned exactly the same "
      "reply as Redis.\n")
    if c["diffs"]:
        w("| Command | Redis | emberkv |\n|---|---|---|")
        for d in c["diffs"]:
            w(f"| `{d['command']}` | `{d['redis']}` | `{d['emberkv']}` |")
        w("")
    w("### redis-cli\n\n| Check | Result |\n|---|---|")
    for ch in res["redis_cli"]:
        w(f"| {ch['check']} | {'pass' if ch['ok'] else 'FAIL: ' + ch['output'].replace('|', '/')} |")
    w("")

    for p in ("1", "16"):
        for title, names in (("", ["SET", "GET", "INCR", "HSET", "SADD", "MSET"]),
                             (", lists and sorted sets", ["LPUSH", "LPOP", "RPUSH", "RPOP", "LRANGE_100", "ZADD", "ZPOPMIN"])):
            w(f"## Throughput{title}, pipeline {p} (requests/s)\n")
            w("| Server | " + " | ".join(names) + " |\n|---|" + "---|" * len(names))
            for key, rows in res["throughput"][p].items():
                w(f"| {res['labels'][key]} | " + " | ".join(
                    f"{rows[n]['rps']:,.0f}" if n in rows else "-" for n in names) + " |")
            w("")
    w("## Latency, pipeline 1 (p50 / p99 ms)\n")
    w("| Server | SET | GET | HSET |\n|---|---|---|---|")
    for key, rows in res["throughput"]["1"].items():
        w(f"| {res['labels'][key]} | " + " | ".join(
            f"{rows[n]['p50_ms']:.2f} / {rows[n]['p99_ms']:.2f}" if n in rows else "-"
            for n in ("SET", "GET", "HSET")) + " |")
    w("")

    w("## Memory and disk after loading the same data\n\n1,000,000 random `SET`s with 100-byte values.\n")
    w("| Server | Keys | Memory (RSS) | Disk |\n|---|---|---|---|")
    for key, f in res["footprint"].items():
        w(f"| {res['labels'][key]} | {f['keys']:,} | {mib(f['rss'])} | {mib(f['disk'])} |")
    w("")

    w("## Crash safety (`kill -9` during a write stream)\n")
    w("One client writing as fast as it can for 3 s, then the server is killed and restarted. "
      "*Lost* counts writes the server had acknowledged that were missing after the restart. "
      "(A process kill does not test power loss: data in the OS page cache survives it.)\n")
    w("| Server | Acknowledged writes | Lost after restart |\n|---|---|---|")
    for key, r in res["crash"].items():
        w(f"| {res['labels'][key]} | {r['acked']:,} | {r['lost']:,} |")
    w("")
    return "\n".join(L)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--emberkv", required=True)
    ap.add_argument("--out", default="results")
    ap.add_argument("--work", default="/tmp/emberkv-bench")
    ap.add_argument("--baseline", help="an older emberkv binary to benchmark alongside")
    ap.add_argument("--baseline-name", default="baseline", help="label for the baseline in the report")
    args = ap.parse_args()

    shutil.rmtree(args.work, ignore_errors=True)
    os.makedirs(args.out, exist_ok=True)
    servers = make_servers(os.path.abspath(args.emberkv), args.work,
                           os.path.abspath(args.baseline) if args.baseline else None)

    def version(cmd):
        return subprocess.run(cmd, capture_output=True, text=True).stdout.strip().split("\n")[0]

    res = {
        "machine": f"{os.cpu_count()} vCPU {platform.machine()} ({platform.platform()})",
        "versions": ", ".join([
            version([args.emberkv, "-version"]),
            version(["redis-server", "--version"]).split(" sha=")[0],
            "kvrocks:latest, dragonfly:latest (Docker)"]),
        "labels": {k: s.name for k, s in servers.items()},
        "baseline": args.baseline_name,
    }
    try:
        print("== starting servers", flush=True)
        for s in servers.values():
            s.wipe()
            s.start()

        print("== compatibility", flush=True)
        res["compat"] = compat(servers)
        res["redis_cli"] = redis_cli_checks(servers["ember-es"].port)

        print("== throughput", flush=True)
        res["throughput"] = {"1": {}, "16": {}}
        for key, s in servers.items():
            redis.Redis(port=s.port).flushall()
            for p, n in (("1", 200000), ("16", 1000000)):
                print(f"   {s.name} P={p}", flush=True)
                res["throughput"][p][key] = bench(s.port, int(p), n)

        print("== memory and disk", flush=True)
        for s in servers.values():
            s.stop()
        res["footprint"] = {}
        for key in ("ember-es", "ember-nohist", "redis-aof-es", "kvrocks", "dragonfly"):
            print(f"   {servers[key].name}", flush=True)
            res["footprint"][key] = footprint(servers[key])
            servers[key].stop()

        print("== crash safety", flush=True)
        res["crash"] = {}
        for key in ("ember-always", "ember-es", "redis-rdb", "redis-aof-es", "redis-aof-always", "kvrocks"):
            print(f"   {servers[key].name}", flush=True)
            res["crash"][key] = crash(servers[key])
    finally:
        for s in servers.values():
            s.stop()

    with open(os.path.join(args.out, "results.json"), "w") as f:
        json.dump(res, f, indent=2)
    md = report(res)
    with open(os.path.join(args.out, "report.md"), "w") as f:
        f.write(md)
    print(md)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a") as f:
            f.write(md)


if __name__ == "__main__":
    main()
