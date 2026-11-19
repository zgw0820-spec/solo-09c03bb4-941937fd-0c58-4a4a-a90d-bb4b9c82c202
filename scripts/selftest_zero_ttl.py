"""零秒负缓存的进程内确定性单测（不依赖网络/数据库，直接驱动 CachingResolver）。

用法：
    python -m scripts.selftest_zero_ttl

覆盖：
1. TTL=0 的 NXDOMAIN 不进入缓存；立刻重查会重新回源（回源次数递增）。
2. 补回记录后，首个新查询拿到 A 应答。
3. 零秒 NXDOMAIN 的同名单飞仍然只回源一次；leader 与等待者都拿到
   该次答复，且报文 DNS ID 各自对应自己的查询。
4. 等待者拿的是“自己等的这一航班”的答复：即使下一次回源（已恢复 A 记录）
   已经完成并写入缓存，等待者收到的仍是本航班的 NXDOMAIN。
"""
import threading
import time

from dnslib import QTYPE, RCODE, RR, SOA, A, DNSRecord

from app.resolver import CachingResolver

PASS, FAIL = 0, 0


def check(label: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  \033[32m[PASS]\033[0m {label}")
    else:
        FAIL += 1
        print(f"  \033[31m[FAIL]\033[0m {label} {detail}")


def make_query(name: str, qid: int) -> DNSRecord:
    q = DNSRecord.question(name, "A")
    q.header.id = qid
    return q


def make_nxdomain(name: str, neg_ttl: int) -> DNSRecord:
    resp = DNSRecord.question(name, "A").reply()
    resp.header.id = 0
    resp.header.set_rcode(RCODE.NXDOMAIN)
    resp.add_auth(
        RR(name, QTYPE.SOA, ttl=neg_ttl,
           rdata=SOA("ns.internal.", "hostmaster.internal.",
                     (2026010101, 7200, 3600, 1209600, neg_ttl)))
    )
    return resp


def make_a(name: str, ip: str, ttl: int) -> DNSRecord:
    resp = DNSRecord.question(name, "A").reply()
    resp.header.id = 0
    resp.add_answer(RR(name, QTYPE.A, ttl=ttl, rdata=A(ip)))
    return resp


def answers_of(reply: DNSRecord) -> list:
    return [str(rr.rdata) for rr in reply.rr if rr.rtype == QTYPE.A]


def wait_until(deadline_s, desc, fn):
    end = time.time() + deadline_s
    while time.time() < end:
        if fn():
            return True
        time.sleep(0.01)
    raise AssertionError(f"等待超时：{desc}")


def main() -> None:
    NAME = "app2.internal"

    print("== Z1. TTL=0 的 NXDOMAIN 不进缓存，立刻重查重新回源 ==")
    r = CachingResolver()
    r._fetch_upstream = lambda qname, qtype: make_nxdomain(qname, 0)  # type: ignore[method-assign]
    rep1 = r.resolve(make_query(NAME, 111))
    check("首次查询回 NXDOMAIN 且 DNS ID 对应",
          rep1 is not None and rep1.header.rcode == RCODE.NXDOMAIN and int(rep1.header.id) == 111)
    check("回源次数=1", r.attempts.get(NAME) == 1, detail=str(r.attempts))
    check("零秒 NXDOMAIN 未进入缓存", NAME not in r.cache, detail=str(list(r.cache)))
    rep2 = r.resolve(make_query(NAME, 222))
    check("立刻重查仍回 NXDOMAIN 且 ID 对应",
          rep2 is not None and rep2.header.rcode == RCODE.NXDOMAIN and int(rep2.header.id) == 222)
    check("立刻重查重新回源（次数=2）", r.attempts.get(NAME) == 2, detail=str(r.attempts))

    print("== Z2. 补回记录后首个新查询拿到 A 应答 ==")
    r._fetch_upstream = lambda qname, qtype: make_a(qname, "10.0.0.99", 30)  # type: ignore[method-assign]
    rep3 = r.resolve(make_query(NAME, 333))
    check("补回后查询回 NOERROR + 新 A 记录",
          rep3 is not None and rep3.header.rcode == RCODE.NOERROR
          and answers_of(rep3) == ["10.0.0.99"], detail=str(answers_of(rep3) if rep3 else None))
    check("回源次数=3", r.attempts.get(NAME) == 3, detail=str(r.attempts))
    check("正缓存已建立", NAME in r.cache and r.cache[NAME].kind == "positive")

    print("== Z3. 零秒 NXDOMAIN 单飞：只回源一次，等待者拿本航班答复 ==")
    r2 = CachingResolver()
    gate = threading.Event()
    calls = []

    def fake_fetch(qname, qtype):
        n = len(calls)
        calls.append((qname, qtype))
        if n == 0:
            gate.wait(5)  # 第一班回源受控，便于让等待者先挂上
            return make_nxdomain(qname, 0)
        return make_a(qname, "10.0.0.77", 30)  # 第二班：记录已补回

    r2._fetch_upstream = fake_fetch  # type: ignore[method-assign]
    results: dict = {}

    def client(qid: int):
        results[qid] = r2.resolve(make_query(NAME, qid))

    t_leader = threading.Thread(target=client, args=(1001,))
    t_leader.start()
    wait_until(3, "leader 进入 inflight", lambda: NAME in r2.inflight)
    waiters = [threading.Thread(target=client, args=(1002 + i,)) for i in range(3)]
    for t in waiters:
        t.start()
    wait_until(3, "等待者挂上 flight", lambda: r2.inflight[NAME].waiters == 3)
    gate.set()
    t_leader.join(5)
    # 第一班结束：零秒 NXDOMAIN 不进缓存
    check("零秒 NXDOMAIN 仍未进缓存", NAME not in r2.cache, detail=str(list(r2.cache)))
    check("leader 拿到 NXDOMAIN 且 ID=1001",
          results.get(1001) is not None and results[1001].header.rcode == RCODE.NXDOMAIN
          and int(results[1001].header.id) == 1001)

    # 第二班：记录已补回，同步完成并写入正缓存 —— 此时部分等待者可能尚未醒来
    rep_next = r2.resolve(make_query(NAME, 2000))
    check("下一班查询拿到补回后的 A 记录",
          rep_next is not None and answers_of(rep_next) == ["10.0.0.77"])
    check("此时缓存已是正缓存", r2.cache[NAME].kind == "positive")

    for t in waiters:
        t.join(5)
    check("等待者全部拿到答复", all(results.get(1002 + i) is not None for i in range(3)))
    check("等待者拿到的是本航班的 NXDOMAIN（不是下一班的 A 记录）",
          all(results.get(1002 + i) is not None
              and results[1002 + i].header.rcode == RCODE.NXDOMAIN for i in range(3)),
          detail=str({k: (v.header.rcode if v else None) for k, v in results.items()}))
    check("等待者答复的 DNS ID 各自对应自己的查询",
          all(int(results[1002 + i].header.id) == 1002 + i for i in range(3)))
    check("两个班次共回源两次（第一班单飞只回源一次）",
          len(calls) == 2, detail=str(calls))

    print(f"\n结果：{PASS} 通过 / {FAIL} 失败")
    raise SystemExit(1 if FAIL else 0)


if __name__ == "__main__":
    main()
