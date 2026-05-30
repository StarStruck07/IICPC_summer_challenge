"""
correctness_probe.py — validates price-time priority and fill accuracy.

This is SEPARATE from the load test. The swarm measures latency/throughput and
can only check that responses are well-formed; it cannot verify *correct* fills
because thousands of concurrent unordered orders have no deterministic ground
truth. This probe fixes that: it resets the book, sends a known, ordered
sequence, and asserts every response matches a Python reference orderbook.

It compares two things per step:
  • status   (accepted / partial / filled / rejected / cancelled / not_found)
  • fills     aggregated as {price: total_qty}  (proves the right qty filled at
              the right price — i.e. price priority + fill accuracy)
Time priority (FIFO at equal price) is checked behaviourally: after a partial
sweep, the order that *should* have been consumed first must be gone while the
later one still rests.

Order IDs differ between the target and the reference, so the probe never
compares IDs directly — it tracks, per step, the id each side returned and uses
the target's own id when issuing a cancel.

Dependencies: none (standard library only).

Usage:
  python correctness_probe.py                          # probe http://localhost:9100
  python correctness_probe.py --target http://host:port
  python correctness_probe.py --selftest               # reference vs reference (no HTTP)

Exit code is non-zero if any scenario fails (handy for CI / gating submissions).

NOTE: the target must support POST /reset (clear the book) and behave
deterministically for the duration of the probe. Flag this as a contract item
for contestant submissions — or run each probe against a freshly-spawned
container instead of relying on /reset.
"""

import sys
import json
import argparse
import urllib.request
import urllib.error


# ──────────────────────────────────────────────────────────────────────────
# Reference orderbook — the source of truth for "correct".
# Tiny books, so linear scans are clearer than heaps. Semantics mirror the
# matching-engine contract exactly.
# ──────────────────────────────────────────────────────────────────────────
class RefBook:
    def __init__(self):
        self.bids = []   # each: {price, qty, oid, live, seq}
        self.asks = []
        self._oid = 0
        self._seq = 0

    def submit(self, order):
        t = order["type"]

        if t == "cancel":
            for book in (self.bids, self.asks):
                for o in book:
                    if o["oid"] == order["order_id"] and o["live"] and o["qty"] > 0:
                        o["live"] = False
                        return o["oid"], "cancelled", []
            return 0, "not_found", []

        side, qty, price = order["side"], order["qty"], order.get("price")
        self._oid += 1
        oid = self._oid

        if side == "buy":
            book = self.asks
            candidates = sorted(
                [o for o in book if o["live"] and o["qty"] > 0],
                key=lambda o: (o["price"], o["seq"]),          # cheapest ask, then FIFO
            )
            crosses = lambda p: price is None or p <= price
        else:
            book = self.bids
            candidates = sorted(
                [o for o in book if o["live"] and o["qty"] > 0],
                key=lambda o: (-o["price"], o["seq"]),         # highest bid, then FIFO
            )
            crosses = lambda p: price is None or p >= price

        remaining = qty
        fills = []
        for o in candidates:
            if remaining <= 0 or not crosses(o["price"]):
                break
            traded = min(remaining, o["qty"])
            o["qty"] -= traded
            if o["qty"] <= 0:
                o["live"] = False
            remaining -= traded
            fills.append((o["price"], traded))

        filled = qty - remaining

        if t == "market":
            if filled == 0:
                return oid, "rejected", fills
            return oid, ("filled" if remaining == 0 else "partial"), fills

        # limit: rest the remainder
        if remaining > 0:
            self._seq += 1
            entry = {"price": price, "qty": remaining, "oid": oid,
                     "live": True, "seq": self._seq}
            (self.bids if side == "buy" else self.asks).append(entry)

        if filled == 0:
            return oid, "accepted", fills
        return oid, ("filled" if remaining == 0 else "partial"), fills


# ──────────────────────────────────────────────────────────────────────────
# Targets: a thing that can be reset and accept orders. Either a real HTTP
# engine or a RefBook (for --selftest).
# ──────────────────────────────────────────────────────────────────────────
class HttpTarget:
    def __init__(self, base_url):
        self.base = base_url.rstrip("/")

    def _post(self, path, payload):
        data = json.dumps(payload).encode()
        req = urllib.request.Request(self.base + path, data=data,
                                     headers={"Content-Type": "application/json"},
                                     method="POST")
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read().decode())

    def reset(self):
        try:
            self._post("/reset", {})
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise RuntimeError("target has no POST /reset — cannot guarantee a "
                                   "clean book between scenarios")
            raise

    def order(self, payload):
        return self._post("/order", payload)


class RefTarget:
    """A RefBook dressed up as a target, for offline self-testing."""
    def __init__(self):
        self.book = RefBook()

    def reset(self):
        self.book = RefBook()

    def order(self, payload):
        oid, status, fills = self.book.submit(payload)
        return {"order_id": oid, "status": status,
                "fills": [{"price": p, "qty": q} for p, q in fills]}


# ──────────────────────────────────────────────────────────────────────────
# Comparison helpers
# ──────────────────────────────────────────────────────────────────────────
def agg_pairs(pairs):
    d = {}
    for p, q in pairs:
        d[round(float(p), 2)] = d.get(round(float(p), 2), 0) + int(q)
    return d


def agg_target(fills):
    return agg_pairs([(f["price"], f["qty"]) for f in (fills or [])])


# Step constructors
def place(order, label):   return ("place", order, label)
def cancel(ref_idx, label): return ("cancel", ref_idx, label)
def limit(side, price, qty): return {"type": "limit", "side": side, "price": price, "qty": qty}
def market(side, qty):       return {"type": "market", "side": side, "qty": qty}


SCENARIOS = [
    {
        "name": "Price priority — better price fills first regardless of arrival",
        "steps": [
            place(limit("sell", 101, 5), "rest worse ask @101"),
            place(limit("sell", 100, 5), "rest better ask @100 (later arrival)"),
            place(market("buy", 5),      "market buy must take @100, not @101"),
        ],
    },
    {
        "name": "Time priority — FIFO at equal price",
        "steps": [
            place(limit("sell", 100, 5), "ask A @100"),
            place(limit("sell", 100, 5), "ask B @100 (later)"),
            place(market("buy", 5),      "consumes exactly one level @100"),
            cancel(0, "cancel A -> not_found (A should be the one consumed)"),
            cancel(1, "cancel B -> cancelled (B should still rest)"),
        ],
    },
    {
        "name": "Multi-level sweep — partial fill across price levels",
        "steps": [
            place(limit("sell", 100, 5), "ask @100 x5"),
            place(limit("sell", 101, 5), "ask @101 x5"),
            place(market("buy", 8),      "sweep 5@100 + 3@101, status=filled"),
        ],
    },
    {
        "name": "Market order with no liquidity — must reject, not phantom-fill",
        "steps": [
            place(market("buy", 5), "empty book -> rejected, no fills"),
        ],
    },
    {
        "name": "Cancel prevents fill",
        "steps": [
            place(limit("sell", 100, 5), "ask A @100"),
            cancel(0, "cancel A -> cancelled"),
            place(market("buy", 5), "no liquidity now -> rejected"),
        ],
    },
    {
        "name": "Non-crossing limit rests instead of filling",
        "steps": [
            place(limit("buy", 99, 5),  "bid @99 rests -> accepted"),
            place(limit("sell", 101, 5), "ask @101 rests -> accepted"),
            place(limit("buy", 100, 3),  "buy @100 doesn't cross @101 -> accepted"),
        ],
    },
]


def run_scenario(target, scenario):
    target.reset()
    expect = RefBook()
    tgt_oid, exp_oid = {}, {}
    rows = []

    for i, step in enumerate(scenario["steps"]):
        kind = step[0]
        if kind == "place":
            order, label = step[1], step[2]
            actual = target.order(order)
            e_oid, e_status, e_fills = expect.submit(order)
            tgt_oid[i], exp_oid[i] = actual.get("order_id"), e_oid
        else:  # cancel
            j, label = step[1], step[2]
            actual = target.order({"type": "cancel", "order_id": tgt_oid[j]})
            e_oid, e_status, e_fills = expect.submit(
                {"type": "cancel", "order_id": exp_oid[j]})

        exp_resp = (e_status, agg_pairs(e_fills))
        act_resp = (actual.get("status"), agg_target(actual.get("fills")))
        rows.append((label, exp_resp == act_resp, exp_resp, act_resp))

    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", default="http://localhost:9100")
    ap.add_argument("--selftest", action="store_true",
                    help="run reference against itself (no HTTP) to validate the harness")
    args = ap.parse_args()

    if args.selftest:
        make_target = RefTarget
        header = "SELFTEST (reference vs reference)"
    else:
        make_target = lambda: HttpTarget(args.target)
        header = f"TARGET  {args.target}"

    print("═" * 64)
    print(f"  CORRECTNESS PROBE — {header}")
    print("═" * 64)

    passed = 0
    for scenario in SCENARIOS:
        try:
            target = make_target()
            rows = run_scenario(target, scenario)
        except (urllib.error.URLError, RuntimeError) as e:
            print(f"\n  ✗ ERROR  {scenario['name']}")
            print(f"           {e}")
            continue

        ok = all(r[1] for r in rows)
        passed += 1 if ok else 0
        mark = "✓ PASS" if ok else "✗ FAIL"
        print(f"\n  {mark}  {scenario['name']}")
        for label, step_ok, exp, act in rows:
            tick = "  ok " if step_ok else " >>> "
            print(f"    {tick}{label}")
            if not step_ok:
                print(f"          expected: status={exp[0]} fills={exp[1]}")
                print(f"          got     : status={act[0]} fills={act[1]}")

    total = len(SCENARIOS)
    score = 100.0 * passed / total
    print("\n" + "═" * 64)
    print(f"  SCORE: {passed}/{total} properties passed  ({score:.0f}%)")
    print("═" * 64)

    sys.exit(0 if passed == total else 1)


if __name__ == "__main__":
    main()