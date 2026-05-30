"""
Mock Matching Engine — the "fake target" for bot-fleet development.

Stands in for a contestant's submitted orderbook/matching engine so the bot
fleet, telemetry, and leaderboard can be developed end-to-end before the
sandbox pipeline is wired up. It implements a small but *real* price-time
priority orderbook, so the fills it returns are correct and can be used as a
reference by a correctness probe later.

────────────────────────────────────────────────────────────────────────────
SUBMISSION API CONTRACT  (share this with Person 1 + contestants)
────────────────────────────────────────────────────────────────────────────
POST /order
  Limit order : {"type": "limit",  "side": "buy"|"sell", "price": float, "qty": int}
  Market order: {"type": "market", "side": "buy"|"sell", "qty": int}
  Cancel      : {"type": "cancel", "order_id": int}

Response (200):
  {
    "order_id": int,                 # id assigned to this order (0 for cancels)
    "status": "accepted"             # resting on the book (limit, fully unfilled)
            | "partial"              # partially filled, remainder resting/dropped
            | "filled"               # fully filled
            | "rejected"             # bad request / market order with no liquidity
            | "cancelled"            # cancel succeeded
            | "not_found",           # cancel target didn't exist
    "fills": [{"price": float, "qty": int}, ...],   # trades this order generated
    "ts": float                      # server receive time (epoch seconds)
  }

GET /book     -> current top-of-book snapshot (for debugging)
GET /health   -> {"status": "ok"}
POST /reset   -> clears the book (use between correctness scenarios)
────────────────────────────────────────────────────────────────────────────

Run:
  uvicorn mock_engine:app --host 0.0.0.0 --port 9100
or:
  python mock_engine.py
"""

import time
import heapq
import itertools
from threading import Lock

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import List, Literal, Optional

app = FastAPI(title="Mock Matching Engine")

# ──────────────────────────────────────────────────────────────────────────
# Request / response models
# ──────────────────────────────────────────────────────────────────────────
class Order(BaseModel):
    type: Literal["limit", "market", "cancel"]
    side: Optional[Literal["buy", "sell"]] = None
    price: Optional[float] = None
    qty: Optional[int] = Field(default=None, ge=1)
    order_id: Optional[int] = None  # only for cancels


class Fill(BaseModel):
    price: float
    qty: int


class OrderResponse(BaseModel):
    order_id: int
    status: str
    fills: List[Fill]
    ts: float


# ──────────────────────────────────────────────────────────────────────────
# Orderbook with price-time priority
#
# Bids: max-heap on price (we negate), then FIFO by sequence.
# Asks: min-heap on price, then FIFO by sequence.
# Each resting order is [price, seq, order_id, qty, live]; `live` lets us
# lazily skip cancelled/filled entries instead of removing from the heap.
# ──────────────────────────────────────────────────────────────────────────
class OrderBook:
    def __init__(self):
        self.bids = []                 # heap of [-price, seq, oid, qty, live]
        self.asks = []                 # heap of [ price, seq, oid, qty, live]
        self.resting = {}              # order_id -> entry (for cancels)
        self._seq = itertools.count()
        self._oid = itertools.count(1)
        self.lock = Lock()

    def _next_oid(self):
        return next(self._oid)

    def _clean_top(self, heap):
        # Drop dead/zero entries from the top so the best level is valid.
        while heap and (not heap[0][4] or heap[0][3] <= 0):
            heapq.heappop(heap)

    def submit(self, order: Order):
        if order.type == "cancel":
            return self._cancel(order.order_id)

        if order.side not in ("buy", "sell") or not order.qty:
            return 0, "rejected", []

        if order.type == "market":
            return self._match_market(order)

        if order.type == "limit":
            if order.price is None:
                return 0, "rejected", []
            return self._match_limit(order)

        return 0, "rejected", []

    def _cancel(self, oid):
        entry = self.resting.get(oid)
        if not entry or not entry[4] or entry[3] <= 0:
            return 0, "not_found", []
        entry[4] = False
        self.resting.pop(oid, None)
        return 0, "cancelled", []

    def _match_market(self, order):
        oid = self._next_oid()
        remaining = order.qty
        book = self.asks if order.side == "buy" else self.bids
        fills = self._take(book, remaining, limit_price=None, side=order.side)
        filled = sum(f.qty for f in fills)
        if filled == 0:
            return oid, "rejected", []          # no liquidity
        status = "filled" if filled == order.qty else "partial"
        return oid, status, fills               # market remainder is dropped

    def _match_limit(self, order):
        oid = self._next_oid()
        opposite = self.asks if order.side == "buy" else self.bids
        fills = self._take(opposite, order.qty, limit_price=order.price, side=order.side)
        filled = sum(f.qty for f in fills)
        remaining = order.qty - filled

        if remaining > 0:
            # Rest the remainder on our own side of the book.
            seq = next(self._seq)
            if order.side == "buy":
                entry = [-order.price, seq, oid, remaining, True]
                heapq.heappush(self.bids, entry)
            else:
                entry = [order.price, seq, oid, remaining, True]
                heapq.heappush(self.asks, entry)
            self.resting[oid] = entry

        if filled == 0:
            return oid, "accepted", fills
        return oid, ("filled" if remaining == 0 else "partial"), fills

    def _take(self, heap, qty, limit_price, side):
        """Consume liquidity from `heap` up to qty, respecting limit_price."""
        fills = []
        while qty > 0 and heap:
            self._clean_top(heap)
            if not heap:
                break
            top = heap[0]
            book_price = -top[0] if heap is self.bids else top[0]

            if limit_price is not None:
                # buy crosses asks priced <= limit; sell crosses bids priced >= limit
                if side == "buy" and book_price > limit_price:
                    break
                if side == "sell" and book_price < limit_price:
                    break

            avail = top[3]
            traded = min(avail, qty)
            top[3] -= traded
            qty -= traded
            fills.append(Fill(price=book_price, qty=traded))

            if top[3] <= 0:
                top[4] = False
                heapq.heappop(heap)
                self.resting.pop(top[2], None)
        return fills

    def snapshot(self):
        self._clean_top(self.bids)
        self._clean_top(self.asks)
        best_bid = -self.bids[0][0] if self.bids else None
        best_ask = self.asks[0][0] if self.asks else None
        return {
            "best_bid": best_bid,
            "best_ask": best_ask,
            "bid_depth": sum(e[3] for e in self.bids if e[4]),
            "ask_depth": sum(e[3] for e in self.asks if e[4]),
        }


book = OrderBook()


# ──────────────────────────────────────────────────────────────────────────
# Endpoints
# ──────────────────────────────────────────────────────────────────────────
@app.post("/order", response_model=OrderResponse)
def place_order(order: Order):
    ts = time.time()
    with book.lock:
        oid, status, fills = book.submit(order)
    return OrderResponse(order_id=oid, status=status, fills=fills, ts=ts)


@app.get("/book")
def get_book():
    with book.lock:
        return book.snapshot()


@app.post("/reset")
def reset():
    global book
    with book.lock:
        book = OrderBook()
    return {"status": "reset"}


@app.get("/health")
def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=9100)