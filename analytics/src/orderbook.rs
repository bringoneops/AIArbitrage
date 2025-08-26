use std::collections::BTreeMap;
use std::collections::HashMap;
use canonicalizer::{L2Diff, Snapshot};
use ordered_float::OrderedFloat;
use serde::Deserialize;

/// Best bid/ask ticker update.
#[derive(Debug, Deserialize)]
pub struct BookTicker {
    pub agent: String,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "bp")]
    pub bid_px: String,
    #[serde(rename = "bq")]
    pub bid_qty: String,
    #[serde(rename = "ap")]
    pub ask_px: String,
    #[serde(rename = "aq")]
    pub ask_qty: String,
    #[serde(rename = "ts")]
    pub timestamp: i64,
}

#[derive(Default, Debug)]
pub struct OrderBook {
    bids: BTreeMap<OrderedFloat<f64>, f64>,
    asks: BTreeMap<OrderedFloat<f64>, f64>,
}

impl OrderBook {
    pub fn apply_snapshot(&mut self, snap: Snapshot) {
        self.bids.clear();
        self.asks.clear();
        for [p, q] in snap.bids {
            if let (Ok(p), Ok(q)) = (p.parse::<f64>(), q.parse::<f64>()) {
                self.bids.insert(OrderedFloat(p), q);
            }
        }
        for [p, q] in snap.asks {
            if let (Ok(p), Ok(q)) = (p.parse::<f64>(), q.parse::<f64>()) {
                self.asks.insert(OrderedFloat(p), q);
            }
        }
    }

    pub fn apply_l2diff(&mut self, diff: L2Diff) {
        for [p, q] in diff.bids {
            if let (Ok(p), Ok(q)) = (p.parse::<f64>(), q.parse::<f64>()) {
                let p = OrderedFloat(p);
                if q == 0.0 {
                    self.bids.remove(&p);
                } else {
                    self.bids.insert(p, q);
                }
            }
        }
        for [p, q] in diff.asks {
            if let (Ok(p), Ok(q)) = (p.parse::<f64>(), q.parse::<f64>()) {
                let p = OrderedFloat(p);
                if q == 0.0 {
                    self.asks.remove(&p);
                } else {
                    self.asks.insert(p, q);
                }
            }
        }
    }

    pub fn apply_ticker(&mut self, tick: BookTicker) {
        if let (Ok(p), Ok(q)) = (tick.bid_px.parse::<f64>(), tick.bid_qty.parse::<f64>()) {
            let p = OrderedFloat(p);
            if q == 0.0 {
                self.bids.remove(&p);
            } else {
                self.bids.insert(p, q);
            }
        }
        if let (Ok(p), Ok(q)) = (tick.ask_px.parse::<f64>(), tick.ask_qty.parse::<f64>()) {
            let p = OrderedFloat(p);
            if q == 0.0 {
                self.asks.remove(&p);
            } else {
                self.asks.insert(p, q);
            }
        }
    }

    pub fn best_bid(&self) -> Option<(f64, f64)> {
        self.bids.iter().next_back().map(|(&p, &q)| (p.into_inner(), q))
    }

    pub fn best_ask(&self) -> Option<(f64, f64)> {
        self.asks.iter().next().map(|(&p, &q)| (p.into_inner(), q))
    }
}

/// Store books keyed by symbol.
#[derive(Default)]
pub struct BookStore {
    books: HashMap<String, OrderBook>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum BookEvent {
    #[serde(rename = "snapshot")]
    Snapshot(Snapshot),
    #[serde(rename = "l2_diff")]
    L2Diff(L2Diff),
    #[serde(rename = "book_ticker")]
    BookTicker(BookTicker),
}

impl BookStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn apply_line(&mut self, line: &str) {
        if let Ok(event) = serde_json::from_str::<BookEvent>(line) {
            match event {
                BookEvent::Snapshot(s) => {
                    self.books.entry(s.symbol.clone()).or_default().apply_snapshot(s);
                }
                BookEvent::L2Diff(d) => {
                    self.books.entry(d.symbol.clone()).or_default().apply_l2diff(d);
                }
                BookEvent::BookTicker(t) => {
                    self.books.entry(t.symbol.clone()).or_default().apply_ticker(t);
                }
            }
        }
    }

    pub fn book(&self, symbol: &str) -> Option<&OrderBook> {
        self.books.get(symbol)
    }
}

