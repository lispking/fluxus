use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Counter for accumulating values
#[derive(Debug, Default)]
pub struct Counter {
    value: AtomicU64,
}

impl Counter {
    pub fn new() -> Self {
        Self {
            value: AtomicU64::new(0),
        }
    }

    pub fn increment(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add(&self, value: u64) {
        self.value.fetch_add(value, Ordering::Relaxed);
    }

    pub fn value(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

/// Gauge for tracking current value
#[derive(Debug, Default)]
pub struct Gauge {
    value: AtomicI64,
}

impl Gauge {
    pub fn new() -> Self {
        Self {
            value: AtomicI64::new(0),
        }
    }

    pub fn set(&self, value: i64) {
        self.value.store(value, Ordering::Relaxed);
    }

    pub fn value(&self) -> i64 {
        self.value.load(Ordering::Relaxed)
    }
}

/// Timer for measuring durations
#[derive(Debug)]
pub struct Timer {
    start: Instant,
    duration_counter: Counter,
    count_counter: Counter,
}

impl Default for Timer {
    fn default() -> Self {
        Self::new()
    }
}

impl Timer {
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
            duration_counter: Counter::new(),
            count_counter: Counter::new(),
        }
    }

    pub fn start(&mut self) {
        self.start = Instant::now();
    }

    pub fn stop(&mut self) {
        let duration = self.start.elapsed();
        self.duration_counter.add(duration.as_micros() as u64);
        self.count_counter.increment();
    }

    /// Record a duration directly
    pub fn record(&mut self, duration: Duration) {
        self.duration_counter.add(duration.as_micros() as u64);
        self.count_counter.increment();
    }

    pub fn average_duration_micros(&self) -> u64 {
        let total = self.duration_counter.value();
        let count = self.count_counter.value();
        if count == 0 { 0 } else { total / count }
    }
}

/// Metrics collection for pipeline monitoring
#[derive(Debug, Default)]
pub struct Metrics {
    counters: HashMap<String, Arc<Counter>>,
    gauges: HashMap<String, Arc<Gauge>>,
    timers: HashMap<String, Arc<Timer>>,
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn counter(&mut self, name: &str) -> Arc<Counter> {
        self.counters
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(Counter::new()))
            .clone()
    }

    pub fn gauge(&mut self, name: &str) -> Arc<Gauge> {
        self.gauges
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(Gauge::new()))
            .clone()
    }

    pub fn timer(&mut self, name: &str) -> Arc<Timer> {
        self.timers
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(Timer::new()))
            .clone()
    }

    pub fn snapshot(&self) -> HashMap<String, MetricValue> {
        let mut snapshot = HashMap::new();

        for (name, counter) in &self.counters {
            snapshot.insert(name.clone(), MetricValue::Counter(counter.value()));
        }

        for (name, gauge) in &self.gauges {
            snapshot.insert(name.clone(), MetricValue::Gauge(gauge.value()));
        }

        for (name, timer) in &self.timers {
            snapshot.insert(
                name.clone(),
                MetricValue::Timer {
                    avg_micros: timer.average_duration_micros(),
                    count: timer.count_counter.value(),
                },
            );
        }

        snapshot
    }
}

#[derive(Debug, Clone)]
pub enum MetricValue {
    Counter(u64),
    Gauge(i64),
    Timer { avg_micros: u64, count: u64 },
}
