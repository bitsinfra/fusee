//! Optional OpenTelemetry instruments. The application configures the
//! providers and exporters. Fusee does not create a network client.
use opentelemetry::{
    KeyValue, global,
    metrics::{Counter, Histogram},
    trace::{Span, Tracer},
};
use std::time::{Duration, SystemTime};

#[derive(Debug)]
pub struct Telemetry {
    records: Counter<u64>,
    steps: Counter<u64>,
    batch_size: Histogram<u64>,
    flush_time: Histogram<u64>,
    gate_time: Histogram<u64>,
    traces: bool,
}

impl Telemetry {
    pub(crate) fn new(traces: bool) -> Self {
        let meter = global::meter("fusee");
        Self {
            records: meter.u64_counter("fusee.log.records").build(),
            steps: meter.u64_counter("fusee.steps.executed").build(),
            batch_size: meter.u64_histogram("fusee.flush.records").build(),
            flush_time: meter
                .u64_histogram("fusee.flush.duration")
                .with_unit("us")
                .build(),
            gate_time: meter
                .u64_histogram("fusee.gate.duration")
                .with_unit("us")
                .build(),
            traces,
        }
    }

    fn label(shard: u32) -> [KeyValue; 1] {
        [KeyValue::new("fusee.shard", i64::from(shard))]
    }

    pub(crate) fn append(&self, shard: u32) {
        self.records.add(1, &Self::label(shard));
    }
    pub(crate) fn step(&self, shard: u32) {
        self.steps.add(1, &Self::label(shard));
    }
    pub(crate) fn batch(&self, shard: u32, size: u32) {
        self.batch_size.record(u64::from(size), &Self::label(shard));
    }

    pub(crate) fn flush(&self, shard: u32, us: u64) {
        self.flush_time.record(us, &Self::label(shard));
        self.span("fusee.log.flush", shard, us);
    }
    pub(crate) fn gate(&self, shard: u32, us: u64) {
        self.gate_time.record(us, &Self::label(shard));
        self.span("fusee.durable_gate.wait", shard, us);
    }

    fn span(&self, name: &'static str, shard: u32, us: u64) {
        if self.traces {
            let end = SystemTime::now();
            let tracer = global::tracer("fusee");
            let mut span = tracer
                .span_builder(name)
                .with_start_time(end.checked_sub(Duration::from_micros(us)).unwrap_or(end))
                .with_attributes(Self::label(shard))
                .start(&tracer);
            span.end_with_timestamp(end);
        }
    }
}
