//! Scheduled arrivals to a resident SDK worker over one full-duplex socket.
//! The writer never waits for completions in open loop. A nine-probe clock
//! calibration places activity timestamps on the generator's monotonic clock.
use super::{Opts, hist, machine, summary};
use fusee::Error;
use serde_json::{Value, json};
use std::collections::HashSet;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

fn invalid(message: &str) -> Error {
    Error::failed(format!("external benchmark: {message}"))
}

fn send(socket: &mut TcpStream, request: Value) -> Result<(), Error> {
    let mut message = request.to_string().into_bytes();
    message.push(b'\n');
    socket.write_all(&message)?;
    Ok(())
}

fn receive(reader: &mut BufReader<TcpStream>) -> Result<Value, Error> {
    let mut line = String::new();
    if reader.read_line(&mut line)? == 0 {
        return Err(invalid("worker disconnected"));
    }
    let value: Value = serde_json::from_str(&line).map_err(|e| invalid(&e.to_string()))?;
    if let Some(error) = value.get("error") {
        return Err(invalid(&error.to_string()));
    }
    Ok(value)
}

fn number(v: &Value) -> Result<i128, Error> {
    v.as_str()
        .and_then(|n| n.parse::<i128>().ok())
        .ok_or_else(|| invalid("missing clock timestamp"))
}

fn pace(at: Instant) {
    let slack = Duration::from_micros(75);
    if let Some(rest) = at
        .saturating_duration_since(Instant::now())
        .checked_sub(slack)
    {
        std::thread::sleep(rest);
    }
    while Instant::now() < at {
        std::hint::spin_loop();
    }
}

pub fn run(kind: &str, o: &Opts) -> Result<Value, Error> {
    let sequential = match kind {
        "fanout" | "service-1" | "service-50" | "signal" => true,
        "open" => false,
        _ => return Err(invalid("unknown workload")),
    };
    let n = if sequential {
        u64::from(o.trials)
    } else {
        (o.rate * o.seconds) as u64
    };
    if !(1..=10_000_000).contains(&n) {
        return Err(invalid("arrival count out of range"));
    }
    let steps = match kind {
        "fanout" => 100,
        "signal" => 1,
        _ => o.steps,
    };
    let mut socket = TcpStream::connect(
        o.server
            .as_deref()
            .ok_or_else(|| invalid("missing --server host:port"))?,
    )?;
    socket.set_nodelay(true)?;
    socket.set_read_timeout(Some(Duration::from_secs(600)))?;
    let mut reader = BufReader::new(socket.try_clone()?);
    let epoch = Instant::now();
    let mut best = (u128::MAX, 0i128);
    let mut metadata = Value::Null;
    for _ in 0..9 {
        let before = epoch.elapsed().as_nanos();
        send(&mut socket, json!({"action":"clock"}))?;
        let reply = receive(&mut reader)?;
        let after = epoch.elapsed().as_nanos();
        let server = number(&reply["clock_ns"])?;
        if after - before < best.0 {
            best = (after - before, server - ((before + after) / 2) as i128);
        }
        metadata = reply["engine"].clone();
    }
    if metadata["version"].as_str().is_none() || metadata["durability"].as_str().is_none() {
        return Err(invalid("missing engine version or durability"));
    }
    let request = |id: u64| json!({"action":"run","id":id,"workload":kind,"steps":steps});
    send(&mut socket, request(n))?;
    let warm = receive(&mut reader)?;
    if warm["id"].as_u64() != Some(n) {
        return Err(invalid("missing warmup"));
    }
    let origin = Instant::now() + Duration::from_millis(25);
    let origin_ns = origin.duration_since(epoch).as_nanos() as i128;
    let offset = |i: u64| Duration::from_secs_f64(i as f64 / o.rate);
    let producer = if sequential {
        None
    } else {
        let mut writer = socket.try_clone()?;
        let kind = kind.to_string();
        let rate = o.rate;
        Some(std::thread::spawn(move || -> Result<(), Error> {
            for i in 0..n {
                pace(origin + Duration::from_secs_f64(i as f64 / rate));
                send(
                    &mut writer,
                    json!({"action":"run","id":i,"workload":kind,"steps":steps}),
                )?;
            }
            Ok(())
        }))
    };
    let mut workflow = hist();
    let mut first = hist();
    let mut gaps = hist();
    let mut signal = hist();
    let mut seen = HashSet::new();
    let mut samples = Vec::new();
    for i in 0..n {
        let sent = if sequential {
            if i == 0 {
                pace(origin);
            }
            let t = epoch.elapsed().as_nanos() as i128;
            send(&mut socket, request(i))?;
            t
        } else {
            0
        };
        let row = receive(&mut reader)?;
        let done = epoch.elapsed().as_nanos() as i128;
        let id = row["id"]
            .as_u64()
            .ok_or_else(|| invalid("missing result ID"))?;
        if id >= n || !seen.insert(id) {
            return Err(invalid("duplicate or foreign result"));
        }
        let scheduled = if sequential {
            sent
        } else {
            origin_ns + offset(id).as_nanos() as i128
        };
        let marks = row["body_ns"]
            .as_array()
            .ok_or_else(|| invalid("missing activity observations"))?;
        if marks.len() != steps as usize {
            return Err(invalid("wrong activity count"));
        }
        let mut earliest = i128::MAX;
        let mut previous = None;
        for mark in marks {
            let begin = number(&mark[0])?;
            let end = number(&mark[1])?;
            if end < begin {
                return Err(invalid("negative activity time"));
            }
            earliest = earliest.min(begin);
            if let Some(prev) = previous
                && kind != "fanout"
            {
                let gap: i128 = begin - prev;
                if gap < 0 {
                    return Err(invalid("dependent activities overlap"));
                }
                gaps.record((gap as u64 / 1000).max(1))
                    .map_err(|e| invalid(&e.to_string()))?;
            }
            previous = Some(end);
        }
        if kind == "signal" {
            let elapsed = earliest - number(&row["signal_sent_ns"])?;
            if elapsed < 0 {
                return Err(invalid("workflow resumed before signal"));
            }
            signal
                .record((elapsed as u64 / 1000).max(1))
                .map_err(|e| invalid(&e.to_string()))?;
        }
        let start_to_body = earliest - best.1 - scheduled;
        if start_to_body < -(best.0 as i128) {
            return Err(invalid("first activity precedes the scheduled arrival"));
        }
        first
            .record((start_to_body.max(0) as u64 / 1000).max(1))
            .map_err(|e| invalid(&e.to_string()))?;
        workflow
            .record(((done - scheduled) as u64 / 1000).max(1))
            .map_err(|e| invalid(&e.to_string()))?;
        samples.push(json!({"scheduled_ns":scheduled.to_string(),"completed_ns":done.to_string(),"observation":row}));
    }
    if let Some(p) = producer {
        p.join().map_err(|_| invalid("arrival thread failed"))??;
    }
    let elapsed = if sequential {
        origin.elapsed().as_secs_f64()
    } else {
        origin.elapsed().as_secs_f64().max(o.seconds)
    };
    let achieved = n as f64 * f64::from(steps) / elapsed;
    let output = json!({"machine":machine(),"engine":metadata,"workload":kind,"load":if sequential {"closed"}else{"open"},
        "offered":n,"completed":seen.len(),"steps_per_workflow":steps,"offered_starts_per_s":if sequential {None}else{Some(o.rate)},
        "achieved_steps_per_s":achieved,"usd_per_million_steps":o.usd_per_hour.map(|p|p*1e6/(3600.0*achieved)),
        "window_s":elapsed,"workflow_latency_from_schedule":summary(&workflow),"start_to_first_step":summary(&first),
        "step_to_step":summary(&gaps),"signal_to_resumed":summary(&signal),"clock_probe_rtt_ns":best.0 as u64,"worker_minus_generator_ns":best.1.to_string(),"samples":samples});
    Ok(output)
}
