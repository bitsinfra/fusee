wit_bindgen::generate!({ path: "../wit", world: "bench-workflow", generate_all });

struct Component;
export!(Component);

impl exports::fusee::bench::workflow::Guest for Component {
    fn recovery(run: String, mut value: u32) -> Result<u32, ()> {
        for i in 0..3 { value = fusee::bench::activity::crash_effect(&run, i, value)?; }
        Ok(value)
    }

    fn sleeper(slot: u32, deadline_ms: u64) -> Result<u32, ()> {
        use obelisk::types::time::{Datetime, ScheduleAt};
        let at = Datetime { seconds: deadline_ms / 1000, nanoseconds: ((deadline_ms % 1000) * 1_000_000) as u32 };
        obelisk::workflow::workflow_support::sleep(ScheduleAt::At(at), Some("wake"))?;
        obelisk::log::log::info(&format!("awake {slot}"));
        Ok(slot)
    }

    fn await_signal(challenge: String) -> Result<String, ()> {
        use obelisk::types::function::Function;
        use obelisk::workflow::{workflow_dynamic_support, workflow_support};
        let set = workflow_support::join_set_create();
        let function = Function { interface_name: "fusee:bench/signals".into(), function_name: "receive".into() };
        let params = serde_json::to_string(&[&challenge]).map_err(|_| ())?;
        let child = workflow_dynamic_support::submit_json(&set, &function, &params).map_err(|_| ())?;
        obelisk::log::log::info(&format!("waiting {}", child.id));
        let response = workflow_support::join_next(&set).map_err(|_| ())?.map_err(|_| ())?.ok_or(())?;
        obelisk::log::log::info(&format!("resumed {challenge}"));
        let value: String = serde_json::from_str(&response).map_err(|_| ())?;
        if value != challenge { return Err(()); }
        Ok(value)
    }

    fn chain(count: u32) -> Result<Vec<(u32, u64, u64)>, ()> {
        let mut value = 0;
        let mut marks = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let sample = fusee::bench::activity::noop(value)?;
            value = sample.0;
            marks.push(sample);
        }
        Ok(marks)
    }
    fn matrix(kind: String, count: u32) -> Result<Vec<(u32, u64, u64)>, ()> {
        use obelisk::types::function::Function;
        use obelisk::workflow::{workflow_dynamic_support, workflow_support};
        if kind == "open" { return Self::chain(count); }
        let mut marks = Vec::with_capacity(count as usize);
        if kind == "fanout" && count == 100 {
            let group = workflow_support::join_set_create();
            let function = Function { interface_name: "fusee:bench/activity".into(), function_name: "noop".into() };
            for value in 0..count {
                workflow_dynamic_support::submit_json(&group, &function, &format!("[{value}]")).map_err(|_| ())?;
            }
            for _ in 0..count {
                let result = workflow_support::join_next(&group).map_err(|_| ())?
                    .map_err(|_| ())?.ok_or(())?;
                marks.push(serde_json::from_str(&result).map_err(|_| ())?);
            }
            return Ok(marks);
        }
        let ms = match kind.as_str() { "service-1"=>1, "service-50"=>50, _=>return Err(()) };
        let mut value = 0;
        for _ in 0..count {
            let mark = fusee::bench::activity::service_increment(value, ms)?;
            value = mark.0;
            marks.push(mark);
        }
        Ok(marks)
    }

}
