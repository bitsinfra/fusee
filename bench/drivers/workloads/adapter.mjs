// All resident workers use the same observer. The generator owns arrivals,
// deadlines and completion timestamps. Each run has a unique activity trace.
import http from "node:http";
import net from "node:net";
import { randomUUID } from "node:crypto";
import { createInterface } from "node:readline";

const traces = new Map();
let mockUrl;
export async function activity(input, ordinal, value) {
  const trace = traces.get(input.attempt);
  if (!trace) throw new Error("activity belongs to an unknown attempt");
  const start = process.hrtime.bigint();
  if (input.workload === "service-1" || input.workload === "service-50") {
    const response = await fetch(mockUrl + "/" + input.workload);
    if (!response.ok || await response.text() !== "ok") throw new Error("mock service failed");
  }
  const result = value + 1;
  const end = process.hrtime.bigint();
  if (trace.bodies[ordinal]) throw new Error("activity executed more than once in a fault-free trial");
  trace.bodies[ordinal] = [String(start), String(end)];
  return result;
}

// Only the first observed resumption counts. SDK replay can re-enter the
// handler; the random challenge binds the delivered signal to this run.
export function waiting(input) {
  traces.get(input.attempt)?.ready.resolve();
}

export function resumed(input, value) {
  const trace = traces.get(input.attempt);
  if (!trace?.signalSent || value !== input.challenge) throw Error("incorrect external signal");
  if (!trace.bodies[0]) {
    const stamp = String(process.hrtime.bigint());
    trace.bodies[0] = [stamp, stamp];
  }
  return 1;
}

export async function externalSignal(input, result, send) {
  const trace = traces.get(input.attempt);
  // Propagate an early workflow failure even if it never reaches the wait.
  let timer;
  try {
    await Promise.race([trace.ready.promise, result.then(() => {throw Error("workflow completed before signal")}),
      new Promise((_,reject) => {timer = setTimeout(() => reject(Error("signal wait registration timed out")),60_000);})]);
    trace.signalSent = String(process.hrtime.bigint());
    await send(input.challenge);
    return await result;
  } finally { clearTimeout(timer); }
}

export async function serveAdapter(engine, execute) {
  const mock = http.createServer((req, res) => {
    const duration = {"/service-1":1,"/service-50":50}[req.url];
    if (!duration) { res.writeHead(400).end(); return; }
    setTimeout(() => res.end("ok"), duration);
  });
  await new Promise((resolve,reject) => mock.once("error",reject).listen(0,"127.0.0.1",resolve));
  mockUrl = "http://127.0.0.1:" + mock.address().port;
  const serve = net.createServer(socket => {
    socket.setNoDelay(true);
    socket.on("error", () => socket.destroy());
    const lines = createInterface({input:socket});
    lines.on("line", line => {
      void (async () => {
        const req = JSON.parse(line);
        if (req.action === "clock") return {clock_ns:String(process.hrtime.bigint()), engine};
        if (req.action !== "run" || !Number.isSafeInteger(req.steps) || req.steps < 1) throw Error("invalid benchmark request");
        const attempt = randomUUID();
        const trace = {bodies:[], ready:Promise.withResolvers()};
        traces.set(attempt,trace);
        try {
          const input = {...req,attempt,challenge:attempt};
          const value = await execute(input);
          const expected = req.workload === "fanout" ? 5050 : req.steps;
          if (value !== expected || trace.bodies.length !== req.steps || Array.from(trace.bodies).some(mark => !mark)) {
            throw Error(`incorrect workflow result or missing activities: ${value}/${expected}, ${trace.bodies.length}/${req.steps}`);
          }
          if (req.workload==="signal" && !trace.signalSent) throw Error("missing external signal observation");
          return {id:req.id,attempt,value,body_ns:trace.bodies,signal_sent_ns:trace.signalSent};
        } finally { traces.delete(attempt); }
      })().then(reply => socket.write(JSON.stringify(reply) + "\n"),
        error => socket.write(JSON.stringify({error:error.message ?? String(error)}) + "\n"));
    });
  });
  await new Promise((resolve,reject) => serve.once("error",reject).listen(Number(process.env.FUSEE_ADAPTER_PORT ?? 29733),"127.0.0.1",resolve));
  console.error(`ready: ${engine.name} external benchmark port ${serve.address().port}`);
}
