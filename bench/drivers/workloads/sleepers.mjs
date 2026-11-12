// The controller owns admission windows and all per-workflow measurements.
// Workers emit a marker on entering the SDK timer and one on returning.
import { createConnection } from "node:net";
import { createInterface } from "node:readline";
import { once } from "node:events";

const sock=createConnection(Number(process.env.FUSEE_SLEEP_PORT),"127.0.0.1");
const campaign=process.env.FUSEE_SLEEP_CAMPAIGN;
const send=message=>sock.write(JSON.stringify(message)+"\n");
export function entered(input){if(input.campaign===campaign)send({event:"entered",i:input.i,at:Date.now()});}
export function throttled(input,delay){send({event:"throttled",i:input.i,backoff_ms:delay});}
export function woke(input){
  const at=Date.now();
  if(input.campaign===campaign)send({event:"woke",i:input.i,at});
  return input.i;
}
export async function serveSleep(submit,readResult){
  if(sock.connecting)await once(sock,"connect");
  send({event:"ready",pid:process.pid});
  const rl=createInterface({input:sock});
  rl.on("line",line=>{
    const cmd=JSON.parse(line);
    (async()=>{
      if(cmd.action==="start"){
        const receipt=await submit(cmd);
        if(!receipt)throw Error("missing durable start receipt");
        send({event:"ack",i:cmd.i,receipt});
      }else if(cmd.action==="result"){
        const value=await readResult(cmd.receipt,cmd.i);
        if(value!==cmd.i)throw Error("workflow returned the wrong ID");
        send({event:"done",i:cmd.i,value});
      }else throw Error("unknown sleep command");
    })().catch(e=>send({event:"error",i:cmd.i,message:String(e.message??e)}));
  });
}
