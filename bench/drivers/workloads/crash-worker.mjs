// The controller and external service survive a worker SIGKILL. The engine's
// SDK alone owns admission, recovery and result lookup for each adapter.
const witness=process.env.FUSEE_WITNESS;
export async function effect(input,seq,value){
  const res=await fetch(witness,{method:"POST",headers:{"content-type":"application/json"},
    body:JSON.stringify({run:input.run,seq,value,generation:Number(process.env.FUSEE_GENERATION)})});
  if(!res.ok)throw Error("external service HTTP "+res.status);
  return (await res.json()).value;
}

export async function runChain(input, step){
  let value=input.value;
  for(let i=0;i<input.steps;i++)value=await step(input,i,value);
  return value;
}

export function serveRecovery(start, attach){
  const watching=new Set();
  process.on("message",async command=>{
    const {run}=command;
    if(watching.has(run))return;
    watching.add(run);
    try{
      let receipt=command.receipt;
      if(command.kind==="start"){
        receipt=await start(command.input);
        process.send({kind:"ack",run,receipt});
      }
      const value=await attach(receipt,command.input);
      process.send({kind:"done",run,value});
    }catch(error){
      // The caller correlates failures to a run. Avoid SDK request headers.
      process.send({kind:"error",run,message:String(error.message??error)});
    }finally{watching.delete(run);}
  });
  process.send({kind:"ready"});
}
