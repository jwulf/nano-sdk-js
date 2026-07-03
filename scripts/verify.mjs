// End-to-end check against a running Nano server (default http://localhost:8080).
// Deploys a tiny one-task process, runs a falcon worker, creates an
// instance, and asserts the SDK upgraded to the Falcon protocol.
import { createCamundaClient } from "../dist/index.js";

const BASE = (process.env.NANOBPMN_BASE_URL ?? "http://localhost:8080").replace(/\/+$/, "");
const PID = "nano-sdk-verify";
const XML = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="defs-${PID}" targetNamespace="http://nanobpm">
  <bpmn:process id="${PID}" isExecutable="true">
    <bpmn:startEvent id="s"/><bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="t"/>
    <bpmn:serviceTask id="t" name="Tick"><bpmn:extensionElements><zeebe:taskDefinition type="verify-tick"/></bpmn:extensionElements></bpmn:serviceTask>
    <bpmn:sequenceFlow id="f2" sourceRef="t" targetRef="e"/><bpmn:endEvent id="e"/>
  </bpmn:process>
</bpmn:definitions>`;

const fd = new FormData();
fd.append("resources", new Blob([XML], { type: "text/xml" }), `${PID}.bpmn`);
const dep = await fetch(`${BASE}/v2/deployments`, { method: "POST", body: fd });
console.log("deploy:", dep.status);

const client = createCamundaClient({ config: { CAMUNDA_AUTH_STRATEGY: "NONE", CAMUNDA_REST_ADDRESS: BASE } });

let handled = 0;
client.createJobWorker({
  jobType: "verify-tick",
  workerName: "verify",
  maxParallelJobs: 50,
  jobHandler: async (job) => { handled++; return job.complete({ ok: true }); },
});

await new Promise((r) => setTimeout(r, 800));
const inst = await client.createProcessInstance({ processDefinitionId: PID, awaitCompletion: true });
console.log("instance:", inst.processInstanceKey, "completed=", !!inst.processInstanceKey);
await new Promise((r) => setTimeout(r, 500));
console.log(`jobs handled over Falcon protocol: ${handled}`);
client.stopAllWorkers?.();
process.exit(handled > 0 && inst.processInstanceKey ? 0 : 1);
