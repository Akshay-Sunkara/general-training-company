import { IngressClient } from "livekit-server-sdk";

const ic = new IngressClient(
  process.env.LIVEKIT_URL!,
  process.env.LIVEKIT_API_KEY!,
  process.env.LIVEKIT_API_SECRET!,
);

console.log(`LIVEKIT_URL=${process.env.LIVEKIT_URL}`);
const ingresses = await ic.listIngress({});
console.log(`ingress objects on this project: ${ingresses.length}`);
for (const i of ingresses) {
  console.log(`  ${i.ingressId} room=${i.roomName} status=${i.state?.status}`);
}
console.log("creds OK — project reachable and ingress API enabled");
