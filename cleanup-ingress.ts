import { IngressClient } from "livekit-server-sdk";

const client = new IngressClient(
  process.env.LIVEKIT_URL!,
  process.env.LIVEKIT_API_KEY!,
  process.env.LIVEKIT_API_SECRET!,
);

const list = await client.listIngress({});
console.log(`found ${list.length} ingress objects`);
let deleted = 0;
for (const i of list) {
  try {
    await client.deleteIngress(i.ingressId);
    deleted++;
    console.log(`deleted ${i.ingressId} room=${i.roomName} name=${i.name}`);
  } catch (e) {
    console.error(`failed ${i.ingressId}:`, (e as Error).message);
  }
}
console.log(`done — deleted ${deleted}/${list.length}`);
