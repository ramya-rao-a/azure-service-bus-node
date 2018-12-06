import { Namespace } from "../lib";
import * as dotenv from "dotenv";
dotenv.config();

const str = process.env.SERVICEBUS_CONNECTION_STRING || "";
const path = process.env.QUEUE_NAME || "";
const numberOfMessages: number = 10;

let ns: Namespace;
async function main(): Promise<void> {
  ns = Namespace.createFromConnectionString(str);
  const batchMsgs = [];
  const client = ns.createQueueClient(path);
  for (let i = 0; i < numberOfMessages; i++) {
    await client.send({ body: `Hello sb world!! ${i + 1}` });
    console.log(">>>>>> Sent message number: %d", i + 1);

    batchMsgs.push({ body: `Hello sb batch world!! ${i + 1}` });
  }

  await client.sendBatch(batchMsgs);
  console.log('Batch sent');
}

main().then(() => {
  console.log(">>>> Calling close....");
  return ns.close();
}).catch((err) => {
  console.log("error: ", err);
  return ns.close();
});
