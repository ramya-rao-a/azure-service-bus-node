import {
  delay, ReceiveMode, Namespace
} from "../../lib";
import * as dotenv from "dotenv";
dotenv.config();

const str = process.env.SERVICEBUS_CONNECTION_STRING || "";
const path = "ramya-session-queue";

let ns: Namespace;
async function main(): Promise<void> {
  ns = Namespace.createFromConnectionString(str);
  const client = ns.createQueueClient(path, { receiveMode: ReceiveMode.peekLock });
  const messageSession = await client.acceptSession();

  const result = await messageSession.receiveBatch(10);
  console.log(">>>>>>> Number of received messages: %d.", result.length);
  console.log(">>>>>>> List of received messages: %O.", result.map(x => x.body.toString()));
  await delay(5000);
  await messageSession.close();
}

main().then(() => {
  console.log(">>>> Calling close....");
  return ns.close();
}).catch((err) => {
  console.log("error: ", err);
});
