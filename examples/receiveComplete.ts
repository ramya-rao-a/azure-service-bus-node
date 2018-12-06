import { OnMessage, OnError, MessagingError, delay, ServiceBusMessage, ReceiveMode, Namespace } from "../lib";
import * as dotenv from "dotenv";
dotenv.config();

const str = process.env.SERVICEBUS_CONNECTION_STRING || "";
const path = process.env.QUEUE_NAME || "";

let ns: Namespace;
async function main(): Promise<void> {
  ns = Namespace.createFromConnectionString(str);
  const client = ns.createQueueClient(path, { receiveMode: ReceiveMode.peekLock });
  const onMessage: OnMessage = async (brokeredMessage: ServiceBusMessage) => {
    console.log(">>>>> Message Body:", brokeredMessage.body ? brokeredMessage.body.toString() : undefined);
    await brokeredMessage.complete();
  };
  const onError: OnError = (err: MessagingError | Error) => {
    console.log(">>>>> Error occurred: ", err);
  };
  const rcvHandler = client.receive(onMessage, onError, { autoComplete: false });
  await delay(5000);
  await rcvHandler.stop();
}

main().then(() => {
  console.log(">>>> Calling close....");
  return ns.close();
}).catch((err) => {
  console.log("error: ", err);
});
