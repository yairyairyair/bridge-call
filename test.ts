import { DialClient } from "@getdial/sdk";

const dial = new DialClient({ apiKey: process.env.DIAL_API_KEY! });

const numbers = await dial.listNumbers();
console.log(numbers);

const call = await dial.makeCall({
    fromNumberId: 'cmq9uck47001i15pdlk9iww64',
    to: '+972527470084',
    outboundInstruction: 'Schedule an appointment at that doctors office for me.',
    language: 'en-US',
})

console.log(call);
