import { DialClient } from "@getdial/sdk";

const dial = new DialClient({ apiKey: process.env.DIAL_API_KEY! });

const numbers = await dial.listNumbers();
console.log(numbers);

const call = await dial.makeCall({
    fromNumberId: 'cmq9uck47001i15pdlk9iww64',
    to: '+972527470084',
    outboundInstruction: 'You are a Yogas Tsedef, a friendly, concise voice agent on a phone call. Keep replies short and natural.',
    language: 'he-IL',
})

console.log(call);
