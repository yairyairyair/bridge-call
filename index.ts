import http from "node:http";
import { readFile } from "node:fs/promises";
import { WebSocketServer, type WebSocket } from "ws";
import { gateway, streamText, stepCountIs, tool, type ModelMessage } from "ai";
import { z } from "zod";
import {
    DialClient,
    parseDialMessage,
    serializeServerMessage,
    verifyDialSignature,
    type DialServerMessage,
    type TranscriptItem,
} from "@getdial/sdk";
import { randomUUID } from "node:crypto";

const DIAL_API_KEY = process.env.DIAL_API_KEY;
if (!DIAL_API_KEY) throw new Error("DIAL_API_KEY is required");
const dial = new DialClient({ apiKey: DIAL_API_KEY });

const PORT = Number(process.env.PORT || 8080);
const SIGNING_SECRET = process.env.DIAL_SIGNING_SECRET;
if (!SIGNING_SECRET) throw new Error("DIAL_SIGNING_SECRET is required");

const model = gateway('google/gemini-2.5-flash');


// Used until `call_connected` arrives with Dial's per-call instruction (the
// system_prompt — your outbound/inbound instruction plus Dial's general context
// like the current time and the voice's gender).
const DEFAULT_PROMPT = `You are a Lydia, a friendly, concise voice agent on a phone call. Keep replies short and natural.
Your goal is to help the user's request and you are NOT talking to the user but you are talking to the business that the user is calling.
You are basically a bridge between the user and the business, the user a deaf person so he gives you a task to call for him and you are doing that for him.
If you need any input from the user use the ask_user tool.
Always answer to the user in the same language as spoken by the user or the business.
`;

// Appended to the active system prompt so the model knows it can hang up.
const END_CALL_HINT =
    "When the conversation is finished or the caller wants to hang up, call the end_call tool " +
    "with a brief, natural farewell instead of replying with text.";

// Function tools live *inside* this server. The Dial protocol abstracts tools
// away — there's no tool channel on the wire — so when the model calls end_call
// we simply send a `response` with `end_call: true`, which tells Dial to hang
// up after the farewell is spoken. The tool has no `execute`, so the SDK surfaces
// the call to us (as a `tool-call` stream part) instead of running a tool loop.
//
// `ask_user` *does* have an execute: it asks the (deaf) user a question through
// the browser chat and blocks until they type an answer back. Tools are built
// per call so the execute closure knows which call's UI to talk to.
function makeTools(callId: string) {
    return {
        ask_user: tool({
            description:
                "Ask the user a question. Use when you need to know more about the user's needs or preferences.",
            inputSchema: z.object({
                question: z.string().describe("The question to ask the user."),
            }),
            outputSchema: z.object({
                answer: z.string().describe("The answer to the question."),
            }),
            execute: async ({ question }) => {
                const answer = await askUserViaFrontend(callId, question);
                return {
                    answer: `The answer to the question: ${answer}`,
                };
            },
        }),
        end_call: tool({
            description:
                "End the phone call. Use when the task is complete, the caller says goodbye, or the conversation has naturally concluded.",
            inputSchema: z.object({
                farewell: z.string().describe("A short, natural goodbye to say before hanging up."),
            }),
        }),
    };
}

const server = http.createServer();
const wss = new WebSocketServer({ noServer: true });

// ---------------------------------------------------------------------------
// Web UI + JSON API
//
// The same HTTP server that Dial upgrades to a WebSocket also serves the
// browser-facing app for our (deaf) users: a page to type a phone number and
// a task, a list of available "from" numbers, an endpoint to place the call,
// and an endpoint to poll a call's live status + transcript.
// ---------------------------------------------------------------------------

function sendJson(res: http.ServerResponse, status: number, body: unknown): void {
    const payload = JSON.stringify(body);
    res.writeHead(status, { "Content-Type": "application/json" });
    res.end(payload);
}

// ---------------------------------------------------------------------------
// Live transcript fan-out (server -> browser)
//
// Dial streams `transcript_update` frames to us over the call WebSocket. We
// re-broadcast each one to any browsers watching that call via Server-Sent
// Events, so the (deaf) user reads the conversation as it happens. The latest
// full transcript is cached so a browser that connects mid-call gets caught up
// immediately.
// ---------------------------------------------------------------------------

const sseClients = new Map<string, Set<http.ServerResponse>>();
const latestTranscript = new Map<string, TranscriptItem[]>();
const endedCalls = new Set<string>();

// Questions the AI asked that are still waiting for the user to type an answer.
// Keyed by a per-question id; `resolve` unblocks the `ask_user` tool's execute.
type PendingQuestion = { callId: string; question: string; resolve: (answer: string) => void };
const pendingQuestions = new Map<string, PendingQuestion>();

// Push a question to the browser and block until the user answers it (or the
// call ends, in which case we resolve with an empty answer so the model loop
// doesn't hang).
function askUserViaFrontend(callId: string, question: string): Promise<string> {
    const questionId = randomUUID();
    return new Promise<string>((resolve) => {
        pendingQuestions.set(questionId, { callId, question, resolve });
        broadcast(callId, "question", { questionId, question });
    });
}

function sseSend(res: http.ServerResponse, event: string, data: unknown): void {
    res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
}

function broadcast(callId: string, event: string, data: unknown): void {
    const clients = sseClients.get(callId);
    if (!clients) return;
    for (const res of clients) sseSend(res, event, data);
}

function publishTranscript(callId: string, transcript: TranscriptItem[]): void {
    latestTranscript.set(callId, transcript);
    broadcast(callId, "transcript", { transcript });
}

function publishEnded(callId: string): void {
    endedCalls.add(callId);
    // Unblock any unanswered questions so the model loop can settle.
    for (const [id, pending] of pendingQuestions) {
        if (pending.callId === callId) {
            pending.resolve("");
            pendingQuestions.delete(id);
        }
    }
    broadcast(callId, "ended", {});
    // Give late subscribers a short grace window, then forget the call.
    setTimeout(() => {
        latestTranscript.delete(callId);
        endedCalls.delete(callId);
    }, 60_000);
}

function readBody(req: http.IncomingMessage): Promise<string> {
    return new Promise((resolve, reject) => {
        let data = "";
        req.on("data", (chunk) => {
            data += chunk;
            if (data.length > 1_000_000) {
                reject(new Error("Request body too large"));
                req.destroy();
            }
        });
        req.on("end", () => resolve(data));
        req.on("error", reject);
    });
}

server.on("request", async (req, res) => {
    const url = new URL(req.url ?? "/", "http://localhost");
    const path = url.pathname;

    try {
        // Serve the single-page app.
        if (req.method === "GET" && (path === "/" || path === "/index.html")) {
            const html = await readFile(new URL("./public/index.html", import.meta.url), "utf8");
            res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
            res.end(html);
            return;
        }

        // List the account's phone numbers to use as the caller ID.
        if (req.method === "GET" && path === "/api/numbers") {
            const numbers = await dial.listNumbers();
            sendJson(res, 200, numbers.map((n) => ({ id: n.id, number: n.number, nickname: n.nickname })));
            return;
        }

        // Place an outbound call carrying the user's request as the instruction.
        if (req.method === "POST" && path === "/api/call") {
            const body = JSON.parse((await readBody(req)) || "{}") as {
                to?: string;
                task?: string;
                fromNumberId?: string;
                language?: string;
            };

            const to = body.to?.trim();
            const task = body.task?.trim();
            if (!to || !task) {
                sendJson(res, 400, { error: "Both a phone number and a task are required." });
                return;
            }

            // Default the caller ID to the first available number when not chosen.
            let fromNumberId = body.fromNumberId?.trim();
            if (!fromNumberId) {
                const numbers = await dial.listNumbers();
                fromNumberId = numbers[0]?.id;
            }
            if (!fromNumberId) {
                sendJson(res, 400, { error: "No phone number available to call from." });
                return;
            }

            const call = await dial.makeCall({
                to,
                fromNumberId,
                outboundInstruction: task,
                language: body.language?.trim() || undefined,
            });
            sendJson(res, 200, { id: call.id, status: call.status });
            return;
        }

        // Deliver the user's typed answer to a pending `ask_user` question.
        if (req.method === "POST" && path === "/api/answer") {
            const body = JSON.parse((await readBody(req)) || "{}") as {
                questionId?: string;
                answer?: string;
            };
            const questionId = body.questionId?.trim();
            const pending = questionId ? pendingQuestions.get(questionId) : undefined;
            if (!pending) {
                sendJson(res, 404, { error: "Question not found or already answered." });
                return;
            }
            pendingQuestions.delete(questionId!);
            pending.resolve((body.answer ?? "").toString());
            sendJson(res, 200, { ok: true });
            return;
        }

        // Live transcript stream for a call (Server-Sent Events).
        if (req.method === "GET" && path.startsWith("/api/stream/")) {
            const callId = decodeURIComponent(path.slice("/api/stream/".length));
            res.writeHead(200, {
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                Connection: "keep-alive",
            });
            res.write(": connected\n\n");

            let set = sseClients.get(callId);
            if (!set) {
                set = new Set();
                sseClients.set(callId, set);
            }
            set.add(res);

            // Catch a mid-call subscriber up to the current state.
            const current = latestTranscript.get(callId);
            if (current) sseSend(res, "transcript", { transcript: current });
            for (const [id, pending] of pendingQuestions) {
                if (pending.callId === callId) sseSend(res, "question", { questionId: id, question: pending.question });
            }
            if (endedCalls.has(callId)) sseSend(res, "ended", {});

            // Heartbeat so proxies don't drop the idle connection.
            const heartbeat = setInterval(() => res.write(": ping\n\n"), 20_000);

            req.on("close", () => {
                clearInterval(heartbeat);
                set?.delete(res);
                if (set && set.size === 0) sseClients.delete(callId);
            });
            return;
        }

        // Poll a call's status + transcript (fallback / final state).
        if (req.method === "GET" && path.startsWith("/api/call/")) {
            const callId = decodeURIComponent(path.slice("/api/call/".length));
            const call = await dial.getCall(callId);
            sendJson(res, 200, {
                id: call.id,
                status: call.status,
                duration: call.duration,
                transcript: call.transcript,
                terminationType: call.terminationType,
            });
            return;
        }

        res.writeHead(404, { "Content-Type": "text/plain" });
        res.end("Not found");
    } catch (err) {
        console.error("[http]", err);
        sendJson(res, 500, { error: err instanceof Error ? err.message : "Internal server error" });
    }
});

server.on("upgrade", (req, socket, head) => {
    const url = new URL(req.url ?? "", "http://localhost");
    const callId = url.pathname.split("/").filter(Boolean).pop();
    const signature = req.headers["x-dial-signature"];

    // Authorize at request time: confirm the connection is genuinely from Dial.
    if (!callId || !signature || !verifyDialSignature(SIGNING_SECRET, String(signature), callId)) {
        socket.write("HTTP/1.1 401 Unauthorized\r\n\r\n");
        socket.destroy();
        return;
    }
    wss.handleUpgrade(req, socket, head, (ws) => handleCall(ws, callId));
});

/** Map the Dial transcript to AI SDK model messages under the given system prompt. */
function toMessages(instruction: string, transcript: TranscriptItem[]): ModelMessage[] {
    return [
        { role: "system", content: `${instruction}\n\n${END_CALL_HINT}` },
        ...transcript.map((t): ModelMessage =>
            t.role === "agent" ? { role: "assistant", content: t.content } : { role: "user", content: t.content },
        ),
    ];
}

function handleCall(ws: WebSocket, callId: string): void {
    console.log(`[${callId}] connected`);
    let inFlight: AbortController | null = null; // the current model stream
    let systemInstruction = DEFAULT_PROMPT; // replaced by call_connected.instruction
    const tools = makeTools(callId); // ask_user routes back to this call's browser

    const cancelInFlight = (): void => {
        if (inFlight) {
            inFlight.abort();
            inFlight = null;
        }
    };

    async function answer(responseId: number, transcript: TranscriptItem[]): Promise<void> {
        // Interrupt focus: a new turn supersedes any response still streaming.
        cancelInFlight();
        const controller = new AbortController();
        inFlight = controller;
        console.log('answer', transcript);
        try {
            const result = streamText({
                model,
                messages: toMessages(systemInstruction, transcript),
                tools,
                toolChoice: "auto",
                // Let the agent continue after ask_user: ask -> read the typed
                // answer -> resume speaking to the business in the same turn.
                stopWhen: stepCountIs(5),
                abortSignal: controller.signal,
                providerOptions: {
                    google: {
                        thinkingConfig: {
                            thinkingBudget: 0,
                        }
                    },
                },
                onFinish: (result) => {
                    console.log('result', result);
                },
            });

            let endCallFarewell: string | null = null;
            for await (const part of result.fullStream) {
                if (controller.signal.aborted) return; // superseded by a newer turn
                if (part.type === "text-delta") {
                    ws.send(serializeServerMessage({ type: "response", response_id: responseId, content: part.text, content_complete: false }));
                } else if (part.type === "tool-call" && part.toolName === "end_call") {
                    // `input` is already validated/parsed against the tool's schema.
                    const farewell = (part.input as { farewell?: string }).farewell;
                    endCallFarewell = farewell?.trim() ? farewell : "Thanks for calling — goodbye!";
                } else if (part.type === "error") {
                    throw part.error;
                }
            }
            if (controller.signal.aborted) return;

            if (endCallFarewell !== null) {
                // Map the model's tool call to the protocol's end_call: speak the
                // farewell, then Dial hangs up.
                ws.send(serializeServerMessage({ type: "response", response_id: responseId, content: endCallFarewell, content_complete: true, end_call: true }));
            } else {
                ws.send(serializeServerMessage({ type: "response", response_id: responseId, content: "", content_complete: true }));
            }
        } catch (err) {
            if (controller.signal.aborted) return; // expected on interrupt
            console.error(`[${callId}] model error`, err);
        } finally {
            if (inFlight === controller) inFlight = null;
        }
    }

    ws.on("message", (raw) => {
        let msg: DialServerMessage;
        try {
            msg = parseDialMessage(raw.toString());
        } catch {
            return; // ignore frames we don't recognize
        }
        // Print every message arriving from Dial (already debounced/deduped by Dial).
        console.log(`[${callId}] <- ${msg.type}`, JSON.stringify(msg));
        switch (msg.type) {
            case "call_connected":
                // Sent on connect (and reconnect). Use Dial's per-call instruction
                // (system_prompt + general context) as the system prompt; falls back to
                // DEFAULT_PROMPT when absent.
                if (msg.instruction) systemInstruction = DEFAULT_PROMPT + `User's request: ${msg.instruction}`;
                break;
            case "ping_pong":
                // Keepalive: echo it straight back so Dial knows we're alive.
                ws.send(serializeServerMessage({ type: "ping_pong", timestamp: msg.timestamp }));
                break;
            case "transcript_update":
                // Live transcript — forward to any browser watching this call.
                publishTranscript(callId, msg.transcript);
                break;
            case "response_required":
            case "reminder_required":
                // These also carry the latest transcript; keep watchers in sync.
                publishTranscript(callId, msg.transcript);
                void answer(msg.response_id, msg.transcript);
                break;
        }
    });

    ws.on("close", () => {
        cancelInFlight();
        publishEnded(callId);
        console.log(`[${callId}] closed`);
    });
}

server.listen(PORT, () => {
    console.log(`Self-Hosted playbook server listening on :${PORT}`);
    console.log(`Web app:  http://localhost:${PORT}`);
});
