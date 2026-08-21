// Jarvis Realtime - the real-time conversation engine, built on top of the
// proven streaming foundation.
//
// How a turn works, entirely inside one persistent WebSocket connection
// (no separate HTTP requests per turn, unlike the old Netlify approach):
//   1. Incoming audio is buffered while the caller is speaking (detected by
//      simple amplitude-based voice activity detection - not a proper VAD
//      library, just "is this louder than background noise").
//   2. Once sustained silence follows speech, the buffered audio is sent to
//      Google Speech-to-Text (synchronous REST call, not true streaming
//      transcription - a real but honest limitation for this phase).
//   3. The transcript goes to Claude along with the running conversation
//      history and the task for this call.
//   4. Claude's reply is spoken back via Google TTS, streamed straight into
//      the call.
//   5. If Claude signals the task is done, the call hangs up afterward via
//      SignalWire's REST API.
//
// Known limitations, being upfront about scope: no true word-by-word
// streaming transcription (there's a real, if usually brief, pause after
// the caller stops talking before we start processing), and no
// interruption/barge-in handling - the server ignores incoming audio while
// it's already generating a response. Both are real further-refinement
// work, not part of this phase.

const http = require("http");
const WebSocket = require("ws");

const PORT = process.env.PORT || 3000;
const ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages";
const MODEL = "claude-haiku-4-5-20251001";

// Voice activity detection tuning - almost certainly needs real-world
// adjustment once tested, the same way speechTimeout needed tuning on the
// Netlify side. Amplitude is on a 0-~8000 linear PCM scale after mulaw
// decoding; silenceMsToEndTurn is how long the caller needs to be quiet
// before we treat their turn as finished.
const SILENCE_AMPLITUDE_THRESHOLD = 400;
const SILENCE_MS_TO_END_TURN = 900;
const MS_PER_CHUNK = 20;

// Safety caps - without these, a broken call could loop forever.
const MAX_TURNS = 8;
const MAX_CALL_DURATION_MS = 5 * 60 * 1000; // 5 minutes

const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("Jarvis Realtime server is up.\n");
});

const wss = new WebSocket.Server({ server, path: "/media-stream" });

wss.on("connection", (ws) => {
  console.log("New Media Stream connection opened");

  const state = {
    streamSid: null,
    callSid: null,
    task: "Have a brief, friendly conversation and help with whatever comes up.",
    history: [],
    audioBuffer: [],
    hasSpeech: false,
    silenceMs: 0,
    processing: false,
    turnCount: 0,
    callStartedAt: Date.now(),
    safetyTimer: null
  };

  ws.on("message", async (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch (e) {
      console.error("Failed to parse incoming message", e);
      return;
    }

    switch (msg.event) {
      case "connected":
        console.log("Stream connected", msg);
        break;

      case "start": {
        state.streamSid = msg.start?.streamSid;
        state.callSid = msg.start?.callSid;
        const customTask = msg.start?.customParameters?.task;
        if (customTask) {
          state.task = customTask;
        }
        console.log("Stream started", { streamSid: state.streamSid, callSid: state.callSid, task: state.task });

        // Safety net - if a call somehow runs long without wrapping up
        // (a bug, a confused loop, whatever), don't let it run forever.
        state.safetyTimer = setTimeout(async () => {
          console.warn("Max call duration reached - hanging up", { callSid: state.callSid });
          await sendTelegram(`Ava hung up "${state.task}" after running too long without finishing.`);
          if (state.callSid) await hangUpCall(state.callSid);
        }, MAX_CALL_DURATION_MS);

        // Speak the opening line right away, same as the Netlify version -
        // identify yourself and state the purpose immediately rather than
        // waiting to hear anything first.
        try {
          const opening = await generateOpening(state.task);
          state.history.push({ role: "assistant", content: opening });
          const audio = await generateMulawAudio(opening);
          sendAudioToStream(ws, state.streamSid, audio);
          console.log("Sent opening line", { opening });
        } catch (err) {
          console.error("Failed to generate/send opening line", err);
        }
        break;
      }

      case "media": {
        if (state.processing) break; // half-duplex for now - see limitations note above
        handleIncomingAudio(ws, state, msg.media.payload);
        break;
      }

      case "stop":
        console.log("Stream stopped", { streamSid: state.streamSid });
        if (state.safetyTimer) clearTimeout(state.safetyTimer);
        break;

      default:
        console.log("Unhandled event type", msg.event);
    }
  });

  ws.on("close", () => {
    console.log("Media Stream connection closed");
    if (state.safetyTimer) clearTimeout(state.safetyTimer);
  });

  ws.on("error", (err) => {
    console.error("WebSocket error", err);
  });
});

function handleIncomingAudio(ws, state, base64Payload) {
  const mulawChunk = Buffer.from(base64Payload, "base64");
  state.audioBuffer.push(mulawChunk);

  const amplitude = estimateAmplitude(mulawChunk);
  if (amplitude > SILENCE_AMPLITUDE_THRESHOLD) {
    state.hasSpeech = true;
    state.silenceMs = 0;
  } else if (state.hasSpeech) {
    state.silenceMs += MS_PER_CHUNK;
  }

  if (state.hasSpeech && state.silenceMs >= SILENCE_MS_TO_END_TURN) {
    processTurn(ws, state);
  }
}

async function processTurn(ws, state) {
  state.processing = true;
  const audioToTranscribe = Buffer.concat(state.audioBuffer);
  state.audioBuffer = [];
  state.hasSpeech = false;
  state.silenceMs = 0;

  state.turnCount++;
  if (state.turnCount > MAX_TURNS) {
    console.warn("Max turn count reached - bailing out", { callSid: state.callSid });
    try {
      const audio = await generateMulawAudio("Sorry, I'm having trouble completing this. I'll let Paul know. Have a good day.");
      sendAudioToStream(ws, state.streamSid, audio);
    } catch (err) {
      console.error("Failed to speak bail-out message", err);
    }
    await sendTelegram(`Couldn't complete "${state.task}" - the call went on too long without wrapping up.`);
    if (state.safetyTimer) clearTimeout(state.safetyTimer);
    setTimeout(() => { if (state.callSid) hangUpCall(state.callSid); }, 1500);
    state.processing = false;
    return;
  }

  try {
    const transcript = await transcribeMulaw(audioToTranscribe);
    console.log("Transcribed turn", { transcript });

    if (!transcript || !transcript.trim()) {
      state.processing = false;
      return;
    }

    state.history.push({ role: "user", content: transcript });
    const reply = await generateReply(state);
    console.log("Generated reply", { reply });

    state.history.push({ role: "assistant", content: JSON.stringify(reply) });

    // A menu just came up - press the matching digit(s) via generated DTMF
    // tones, sent as regular audio over the same stream.
    if (reply.pressDigits) {
      const dtmfAudio = generateDtmfSequence(reply.pressDigits);
      sendAudioToStream(ws, state.streamSid, dtmfAudio);
      console.log("Pressed digits", { pressDigits: reply.pressDigits });
    }

    // "waiting" means what we heard was purely transitional (hold message,
    // "please wait") - nothing to say, just keep listening quietly.
    if (reply.say && !reply.waiting) {
      const audio = await generateMulawAudio(reply.say);
      sendAudioToStream(ws, state.streamSid, audio);
    }

    if (reply.done) {
      await sendTelegram(`Done - ${reply.summary}`);
      if (state.safetyTimer) clearTimeout(state.safetyTimer);
      if (state.callSid) {
        // Give the audio a moment to actually finish playing before
        // hanging up.
        setTimeout(() => hangUpCall(state.callSid), 1500);
      }
    }
  } catch (err) {
    console.error("Turn processing failed", err);
  }

  state.processing = false;
}

// Estimates loudness of a chunk of mulaw-encoded audio by decoding to
// linear PCM and averaging the absolute sample values - a simple stand-in
// for proper voice activity detection.
function estimateAmplitude(mulawBuffer) {
  let sum = 0;
  for (let i = 0; i < mulawBuffer.length; i++) {
    sum += Math.abs(mulawDecodeSample(mulawBuffer[i]));
  }
  return mulawBuffer.length ? sum / mulawBuffer.length : 0;
}

// Standard ITU-T G.711 mu-law to linear PCM decode for a single byte.
function mulawDecodeSample(muByte) {
  muByte = ~muByte & 0xff;
  const sign = muByte & 0x80;
  const exponent = (muByte >> 4) & 0x07;
  const mantissa = muByte & 0x0f;
  let sample = ((mantissa << 3) + 0x84) << exponent;
  sample -= 0x84;
  return sign ? -sample : sample;
}

// Standard ITU-T G.711 linear PCM to mu-law encode for a single sample -
// the inverse of the decode above. Used to generate DTMF tones directly
// as mulaw audio, since Media Streams has no separate "press this digit"
// event - digits have to be sent as actual tone audio, the same as a real
// phone would produce.
function mulawEncodeSample(sample) {
  const BIAS = 0x84;
  const CLIP = 32635;

  let sign = 0;
  if (sample < 0) {
    sign = 0x80;
    sample = -sample;
  }
  if (sample > CLIP) sample = CLIP;
  sample += BIAS;

  let exponent = 7;
  for (let expMask = 0x4000; (sample & expMask) === 0 && exponent > 0; exponent--, expMask >>= 1) {}

  const mantissa = (sample >> (exponent + 3)) & 0x0f;
  return ~(sign | (exponent << 4) | mantissa) & 0xff;
}

const DTMF_FREQUENCIES = {
  "1": [697, 1209], "2": [697, 1336], "3": [697, 1477],
  "4": [770, 1209], "5": [770, 1336], "6": [770, 1477],
  "7": [852, 1209], "8": [852, 1336], "9": [852, 1477],
  "*": [941, 1209], "0": [941, 1336], "#": [941, 1477]
};
const MULAW_SILENCE_BYTE = mulawEncodeSample(0);

// Generates a single DTMF tone (a dual-frequency sine wave, same as a real
// phone keypad produces) as raw mulaw audio.
function generateDtmfTone(digit, durationMs, sampleRate) {
  const freqs = DTMF_FREQUENCIES[digit];
  if (!freqs) return Buffer.alloc(0);

  const numSamples = Math.floor((sampleRate * durationMs) / 1000);
  const audio = Buffer.alloc(numSamples);
  const amplitude = 8000;

  for (let i = 0; i < numSamples; i++) {
    const t = i / sampleRate;
    const sample = amplitude * 0.5 * (Math.sin(2 * Math.PI * freqs[0] * t) + Math.sin(2 * Math.PI * freqs[1] * t));
    audio[i] = mulawEncodeSample(Math.round(sample));
  }

  return audio;
}

// Generates a full sequence of DTMF digits with brief silence between each
// tone, matching standard phone dialing timing.
function generateDtmfSequence(digits, toneDurationMs = 200, gapDurationMs = 100, sampleRate = 8000) {
  const gapSamples = Math.floor((sampleRate * gapDurationMs) / 1000);
  const silence = Buffer.alloc(gapSamples, MULAW_SILENCE_BYTE);

  const parts = [];
  for (const digit of digits) {
    if (DTMF_FREQUENCIES[digit]) {
      parts.push(generateDtmfTone(digit, toneDurationMs, sampleRate));
      parts.push(silence);
    }
  }
  return Buffer.concat(parts);
}

async function transcribeMulaw(mulawBuffer) {
  const apiKey = process.env.GOOGLE_TTS_API_KEY;
  if (!apiKey) {
    throw new Error("GOOGLE_TTS_API_KEY not configured");
  }

  const res = await fetch(`https://speech.googleapis.com/v1/speech:recognize?key=${apiKey}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      config: {
        encoding: "MULAW",
        sampleRateHertz: 8000,
        languageCode: "en-US",
        model: "phone_call"
      },
      audio: { content: mulawBuffer.toString("base64") }
    })
  });

  if (!res.ok) {
    const errText = await res.text();
    throw new Error(`Google Speech-to-Text failed: ${errText}`);
  }

  const data = await res.json();
  return (data.results || []).map(r => r.alternatives?.[0]?.transcript || "").join(" ").trim();
}

async function generateOpening(task) {
  const res = await fetch(ANTHROPIC_API_URL, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-api-key": process.env.ANTHROPIC_API_KEY,
      "anthropic-version": "2023-06-01"
    },
    body: JSON.stringify({
      model: MODEL,
      max_tokens: 100,
      system: `You are Paul's AI phone assistant, about to start a call on his behalf to accomplish: "${task}". Your name is Ava. Write ONE short, natural opening line - identify yourself briefly, then state your purpose clearly. You are the caller with a request - never phrase this as offering to help them. Respond with ONLY the line to say, nothing else.`,
      messages: [{ role: "user", content: "Write the opening line." }]
    })
  });

  const data = await res.json();
  const text = data?.content?.find(b => b.type === "text")?.text;
  return (text || `Hi, this is Ava, calling for Paul - ${task}.`).trim();
}

async function generateReply(state) {
  const res = await fetch(ANTHROPIC_API_URL, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-api-key": process.env.ANTHROPIC_API_KEY,
      "anthropic-version": "2023-06-01"
    },
    body: JSON.stringify({
      model: MODEL,
      max_tokens: 150,
      system: `You are Paul's AI assistant, currently on a live phone call to: ${state.task}. Your name is Ava. You are the caller - never ask "what can I help you with," that's their line, not yours.

Sound like a friendly, casual human - use contractions, keep replies short (one short sentence, occasionally two). If this is an order or booking, see it through completely rather than stopping at just getting a price.

Write money and numbers the way you'd actually SAY them out loud, never with symbols - "one ninety-nine" instead of "$1.99".

What you just heard might be: a real reply from a person, an automated phone menu ("press 1 for..."), or a purely transitional message (hold music description, "please wait", a recorded disclaimer) with nothing to actually respond to.

If it's a menu, set pressDigits to the digit(s) that best match what's needed and leave say empty. If it's purely transitional, set waiting true and leave say empty - just keep listening quietly. Otherwise, respond normally.

Respond with ONLY valid JSON, no other text: {"say": "what to say next, or empty string if pressing digits or waiting", "pressDigits": "digit(s) to press if a menu just came up, otherwise empty string", "waiting": true if this was purely transitional with nothing to respond to, otherwise false, "done": true or false, "summary": "one short sentence for Paul summarizing the outcome, only if done is true"}

Set done to true once the task is confirmed complete and "say" contains a brief, warm goodbye.`,
      messages: state.history
        .filter(h => h.role === "user" || h.role === "assistant")
        .map(h => ({ role: h.role, content: h.content }))
    })
  });

  const data = await res.json();
  const text = data?.content?.find(b => b.type === "text")?.text || "{}";

  try {
    const parsed = JSON.parse(text.replace(/```json|```/g, "").trim());
    if (!parsed || (!parsed.say && !parsed.pressDigits && !parsed.waiting)) {
      throw new Error("Missing say/pressDigits/waiting in response");
    }
    return parsed;
  } catch (e) {
    console.error("generateReply parse failed", { text, error: e.message });
    return { say: "Sorry, could you say that again?", pressDigits: "", waiting: false, done: false, summary: "" };
  }
}

async function sendTelegram(message) {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.TELEGRAM_CHAT_ID;
  if (!token || !chatId) {
    console.error("Telegram not configured - cannot send notification", { message });
    return;
  }

  try {
    const res = await fetch(`https://api.telegram.org/bot${token}/sendMessage`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ chat_id: chatId, text: `🤵 Ava: ${message}` })
    });
    if (!res.ok) {
      const errText = await res.text();
      console.error("Telegram send failed", errText);
    }
  } catch (err) {
    console.error("Failed to send Telegram message", err);
  }
}

async function hangUpCall(callSid) {
  const space = process.env.SIGNALWIRE_SPACE_URL;
  const projectId = process.env.SIGNALWIRE_PROJECT_ID;
  const token = process.env.SIGNALWIRE_API_TOKEN;
  if (!space || !projectId || !token) {
    console.error("SignalWire credentials not configured - cannot hang up call");
    return;
  }

  const auth = Buffer.from(`${projectId}:${token}`).toString("base64");
  try {
    const res = await fetch(`https://${space}/api/laml/2010-04-01/Accounts/${projectId}/Calls/${callSid}.json`, {
      method: "POST",
      headers: {
        Authorization: `Basic ${auth}`,
        "Content-Type": "application/x-www-form-urlencoded"
      },
      body: new URLSearchParams({ Status: "completed" })
    });
    console.log("Hung up call", { callSid, status: res.status });
  } catch (err) {
    console.error("Failed to hang up call", err);
  }
}

// Generates speech via Google Cloud Text-to-Speech directly in 8kHz mulaw
// format, which is exactly what SignalWire's Media Streams expects for
// outbound audio - no format conversion needed on our end.
async function generateMulawAudio(text) {
  const apiKey = process.env.GOOGLE_TTS_API_KEY;
  if (!apiKey) {
    throw new Error("GOOGLE_TTS_API_KEY not configured");
  }

  const res = await fetch(`https://texttospeech.googleapis.com/v1/text:synthesize?key=${apiKey}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      input: { text },
      voice: { languageCode: "en-US", name: "en-US-Neural2-F" },
      audioConfig: { audioEncoding: "MULAW", sampleRateHertz: 8000 }
    })
  });

  if (!res.ok) {
    const errText = await res.text();
    throw new Error(`Google TTS failed: ${errText}`);
  }

  const data = await res.json();
  if (!data.audioContent) {
    throw new Error(`Google TTS response missing audioContent: ${JSON.stringify(data)}`);
  }

  return Buffer.from(data.audioContent, "base64");
}

// Sends raw mulaw audio to SignalWire in properly-sized chunks, per the
// Media Streams protocol (base64-encoded payload inside a "media" event).
function sendAudioToStream(ws, streamSid, audioBuffer) {
  const chunkSize = 320; // 20ms of 8kHz mulaw audio per chunk, matches SignalWire's expected frame size
  for (let i = 0; i < audioBuffer.length; i += chunkSize) {
    const chunk = audioBuffer.subarray(i, i + chunkSize);
    const message = {
      event: "media",
      streamSid: streamSid,
      media: { payload: chunk.toString("base64") }
    };
    ws.send(JSON.stringify(message));
  }
}

server.listen(PORT, () => {
  console.log(`Jarvis Realtime server listening on port ${PORT}`);
});
