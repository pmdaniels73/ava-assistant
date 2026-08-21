// Jarvis Realtime - the foundation for true streaming voice calls.
//
// This is a persistent server (not a serverless function, since it needs to
// hold a live connection open for the duration of a call) that implements
// SignalWire's Media Streams protocol: SignalWire opens a WebSocket to this
// server when a call connects, streams the caller's audio to us in real
// time, and plays back whatever audio we send in return - all while the
// call is happening, no request/response round trips per turn.
//
// Tonight's milestone: prove the pipeline works end to end - receive real
// call audio, generate a spoken test phrase via Azure TTS in the exact
// format SignalWire expects, and stream it back so it's actually heard on
// the call. The full AI conversation loop (streaming speech recognition +
// streaming Claude responses) is the next phase, built on top of this.

const http = require("http");
const WebSocket = require("ws");

const PORT = process.env.PORT || 3000;

const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("Jarvis Realtime server is up.\n");
});

const wss = new WebSocket.Server({ server, path: "/media-stream" });

wss.on("connection", (ws) => {
  console.log("New Media Stream connection opened");
  let streamSid = null;

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

      case "start":
        streamSid = msg.start?.streamSid;
        console.log("Stream started", { streamSid, callSid: msg.start?.callSid, format: msg.start?.mediaFormat });

        // Prove the round trip works: generate a short test phrase in the
        // exact audio format SignalWire expects (8kHz mulaw) and stream it
        // straight back so it's actually heard on the live call.
        try {
          const audioBuffer = await generateMulawAudio("Hi, this is the new real-time system. If you can hear this clearly, the foundation is working.");
          sendAudioToStream(ws, streamSid, audioBuffer);
          console.log("Sent test audio back to caller");
        } catch (err) {
          console.error("Failed to generate/send test audio", err);
        }
        break;

      case "media":
        // Real caller audio arriving in real time - just log for now that
        // we're receiving it, to confirm inbound streaming works. The next
        // phase feeds this into streaming speech recognition.
        break;

      case "stop":
        console.log("Stream stopped", { streamSid });
        break;

      default:
        console.log("Unhandled event type", msg.event);
    }
  });

  ws.on("close", () => {
    console.log("Media Stream connection closed");
  });

  ws.on("error", (err) => {
    console.error("WebSocket error", err);
  });
});

// Generates speech via Azure TTS directly in 8kHz mulaw format, which is
// exactly what SignalWire's Media Streams expects for outbound audio - no
// format conversion needed on our end.
async function generateMulawAudio(text) {
  const key = process.env.AZURE_SPEECH_KEY;
  const region = process.env.AZURE_SPEECH_REGION;
  if (!key || !region) {
    throw new Error("AZURE_SPEECH_KEY / AZURE_SPEECH_REGION not configured");
  }

  const ssml = `<speak version='1.0' xml:lang='en-US'><voice xml:lang='en-US' name='en-US-AvaNeural'>${escapeSsml(text)}</voice></speak>`;

  const res = await fetch(`https://${region}.tts.speech.microsoft.com/cognitiveservices/v1`, {
    method: "POST",
    headers: {
      "Ocp-Apim-Subscription-Key": key,
      "Content-Type": "application/ssml+xml",
      "X-Microsoft-OutputFormat": "raw-8khz-8bit-mono-mulaw",
      "User-Agent": "JarvisRealtime"
    },
    body: ssml
  });

  if (!res.ok) {
    const errText = await res.text();
    throw new Error(`Azure TTS failed: ${errText}`);
  }

  const arrayBuffer = await res.arrayBuffer();
  return Buffer.from(arrayBuffer);
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

function escapeSsml(str) {
  return String(str).replace(/[<>&'"]/g, c => ({
    "<": "&lt;", ">": "&gt;", "&": "&amp;", "'": "&apos;", '"': "&quot;"
  }[c]));
}

server.listen(PORT, () => {
  console.log(`Jarvis Realtime server listening on port ${PORT}`);
});
