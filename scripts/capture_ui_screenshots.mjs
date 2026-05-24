import { spawn } from "node:child_process";
import fs from "node:fs/promises";
import fsSync from "node:fs";
import path from "node:path";

const repoRoot = process.cwd();
const chromeCandidates = [
  "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
  "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe",
  "C:\\Program Files\\Microsoft\\Edge\\Application\\msedge.exe",
  "C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe",
];
const chromePath = chromeCandidates.find((candidate) => {
  try {
    fsSync.statSync(candidate);
    return true;
  } catch {
    return false;
  }
});

if (!chromePath) {
  throw new Error("No Chrome or Edge executable found in the expected Windows install paths.");
}

const outputDir = path.join(repoRoot, "docs", "screenshots");
const profileDir = path.join(repoRoot, ".tmp", "chrome-ui-screenshot-profile");
const remoteDebuggingPort = Number(process.env.CHROME_DEBUG_PORT || "9224");
const appUrl = process.env.STREAMLIT_URL || "http://localhost:8501";

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForJson(url, attempts = 60) {
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    try {
      const response = await fetch(url);
      if (response.ok) {
        return await response.json();
      }
    } catch {
      // Browser is still starting.
    }
    await sleep(500);
  }
  throw new Error(`Timed out waiting for ${url}`);
}

async function connectCdp(webSocketDebuggerUrl) {
  const socket = new WebSocket(webSocketDebuggerUrl);
  let nextId = 1;
  const pending = new Map();

  socket.addEventListener("message", (event) => {
    const payload = JSON.parse(event.data);
    if (!payload.id || !pending.has(payload.id)) {
      return;
    }
    const { resolve, reject } = pending.get(payload.id);
    pending.delete(payload.id);
    if (payload.error) {
      reject(new Error(payload.error.message));
    } else {
      resolve(payload.result);
    }
  });

  await new Promise((resolve, reject) => {
    socket.addEventListener("open", resolve, { once: true });
    socket.addEventListener("error", reject, { once: true });
  });

  return {
    send(method, params = {}) {
      const id = nextId;
      nextId += 1;
      const promise = new Promise((resolve, reject) => pending.set(id, { resolve, reject }));
      socket.send(JSON.stringify({ id, method, params }));
      return promise;
    },
    close() {
      socket.close();
    },
  };
}

async function evaluate(client, expression, awaitPromise = false) {
  const result = await client.send("Runtime.evaluate", {
    expression,
    awaitPromise,
    returnByValue: true,
  });
  if (result.exceptionDetails) {
    throw new Error(result.exceptionDetails.text || "Runtime.evaluate failed");
  }
  return result.result?.value;
}

async function waitForText(client, text, timeoutMs = 120000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const found = await evaluate(
      client,
      `document.body && document.body.innerText.includes(${JSON.stringify(text)})`
    );
    if (found) {
      return;
    }
    await sleep(1000);
  }
  throw new Error(`Timed out waiting for page text: ${text}`);
}

async function clickByText(client, text) {
  const point = await evaluate(
    client,
    `(() => {
      const candidates = Array.from(document.querySelectorAll("button, label, [role='radio']"));
      const exact = candidates.find((candidate) => candidate.innerText.trim() === ${JSON.stringify(text)});
      const match = exact || candidates.find((candidate) =>
        candidate.innerText.trim().includes(${JSON.stringify(text)})
      );
      if (!match) return null;
      const rect = match.getBoundingClientRect();
      return { x: rect.left + rect.width / 2, y: rect.top + rect.height / 2 };
    })()`
  );
  if (!point) {
    throw new Error(`Could not find clickable text: ${text}`);
  }
  await client.send("Input.dispatchMouseEvent", {
    type: "mousePressed",
    x: point.x,
    y: point.y,
    button: "left",
    clickCount: 1,
  });
  await client.send("Input.dispatchMouseEvent", {
    type: "mouseReleased",
    x: point.x,
    y: point.y,
    button: "left",
    clickCount: 1,
  });
}

async function clickRadioByText(client, text) {
  const point = await evaluate(
    client,
    `(() => {
      const labels = Array.from(document.querySelectorAll("label[data-baseweb='radio']"));
      const match = labels.find((candidate) => candidate.innerText.trim() === ${JSON.stringify(text)});
      if (!match) return null;
      const rect = match.getBoundingClientRect();
      return { x: rect.left + rect.width / 2, y: rect.top + rect.height / 2 };
    })()`
  );
  if (!point) {
    throw new Error(`Could not find radio option: ${text}`);
  }
  await client.send("Input.dispatchMouseEvent", {
    type: "mousePressed",
    x: point.x,
    y: point.y,
    button: "left",
    clickCount: 1,
  });
  await client.send("Input.dispatchMouseEvent", {
    type: "mouseReleased",
    x: point.x,
    y: point.y,
    button: "left",
    clickCount: 1,
  });
}

async function capture(client, outputPath) {
  const screenshot = await client.send("Page.captureScreenshot", {
    format: "png",
    captureBeyondViewport: false,
    fromSurface: true,
  });
  await fs.writeFile(outputPath, Buffer.from(screenshot.data, "base64"));
}

async function scrollTextIntoView(client, text) {
  await evaluate(
    client,
    `(() => {
      const match = Array.from(document.querySelectorAll("*"))
        .find((element) => element.innerText && element.innerText.trim() === ${JSON.stringify(text)});
      if (!match) return false;
      match.scrollIntoView({ block: "start", inline: "nearest" });
      window.scrollBy(0, -120);
      return true;
    })()`
  );
}

await fs.mkdir(outputDir, { recursive: true });
await fs.rm(profileDir, { recursive: true, force: true });
await fs.mkdir(profileDir, { recursive: true });

const browser = spawn(chromePath, [
  "--headless=new",
  "--disable-gpu",
  "--disable-dev-shm-usage",
  "--no-sandbox",
  "--no-first-run",
  "--no-default-browser-check",
  `--remote-debugging-port=${remoteDebuggingPort}`,
  `--user-data-dir=${profileDir}`,
  "--window-size=1440,1200",
  appUrl,
]);

try {
  const targets = await waitForJson(`http://localhost:${remoteDebuggingPort}/json/list`);
  const pageTarget = targets.find((target) => target.type === "page") || targets[0];
  const client = await connectCdp(pageTarget.webSocketDebuggerUrl);

  await client.send("Page.enable");
  await client.send("Runtime.enable");
  await waitForText(client, "Agentic DB Migrator");
  await waitForText(client, "Reset Target Demo DB");
  await scrollTextIntoView(client, "Demo Reset");
  await sleep(1000);
  await capture(client, path.join(outputDir, "streamlit_config_reset.png"));
  console.log("Captured streamlit_config_reset.png");

  await client.send("Page.navigate", { url: appUrl });
  await waitForText(client, "Agentic DB Migrator");
  await waitForText(client, "Analyze");
  await clickRadioByText(client, "Analyze");
  await waitForText(client, "Run Analyze");
  await clickByText(client, "Run Analyze");
  await waitForText(client, "Latest Pre-Migration Summary");
  await sleep(1000);
  await capture(client, path.join(outputDir, "streamlit_analyze_summary.png"));
  console.log("Captured streamlit_analyze_summary.png");

  await clickRadioByText(client, "Approve");
  try {
    await waitForText(client, "Per-Table Load Strategy");
  } catch (error) {
    const visibleText = await evaluate(client, "document.body ? document.body.innerText : ''");
    console.error(visibleText.slice(0, 2000));
    throw error;
  }
  await scrollTextIntoView(client, "Per-Table Load Strategy");
  await sleep(1000);
  await capture(client, path.join(outputDir, "streamlit_approve_strategies.png"));
  console.log("Captured streamlit_approve_strategies.png");

  client.close();
} finally {
  browser.kill();
}
