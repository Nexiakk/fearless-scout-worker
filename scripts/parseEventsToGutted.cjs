/**
 * parseEventsToGutted.cjs
 *
 * Sprint 7.1 orchestration: download a Grid events JSONL, stream-parse it,
 * run the cheese detectors, write the gutted+enriched JSONL, gzip it, and
 * PUT it to Cloudflare R2.
 *
 * ─── Gutted JSONL format contract (consumed by replayService in 7.2) ───
 * Every line is one JSON object with a `k` kind marker:
 *   {"v":1,"meta":{seriesId, gameNumber, durationMs, duration, patch,
 *     winner, teams:{blue,red}, picks:{blue,red}, players:[{id, teamID,
 *     champion, role, name, puuid} ×10]}}   — first line
 *   {"k":"t","t":<SECONDS int>,"x":[10],"y":[10],"h":[10],"m":[10],"a":[10]}
 *     — 1 Hz tick; arrays aligned to meta.players order; h=hp, m=maxHp,
 *       a=alive 0/1. NOTE: tick `t` is in SECONDS; all event `t`s are ms.
 *   {"k":"ck","t":ms,"a":killer,"v":victim,"kt":killerTeamID,"vt":victimTeamID,
 *     "x","y","as":[assists]}                                — champion kill
 *   {"k":"mk","t":ms,"a":killer,"kt","m":monsterType,"x","y"} — monster kill
 *   {"k":"ms","t":ms,"m":monsterName,"x","y"}                 — spawn
 *   {"k":"w","t":ms,"p":placer,"wt":wardType,"x","y"}         — ward placed
 *   {"k":"wk","t":ms,"a":killer,"wt":wardType,"x","y"}        — ward killed
 *   {"k":"i","t":ms,"p":participant,"i":itemID,"s":slot}      — item purchase
 *   {"k":"l","t":ms,"p":participant,"s":skillSlot,"lv":level} — level-up
 *   {"k":"b","t":ms,"kt":teamID,"b":buildingType,"ti":tier,"x","y"} — structure
 *   {"k":"go","t":ms,"win":winningTeam}                        — game over
 *   {"k":"cheese","flags":[...]}      — cheese/anomaly flags (7.6)
 *   {"k":"lvl1","clusters":{blue,red}} — top-5 90s cluster centroids (7.3)
 *   {"k":"jungle","patterns":[...]}    — first-clear classification (7.3)
 *   {"k":"clips","clips":[...]}        — objective clips (7.4)
 *   {"k":"heat","g":{blue,red,vision,kills}} — 4×1024 32×32 grids (7.5)
 * `a` is the actor (killer) — the `k` kind marker must stay unique per line.
 *
 * The file is served via the replays-worker CDN with immutable caching;
 * gzip at rest only. Memory: the 112 MB raw file is never held in memory —
 * readline streams line by line (~12 KB max line). Peak heap stays well
 * under 50 MB.
 */

const readline = require("readline");
const { Readable } = require("stream");
const zlib = require("zlib");

const grid = require("./grid-client.cjs");
const r2 = require("./r2-client.cjs");
const { createAggregate, accumulateLine, finalizeAggregate } = require("./parseEventsToAggregate.cjs");
const { detectCheese } = require("./cheeseDetector.cjs");

function r2Key(workspaceId, seriesId, gameNumber) {
  return `events/${workspaceId}/${seriesId}/${gameNumber}.jsonl.gz`;
}

/**
 * Stream the gutted file's computed blocks into JSON lines.
 * Returns { lines, eventCount, tickCount }.
 */
function buildGuttedLines(agg, meta, cheeseFlags) {
  const playerIds = agg.players.map((p) => p.id);
  const lines = [];

  const metaLine = {
    v: 1,
    meta: {
      seriesId: meta.seriesId,
      gameNumber: meta.gameNumber,
      durationMs: agg.durationMs,
      duration: Math.round(agg.durationMs / 1000),
      patch: agg.patch,
      winner: agg.sideSummary.winner,
      teams: { blue: meta.blueTeam || null, red: meta.redTeam || null },
      picks: {
        blue: agg.sideSummary.bluePicks || [],
        red: agg.sideSummary.redPicks || [],
      },
      players: agg.players,
    },
  };
  lines.push(JSON.stringify(metaLine));

  // 1 Hz ticks — aligned arrays over playerIds order.
  const idx = new Map(playerIds.map((id, i) => [id, i]));
  for (const frame of agg.ticks) {
    const n = playerIds.length;
    const x = new Array(n).fill(0);
    const y = new Array(n).fill(0);
    const h = new Array(n).fill(0);
    const m = new Array(n).fill(0);
    const a = new Array(n).fill(0);
    for (const p of frame.players.values()) {
      const i = idx.get(p.id);
      if (i === undefined) continue;
      x[i] = Math.round(p.x);
      y[i] = Math.round(p.z);
      h[i] = Math.round(p.hp);
      m[i] = Math.round(p.maxHp);
      a[i] = p.alive ? 1 : 0;
    }
    lines.push(JSON.stringify({ k: "t", t: frame.t, x, y, h, m, a }));
  }
  const tickCount = agg.ticks.length;

  let eventCount = 0;
  const push = (line) => {
    lines.push(JSON.stringify(line));
    eventCount++;
  };

  for (const e of agg.events.championKills) {
    push({
      k: "ck", t: e.t, a: e.killer, v: e.victim,
      kt: e.killerTeamID, vt: e.victimTeamID,
      x: e.position ? Math.round(e.position.x) : null,
      y: e.position ? Math.round(e.position.z) : null,
      as: e.assistants,
    });
  }
  for (const e of agg.events.monsterKills) {
    push({
      k: "mk", t: e.t, a: e.killer, kt: e.killerTeamID, m: e.monsterType,
      x: e.position ? Math.round(e.position.x) : null,
      y: e.position ? Math.round(e.position.z) : null,
    });
  }
  for (const e of agg.events.monsterSpawns) {
    push({
      k: "ms", t: e.t, m: e.monsterName,
      x: e.position ? Math.round(e.position.x) : null,
      y: e.position ? Math.round(e.position.z) : null,
    });
  }
  for (const e of agg.events.wards) {
    push({
      k: "w", t: e.t, p: e.placer, wt: e.wardType,
      x: e.position ? Math.round(e.position.x) : null,
      y: e.position ? Math.round(e.position.z) : null,
    });
  }
  for (const e of agg.events.wardKills) {
    push({
      k: "wk", t: e.t, a: e.killer, wt: e.wardType,
      x: e.position ? Math.round(e.position.x) : null,
      y: e.position ? Math.round(e.position.z) : null,
    });
  }
  for (const e of agg.events.items) {
    push({ k: "i", t: e.t, p: e.participant, i: e.itemID, s: e.slot });
  }
  for (const e of agg.events.levels) {
    push({ k: "l", t: e.t, p: e.participant, s: e.skillSlot, lv: e.level });
  }
  for (const e of agg.events.buildings) {
    push({
      k: "b", t: e.t, kt: e.teamID, b: e.buildingType, ti: e.turretTier,
      x: e.position ? Math.round(e.position.x) : null,
      y: e.position ? Math.round(e.position.z) : null,
    });
  }
  if (agg.events.gameOver) {
    push({ k: "go", t: agg.events.gameOver.t, win: agg.events.gameOver.winningTeam });
  }

  // Computed blocks.
  lines.push(JSON.stringify({ k: "cheese", flags: cheeseFlags }));
  lines.push(JSON.stringify({ k: "lvl1", clusters: agg.lvl1Clusters }));
  lines.push(JSON.stringify({ k: "jungle", patterns: agg.junglePatterns }));
  lines.push(JSON.stringify({ k: "clips", clips: agg.objectiveClips }));
  lines.push(
    JSON.stringify({
      k: "heat",
      g: {
        blue: agg.heatmaps.blue,
        red: agg.heatmaps.red,
        vision: agg.heatmaps.vision,
        kills: agg.heatmaps.kills,
      },
    }),
  );

  return { lines, eventCount, tickCount };
}

/**
 * Process one game's events stream end-to-end.
 *
 * @param {object} opts
 * @param {Readable} opts.stream       — events JSONL stream (from grid.downloadEventsJsonl)
 * @param {string} opts.seriesId
 * @param {number|string} opts.gameNumber
 * @param {string} opts.workspaceId
 * @param {object} [opts.meta]         — { bluePicks, redPicks, winnerSide, blueTeam, redTeam }
 * @param {boolean} [opts.putR2=true]  — set false to skip the upload (dry run / inspect)
 * @returns {Promise<object>} metadata for the grid_events_files row
 */
async function processGameEvents({ stream, seriesId, gameNumber, workspaceId, meta = {}, putR2 = true }) {
  const agg = createAggregate();
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  for await (const line of rl) {
    if (!line.trim()) continue;
    accumulateLine(agg, line);
  }

  const finalized = finalizeAggregate(agg, {
    bluePicks: meta.bluePicks,
    redPicks: meta.redPicks,
    winnerSide: meta.winnerSide,
  });
  const cheeseFlags = detectCheese(finalized);

  const { lines, eventCount, tickCount } = buildGuttedLines(finalized, {
    seriesId,
    gameNumber,
    blueTeam: meta.blueTeam || null,
    redTeam: meta.redTeam || null,
  }, cheeseFlags);

  const rawGutted = lines.join("\n") + "\n";
  const gzBuffer = zlib.gzipSync(Buffer.from(rawGutted, "utf8"), { level: 9 });
  const key = r2Key(workspaceId, seriesId, gameNumber);

  if (putR2) {
    await r2.putObject(key, gzBuffer);
  }

  return {
    r2Key: key,
    rawSizeBytes: Buffer.byteLength(rawGutted),
    guttedSizeBytes: gzBuffer.length,
    eventCount,
    tickCount,
    cheeseFlags,
    objectiveClips: finalized.objectiveClips,
    lvl1Clusters: finalized.lvl1Clusters,
    junglePatterns: finalized.junglePatterns,
    heatmaps: finalized.heatmaps,
    eventBreakdown: {
      championKills: finalized.events.championKills.length,
      monsterKills: finalized.events.monsterKills.length,
      monsterSpawns: finalized.events.monsterSpawns.length,
      wards: finalized.events.wards.length,
      wardKills: finalized.events.wardKills.length,
      items: finalized.events.items.length,
      levels: finalized.events.levels.length,
      buildings: finalized.events.buildings.length,
    },
    duration: finalized.durationMs,
    patch: finalized.patch,
    sideSummary: finalized.sideSummary,
    players: finalized.players,
    unknownCount: finalized.unknownCount,
    linesProcessed: finalized.linesProcessed,
    pregameCount: finalized.pregameCount,
  };
}

module.exports = { processGameEvents, buildGuttedLines, r2Key };
