/**
 * parseEventsToAggregate.cjs
 *
 * Pure stream-parser for Grid's per-game events JSONL (Sprint 7.1).
 *
 * The Grid events file is a JSONL of FULL game-state snapshots — one line
 * per state update (~1 Hz full-state lines carrying `participants[]` with
 * per-player position/HP, plus sub-second event lines whose event type is
 * encoded by field signature, discriminated by `rfc461Schema` + fields).
 * There is no `eventType` discriminator and no literal "CHAMPION_KILL":
 * a kill is a line with `killType` + `killer`/`victim`, a monster kill the
 * same plus `monsterType`, an item purchase `itemID` + `inventorySlot`, etc.
 *
 * Verified against real data (WLG HLL Summer 2026, series 2966386 game 1,
 * 2026-08-02): 112 MB raw, ~9,392 lines, `gameTime` in ms (0 → 1,963,913),
 * `sequenceIndex` monotonic, `gameState` present only during
 * CHAMP_SELECT / POST_CHAMP_SELECT (skipped — not game time).
 *
 * This module is pure: no I/O. It exposes:
 *   classifyLine(j)           — kind + parsed event payload
 *   createAggregate()         — empty in-memory aggregate
 *   accumulateLine(agg, line) — one JSONL line into the aggregate
 *   finalizeAggregate(agg)    — computed fields (clusters, jungle, clips)
 *
 * The aggregate shape is consumed by cheeseDetector.cjs and
 * parseEventsToGutted.cjs (gutted JSONL writer).
 */

const geofences = require("./geofences.cjs");

const TICK_SECOND_MS = 1000;
const GRID_SIZE = 32;
const GRID_CELLS = GRID_SIZE * GRID_SIZE;

// ─── Event classification ───────────────────────────────────────────────

const OBJECTIVE_TYPES = ["dragon", "baron", "herald", "grub", "voidgrub"];
const CAMP_TYPES = [
  "raptor", "raptors", "krug", "krugs", "wolf", "wolves",
  "gromp", "blue", "red", "scuttle", "scuttlecrab",
];

// Schemas that carry no information our features use (ability casts, recall
// channeling, role assignment, reconnects, item teardown, ...). Classified
// as 'skip' so 'unknown' stays meaningful. Verified against real data
// (WLG HLL Summer 2026, 2026-08-02).
const SKIP_SCHEMAS = new Set([
  "skill_used",
  "channeling_started",
  "channeling_ended",
  "role_selected",
  "reconnect",
  "item_destroyed",
  "item_sold",
  "summoner_spell_used",
  "queued_dragon_info",
  "pause_ended",
  "pause_started",
  "turret_plate_gold_earned",
  "building_gold_grant",
  "role_bound_quest_completed",
  "objective_bounty_prestart",
]);

// Riot wardType comes through as a string in this stream ("yellowTrinket",
// "controlWard", "sightWard", "blueTrinket").
function isControlWard(wardType) {
  if (typeof wardType === "number") return wardType === 2;
  if (typeof wardType === "string") {
    const t = wardType.toLowerCase();
    return t.includes("control");
  }
  return false;
}

function posOf(j) {
  if (j && j.position && typeof j.position.x === "number") {
    const z = typeof j.position.z === "number" ? j.position.z : j.position.y;
    if (typeof z === "number") return { x: j.position.x, z };
  }
  return null;
}

/**
 * Classify one parsed JSONL line.
 * @returns {object} { kind, data } where kind ∈:
 *   'pregame' | 'tick' | 'champ_kill' | 'monster_kill' | 'monster_spawn' |
 *   'ward' | 'item' | 'level' | 'building' | 'game_over' | 'unknown'
 */
function classifyLine(j) {
  if (!j || typeof j !== "object") return { kind: "unknown", data: null };

  // Champ-select stage — not game time, skipped entirely.
  if (j.gameState === "CHAMP_SELECT" || j.gameState === "POST_CHAMP_SELECT") {
    return { kind: "pregame", data: null };
  }

  // Known-irrelevant event schemas (ability casts, recall channeling).
  if (typeof j.rfc461Schema === "string" && SKIP_SCHEMAS.has(j.rfc461Schema)) {
    return { kind: "skip", data: null };
  }

  // End of game — two real shapes: { gameOver: true, winningTeam } and the
  // `game_end` schema ({ winningTeam, wallTime }).
  if (j.gameOver === true || (j.rfc461Schema === "game_end" && j.winningTeam !== undefined)) {
    return {
      kind: "game_over",
      data: { t: j.gameTime, winningTeam: j.winningTeam ?? null },
    };
  }

  // Full-state 1 Hz tick line (10-player snapshot with positions).
  if (Array.isArray(j.participants) && j.participants.length > 0) {
    return {
      kind: "tick",
      data: {
        t: j.gameTime,
        gameVersion: j.gameVersion,
        participants: j.participants,
      },
    };
  }

  // Champion kill — `victim` only exists on champion kills. The
  // `champion_kill_special` schema (first blood etc.) has killer + position
  // but no victim — both classify here.
  if (j.victim !== undefined || j.rfc461Schema === "champion_kill_special") {
    return {
      kind: "champ_kill",
      data: {
        t: j.gameTime,
        killer: j.killer ?? null,
        victim: j.victim ?? null,
        killerTeamID: j.killerTeamID ?? null,
        victimTeamID: j.victimTeamID ?? null,
        position: posOf(j),
        assistants: Array.isArray(j.assistants) ? j.assistants : [],
        killStreak: j.killStreakLength ?? null,
        shutdownBounty: j.shutdownBounty ?? null,
        bountyGold: j.bounty ?? null,
        special: j.killType ?? null,
      },
    };
  }

  // Monster kill (camp or objective): killType + monsterType.
  if (j.killType !== undefined && j.monsterType !== undefined) {
    return {
      kind: "monster_kill",
      data: {
        t: j.gameTime,
        killer: j.killer ?? null,
        killerTeamID: j.killerTeamID ?? null,
        monsterType: String(j.monsterType).toLowerCase(),
        position: posOf(j),
      },
    };
  }

  // Generic kill (killType without victim/monsterType) — count, don't crash.
  if (j.killType !== undefined) {
    return { kind: "unknown", data: { t: j.gameTime, note: "untyped kill" } };
  }

  // Camp/objective spawn (neutral_minion_spawn: monsterType + position;
  // queued_epic_monster_info: monsterName + spawnTime).
  if (j.monsterType !== undefined || (j.monsterName !== undefined && j.spawnTime !== undefined)) {
    return {
      kind: "monster_spawn",
      data: {
        t: j.gameTime,
        monsterName: String(
          j.monsterType ?? j.monsterName,
        ).toLowerCase(),
        position: posOf(j),
      },
    };
  }

  // Ward placement.
  if (j.wardType !== undefined && j.placer !== undefined) {
    return {
      kind: "ward",
      data: {
        t: j.gameTime,
        placer: j.placer,
        wardType: j.wardType,
        position: posOf(j),
      },
    };
  }

  // Ward destroyed (replay markers + vision stats).
  if (j.wardType !== undefined && j.killer !== undefined) {
    return {
      kind: "ward_kill",
      data: {
        t: j.gameTime,
        killer: j.killer,
        wardType: j.wardType,
        position: posOf(j),
      },
    };
  }

  // Item purchase (starter purchases lack `inventorySlot`).
  if (j.itemID !== undefined && (j.participantID !== undefined || j.participant !== undefined)) {
    return {
      kind: "item",
      data: {
        t: j.gameTime,
        participant: j.participantID ?? j.participant ?? null,
        itemID: j.itemID,
        slot: j.inventorySlot ?? null,
      },
    };
  }

  // Champion level-up — two real shapes: skill_level_up ({ evolved,
  // skillSlot, participant }) and champion_level_up ({ level, participant }).
  if (
    j.evolved !== undefined ||
    (typeof j.level === "number" && (j.participantID !== undefined || j.participant !== undefined))
  ) {
    return {
      kind: "level",
      data: {
        t: j.gameTime,
        participant: j.participant ?? j.participantID ?? null,
        level: j.level ?? null,
        skillSlot: j.skillSlot ?? null,
        evolved: j.evolved ?? null,
      },
    };
  }

  // Structure event (tower/inhibitor/nexus; turret plates use the
  // `turret_plate_destroyed` schema with teamID + position, no buildingType).
  if (j.buildingType !== undefined || j.rfc461Schema === "turret_plate_destroyed") {
    return {
      kind: "building",
      data: {
        t: j.gameTime,
        teamID: j.teamID ?? null,
        buildingType: j.buildingType
          ? String(j.buildingType).toLowerCase()
          : "turret_plate",
        turretTier: j.turretTier ?? null,
        lane: j.lane ?? null,
        position: posOf(j),
      },
    };
  }

  return { kind: "unknown", data: { t: j.gameTime ?? null } };
}

// ─── Aggregate ──────────────────────────────────────────────────────────

function createAggregate() {
  return {
    // participantID → player descriptor (from the first tick line)
    players: new Map(),
    // integer-second → tick frame { t, players: [{ id, x, z, hp, maxHp, alive, level, teamID }] }
    ticks: new Map(),
    durationMs: 0,
    patch: null,
    events: {
      championKills: [],
      monsterKills: [],
      monsterSpawns: [],
      wards: [],
      wardKills: [],
      items: [],
      levels: [],
      buildings: [],
      gameOver: null,
    },
    unknownCount: 0,
    pregameCount: 0,
    linesProcessed: 0,
    // 32×32 grids (Uint32Array)
    heatmaps: {
      blue: new Uint32Array(GRID_CELLS),
      red: new Uint32Array(GRID_CELLS),
      vision: new Uint32Array(GRID_CELLS),
      kills: new Uint32Array(GRID_CELLS),
    },
  };
}

/**
 * Register/upsert a player descriptor. The game_info line (first tick
 * line) carries champion/role but only summonerName/riotId — playerName
 * arrives on the stats_update lines. Name priority:
 * playerName → riotId.displayName → summonerName. Champion/role are kept
 * from the first registration (game_info is authoritative for them).
 */
function registerPlayer(agg, p) {
  if (!p || typeof p.participantID !== "number") return;
  const existing = agg.players.get(p.participantID);
  if (existing) {
    if (existing.name === null) {
      const name =
        p.playerName ||
        (p.riotId && p.riotId.displayName) ||
        p.summonerName ||
        null;
      if (name) existing.name = name;
    }
    return;
  }
  agg.players.set(p.participantID, {
    id: p.participantID,
    teamID: p.teamID ?? null,
    champion: p.championName ?? null,
    role: p.role ? String(p.role).toLowerCase() : null,
    name:
      p.playerName ||
      (p.riotId && p.riotId.displayName) ||
      p.summonerName ||
      null,
    puuid: p.puuid ?? null,
  });
}

function accumulateLine(agg, rawLine) {
  let j;
  try {
    j = JSON.parse(rawLine);
  } catch {
    agg.unknownCount++;
    return;
  }
  if (!j || typeof j !== "object") {
    agg.unknownCount++;
    return;
  }

  agg.linesProcessed++;
  const { kind, data } = classifyLine(j);
  if (kind === "pregame" || kind === "skip") {
    if (kind === "pregame") agg.pregameCount++;
    return;
  }
  if (kind === "unknown") {
    agg.unknownCount++;
    return;
  }

  if (typeof j.gameTime === "number" && j.gameTime > agg.durationMs) {
    agg.durationMs = j.gameTime;
  }
  if (!agg.patch && j.gameVersion) {
    agg.patch = String(j.gameVersion).split(".").slice(0, 2).join(".");
  }

  switch (kind) {
    case "tick": {
      // The game-start line (no gameTime) still carries the full player
      // descriptors — register players before the time guard so roles and
      // champions are known even if no positional tick ever follows.
      for (const p of data.participants) {
        registerPlayer(agg, p);
      }
      const t = data.t;
      if (typeof t !== "number") return;
      for (const p of data.participants) {
        if (!p || typeof p.participantID !== "number") continue;
        const pos = posOf(p);
        if (!pos) continue;
        const frame = agg.ticks.get(Math.floor(t / TICK_SECOND_MS)) || {
          t: Math.floor(t / TICK_SECOND_MS),
          players: new Map(),
        };
        frame.players.set(p.participantID, {
          id: p.participantID,
          x: pos.x,
          z: pos.z,
          hp: p.health ?? 0,
          maxHp: p.healthMax ?? 0,
          alive: p.alive ? 1 : 0,
          level: p.level ?? 0,
          teamID: p.teamID ?? null,
        });
        agg.ticks.set(frame.t, frame);
        // Presence heatmap (per side).
        const cell = geofences.gridCell(pos);
        if (p.teamID === 100) agg.heatmaps.blue[cell.index]++;
        else if (p.teamID === 200) agg.heatmaps.red[cell.index]++;
      }
      break;
    }
    case "champ_kill": {
      agg.events.championKills.push(data);
      if (data.position) {
        agg.heatmaps.kills[geofences.gridCell(data.position).index]++;
      }
      break;
    }
    case "monster_kill":
      agg.events.monsterKills.push(data);
      break;
    case "monster_spawn":
      agg.events.monsterSpawns.push(data);
      break;
    case "ward": {
      agg.events.wards.push({
        ...data,
        isControl: isControlWard(data.wardType),
      });
      if (data.position) {
        agg.heatmaps.vision[geofences.gridCell(data.position).index]++;
      }
      break;
    }
    case "ward_kill": {
      agg.events.wardKills.push(data);
      if (data.position) {
        agg.heatmaps.vision[geofences.gridCell(data.position).index]++;
      }
      break;
    }
    case "item":
      agg.events.items.push(data);
      break;
    case "level":
      agg.events.levels.push(data);
      break;
    case "building":
      agg.events.buildings.push(data);
      break;
    case "game_over":
      agg.events.gameOver = data;
      break;
  }
}

// ─── Computed fields (finalize) ─────────────────────────────────────────

const OBJECTIVE_SET = new Set(OBJECTIVE_TYPES);
const CAMP_TOKENS = ["raptor", "krug", "wolf", "gromp", "blue", "red", "scuttle"];

function isObjective(monsterType) {
  return OBJECTIVE_SET.has(monsterType);
}

// Substring match: real stream values include "red" (raptor sample), "RedCamp"
// (spawn), "BlueCamp" etc.
function isCamp(monsterType) {
  return CAMP_TOKENS.some((t) => monsterType.includes(t));
}

/**
 * Top-N cluster cells per side from the first `windowMs` of tick positions.
 * @returns {Array<{ x, z, count, index }>} sorted by count desc
 */
function computeLvl1Clusters(agg, windowMs, topN = 5) {
  const windowSeconds = Math.floor(windowMs / TICK_SECOND_MS);
  const perSide = { blue: new Map(), red: new Map() };
  for (const frame of agg.ticks.values()) {
    if (frame.t > windowSeconds) break;
    for (const p of frame.players.values()) {
      const side = p.teamID === 100 ? "blue" : p.teamID === 200 ? "red" : null;
      if (!side) continue;
      const cell = geofences.gridCell({ x: p.x, z: p.z });
      const map = perSide[side];
      map.set(cell.index, (map.get(cell.index) || 0) + 1);
    }
  }
  const out = { blue: [], red: [] };
  for (const side of ["blue", "red"]) {
    const cells = [...perSide[side].entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, topN);
    out[side] = cells.map(([index, count]) => ({
      ...geofences.cellCenter(index),
      count,
      index,
    }));
  }
  return out;
}

/**
 * First-clear classification per side's jungler (from monster kills).
 * @returns {Array<{ side, participantID, label, camps }>}
 */
function computeJunglePatterns(agg, firstClearWindowMs = 240000) {
  const junglers = [];
  for (const p of agg.players.values()) {
    if (p.role === "jungle") {
      junglers.push({ side: p.teamID === 100 ? "blue" : "red", id: p.id });
    }
  }
  return junglers.map(({ side, id }) => {
    const camps = agg.events.monsterKills
      .filter(
        (k) =>
          k.killer === id &&
          isCamp(k.monsterType) &&
          typeof k.t === "number" &&
          k.t <= firstClearWindowMs,
      )
      .map((k) => ({ t: k.t, monsterType: k.monsterType, ...(k.position || {}) }))
      .sort((a, b) => a.t - b.t);
    const n = camps.length;
    const label =
      n >= 6 ? "full-clear" : n >= 4 ? "partial" : n === 3 ? "3-camp" : n >= 1 ? "1-camp" : "none";
    return { side, participantID: id, label, camps };
  });
}

/**
 * Objective clips: for every objective monster kill (dragon/baron/herald/
 * grub), a [t-90s, t+30s] window with per-side control-ward counts.
 * @returns {Array<{ type, t, teamID, x, z, wards: {100, 200} }>}
 */
function computeObjectiveClips(agg, windowBeforeMs = 90000, windowAfterMs = 30000) {
  const playerTeam = new Map();
  for (const p of agg.players.values()) playerTeam.set(p.id, p.teamID);

  const clips = [];
  for (const k of agg.events.monsterKills) {
    if (!isObjective(k.monsterType) || typeof k.t !== "number") continue;
    const start = k.t - windowBeforeMs;
    const end = k.t + windowAfterMs;
    const wards = { 100: 0, 200: 0 };
    for (const w of agg.events.wards) {
      if (typeof w.t !== "number" || w.t < start || w.t > end) continue;
      // Control ward (Riot wardType "controlWard" / 2).
      if (!w.isControl) continue;
      const team = playerTeam.get(w.placer);
      if (team === 100 || team === 200) wards[team]++;
    }
    clips.push({
      type: k.monsterType,
      t: k.t,
      teamID: k.killerTeamID ?? null,
      x: k.position ? Math.round(k.position.x) : null,
      z: k.position ? Math.round(k.position.z) : null,
      wards,
    });
  }
  return clips.sort((a, b) => a.t - b.t);
}

/**
 * Produce the final computed aggregate (everything the gutted file + cheese
 * detectors need).
 */
function finalizeAggregate(agg, opts = {}) {
  const lvl1 = computeLvl1Clusters(agg, opts.lvl1WindowMs ?? 90000);
  const jungle = computeJunglePatterns(agg, opts.firstClearWindowMs ?? 240000);
  const clips = computeObjectiveClips(agg);

  const sideSummary = {
    bluePicks: opts.bluePicks || [],
    redPicks: opts.redPicks || [],
    winner: opts.winnerSide || null,
  };

  return {
    players: [...agg.players.values()].sort((a, b) => a.id - b.id),
    ticks: [...agg.ticks.values()].sort((a, b) => a.t - b.t),
    durationMs: agg.durationMs,
    patch: agg.patch,
    events: agg.events,
    unknownCount: agg.unknownCount,
    linesProcessed: agg.linesProcessed,
    pregameCount: agg.pregameCount,
    heatmaps: {
      blue: Array.from(agg.heatmaps.blue),
      red: Array.from(agg.heatmaps.red),
      vision: Array.from(agg.heatmaps.vision),
      kills: Array.from(agg.heatmaps.kills),
    },
    lvl1Clusters: lvl1,
    junglePatterns: jungle,
    objectiveClips: clips,
    sideSummary,
  };
}

module.exports = {
  classifyLine,
  createAggregate,
  accumulateLine,
  finalizeAggregate,
  isObjective,
  isCamp,
  OBJECTIVE_TYPES,
  CAMP_TYPES,
  GRID_SIZE,
  GRID_CELLS,
};
