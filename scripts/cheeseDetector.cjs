/**
 * cheeseDetector.cjs
 *
 * The 5 cheese/anomaly detection algorithms (Sprint 7.1, §2.17).
 *
 * Pure functions over the finalizeAggregate() output — no I/O, no per-frame
 * iteration. Results are embedded in the gutted R2 JSONL as `cheese_flags`
 * (consumed by the Patterns page Threat Matrix in Sprint 7.6 and by Replay
 * clip links in Sprint 7.2).
 *
 * All thresholds live in CHEESE_THRESHOLDS (hardcoded for MVP; a Firestore
 * doc is a documented follow-up).
 */

const geofences = require("./geofences.cjs");

const CHEESE_THRESHOLDS = {
  lvl2Gank: {
    campBeforeMs: 135000, // first camp kill at t ≤ 2:15
    killBeforeMs: 150000, // kill lands before 2:30
    confidenceAnchorMs: 105000, // 1:45 → confidence 1.0
    confidenceWindowMs: 45000, // 0:45 → confidence → 0
  },
  deathbrush: {
    windowStartMs: 50000,
    windowEndMs: 85000,
    killBeforeMs: 95000,
    stationarySeconds: 12,
    maxMovePerSecond: 200,
    minPlayers: 2,
    adjacentBuffer: 400, // units beyond the bush radius
  },
  verticalJungle: {
    firstCampBeforeMs: 105000, // first camp in own jungle at t ≤ 1:45
    crossBeforeMs: 120000, // crossing river by 2:00
  },
  firstBlood: {
    bushSeverity: "HIGH",
    riverSeverity: "MEDIUM",
  },
  habitualWard: {
    earlyMs: 90000, // first 90s of the game
    minCount: 3,
    cellShare: 0.8, // a cell holding ≥ 80% of the team's early wards
  },
};

function clamp01(v) {
  return Math.max(0, Math.min(1, v));
}

function dist(a, b) {
  const dx = a.x - b.x;
  const dz = a.z - b.z;
  return Math.sqrt(dx * dx + dz * dz);
}

/**
 * 1. Level 2 cheese gank — jungler camps at t ≤ 2:15, then a champion kill
 *    in a lane polygon before 2:30.
 */
function detectLvl2CheeseGank(agg, th) {
  const flags = [];
  const junglers = new Map(); // participantID → side
  for (const p of agg.players) {
    if (p.role === "jungle") junglers.set(p.id, p.side ?? (p.teamID === 100 ? "blue" : "red"));
  }

  const candidates = [];
  for (const camp of agg.events.monsterKills) {
    if (typeof camp.t !== "number" || camp.t > th.campBeforeMs) continue;
    if (!junglers.has(camp.killer)) continue;

    // Find a champion kill after the camp, in a lane polygon, before 2:30.
    const kill = agg.events.championKills.find(
      (k) =>
        typeof k.t === "number" &&
        k.t > camp.t &&
        k.t <= th.killBeforeMs &&
        k.position &&
        geofences.laneAt(k.position),
    );
    if (!kill) continue;

    const confidence = clamp01(1 - (camp.t - th.confidenceAnchorMs) / th.confidenceWindowMs);
    candidates.push({
      killKey: `${kill.t}:${kill.killer}:${kill.victim}`,
      flag: {
        type: "LVL2_CHEESE_GANK",
        t: camp.t,
        actor: kill.killer ?? null,
        target: kill.victim ?? null,
        killerTeamID: kill.killerTeamID ?? null,
        pos: { x: Math.round(kill.position.x), z: Math.round(kill.position.z) },
        lane: geofences.laneAt(kill.position),
        camp: camp.monsterType,
        confidence: Math.round(confidence * 100) / 100,
      },
    });
  }

  // One flag per kill: a jungler's earlier camps can all satisfy the "camp
  // before the kill" check, which would over-count a single gank (Sprint
  // 7.6 fix — the Threat Matrix counts occurrences). Keep the EARLIEST camp
  // per kill so `t` reflects when the gank started.
  const perKill = new Map();
  for (const c of candidates) {
    const existing = perKill.get(c.killKey);
    if (!existing || c.flag.t < existing.t) perKill.set(c.killKey, c.flag);
  }
  flags.push(...perKill.values());
  return flags;
}

/**
 * 2. Level 1 deathbrush stack — ≥2 players stationary in a cheese bush for
 *    ≥12s between 0:50 and 1:25, then a kill in/adjacent to the same bush
 *    before 1:35.
 */
function detectLvl1Deathbrush(agg, th) {
  const flags = [];
  const ticks = agg.ticks;

  for (const bush of geofences.CHEESE_BUSHES) {
    const candidates = new Map(); // participantID → { stationary: 0, positions: [] }

    for (const frame of ticks) {
      const tMs = frame.t * 1000;
      if (tMs < th.windowStartMs || tMs > th.windowEndMs) continue;
      for (const p of frame.players.values()) {
        const pt = { x: p.x, z: p.z };
        if (!geofences.pointInBush(pt, bush)) continue;
        let cand = candidates.get(p.id);
        if (!cand) {
          cand = { id: p.id, stationary: 0, positions: [], last: null };
          candidates.set(p.id, cand);
        }
        cand.positions.push(pt);
        if (cand.last && dist(cand.last, pt) <= th.maxMovePerSecond) {
          cand.stationary++;
        } else if (cand.last) {
          cand.stationary = 0;
        }
        cand.last = pt;
      }
    }

    const stationaryPlayers = [...candidates.values()].filter(
      (c) => c.stationary >= th.stationarySeconds,
    );
    if (stationaryPlayers.length < th.minPlayers) continue;

    const kill = agg.events.championKills.find(
      (k) =>
        typeof k.t === "number" &&
        k.t <= th.killBeforeMs &&
        k.position &&
        geofences.distanceToBush(k.position, bush) <= th.adjacentBuffer,
    );
    if (!kill) continue;

    flags.push({
      type: "LVL1_DEATHBRUSH",
      t: kill.t,
      brush: bush.name,
      positions: stationaryPlayers.flatMap((c) => c.positions.slice(0, 3)),
      // Doc contract: "≥2 players stationary in a Cheese Bush polygon for
      // ≥12s". Only the players who actually satisfied the stationary
      // condition belong in `participants` — previously every player who
      // merely ENTERED the bush inside the window was listed, over-counting
      // the stack (Sprint 7.6 fix).
      participants: stationaryPlayers.map((c) => c.id),
      victim: kill.victim ?? null,
    });
  }
  return flags;
}

/**
 * 3. Level 2 vertical jungle — first camp in own jungle at t ≤ 1:45,
 *    position crosses the river into enemy jungle by 2:00, then a second
 *    camp kill in the enemy jungle.
 */
function detectLvl2VerticalJungle(agg, th) {
  const flags = [];
  const junglers = [];
  for (const p of agg.players) {
    if (p.role === "jungle") junglers.push(p);
  }

  for (const jungler of junglers) {
    const camps = agg.events.monsterKills
      .filter(
        (k) =>
          k.killer === jungler.id &&
          typeof k.t === "number" &&
          k.t <= th.crossBeforeMs &&
          k.position,
      )
      .sort((a, b) => a.t - b.t);
    if (camps.length < 2) continue;

    const first = camps[0];
    const firstInOwn = geofences.isOwnHalf(jungler.teamID, first.position);
    if (first.t > th.firstCampBeforeMs || firstInOwn !== true) continue;

    // Crossing evidence: the jungler's tick positions enter the enemy half.
    let crossed = null;
    for (const frame of agg.ticks) {
      const tMs = frame.t * 1000;
      if (tMs < first.t || tMs > th.crossBeforeMs) continue;
      const p = frame.players.get(jungler.id);
      if (!p) continue;
      if (geofences.isOwnHalf(jungler.teamID, { x: p.x, z: p.z }) === false) {
        crossed = tMs;
        break;
      }
    }

    const second = camps.find(
      (c) =>
        c.t > first.t &&
        c.t > (crossed ?? 0) &&
        geofences.isOwnHalf(jungler.teamID, c.position) === false,
    );
    if (!second) continue;

    flags.push({
      type: "LVL2_VERTICAL_JUNGLE",
      t: second.t,
      jungler: jungler.id,
      side: jungler.teamID === 100 ? "blue" : "red",
      camps: camps.map((c) => ({
        t: c.t,
        monsterType: c.monsterType,
        x: Math.round(c.position.x),
        z: Math.round(c.position.z),
      })),
    });
  }
  return flags;
}

/**
 * 4. Predictable first blood — the first champion kill's position: in a
 *    cheese bush → HIGH severity; in the river band → MEDIUM.
 */
function detectPredictableFirstBlood(agg, th) {
  const kills = agg.events.championKills.filter((k) => typeof k.t === "number");
  if (kills.length === 0) return [];
  const fb = kills.reduce((a, b) => (a.t < b.t ? a : b));
  if (!fb.position) return [];

  const bush = geofences.bushAt(fb.position);
  let severity = null;
  if (bush) severity = th.bushSeverity;
  else if (geofences.pointInRiver(fb.position)) severity = th.riverSeverity;
  if (!severity) return [];

  return [
    {
      type: "PREDICTABLE_FIRST_BLOOD",
      t: fb.t,
      pos: { x: Math.round(fb.position.x), z: Math.round(fb.position.z) },
      severity,
      bush: bush ? bush.name : null,
      victim: fb.victim ?? null,
    },
  ];
}

/**
 * 5. Habitual ward placement — per team, cells holding ≥ 80% of the team's
 *    first-90s ward placements (≥ CHEESE_THRESHOLDS.habitualWard.minCount).
 */
function detectHabitualWard(agg, th) {
  const playerTeam = new Map();
  for (const p of agg.players) playerTeam.set(p.id, p.teamID);

  const perTeam = { 100: new Map(), 200: new Map() };
  for (const w of agg.events.wards) {
    if (typeof w.t !== "number" || w.t > th.earlyMs || !w.position) continue;
    const team = playerTeam.get(w.placer);
    if (team !== 100 && team !== 200) continue;
    const cell = geofences.gridCell(w.position);
    const map = perTeam[team];
    map.set(cell.index, (map.get(cell.index) || 0) + 1);
  }

  const flags = [];
  for (const team of [100, 200]) {
    const map = perTeam[team];
    let total = 0;
    for (const count of map.values()) total += count;
    if (total === 0) continue;
    for (const [index, count] of map.entries()) {
      if (count < th.minCount) continue;
      const percent = count / total;
      if (percent < th.cellShare) continue;
      const center = geofences.cellCenter(index);
      flags.push({
        type: "HABITUAL_WARD",
        teamID: team,
        cell: { cx: index % 32, cz: Math.floor(index / 32) },
        count,
        percent: Math.round(percent * 100),
        description: `${team === 100 ? "Blue" : "Red"} places ${count}/${total} early wards in one cell`,
      });
    }
  }
  return flags;
}

/**
 * Run all 5 detectors over a finalized aggregate.
 * @returns {Array} combined cheese_flags
 */
function detectCheese(agg, thresholds = CHEESE_THRESHOLDS) {
  return [
    ...detectLvl2CheeseGank(agg, thresholds.lvl2Gank),
    ...detectLvl1Deathbrush(agg, thresholds.deathbrush),
    ...detectLvl2VerticalJungle(agg, thresholds.verticalJungle),
    ...detectPredictableFirstBlood(agg, thresholds.firstBlood),
    ...detectHabitualWard(agg, thresholds.habitualWard),
  ];
}

module.exports = {
  detectCheese,
  detectLvl2CheeseGank,
  detectLvl1Deathbrush,
  detectLvl2VerticalJungle,
  detectPredictableFirstBlood,
  detectHabitualWard,
  CHEESE_THRESHOLDS,
};
