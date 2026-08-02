/**
 * geofences.cjs
 *
 * Map geometry for the cheese/anomaly detectors (Sprint 7.1, re-derived
 * 2026-08-02 in Sprint 7.6 from measured event data — see
 * scripts/measure-geofences.cjs).
 *
 * Summoner's Rift coordinate system as used by Grid's events stream:
 *   - x ∈ [0, 14870], z ∈ [0, 14981] (z is the "vertical" map axis; the
 *     events stream reports event/player positions as { x, z }).
 *   - BLUE base at (130..662, 135..675) — bottom-left (small x AND small z).
 *   - RED base at (14055..14589, 14170..14673) — top-right (large x AND z).
 *   - The river follows the diagonal x + z ≈ 15200 (measured: blue fountain
 *     sums 265..1337, red sums 28225..29262; baron spawn (5008,10471) sums
 *     15479, dragon spawn (9837,4397) sums 14234).
 *
 * MEASURED LANDMARKS (12-game HLL Summer 2026 seed + 3 earlier games):
 *   - baron/herald/grub pit:  spawns cluster at (5008,10471)/(4925,10429)/
 *     (4940,10428) — the top pit.
 *   - dragon pit:             all dragon variants spawn at (9837,4397).
 *   - top tribush:            heavy two-sided early warding at (3485,11470).
 *   - bot tribush:            mirror across the river diagonal (11715,3730).
 *   - river entrance bushes:  (6273,8193) / (7667,4916) — early-ward
 *     hotspots on the top/bot sides of mid river.
 *   - mid pixel bush:         (7600,7600) — map center, on the river line.
 *
 * All polygons are rectangles (axis-aligned first cut) with a `name`.
 * Positions are plain { x, z } objects. `pointInPolygon` checks a rect by
 * name; `closestDistanceToPolygon` gives the distance from a point to the
 * nearest edge (used for the "in/adjacent to the bush" kill check).
 *
 * CHEESE_THRESHOLDS / tuning is a documented follow-up (§2.17); these
 * constants are the corrected first cut.
 */

const MAP = {
  width: 14870,
  height: 14981,
  // The river follows the blue↔red diagonal; split:
  //   blue's own half:  x + z < RIVER_SPLIT
  //   red's own half:   x + z > RIVER_SPLIT
  riverSplit: 15200,
};

// 4 jungle quadrants (own/enemy side detection for vertical jungle).
// Blue = bottom-left half (x+z < riverSplit), Red = top-right half.
// Rects are documentation-grade (isOwnHalf is the authoritative check);
// the overlap bands cover the diagonal river zone.
const JUNGLE_QUADRANTS = [
  { name: "BLUE_TOP", minX: 0, minZ: 7000, maxX: 9000, maxZ: MAP.height },
  { name: "BLUE_BOT", minX: 0, minZ: 0, maxX: 9000, maxZ: 8000 },
  { name: "RED_TOP", minX: 6000, minZ: 7000, maxX: MAP.width, maxZ: MAP.height },
  { name: "RED_BOT", minX: 6000, minZ: 0, maxX: MAP.width, maxZ: 8000 },
];

// Lane polygons (for the Lvl 2 cheese-gank "kill landed in a lane" check).
// Top lane = top-left corner region (the L where blue's top laner crosses
// the river at x≈3400, z≈12000); Bot lane = the mirrored bottom-right
// corner; Mid = the diagonal band around the river. Measured: first-blood
// kills cluster in the top corner (4962,12618), (3097,12708) and the bot
// corner (13701,3619), (13503,3922).
const LANES = [
  { name: "top", type: "corner", minX: 0, minZ: 10400, maxX: 6500, maxZ: MAP.height },
  { name: "bot", type: "corner", minX: 8300, minZ: 0, maxX: MAP.width, maxZ: 6000 },
  {
    name: "mid",
    type: "band",
    // |(x + z) - riverSplit| < bandHalfWidth
    bandHalfWidth: 2300,
  },
];

// Cheese bushes — center + radius, measured from real event data
// (Sprint 7.6; see header for the derivation).
const CHEESE_BUSHES = [
  { name: "top_tribush", x: 3485, z: 11470, radius: 900 },
  { name: "bot_tribush", x: 11715, z: 3730, radius: 900 },
  { name: "river_pixel_bush", x: 7600, z: 7600, radius: 700 },
  { name: "river_entrance_top", x: 6273, z: 8193, radius: 900 },
  { name: "river_entrance_bot", x: 7667, z: 4916, radius: 900 },
  { name: "dragon_pit", x: 9837, z: 4397, radius: 1000 },
  { name: "baron_pit", x: 5008, z: 10471, radius: 1000 },
];

// "River polygon" for the predictable-first-blood severity check: the mid
// band plus the river entrance bushes.
const RIVER_BAND_HALF_WIDTH = 2500;

function pointInRect(p, rect) {
  return (
    p.x >= rect.minX &&
    p.x <= rect.maxX &&
    p.z >= rect.minZ &&
    p.z <= rect.maxZ
  );
}

function pointInLane(p, lane) {
  if (lane.type === "corner") return pointInRect(p, lane);
  if (lane.type === "band") {
    return Math.abs(p.x + p.z - MAP.riverSplit) <= lane.bandHalfWidth;
  }
  return false;
}

function pointInRiver(p) {
  return Math.abs(p.x + p.z - MAP.riverSplit) <= RIVER_BAND_HALF_WIDTH;
}

function pointInBush(p, bush) {
  const dx = p.x - bush.x;
  const dz = p.z - bush.z;
  return dx * dx + dz * dz <= bush.radius * bush.radius;
}

function distanceToBush(p, bush) {
  const dx = p.x - bush.x;
  const dz = p.z - bush.z;
  return Math.sqrt(dx * dx + dz * dz) - bush.radius;
}

/** Which bush contains p (or null). */
function bushAt(p) {
  for (const bush of CHEESE_BUSHES) {
    if (pointInBush(p, bush)) return bush;
  }
  return null;
}

/** Which lane contains p (or null). */
function laneAt(p) {
  for (const lane of LANES) {
    if (pointInLane(p, lane)) return lane.name;
  }
  return null;
}

/** Own half check for a team: 100 = blue, 200 = red. */
function isOwnHalf(teamID, p) {
  if (teamID === 100) return p.x + p.z < MAP.riverSplit;
  if (teamID === 200) return p.x + p.z > MAP.riverSplit;
  return null;
}

/** Grid bin (0..31, 0..31) for a position on the 32×32 heatmap. */
function gridCell(p) {
  const cx = Math.min(31, Math.max(0, Math.floor((p.x / MAP.width) * 32)));
  const cz = Math.min(31, Math.max(0, Math.floor((p.z / MAP.height) * 32)));
  return { cx, cz, index: cz * 32 + cx };
}

/** Center (map units) of a 32×32 grid cell. */
function cellCenter(index) {
  const cx = index % 32;
  const cz = Math.floor(index / 32);
  return {
    x: Math.round(((cx + 0.5) / 32) * MAP.width),
    z: Math.round(((cz + 0.5) / 32) * MAP.height),
  };
}

module.exports = {
  MAP,
  JUNGLE_QUADRANTS,
  LANES,
  CHEESE_BUSHES,
  RIVER_BAND_HALF_WIDTH,
  pointInRect,
  pointInLane,
  pointInRiver,
  pointInBush,
  distanceToBush,
  bushAt,
  laneAt,
  isOwnHalf,
  gridCell,
  cellCenter,
};
