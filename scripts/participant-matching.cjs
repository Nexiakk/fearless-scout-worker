/**
 * participant-matching.cjs
 *
 * Sprint 5.4 — Grid team import participant matching.
 *
 * Grid gives us a per-game roster with GRID player IDs via the Series State
 * API (`seriesState → games → teams → players { id, name, character { name } }`),
 * while the Riot end-state summary file gives per-participant side +
 * riotIdGameName. This module bridges the two and matches participants to
 * our scouted players:
 *
 *   1. participant → roster entry   (same side, normalized-name containment,
 *                                    falling back to champion + side)
 *   2. roster entry → our player    (exact gridPlayerId when set)
 *   3. fallback                     (normalized name match for players without
 *                                    a gridPlayerId)
 *
 * Pure module — no I/O, no env access — so the matching logic is directly
 * testable against real Grid data.
 */

const MIN_NAME_MATCH_LENGTH = 3;

/**
 * Normalize a name for matching: lowercase, strip spaces/underscores/
 * hyphens/ampersands/dots and the like. "WLG Mietek" → "wlgmietek",
 * "Mietek" → "mietek".
 */
function normalizeName(str) {
  return String(str || "")
    .toLowerCase()
    .replace(/[\s_'\-&\.#@]/g, "");
}

/**
 * Build a per-game roster map from a seriesState payload.
 *
 * @param {object} seriesState — raw seriesState GraphQL response
 * @param {object} [championMapper] — ChampionNameMapper instance (canonicalizes
 *                                    Grid character names to Riot champion IDs)
 * @returns {Map<string, Array<{gridPlayerId, name, championId, side}>>}
 *          keyed by game.id
 */
function buildGameRosterMap(seriesState, championMapper) {
  const map = new Map();
  if (!seriesState || !Array.isArray(seriesState.games)) return map;

  for (const game of seriesState.games) {
    const roster = [];
    for (const team of game.teams || []) {
      const side = String(team.side || "").toLowerCase();
      for (const p of team.players || []) {
        const rawChampion = p.character?.name;
        const championId = championMapper
          ? championMapper.toChampionId(rawChampion)
          : rawChampion || null;
        roster.push({
          gridPlayerId: String(p.id),
          name: p.name || "",
          championId,
          side,
        });
      }
    }
    map.set(game.id, roster);
  }
  return map;
}

/**
 * Find the roster entry for a participant: same side first, then
 * normalized-name containment, then champion + side.
 *
 * @returns {object|null} roster entry { gridPlayerId, name, championId, side }
 */
function matchRosterEntry(participant, roster) {
  if (!participant || !Array.isArray(roster) || roster.length === 0) return null;

  const pSide = String(participant.side || "").toLowerCase();
  const sameSide = roster.filter((r) => r.side === pSide);
  if (sameSide.length === 0) return null;

  // 1. Name containment — "WLG Mietek" contains "Mietek". Prefer exact.
  if (participant.playerName) {
    const pNameNorm = normalizeName(participant.playerName);
    const nameCandidates = sameSide.filter((r) => {
      const rn = normalizeName(r.name);
      return (
        rn.length >= MIN_NAME_MATCH_LENGTH &&
        (pNameNorm === rn || pNameNorm.includes(rn))
      );
    });
    if (nameCandidates.length === 1) return nameCandidates[0];
    // Multiple name hits — prefer the longest roster name (most specific).
    if (nameCandidates.length > 1) {
      return nameCandidates.sort(
        (a, b) => normalizeName(b.name).length - normalizeName(a.name).length,
      )[0];
    }
  }

  // 2. Champion + side (mirror picks are rare; names already tried above)
  if (participant.championName) {
    const pChamp = String(participant.championName).toLowerCase();
    const champHits = sameSide.filter(
      (r) => r.championId && String(r.championId).toLowerCase() === pChamp,
    );
    if (champHits.length === 1) return champHits[0];
  }

  // 3. Single player on the side — last resort
  if (sameSide.length === 1) return sameSide[0];

  return null;
}

/**
 * Match a roster entry to one of our scouted players by exact gridPlayerId.
 *
 * @returns {object|null} the player object, or null when no match
 */
function matchPlayerByGridId(rosterEntry, players) {
  if (!rosterEntry || !rosterEntry.gridPlayerId) return null;
  const gid = String(rosterEntry.gridPlayerId);
  return (
    players.find(
      (pl) => pl.gridPlayerId && String(pl.gridPlayerId) === gid,
    ) || null
  );
}

/**
 * Fallback name match against riotId / name / playerOverrideName.
 * Same containment semantics as the roster match.
 */
function matchPlayerByName(participant, players) {
  if (!participant || !participant.playerName) return null;
  const pNameNorm = normalizeName(participant.playerName);

  for (const pl of players) {
    const names = [pl.riotId, pl.name, pl.playerOverrideName]
      .filter(Boolean)
      .map(normalizeName);
    const hit = names.some(
      (n) =>
        n.length >= MIN_NAME_MATCH_LENGTH &&
        (pNameNorm === n || pNameNorm.includes(n)),
    );
    if (hit) return pl;
  }
  return null;
}

/**
 * Full pipeline: participant → roster entry → our player.
 * Grid player ID match when the player has `gridPlayerId`, name match
 * otherwise (and when the roster mapping itself fails).
 *
 * @param {object} participant — from parseRiotSummaryFile
 * @param {Array|null} gameRoster — roster entries for this game
 * @param {Array} players — our scouted players (PLAYERS_JSON)
 * @returns {object|null} matched player object
 */
function matchParticipantToPlayer(participant, gameRoster, players) {
  const rosterEntry = matchRosterEntry(participant, gameRoster);
  const byGridId = matchPlayerByGridId(rosterEntry, players);
  if (byGridId) return byGridId;
  return matchPlayerByName(participant, players);
}

/**
 * Find the opponent's champion at the same role on the other side.
 * Fills `competitive_games.opponent_champion` for Grid rows (previously NULL).
 */
function findOpponentChampion(participant, participants) {
  if (!participant || !Array.isArray(participants)) return null;
  const pSide = String(participant.side || "").toLowerCase();
  const pRole = String(participant.role || "").toLowerCase();
  if (!pRole) return null;

  const opponent = participants.find(
    (o) =>
      o &&
      o !== participant &&
      String(o.side || "").toLowerCase() !== pSide &&
      String(o.role || "").toLowerCase() === pRole,
  );
  return opponent?.championName || null;
}

module.exports = {
  normalizeName,
  buildGameRosterMap,
  matchRosterEntry,
  matchPlayerByGridId,
  matchPlayerByName,
  matchParticipantToPlayer,
  findOpponentChampion,
};
