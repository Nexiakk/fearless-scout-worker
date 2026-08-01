/**
 * source-split.cjs
 *
 * Sprint 5.5 — Data source provenance (worker side).
 *
 * Computes the per-player Grid vs Leaguepedia source split for competitive
 * games and writes it to Firestore as `scoutedPlayers/{playerId}/sources`
 * (the "last-known source split" cache the UI renders instantly).
 *
 * Sources are PEER-LEVEL: both Grid and Leaguepedia contribute rows and
 * there is no primary/fallback label anywhere in the output (§2.3a).
 *
 * Unique-game dedup rule: a game present in both sources counts ONCE in
 * totals. Dedup key: (champion, game_date, opponent, win).
 *   - total      — unique games
 *   - grid / lp  — that source's own rows (a both-source game contributes
 *                  one row to each side)
 *   - breakdown  — { gridOnly, lpOnly, both } over unique games
 *
 * The SQL builders and shape functions are mirrored in the frontend
 * (src/services/scouting/sourceSplit.js); scripts/test-sprint55-sources.js
 * cross-validates that both implementations produce identical output on
 * the same rows, so the two copies can't drift.
 *
 * firebase-admin is intentionally required lazily (inside the write
 * function) so this module can be required by the test harness without
 * the worker's full dependency tree installed.
 */

const { createClient } = require('@libsql/client')

// ─── SQL builders ───────────────────────────────────────────────────────

/**
 * Per-champion source breakdown over UNIQUE games.
 * Inner query collapses both-source duplicates to one game per
 * (champion, game_date, opponent, win); outer query aggregates per
 * champion: total (unique), per-source games/wins, and the overlap
 * breakdown { gridOnly, lpOnly, both }.
 */
function buildChampionSourcesSql({ playerId, workspaceId, startDate = null, endDate = null }) {
  const conditions = ['player_id = ?', 'workspace_id = ?']
  const args = [playerId, workspaceId]
  if (startDate) { conditions.push('game_date >= ?'); args.push(startDate) }
  if (endDate) { conditions.push('game_date <= ?'); args.push(endDate) }

  return {
    sql: `SELECT champion,
                 COUNT(*) AS total_games,
                 SUM(win) AS total_wins,
                 SUM(is_grid) AS grid_games,
                 SUM(is_grid * win) AS grid_wins,
                 SUM(is_lp) AS lp_games,
                 SUM(is_lp * win) AS lp_wins,
                 SUM(CASE WHEN is_grid = 1 AND is_lp = 1 THEN 1 ELSE 0 END) AS both_games,
                 SUM(CASE WHEN is_grid = 1 AND is_lp = 0 THEN 1 ELSE 0 END) AS grid_only,
                 SUM(CASE WHEN is_grid = 0 AND is_lp = 1 THEN 1 ELSE 0 END) AS lp_only
          FROM (
            SELECT champion, game_date, opponent, win,
                   MAX(CASE WHEN source = 'grid' THEN 1 ELSE 0 END) AS is_grid,
                   MAX(CASE WHEN source = 'leaguepedia' THEN 1 ELSE 0 END) AS is_lp
            FROM competitive_games
            WHERE ${conditions.join(' AND ')}
            GROUP BY champion, game_date, opponent, win
          ) t
          GROUP BY champion
          ORDER BY total_games DESC, champion`,
    args,
  }
}

/**
 * Aggregate stats per source, over each source's own rows.
 * Returns one row per source ('grid' | 'leaguepedia').
 */
function buildStatsBySourceSql({ playerId, workspaceId, startDate = null, endDate = null }) {
  const conditions = ['player_id = ?', 'workspace_id = ?']
  const args = [playerId, workspaceId]
  if (startDate) { conditions.push('game_date >= ?'); args.push(startDate) }
  if (endDate) { conditions.push('game_date <= ?'); args.push(endDate) }

  return {
    sql: `SELECT source,
                 COUNT(*) AS games,
                 SUM(CASE WHEN win = 1 THEN 1 ELSE 0 END) AS wins,
                 ROUND(AVG(kills), 1) AS avg_kills,
                 ROUND(AVG(deaths), 1) AS avg_deaths,
                 ROUND(AVG(assists), 1) AS avg_assists,
                 ROUND(AVG(cs), 0) AS avg_cs,
                 ROUND(AVG(gold), 0) AS avg_gold,
                 ROUND(AVG(vision), 0) AS avg_vision,
                 ROUND(AVG(damage), 0) AS avg_damage,
                 ROUND(AVG(duration), 0) AS avg_duration
          FROM competitive_games
          WHERE ${conditions.join(' AND ')}
          GROUP BY source`,
    args,
  }
}

/**
 * Aggregate stats over UNIQUE games. For games present in both sources the
 * canonical row is the Grid row (richer data); the LP row is only used for
 * games Grid doesn't cover.
 */
function buildUniqueGamesTotalsSql({ playerId, workspaceId, startDate = null, endDate = null }) {
  const conditions = ['player_id = ?', 'workspace_id = ?']
  const args = [playerId, workspaceId]
  if (startDate) { conditions.push('game_date >= ?'); args.push(startDate) }
  if (endDate) { conditions.push('game_date <= ?'); args.push(endDate) }

  return {
    sql: `SELECT COUNT(*) AS games,
                 SUM(CASE WHEN win = 1 THEN 1 ELSE 0 END) AS wins,
                 ROUND(AVG(kills), 1) AS avg_kills,
                 ROUND(AVG(deaths), 1) AS avg_deaths,
                 ROUND(AVG(assists), 1) AS avg_assists,
                 ROUND(AVG(cs), 0) AS avg_cs,
                 ROUND(AVG(gold), 0) AS avg_gold,
                 ROUND(AVG(vision), 0) AS avg_vision,
                 ROUND(AVG(damage), 0) AS avg_damage,
                 ROUND(AVG(duration), 0) AS avg_duration
          FROM (
            SELECT *, ROW_NUMBER() OVER (
              PARTITION BY champion, game_date, opponent, win
              ORDER BY CASE WHEN source = 'grid' THEN 0 ELSE 1 END
            ) AS rn
            FROM competitive_games
            WHERE ${conditions.join(' AND ')}
          ) t
          WHERE rn = 1`,
    args,
  }
}

// ─── Shape functions (mirrored in frontend sourceSplit.js) ──────────────

const toNum = (v) => (v === null || v === undefined ? 0 : Number(v))

function shapeChampionSources(rows) {
  return (rows || []).map((r) => ({
    champion: r.champion,
    grid: { games: toNum(r.grid_games), wins: toNum(r.grid_wins) },
    lp: { games: toNum(r.lp_games), wins: toNum(r.lp_wins) },
    total: { games: toNum(r.total_games), wins: toNum(r.total_wins) },
    breakdown: {
      gridOnly: toNum(r.grid_only),
      lpOnly: toNum(r.lp_only),
      both: toNum(r.both_games),
    },
  }))
}

function emptyStats() {
  return {
    games: 0, wins: 0, winrate: 0,
    avgKills: 0, avgDeaths: 0, avgAssists: 0, avgKda: 0,
    avgCs: 0, avgGold: 0, avgVision: 0, avgDamage: 0, avgDuration: 0,
  }
}

function shapeStatGroup(r) {
  if (!r) return null
  const games = toNum(r.games)
  const wins = toNum(r.wins)
  const avgKills = toNum(r.avg_kills)
  const avgDeaths = toNum(r.avg_deaths)
  const avgAssists = toNum(r.avg_assists)
  return {
    games,
    wins,
    winrate: games > 0 ? Math.round((wins / games) * 1000) / 10 : 0,
    avgKills,
    avgDeaths,
    avgAssists,
    avgKda: avgDeaths > 0 ? Math.round(((avgKills + avgAssists) / avgDeaths) * 10) / 10 : Math.round((avgKills + avgAssists) * 10) / 10,
    avgCs: toNum(r.avg_cs),
    avgGold: toNum(r.avg_gold),
    avgVision: toNum(r.avg_vision),
    avgDamage: toNum(r.avg_damage),
    avgDuration: toNum(r.avg_duration),
  }
}

function shapeStatsBySource(rowsBySource, totalRow) {
  const bySource = {}
  for (const r of rowsBySource || []) {
    if (r && r.source) bySource[r.source] = shapeStatGroup(r)
  }
  return {
    grid: bySource.grid || emptyStats(),
    lp: bySource['leaguepedia'] || emptyStats(),
    total: shapeStatGroup(totalRow) || emptyStats(),
  }
}

function computeTotalsFromChampions(perChampion) {
  const totals = { gridGames: 0, lpGames: 0, both: 0, gridOnly: 0, lpOnly: 0, totalUnique: 0 }
  for (const c of perChampion || []) {
    totals.gridGames += c.grid.games
    totals.lpGames += c.lp.games
    totals.both += c.breakdown.both
    totals.gridOnly += c.breakdown.gridOnly
    totals.lpOnly += c.breakdown.lpOnly
    totals.totalUnique += c.total.games
  }
  return totals
}

// ─── Firestore doc payload (pure) ───────────────────────────────────────

/**
 * Build the `scoutedPlayers/{playerId}/sources` doc payload from raw SQL
 * result rows. Pure — no Firestore/Turso side effects, so the test
 * harness can verify the exact shape that gets persisted.
 */
function buildSourceSplitDoc({ championRows, sourceRows, totalRow, scope = null }) {
  const perChampion = shapeChampionSources(championRows)
  return {
    perChampion,
    stats: shapeStatsBySource(sourceRows, totalRow),
    totals: computeTotalsFromChampions(perChampion),
    scope,
  }
}

// ─── Firestore write ────────────────────────────────────────────────────

/**
 * Compute and write the source-split cache doc for every player.
 *
 * @param {Object} params
 * @param {import('@libsql/client').Client} params.turso
 * @param {import('firebase-admin').firestore.Firestore} [params.db] — Firestore
 *   (from ./firebase-admin). When null, only computes and logs (no writes).
 * @param {string} params.workspaceId
 * @param {Array<{playerId: string}>} params.players
 * @param {Object} [params.scope] — { startDate, endDate } of the scan window
 * @returns {Promise<{written: number, skipped: number}>}
 */
async function writePlayerSourceSplits({ turso, db, workspaceId, players, scope = null }) {
  if (!turso || !Array.isArray(players) || players.length === 0) {
    return { written: 0, skipped: Array.isArray(players) ? players.length : 0 }
  }

  // Lazy require: keeps this module loadable without firebase-admin installed
  let admin = null
  if (db) {
    try {
      const { getAdmin } = require('./firebase-admin')
      admin = getAdmin()
    } catch (err) {
      console.warn(`[SourceSplit] firebase-admin unavailable (non-fatal): ${err.message}`)
    }
  }

  let written = 0
  for (const player of players) {
    const playerId = player.playerId || player.id
    if (!playerId) continue
    try {
      const championSql = buildChampionSourcesSql({ playerId, workspaceId, ...scope })
      const sourceSql = buildStatsBySourceSql({ playerId, workspaceId, ...scope })
      const totalsSql = buildUniqueGamesTotalsSql({ playerId, workspaceId, ...scope })

      const [championRes, sourceRes, totalsRes] = await Promise.all([
        turso.execute(championSql),
        turso.execute(sourceSql),
        turso.execute(totalsSql),
      ])

      const doc = buildSourceSplitDoc({
        championRows: championRes.rows,
        sourceRows: sourceRes.rows,
        totalRow: totalsRes.rows[0],
        scope,
      })

      if (db && admin) {
        await db
          .collection('workspaces')
          .doc(workspaceId)
          .collection('scoutedPlayers')
          .doc(playerId)
          .set({ ...doc, computedAt: admin.firestore.FieldValue.serverTimestamp() }, { merge: true })
      } else {
        console.log(
          `[SourceSplit] ${playerId}: ${doc.totals.totalUnique} unique games ` +
          `(G ${doc.totals.gridGames} / LP ${doc.totals.lpGames}, ` +
          `G+LP ${doc.totals.both}) — no Firestore, computed only`,
        )
      }
      written++
    } catch (err) {
      console.warn(`[SourceSplit] Failed for ${playerId}: ${err.message}`)
    }
  }
  return { written, skipped: players.length - written }
}

module.exports = {
  buildChampionSourcesSql,
  buildStatsBySourceSql,
  buildUniqueGamesTotalsSql,
  shapeChampionSources,
  shapeStatsBySource,
  computeTotalsFromChampions,
  buildSourceSplitDoc,
  writePlayerSourceSplits,
}
