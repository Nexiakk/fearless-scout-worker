/**
 * Champion Name Mapper (CommonJS for worker)
 *
 * Fetches champion data from Riot Data Dragon API and builds a dynamic
 * mapping from all possible name variants to canonical champion IDs.
 *
 * Replaces the old hardcoded normalizeChampionName() approach.
 *
 * Usage:
 *   const mapper = new ChampionNameMapper();
 *   await mapper.initialize();
 *   const id = mapper.toChampionId("K'Sante"); // 'KSante'
 */

const DATA_DRAGON_BASE = 'https://ddragon.leagueoflegends.com';
const MAX_RETRIES = 3;

/**
 * Normalize a string for matching: lowercase, remove spaces, apostrophes,
 * quotes, hyphens, ampersands, periods, and other special characters.
 */
function normalize(str) {
  return str
    .toLowerCase()
    .replace(/[\s'"\-&\.]/g, '');
}

class ChampionNameMapper {
  constructor() {
    /** @type {Map<string, string>} Map from name variant -> canonical champion ID */
    this._variantToId = new Map();
    /** @type {Map<string, object>} Map from champion ID -> champion info */
    this._championInfo = new Map();
    this._initialized = false;
  }

  /**
   * Fetch champion data from Data Dragon and build the mapping.
   * @param {string} [patchVersion] - Optional patch version. If omitted, fetches latest.
   */
  async initialize(patchVersion) {
    const version = patchVersion || await this._fetchLatestPatch();
    const data = await this._fetchChampionData(version);
    this._buildMapping(data);
    this._initialized = true;
    console.log(`[ChampionNameMapper] Built mapping for ${this._championInfo.size} champions from patch ${version}`);
  }

  /**
   * Convert a champion name variant to a canonical champion ID.
   * @param {string} name - Champion name from any source
   * @returns {string|null} Canonical champion ID, or null if not found
   */
  toChampionId(name) {
    if (!name || !this._initialized) return name || null;

    const trimmed = name.trim();

    // 1. Try exact match first
    const exact = this._variantToId.get(trimmed);
    if (exact) return exact;

    // 2. Try lowercase
    const lower = trimmed.toLowerCase();
    const lowerMatch = this._variantToId.get(lower);
    if (lowerMatch) return lowerMatch;

    // 3. Try normalized (lowercase, no spaces/special chars)
    const normalized = normalize(trimmed);
    const normMatch = this._variantToId.get(normalized);
    if (normMatch) return normMatch;

    // 4. Fallback: return original
    return trimmed;
  }

  /**
   * Get display name from a canonical champion ID.
   * @param {string} championId
   * @returns {string|null}
   */
  getDisplayName(championId) {
    if (!championId) return null;
    const info = this._championInfo.get(championId);
    return info?.name || championId;
  }

  /**
   * Get champion info for a canonical champion ID.
   * @param {string} championId
   * @returns {object|null}
   */
  getChampionInfo(championId) {
    return this._championInfo.get(championId) || null;
  }

  /**
   * Check if mapper is initialized.
   */
  isInitialized() {
    return this._initialized;
  }

  // ─── Private helpers ──────────────────────────────────────────

  async _fetchLatestPatch() {
    const url = `${DATA_DRAGON_BASE}/api/versions.json`;
    for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
      try {
        const res = await fetch(url);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const versions = await res.json();
        return versions[0];
      } catch (err) {
        if (attempt === MAX_RETRIES - 1) throw err;
        await new Promise(r => setTimeout(r, 1000 * Math.pow(2, attempt)));
      }
    }
  }

  async _fetchChampionData(patchVersion) {
    const url = `${DATA_DRAGON_BASE}/cdn/${patchVersion}/data/en_US/champion.json`;
    for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
      try {
        const res = await fetch(url);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const json = await res.json();
        return json.data;
      } catch (err) {
        if (attempt === MAX_RETRIES - 1) throw err;
        await new Promise(r => setTimeout(r, 1000 * Math.pow(2, attempt)));
      }
    }
  }

  _buildMapping(data) {
    this._variantToId.clear();
    this._championInfo.clear();

    for (const [championId, championData] of Object.entries(data)) {
      const displayName = championData.name;
      const imageName = championData.image?.full?.replace('.png', '') || championId;

      this._championInfo.set(championId, {
        id: championId,
        name: displayName,
        imageName: imageName,
      });

      // Generate all possible name variants
      const variants = new Set();

      // 1. Canonical ID as-is
      variants.add(championId);

      // 2. Canonical ID lowercase
      variants.add(championId.toLowerCase());

      // 3. Display name as-is
      variants.add(displayName);

      // 4. Display name lowercase
      variants.add(displayName.toLowerCase());

      // 5. Display name normalized (lowercase, no special chars)
      variants.add(normalize(displayName));

      // Map each variant to the canonical champion ID
      for (const variant of variants) {
        const existing = this._variantToId.get(variant);
        if (!existing || existing === championId) {
          this._variantToId.set(variant, championId);
        }
      }
    }
  }
}

module.exports = { ChampionNameMapper };