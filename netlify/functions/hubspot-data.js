/**
 * Pirros Pipeline Dashboard — HubSpot Live Data Function
 * =======================================================
 * Pulls live deal data from HubSpot on every page load.
 * Champion and Decision Maker are detected via the contact's
 * "Influence" property (4 - Champion, 6 - Decision Maker).
 *
 * Health checks:
 *   - Stagnating:       No activity in 14+ days (Discovery → Commit)
 *   - No Champion:      Discovery → Commit deals with no champion contact
 *   - No DM:            Scoping → Commit deals with no DM contact
 *   - No Next Activity: Missing hs_next_activity_date (Discovery → Commit)
 *   - Hygiene Gaps:     Missing amount, close date, next activity, or no contacts
 *   - Mtg Overdue:      Meeting Booked stage
 *   - Punted:           Reserved for future snapshot tracking
 *
 * Env var: HUBSPOT_API_KEY (Netlify environment variables)
 */

const HUBSPOT_BASE = "https://api.hubapi.com";

// Exact stage labels from HubSpot pipeline settings
const ACTIVE_STAGES   = ["Discovery", "Scoping", "Validation", "Commit"];
const MTG_STAGE       = "Meeting Booked";
const EXCLUDED_STAGES = ["Closed Won - Company", "Lost - Company", "Wrong Person", "Company DQ", "Deal DQ"];

// Influence property values for champion/DM detection
const CHAMPION_INFLUENCE = "4 - Champion";
const DM_INFLUENCE       = "6 - Decision Maker";

// Stages where No Champion should be flagged (Discovery onwards)
const CHAMPION_STAGES = ["Discovery", "Scoping", "Validation", "Commit"];

// Stages where No DM should be flagged (Scoping onwards)
const DM_STAGES = ["Scoping", "Validation", "Commit"];

// Rep email → display name
const REP_EMAILS = {
  "kas@pirros.com":      "Kas",
  "xander@pirros.com":   "Xander",
  "zane@pirros.com":     "Zane",
  "keenan@pirros.com":   "Keenan",
  "tommaso@pirros.com":  "Tommaso",
  "fozhan@pirros.com":   "Fozhan",
};

const REPS = ["Xander", "Keenan", "Zane", "Tommaso", "Kas", "Fozhan"];


exports.handler = async (event) => {
  const headers = {
    "Access-Control-Allow-Origin": "*",
    "Content-Type": "application/json",
  };

  try {
    const apiKey = process.env.HUBSPOT_API_KEY;
    if (!apiKey) {
      return { statusCode: 500, headers, body: JSON.stringify({ error: "HUBSPOT_API_KEY not set" }) };
    }

    // 1. Fetch owner ID → rep name mapping
    const ownerMap = await fetchOwners(apiKey);

    // 2. Fetch all deals with required properties
    const allDeals = await fetchAllDeals(apiKey);

    // 3. Filter to active pipeline + meeting booked only
    const deals = allDeals.filter(d => {
      const stage = (d.properties.dealstage_label || "").trim();
      const excluded = EXCLUDED_STAGES.some(s => stage.toLowerCase() === s.toLowerCase());
      const active   = ACTIVE_STAGES.some(s => stage.toLowerCase() === s.toLowerCase());
      const mtg      = stage.toLowerCase() === MTG_STAGE.toLowerCase();
      return !excluded && (active || mtg);
    });

    // 4. For each deal, fetch associated contacts and their influence values
    //    This tells us which deals have a champion and/or decision maker set
    const dealIds = deals.map(d => d.id);
    const { championDealIds, dmDealIds, dealsWithContacts } = await fetchDealContactRoles(apiKey, dealIds);

    // 5. Classify deals into health check buckets
    const now = Date.now();
    const classified = {
      stag: {}, champ: {}, dm: {},
      next: {}, hyg:   {}, mtg: {}, punt: {},
    };
    REPS.forEach(rep => {
      Object.keys(classified).forEach(k => { classified[k][rep] = []; });
    });

    let totalActiveDeals = 0;

    for (const deal of deals) {
      const props    = deal.properties;
      const stage    = (props.dealstage_label || "").trim();
      const ownerId  = props.hubspot_owner_id || "";
      const repName  = ownerMap[ownerId] || null;

      if (!repName || !REPS.includes(repName)) continue;

      const isMtgBooked  = stage.toLowerCase() === MTG_STAGE.toLowerCase();
      const isActive     = ACTIVE_STAGES.some(s => stage.toLowerCase() === s.toLowerCase());
      const inChampStage = CHAMPION_STAGES.some(s => stage.toLowerCase() === s.toLowerCase());
      const inDMStage    = DM_STAGES.some(s => stage.toLowerCase() === s.toLowerCase());

      if (isActive) totalActiveDeals++;

      const nextActivity = props.hs_next_activity_date
        ? formatDate(props.hs_next_activity_date) : "N/A";

      const dealObj = {
        name:         props.dealname || "Untitled",
        url:          `https://app.hubspot.com/contacts/22763853/deal/${deal.id}`,
        stage,
        amount:       props.amount ? `$${Number(props.amount).toLocaleString()}` : "N/A",
        close:        props.closedate ? formatDate(props.closedate) : "N/A",
        nextActivity,
        extra:        "0",
      };

      // ── Stagnating: no activity in 14+ days ───────────────────────────
      if (isActive) {
        const lastAct = props.hs_last_activity_date || props.notes_last_updated;
        if (lastAct) {
          const daysSince = Math.floor((now - new Date(lastAct).getTime()) / 86400000);
          if (daysSince >= 14) {
            classified.stag[repName].push({ ...dealObj, extra: String(daysSince) });
          }
        } else {
          // No activity date at all — flag with max urgency
          classified.stag[repName].push({ ...dealObj, extra: "14" });
        }
      }

      // ── No Champion: Discovery → Commit with no champion contact ──────
      if (isActive && inChampStage && !championDealIds.has(deal.id)) {
        classified.champ[repName].push(dealObj);
      }

      // ── No Decision Maker: Scoping → Commit with no DM contact ────────
      if (isActive && inDMStage && !dmDealIds.has(deal.id)) {
        classified.dm[repName].push(dealObj);
      }

      // ── No Next Activity Date ──────────────────────────────────────────
      if (isActive && !props.hs_next_activity_date) {
        classified.next[repName].push({ ...dealObj, extra: "-1" });
      }

      // ── Hygiene Gaps: missing amount, close date, next activity, contacts
      if (isActive) {
        const missingAmount   = !props.amount || Number(props.amount) === 0;
        const missingClose    = !props.closedate;
        const missingNext     = !props.hs_next_activity_date;
        const missingContacts = !dealsWithContacts.has(deal.id);
        if (missingAmount || missingClose || missingNext || missingContacts) {
          classified.hyg[repName].push({
            ...dealObj,
            extra: [
              missingAmount   ? "No amount"   : null,
              missingClose    ? "No close date" : null,
              missingNext     ? "No next step" : null,
              missingContacts ? "No contacts"  : null,
            ].filter(Boolean).join(", "),
          });
        }
      }

      // ── Meeting Booked Overdue ─────────────────────────────────────────
      if (isMtgBooked) {
        classified.mtg[repName].push({ ...dealObj, extra: "99" });
      }
    }

    // 6. Summary totals
    const summary = {
      totalDeals:   totalActiveDeals,
      stagnating:   REPS.reduce((n, r) => n + classified.stag[r].length,  0),
      noChampion:   REPS.reduce((n, r) => n + classified.champ[r].length, 0),
      noDM:         REPS.reduce((n, r) => n + classified.dm[r].length,    0),
      noNextStep:   REPS.reduce((n, r) => n + classified.next[r].length,  0),
      hygieneGaps:  REPS.reduce((n, r) => n + classified.hyg[r].length,   0),
      mtgOverdue:   REPS.reduce((n, r) => n + classified.mtg[r].length,   0),
    };
    summary.totalIssues = summary.stagnating + summary.noChampion + summary.noDM +
                          summary.noNextStep + summary.hygieneGaps + summary.mtgOverdue;

    // 7. Rep scorecard
    const repStats = REPS.map(rep => ({
      name:  rep,
      total: deals.filter(d => {
        const s = (d.properties.dealstage_label || "").trim();
        const o = d.properties.hubspot_owner_id || "";
        return ownerMap[o] === rep &&
          ACTIVE_STAGES.some(a => s.toLowerCase() === a.toLowerCase());
      }).length,
      stag:         classified.stag[rep].length,
      champ:        classified.champ[rep].length,
      dm:           classified.dm[rep].length,
      next:         classified.next[rep].length,
      hyg:          classified.hyg[rep].length,
      mtg:          classified.mtg[rep].length,
      punt:         0,
      totalFlagged: classified.stag[rep].length  + classified.champ[rep].length +
                    classified.dm[rep].length     + classified.next[rep].length +
                    classified.hyg[rep].length    + classified.mtg[rep].length,
    }));

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        lastUpdated: new Date().toISOString(),
        summary,
        reps: REPS,
        repStats,
        data: classified,
      }),
    };

  } catch (err) {
    console.error("HubSpot data fetch error:", err);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ error: err.message }),
    };
  }
};


// ─── HubSpot API Helpers ──────────────────────────────────────────────────────

async function fetchAllDeals(apiKey) {
  const deals = [];
  let after   = null;
  const props = [
    "dealname", "dealstage", "hubspot_owner_id", "amount", "closedate",
    "hs_next_activity_date", "hs_last_activity_date", "notes_last_updated",
    "hs_lastmodifieddate",
  ].join(",");

  while (true) {
    const url = `${HUBSPOT_BASE}/crm/v3/objects/deals?limit=100&properties=${props}` +
                (after ? `&after=${after}` : "");
    const resp = await hubspotGet(url, apiKey);
    deals.push(...(resp.results || []));
    if (resp.paging?.next?.after) {
      after = resp.paging.next.after;
    } else {
      break;
    }
  }

  // Map internal stage IDs → human readable labels
  const stageMap = await fetchStageLabels(apiKey);
  deals.forEach(d => {
    d.properties.dealstage_label = stageMap[d.properties.dealstage] || d.properties.dealstage || "";
  });

  return deals;
}


async function fetchStageLabels(apiKey) {
  const map = {};

  // Try v3 pipelines endpoint first
  try {
    const resp = await hubspotGet(`${HUBSPOT_BASE}/crm/v3/pipelines/deals`, apiKey);
    for (const pipeline of (resp.results || [])) {
      for (const stage of (pipeline.stages || [])) {
        map[stage.id] = stage.label;
      }
    }
    if (Object.keys(map).length > 0) {
      console.log(`Loaded ${Object.keys(map).length} stage labels from v3 pipelines`);
      return map;
    }
  } catch (e) {
    console.warn("v3 pipelines endpoint failed:", e.message);
  }

  // Fallback: try CRM v3 properties endpoint to get dealstage enum values
  try {
    const resp = await hubspotGet(
      `${HUBSPOT_BASE}/crm/v3/properties/deals/dealstage`,
      apiKey
    );
    for (const option of (resp.options || [])) {
      map[option.value] = option.label;
    }
    if (Object.keys(map).length > 0) {
      console.log(`Loaded ${Object.keys(map).length} stage labels from properties endpoint`);
      return map;
    }
  } catch (e) {
    console.warn("Properties endpoint failed:", e.message);
  }

  // Last resort: use the deals properties API
  try {
    const resp = await hubspotGet(
      `${HUBSPOT_BASE}/properties/v2/deals/properties/named/dealstage`,
      apiKey
    );
    for (const option of (resp.options || [])) {
      map[option.value] = option.label;
    }
    console.log(`Loaded ${Object.keys(map).length} stage labels from v2 properties`);
  } catch (e) {
    console.warn("v2 properties endpoint failed:", e.message);
  }

  return map;
}


async function fetchOwners(apiKey) {
  const map = {};
  try {
    const resp = await hubspotGet(`${HUBSPOT_BASE}/crm/v3/owners?limit=100`, apiKey);
    for (const owner of (resp.results || [])) {
      const email = (owner.email || "").toLowerCase();
      const name  = REP_EMAILS[email];
      if (name) map[owner.id] = name;
    }
  } catch (e) {
    console.warn("Could not fetch owners:", e.message);
  }
  return map;
}


async function fetchDealContactRoles(apiKey, dealIds) {
  // For each deal, fetch associated contacts and check their influence property.
  // Champion = influence "4 - Champion"
  // DM       = influence "6 - Decision Maker"
  // We also track which deals have any contacts at all (for hygiene check).

  const championDealIds  = new Set();
  const dmDealIds        = new Set();
  const dealsWithContacts = new Set();

  // Step 1: batch fetch contact associations for all deals
  const contactIdsByDeal = new Map(); // dealId → [contactId, ...]

  for (let i = 0; i < dealIds.length; i += 100) {
    const batch = dealIds.slice(i, i + 100);
    try {
      const resp = await hubspotPost(
        `${HUBSPOT_BASE}/crm/v3/associations/deals/contacts/batch/read`,
        { inputs: batch.map(id => ({ id: String(id) })) },
        apiKey
      );
      for (const result of (resp.results || [])) {
        const dealId = result.from?.id;
        if (!dealId) continue;
        const contactIds = (result.to || []).map(t => t.id).filter(Boolean);
        if (contactIds.length > 0) {
          dealsWithContacts.add(dealId);
          contactIdsByDeal.set(dealId, contactIds);
        }
      }
    } catch (e) {
      console.warn("Association batch fetch error:", e.message);
    }
  }

  // Step 2: collect all unique contact IDs across all deals
  const allContactIds = [...new Set([...contactIdsByDeal.values()].flat())];

  // Step 3: batch fetch contact influence properties
  const contactInfluence = new Map(); // contactId → influence value

  for (let i = 0; i < allContactIds.length; i += 100) {
    const batch = allContactIds.slice(i, i + 100);
    try {
      const resp = await hubspotPost(
        `${HUBSPOT_BASE}/crm/v3/objects/contacts/batch/read`,
        {
          inputs:     batch.map(id => ({ id })),
          properties: ["influence"],
        },
        apiKey
      );
      for (const contact of (resp.results || [])) {
        const influence = (contact.properties?.influence || "").trim();
        if (influence) contactInfluence.set(contact.id, influence);
      }
    } catch (e) {
      console.warn("Contact batch fetch error:", e.message);
    }
  }

  // Step 4: for each deal, check if any associated contact has champion/DM influence
  for (const [dealId, contactIds] of contactIdsByDeal.entries()) {
    for (const cid of contactIds) {
      const influence = contactInfluence.get(cid) || "";
      if (influence === CHAMPION_INFLUENCE) championDealIds.add(dealId);
      if (influence === DM_INFLUENCE)       dmDealIds.add(dealId);
    }
  }

  return { championDealIds, dmDealIds, dealsWithContacts };
}


async function hubspotGet(url, apiKey) {
  const resp = await fetch(url, {
    headers: {
      "Authorization": `Bearer ${apiKey}`,
      "Content-Type":  "application/json",
    },
  });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`HubSpot GET ${resp.status}: ${text.slice(0, 300)}`);
  }
  return resp.json();
}


async function hubspotPost(url, body, apiKey) {
  const resp = await fetch(url, {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${apiKey}`,
      "Content-Type":  "application/json",
    },
    body: JSON.stringify(body),
  });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`HubSpot POST ${resp.status}: ${text.slice(0, 300)}`);
  }
  return resp.json();
}


function formatDate(iso) {
  if (!iso) return "N/A";
  return new Date(iso).toLocaleDateString("en-US", { month: "short", day: "numeric" });
}
