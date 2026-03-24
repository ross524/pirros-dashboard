/**
 * Pirros Pipeline Dashboard — HubSpot Live Data Function
 * =======================================================
 * Uses hardcoded stage IDs from the Pirros HubSpot account to avoid
 * any label-matching issues. Stage IDs are permanent in HubSpot.
 *
 * Stage ID mapping (confirmed from HubSpot):
 *   decisionmakerboughtin = Meeting Booked
 *   166376493             = Discovery
 *   closedwon             = Scoping
 *   presentationscheduled = Validation
 *   119042476             = Commit
 *   appointmentscheduled  = Closed Won - Company  (EXCLUDED)
 *   65148438              = Lost - Company         (EXCLUDED)
 *   1179610654            = Wrong Person           (EXCLUDED)
 *   164207423             = Company DQ             (EXCLUDED)
 *   226595252             = Deal DQ                (EXCLUDED)
 *
 * Champion: contact influence property = "4 - Champion"
 * DM:       contact influence property = "6 - Decision Maker"
 */

const HUBSPOT_BASE = "https://api.hubapi.com";

// Active pipeline stage IDs (Discovery → Commit)
const ACTIVE_STAGE_IDS = ["166376493", "closedwon", "presentationscheduled", "119042476"];

// Meeting Booked stage ID
const MTG_STAGE_ID = "decisionmakerboughtin";

// All stages to include (active + meeting booked)
const ALL_INCLUDED_IDS = [...ACTIVE_STAGE_IDS, MTG_STAGE_ID];

// Human readable labels for display
const STAGE_LABELS = {
  "decisionmakerboughtin": "Meeting Booked",
  "166376493":             "Discovery",
  "closedwon":             "Scoping",
  "presentationscheduled": "Validation",
  "119042476":             "Commit",
};

// Stages where No Champion should be flagged (Discovery onwards = all active)
const CHAMPION_STAGE_IDS = ACTIVE_STAGE_IDS;

// Stages where No DM should be flagged (Scoping onwards)
const DM_STAGE_IDS = ["closedwon", "presentationscheduled", "119042476"];

// Influence property values
const CHAMPION_INFLUENCE = "4 - Champion";
const DM_INFLUENCE       = "6 - Decision Maker";

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

    // 2. Fetch all active pipeline + meeting booked deals
    const deals = await fetchPipelineDeals(apiKey);

    // 3. Fetch champion/DM/contact associations for all deals
    const dealIds = deals.map(d => d.id);
    const { championDealIds, dmDealIds, dealsWithContacts } = await fetchDealContactRoles(apiKey, dealIds);

    // 4. Classify deals into health check buckets
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
      const props      = deal.properties;
      const stageId    = props.dealstage || "";
      const stageLabel = STAGE_LABELS[stageId] || stageId;
      const ownerId    = props.hubspot_owner_id || "";
      const repName    = ownerMap[ownerId] || null;

      if (!repName || !REPS.includes(repName)) continue;

      const isMtgBooked = stageId === MTG_STAGE_ID;
      const isActive    = ACTIVE_STAGE_IDS.includes(stageId);

      if (isActive) totalActiveDeals++;

      const nextActivity = props.hs_next_activity_date
        ? formatDate(props.hs_next_activity_date) : "N/A";

      const dealObj = {
        name:         props.dealname || "Untitled",
        url:          `https://app.hubspot.com/contacts/22763853/deal/${deal.id}`,
        stage:        stageLabel,
        amount:       props.amount ? `$${Number(props.amount).toLocaleString()}` : "N/A",
        close:        props.closedate ? formatDate(props.closedate) : "N/A",
        nextActivity,
        extra:        "0",
      };

      // ── Stagnating: no activity in 14+ days (active pipeline only) ────
      if (isActive) {
        const lastAct = props.hs_last_activity_date || props.notes_last_updated;
        if (lastAct) {
          const daysSince = Math.floor((now - new Date(lastAct).getTime()) / 86400000);
          if (daysSince >= 14) {
            classified.stag[repName].push({ ...dealObj, extra: String(daysSince) });
          }
        } else {
          classified.stag[repName].push({ ...dealObj, extra: "14" });
        }
      }

      // ── No Champion: Discovery → Commit ───────────────────────────────
      if (isActive && CHAMPION_STAGE_IDS.includes(stageId) && !championDealIds.has(deal.id)) {
        classified.champ[repName].push(dealObj);
      }

      // ── No DM: Scoping → Commit ────────────────────────────────────────
      if (isActive && DM_STAGE_IDS.includes(stageId) && !dmDealIds.has(deal.id)) {
        classified.dm[repName].push(dealObj);
      }

      // ── No Next Activity Date (active only) ───────────────────────────
      if (isActive && !props.hs_next_activity_date) {
        classified.next[repName].push({ ...dealObj, extra: "-1" });
      }

      // ── Hygiene Gaps: flag once per deal, list what's missing ─────────
      if (isActive) {
        const issues = [];
        if (!props.amount || Number(props.amount) === 0) issues.push("No amount");
        if (!props.closedate)                             issues.push("No close date");
        if (!props.hs_next_activity_date)                 issues.push("No next step");
        if (!dealsWithContacts.has(deal.id))              issues.push("No contacts");
        if (issues.length > 0) {
          classified.hyg[repName].push({
            ...dealObj,
            extra: issues.join(", "),
          });
        }
      }

      // ── Meeting Booked Overdue ─────────────────────────────────────────
      if (isMtgBooked) {
        classified.mtg[repName].push({ ...dealObj, extra: "99" });
      }
    }

    // 5. Summary totals
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

    // 6. Rep scorecard
    const repStats = REPS.map(rep => ({
      name:  rep,
      total: deals.filter(d =>
        ACTIVE_STAGE_IDS.includes(d.properties.dealstage) &&
        ownerMap[d.properties.hubspot_owner_id] === rep
      ).length,
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

async function fetchPipelineDeals(apiKey) {
  // Fetch only active pipeline + meeting booked deals using stage ID filter
  const deals = [];
  let after   = null;
  const props = [
    "dealname", "dealstage", "hubspot_owner_id", "amount", "closedate",
    "hs_next_activity_date", "hs_last_activity_date", "notes_last_updated",
  ].join(",");

  while (true) {
    // Use HubSpot search API to filter by specific stage IDs — much more reliable
    const body = {
      filterGroups: [{
        filters: [{
          propertyName: "dealstage",
          operator:     "IN",
          values:       ALL_INCLUDED_IDS,
        }],
      }],
      properties: props.split(","),
      limit:       100,
    };
    if (after) body.after = after;

    const resp = await hubspotPost(
      `${HUBSPOT_BASE}/crm/v3/objects/deals/search`,
      body,
      apiKey
    );
    deals.push(...(resp.results || []));
    if (resp.paging?.next?.after) {
      after = resp.paging.next.after;
    } else {
      break;
    }
  }

  console.log(`Fetched ${deals.length} pipeline deals`);
  return deals;
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
  const championDealIds   = new Set();
  const dmDealIds         = new Set();
  const dealsWithContacts = new Set();

  // Batch fetch contact associations
  const contactIdsByDeal = new Map();

  for (let i = 0; i < dealIds.length; i += 100) {
    const batch = dealIds.slice(i, i + 100);
    try {
      const resp = await hubspotPost(
        `${HUBSPOT_BASE}/crm/v3/associations/deals/contacts/batch/read`,
        { inputs: batch.map(id => ({ id: String(id) })) },
        apiKey
      );
      for (const result of (resp.results || [])) {
        const dealId     = result.from?.id;
        if (!dealId) continue;
        const contactIds = (result.to || []).map(t => t.id).filter(Boolean);
        if (contactIds.length > 0) {
          dealsWithContacts.add(dealId);
          contactIdsByDeal.set(dealId, contactIds);
        }
      }
    } catch (e) {
      console.warn("Association batch error:", e.message);
    }
  }

  // Collect all unique contact IDs
  const allContactIds = [...new Set([...contactIdsByDeal.values()].flat())];

  // Batch fetch influence property for all contacts
  const contactInfluence = new Map();

  for (let i = 0; i < allContactIds.length; i += 100) {
    const batch = allContactIds.slice(i, i + 100);
    try {
      const resp = await hubspotPost(
        `${HUBSPOT_BASE}/crm/v3/objects/contacts/batch/read`,
        { inputs: batch.map(id => ({ id })), properties: ["influence"] },
        apiKey
      );
      for (const contact of (resp.results || [])) {
        const influence = (contact.properties?.influence || "").trim();
        if (influence) contactInfluence.set(contact.id, influence);
      }
    } catch (e) {
      console.warn("Contact batch error:", e.message);
    }
  }

  // Check each deal's contacts for champion/DM influence
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
