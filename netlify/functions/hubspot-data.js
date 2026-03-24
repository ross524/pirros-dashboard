/**
 * Pirros Pipeline Dashboard — HubSpot Live Data Function
 * =======================================================
 * Uses hardcoded stage IDs confirmed from Pirros HubSpot account.
 *
 * Stage ID mapping:
 *   decisionmakerboughtin = Meeting Booked
 *   166376493             = Discovery
 *   closedwon             = Scoping
 *   presentationscheduled = Validation
 *   119042476             = Commit
 *
 * Health checks:
 *   Stagnating:    No activity in 14+ days (notes_last_updated) — active pipeline only
 *   No Champion:   Discovery onwards — checks contact influence = "4 - Champion"
 *   No DM:         Validation + Commit only — checks contact influence = "6 - Decision Maker"
 *   No Next Step:  notes_next_activity_date is missing OR is before today (not today)
 *   Hygiene Gaps:  Missing amount, close date, next activity, or no contacts — shows what's missing
 *   Mtg Overdue:   Meeting Booked stage where notes_next_activity_date is before today
 */

const HUBSPOT_BASE = "https://api.hubapi.com";

const ACTIVE_STAGE_IDS = ["166376493", "closedwon", "presentationscheduled", "119042476"];
const MTG_STAGE_ID     = "decisionmakerboughtin";
const ALL_INCLUDED_IDS = [...ACTIVE_STAGE_IDS, MTG_STAGE_ID];

const STAGE_LABELS = {
  "decisionmakerboughtin": "Meeting Booked",
  "166376493":             "Discovery",
  "closedwon":             "Scoping",
  "presentationscheduled": "Validation",
  "119042476":             "Commit",
};

// No Champion: Discovery onwards (all active stages)
const CHAMPION_STAGE_IDS = ACTIVE_STAGE_IDS;

// No DM: Validation + Commit only
const DM_STAGE_IDS = ["presentationscheduled", "119042476"];

const CHAMPION_INFLUENCE = "4 - Champion";
const DM_INFLUENCE       = "6 - Decision Maker";

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

    const ownerMap = await fetchOwners(apiKey);
    const deals    = await fetchPipelineDeals(apiKey);
    const dealIds  = deals.map(d => d.id);
    const { championDealIds, dmDealIds, dealsWithContacts } = await fetchDealContactRoles(apiKey, dealIds);

    const now      = Date.now();
    const todayStr = new Date().toISOString().slice(0, 10); // "2026-03-24"

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

      // Next activity date handling
      const nextActRaw  = props.notes_next_activity_date || "";
      const nextActDate = nextActRaw ? nextActRaw.slice(0, 10) : null; // "2026-03-24"
      const nextActStr  = nextActDate ? formatDate(nextActRaw) : "N/A";

      // Is next activity overdue? Flag only if before today (not today)
      const nextActOverdue = nextActDate && nextActDate < todayStr;

      // Last activity for stagnation
      const lastActRaw  = props.notes_last_updated || "";
      const lastActStr  = lastActRaw ? formatDate(lastActRaw) : "N/A";

      const dealObj = {
        name:         props.dealname || "Untitled",
        url:          `https://app.hubspot.com/contacts/22763853/deal/${deal.id}`,
        stage:        stageLabel,
        amount:       props.amount ? `$${Number(props.amount).toLocaleString()}` : "N/A",
        close:        props.closedate ? formatDate(props.closedate) : "N/A",
        nextActivity: nextActStr,
        lastActivity: lastActStr,
        extra:        "0",
      };

      // ── Stagnating: no activity in 14+ days ───────────────────────────
      if (isActive && lastActRaw) {
        const daysSince = Math.floor((now - new Date(lastActRaw).getTime()) / 86400000);
        if (daysSince >= 14) {
          classified.stag[repName].push({ ...dealObj, extra: String(daysSince) });
        }
      } else if (isActive && !lastActRaw) {
        // Never had any activity logged
        classified.stag[repName].push({ ...dealObj, extra: "14+" });
      }

      // ── No Champion: Discovery → Commit ───────────────────────────────
      if (isActive && CHAMPION_STAGE_IDS.includes(stageId) && !championDealIds.has(String(deal.id))) {
        classified.champ[repName].push(dealObj);
      }

      // ── No DM: Validation + Commit only ───────────────────────────────
      if (isActive && DM_STAGE_IDS.includes(stageId) && !dmDealIds.has(String(deal.id))) {
        classified.dm[repName].push(dealObj);
      }

      // ── No Next Activity: missing OR overdue (not including today) ─────
      if (isActive && (!nextActDate || nextActOverdue)) {
        classified.next[repName].push({ ...dealObj, extra: nextActDate || "none" });
      }

      // ── Hygiene Gaps: show exactly what's missing ──────────────────────
      if (isActive) {
        const issues = [];
        if (!props.amount || Number(props.amount) === 0) issues.push("No amount");
        if (!props.closedate)                             issues.push("No close date");
        if (!nextActDate || nextActOverdue)               issues.push("No next step");
        if (!dealsWithContacts.has(String(deal.id)))      issues.push("No contacts");
        if (issues.length > 0) {
          classified.hyg[repName].push({ ...dealObj, extra: issues.join(" · ") });
        }
      }

      // ── Meeting Booked Overdue: ONLY flag if date is set AND is in the past
      // If no date is set, rep may have it booked outside HubSpot — don't flag
      if (isMtgBooked && nextActDate && nextActOverdue) {
        classified.mtg[repName].push({ ...dealObj, extra: nextActDate });
      }
    }

    // Summary
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
        summary, reps: REPS, repStats, data: classified,
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


async function fetchPipelineDeals(apiKey) {
  const deals = [];
  let after   = null;
  const props = [
    "dealname", "dealstage", "hubspot_owner_id", "amount", "closedate",
    "notes_next_activity_date", "notes_last_updated", "num_associated_contacts",
  ];

  while (true) {
    const body = {
      filterGroups: [{ filters: [{ propertyName: "dealstage", operator: "IN", values: ALL_INCLUDED_IDS }] }],
      properties: props,
      limit: 100,
    };
    if (after) body.after = after;

    const resp = await hubspotPost(`${HUBSPOT_BASE}/crm/v3/objects/deals/search`, body, apiKey);
    deals.push(...(resp.results || []));
    after = resp.paging?.next?.after || null;
    if (!after) break;
  }

  console.log(`Fetched ${deals.length} pipeline deals`);
  return deals;
}


async function fetchOwners(apiKey) {
  const map = {};
  try {
    const resp = await hubspotGet(`${HUBSPOT_BASE}/crm/v3/owners?limit=100`, apiKey);
    for (const owner of (resp.results || [])) {
      const name = REP_EMAILS[(owner.email || "").toLowerCase()];
      if (name) map[owner.id] = name;
    }
  } catch (e) { console.warn("Owners fetch error:", e.message); }
  return map;
}


async function fetchDealContactRoles(apiKey, dealIds) {
  const championDealIds   = new Set();
  const dmDealIds         = new Set();
  const dealsWithContacts = new Set();
  const contactIdsByDeal  = new Map();

  // Batch fetch associations
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
        const contactIds = (result.to || []).map(t => t.id).filter(Boolean);
        if (dealId && contactIds.length > 0) {
          dealsWithContacts.add(dealId);
          contactIdsByDeal.set(dealId, contactIds);
        }
      }
    } catch (e) { console.warn("Association batch error:", e.message); }
  }

  // Batch fetch influence for all contacts (paginate in batches of 100)
  const allContactIds    = [...new Set([...contactIdsByDeal.values()].flat())];
  const contactInfluence = new Map();

  console.log(`Fetching influence for ${allContactIds.length} contacts`);

  for (let i = 0; i < allContactIds.length; i += 100) {
    const batch = allContactIds.slice(i, i + 100);
    try {
      const resp = await hubspotPost(
        `${HUBSPOT_BASE}/crm/v3/objects/contacts/batch/read`,
        { inputs: batch.map(id => ({ id: String(id) })), properties: ["influence"] },
        apiKey
      );
      for (const c of (resp.results || [])) {
        const inf = (c.properties?.influence || "").trim();
        if (inf) contactInfluence.set(String(c.id), inf);
      }
    } catch (e) { console.warn(`Contact batch error (batch ${i}-${i+100}):`, e.message); }
  }

  console.log(`Found influence values for ${contactInfluence.size} contacts`);

  // Map influence to deals — use string IDs consistently
  for (const [dealId, contactIds] of contactIdsByDeal.entries()) {
    for (const cid of contactIds) {
      const inf = contactInfluence.get(String(cid)) || "";
      if (inf === CHAMPION_INFLUENCE) championDealIds.add(String(dealId));
      if (inf === DM_INFLUENCE)       dmDealIds.add(String(dealId));
    }
  }

  return { championDealIds, dmDealIds, dealsWithContacts };
}


async function hubspotGet(url, apiKey) {
  const resp = await fetch(url, {
    headers: { "Authorization": `Bearer ${apiKey}`, "Content-Type": "application/json" },
  });
  if (!resp.ok) throw new Error(`HubSpot GET ${resp.status}: ${(await resp.text()).slice(0, 300)}`);
  return resp.json();
}

async function hubspotPost(url, body, apiKey) {
  const resp = await fetch(url, {
    method: "POST",
    headers: { "Authorization": `Bearer ${apiKey}`, "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!resp.ok) throw new Error(`HubSpot POST ${resp.status}: ${(await resp.text()).slice(0, 300)}`);
  return resp.json();
}

function formatDate(iso) {
  if (!iso) return "N/A";
  return new Date(iso).toLocaleDateString("en-US", { month: "short", day: "numeric" });
}
