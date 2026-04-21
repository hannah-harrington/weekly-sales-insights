(function() {
'use strict';

var DATA = null;
var WEEKS = [];
var CURRENT_SELLER = null;
var CURRENT_COACH = null;
var ACTIVE_FILTER = 'all';
var ACTIVE_REGION = 'all';
var CURRENT_WEEK = null;
var IAP_EMAIL = null;
var IDENTITY_ROLE = null;
var IDENTITY_DATA = null;
var LATEST_WEEK = null;
var modalTrigger = null;

var $ = function(s) { return document.querySelector(s); };
var $$ = function(s) { return document.querySelectorAll(s); };
var STORAGE_KEY = 'sales-insights-seller';

function esc(s) {
  if (s == null) return '';
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

/**
 * Render a colour-coded seniority tier badge.
 */
function renderSeniority(tier) {
  if (!tier) return '';
  var map = {
    'C-Suite':  ['csuite',   '★ C-Suite'],
    'VP':       ['vp',       'VP'],
    'Director': ['director', 'Director'],
    'Manager':  ['manager',  'Manager'],
    'IC':       ['ic',       'IC'],
  };
  var entry = map[tier];
  if (!entry) return esc(tier);
  return '<span class="seniority-badge seniority-' + entry[0] + '">' + entry[1] + '</span>';
}

/**
 * Render SFDC enrichment badges for a row.
 * Returns an HTML string for the badge strip, or '' if no sfdc data.
 */
function renderSfdcBadges(sfdc) {
  if (!sfdc) return '';
  var badges = '';

  // Deal status
  if (sfdc.has_deal) {
    var oppLabel = 'Open Deal';
    if (sfdc.open_opps && sfdc.open_opps.length > 0) {
      var o = sfdc.open_opps[0];
      oppLabel = 'Open Deal';
      if (o.stage) oppLabel += ' · ' + o.stage;
      if (o.acv_str) oppLabel += ' · ' + o.acv_str;
      if (sfdc.open_opp_count > 1) oppLabel += ' (+' + (sfdc.open_opp_count - 1) + ' more)';
    }
    badges += '<span class="sfdc-badge sfdc-deal-open">🟡 ' + esc(oppLabel) + '</span>';
  } else if (sfdc.is_cold || sfdc.no_activity) {
    // Only show "No Open Deal" when paired with cold/no-activity — the combination is actionable
    badges += '<span class="sfdc-badge sfdc-deal-none">🟢 No Open Deal · Net New</span>';
  }

  // Last activity
  if (sfdc.no_activity) {
    badges += '<span class="sfdc-badge sfdc-cold">⏱ No Activity on Record</span>';
  } else if (sfdc.days_since_activity !== null && sfdc.days_since_activity !== undefined) {
    var days = sfdc.days_since_activity;
    if (days === 0) {
      badges += '<span class="sfdc-badge sfdc-fresh">⏱ Active Today</span>';
    } else if (days <= 14) {
      badges += '<span class="sfdc-badge sfdc-fresh">⏱ Touched ' + days + 'd ago</span>';
    } else if (days <= 60) {
      badges += '<span class="sfdc-badge sfdc-warm">⏱ Last touch ' + days + 'd ago</span>';
    } else {
      badges += '<span class="sfdc-badge sfdc-cold">⏱ Cold · ' + days + 'd no contact</span>';
    }
  }

  // Engaged contacts
  if (sfdc.engaged_contact_count > 0) {
    var ctLabel = sfdc.engaged_contact_count + ' contact' + (sfdc.engaged_contact_count > 1 ? 's' : '') + ' engaged';
    if (sfdc.engaged_contact_titles && sfdc.engaged_contact_titles.length > 0) {
      ctLabel += ' incl. ' + sfdc.engaged_contact_titles.slice(0, 2).join(', ');
    }
    badges += '<span class="sfdc-badge sfdc-contacts">👤 ' + esc(ctLabel) + '</span>';
  }

  if (!badges) return '';
  return '<div class="sfdc-badges">' + badges + '</div>';
}

function filterVisibleCols(cols, rows) {
  return cols.filter(function(c) {
    return rows.some(function(r) { return r[c.key] && String(r[c.key]).trim() !== ''; });
  });
}

var TYPE_CLS = { mqa:'mqa', mqa_new:'mqa_new', hvp:'hvp', hvp_all:'hvp_all', new_people:'new_people', activity:'activity', all_mqa:'all_mqa', top_leads:'top_leads', anz_high_intent:'anz_high_intent', anz_new_people:'anz_new_people', anz_activity:'anz_activity', anz_website_visits:'anz_website_visits', intent_agentic:'intent_agentic', intent_compete:'intent_compete', intent_international:'intent_international', intent_marketing:'intent_marketing', intent_b2b:'intent_b2b', g2_intent:'g2_intent' };
var RANK_BG = ['rank-bg-1','rank-bg-2','rank-bg-3','rank-bg-4','rank-bg-5'];
var STAT_VIBES = { mqa_new:'newly qualified this week', hvp:'reconsidering Shopify', hvp_all:'browsing enterprise pages', new_people:'fresh contacts this week', activity:'actions they took', all_mqa:'your full MQA pipeline', anz_high_intent:'high intent, no sales touches', anz_new_people:'newly engaged this week', anz_activity:'recent contact activity', anz_website_visits:'website visits this week', intent_agentic:'AI & agentic commerce interest', intent_compete:'evaluating competitors', intent_international:'cross-border & global interest', intent_marketing:'marketing & growth interest', intent_b2b:'B2B & POS interest', g2_intent:'actively comparing vendors on G2' };

function badgeCls(t) { return 'badge-' + (TYPE_CLS[t] || t); }
function pillCls(t) { return 'pill-' + (TYPE_CLS[t] || t); }

function getGreeting() {
  var h = new Date().getHours();
  if (h < 12) return 'Good morning';
  if (h < 17) return 'Good afternoon';
  return 'Good evening';
}

function getFirstName() {
  if (IAP_EMAIL) {
    var parts = IAP_EMAIL.split('@')[0].split('.');
    if (parts.length > 0) return parts[0].charAt(0).toUpperCase() + parts[0].slice(1);
  }
  return '';
}

var SIGNAL_DEFS = {
  mqa_new: { q:"What's an MQA?", a:'A Marketing Qualified Account (MQA) crossed a big engagement threshold through marketing activity \u2014 200+ engagement points in the last 3 months, or multiple senior contacts each showing strong intent. These are the accounts that became marketing-qualified this week. They\u2019re warm and waiting for your outreach.' },
  hvp: { q:'What are High-Value Pages?', a:'These are accounts that previously said no (Closed Lost) but are back on Shopify Plus pages this week. They\u2019re reconsidering. The timing is perfect to re-engage.' },
  hvp_all: { q:'What is High-Value Pages (All)?', a:'All accounts visiting Shopify Plus and enterprise pages this week \u2014 not just lost opps. This is the full picture of who is actively browsing high-value content right now.' },
  new_people: { q:'Who are these new people?', a:'Brand-new contacts from key titles (marketing, ecommerce, C-suite) who engaged with Shopify for the first time this week. Fresh leads who weren\u2019t on your radar before.' },
  activity: { q:'What are these activities?', a:'The specific things (webinars, events, email clicks, form fills) that drove engagement from new contacts. Use these details to personalize your outreach and start real conversations.' },
  all_mqa: { q:'What is All MQA?', a:'A snapshot of every account in your book that currently has MQA status. Not just what moved this week \u2014 your entire MQA universe. Use this to prioritize across your full pipeline.' },
  top_leads: { q:'What are Top Leads?', a:'Your highest-priority contacts from LinkedIn Sales Navigator, ranked by account fit quality. Leads at accounts with active Demandbase intent signals this week are highlighted \u2014 these are the people to reach out to right now.' },
  anz_high_intent: { q:'What are High Intent Accounts?', a:'Accounts showing strong engagement signals in the last 7 days that have not been contacted by sales yet. These are your warmest untouched accounts \u2014 prioritize outreach here.' },
  anz_new_people: { q:'Who are newly engaged people?', a:'Contacts who engaged with Shopify for the first time this week. Fresh inbound signals from people who weren\u2019t previously on your radar.' },
  anz_activity: { q:'What is Contact Activity?', a:'Specific activities (events, webinars, email clicks, campaigns) from contacts at your accounts. Use these details to personalize outreach based on what they actually interacted with.' },
  anz_website_visits: { q:'What are Website Visits?', a:'Accounts visiting Shopify pages in the last 7 days, grouped by account to show which companies are actively researching Shopify right now.' },
  g2_intent: { q:'What is G2 Research?', a:'G2 is a software review and comparison site where buyers research and evaluate vendors before making a decision. When an account appears here, it means someone at that company is actively on G2 comparing commerce platforms right now — not just passively browsing. This is one of the strongest buying intent signals available. Account Grade (A–D) is Demandbase\'s fit score for how well the account matches Shopify\'s ICP. Reach out while they\'re actively in evaluation mode.' }
};

function openModal(type) {
  var st = DATA.signal_types[type];
  var def = SIGNAL_DEFS[type];
  if (!st || !def) return;
  var cls = TYPE_CLS[type] || type;
  var html = '<div class="modal-badge"><span class="badge badge-' + cls + '">' + esc(st.short_label) + '</span></div>';
  html += '<div class="modal-title" id="modalTitle">' + esc(def.q) + '</div>';
  html += '<div class="modal-body">' + esc(def.a) + '</div>';
  $('#modalContent').innerHTML = html;
  modalTrigger = document.activeElement;
  $('#modalOverlay').classList.add('open');
  $('#modalClose').focus();
}
function closeModal() {
  $('#modalOverlay').classList.remove('open');
  if (modalTrigger && modalTrigger.focus) { modalTrigger.focus(); }
  modalTrigger = null;
}

/* ═══════════════════════════════════════════
   Identity — GCP IAP
   ═══════════════════════════════════════════ */
function fetchIAPEmail() {
  var base = location.origin + location.pathname;
  return fetch(base + '?gcp-iap-mode=IDENTITY', {
    credentials: 'same-origin', headers: { 'Accept': 'application/json' }, redirect: 'follow'
  })
  .then(function(r) { if (!r.ok) return null; return r.text(); })
  .then(function(text) {
    if (!text) return null;
    try {
      var data = JSON.parse(text);
      if (data && data.email) {
        var email = data.email.replace('accounts.google.com:', '');
        console.log('[Identity] IAP resolved:', email);
        return email;
      }
    } catch (e) {}
    return null;
  })
  .catch(function() { return null; });
}

function resolveRole(email) {
  if (!email || !DATA.identity) return;
  var ident = DATA.identity;
  var emailLower = email.toLowerCase();
  if (ident.admins.indexOf(emailLower) !== -1) { IDENTITY_ROLE = 'admin'; IDENTITY_DATA = null; return; }
  if (ident.coaches && ident.coaches[emailLower]) { IDENTITY_ROLE = 'coach'; IDENTITY_DATA = ident.coaches[emailLower]; return; }
  if (ident.seller_emails && ident.seller_emails[emailLower]) { IDENTITY_ROLE = 'seller'; IDENTITY_DATA = ident.seller_emails[emailLower]; return; }
}

function resolveIdentity() {
  var params = new URLSearchParams(location.search);
  var sellerParam = params.get('seller');
  if (sellerParam && DATA.sellers[sellerParam]) return sellerParam;
  if (IDENTITY_ROLE === 'admin' || IDENTITY_ROLE === 'coach') return null;
  if (IDENTITY_ROLE === 'seller' && IDENTITY_DATA && DATA.sellers[IDENTITY_DATA]) return IDENTITY_DATA;
  var saved = localStorage.getItem(STORAGE_KEY);
  if (saved && DATA.sellers[saved]) return saved;
  return null;
}

function saveSeller(id) { localStorage.setItem(STORAGE_KEY, id); }
function clearSeller() { localStorage.removeItem(STORAGE_KEY); }

function updateURL(sellerId) {
  var params = new URLSearchParams(location.search);
  if (sellerId) { params.set('seller', sellerId); } else { params.delete('seller'); }
  var qs = params.toString();
  history.pushState({ seller: sellerId }, '', location.pathname + (qs ? '?' + qs : ''));
}

/* ═══════════════════════════════════════════
   Navigation
   ═══════════════════════════════════════════ */
function showSeller(sellerId) {
  if (!DATA.sellers[sellerId]) return showMaster();
  CURRENT_SELLER = sellerId;
  if (IDENTITY_ROLE !== 'admin' && IDENTITY_ROLE !== 'coach') saveSeller(sellerId);
  updateURL(sellerId);
  renderPersonal(sellerId);
  $('#masterView').style.display = 'none';
  $('#personalView').style.display = 'block';
  window.scrollTo(0, 0);
  initReveal();
}

function showMaster() {
  CURRENT_SELLER = null;
  CURRENT_COACH = null;
  updateURL(null);
  renderMaster();
  $('#personalView').style.display = 'none';
  $('#coachView').style.display = 'none';
  $('#masterView').style.display = 'block';
  window.scrollTo(0, 0);
  initReveal();
}

function showCoach(coachSlug) {
  var coachData = getCoachBySlug(coachSlug);
  if (!coachData) return showMaster();
  CURRENT_COACH = coachSlug;
  CURRENT_SELLER = null;
  var params = new URLSearchParams(location.search);
  params.set('coach', coachSlug);
  params.delete('seller');
  var qs = params.toString();
  history.pushState({ coach: coachSlug }, '', location.pathname + (qs ? '?' + qs : ''));
  renderCoach(coachData);
  $('#masterView').style.display = 'none';
  $('#personalView').style.display = 'none';
  $('#coachView').style.display = 'block';
  window.scrollTo(0, 0);
  initReveal();
}

window.addEventListener('popstate', function(e) {
  var sellerId = (e.state && e.state.seller) ? e.state.seller : null;
  var coachSlug = (e.state && e.state.coach) ? e.state.coach : null;
  if (sellerId && DATA && DATA.sellers[sellerId]) {
    CURRENT_SELLER = sellerId;
    renderPersonal(sellerId);
    $('#masterView').style.display = 'none';
    $('#coachView').style.display = 'none';
    $('#personalView').style.display = 'block';
  } else if (coachSlug && DATA) {
    var coachData = getCoachBySlug(coachSlug);
    if (coachData) {
      CURRENT_COACH = coachSlug;
      CURRENT_SELLER = null;
      renderCoach(coachData);
      $('#masterView').style.display = 'none';
      $('#personalView').style.display = 'none';
      $('#coachView').style.display = 'block';
    } else {
      CURRENT_SELLER = null; CURRENT_COACH = null;
      renderMaster();
      $('#personalView').style.display = 'none';
      $('#coachView').style.display = 'none';
      $('#masterView').style.display = 'block';
    }
  } else {
    CURRENT_SELLER = null;
    CURRENT_COACH = null;
    renderMaster();
    $('#personalView').style.display = 'none';
    $('#coachView').style.display = 'none';
    $('#masterView').style.display = 'block';
  }
  initReveal();
});

/* ═══════════════════════════════════════════
   Coach helpers
   ═══════════════════════════════════════════ */
function nameToSlug(name) {
  return name.toLowerCase().replace(/\s+/g, '_').replace(/[^a-z0-9_]/g, '');
}

function getCoachBySlug(slug) {
  var coaches = (DATA && DATA.identity && DATA.identity.coaches) || {};
  for (var email in coaches) {
    if (nameToSlug(coaches[email].name) === slug) return coaches[email];
  }
  return null;
}

function computeTeamTopAccounts(repIds, limit) {
  limit = limit || 10;
  var accounts = {};
  repIds.forEach(function(rid) {
    var seller = DATA.sellers[rid];
    if (!seller) return;
    var signals = seller.signals;
    Object.keys(signals).forEach(function(sigType) {
      if (sigType === 'top_leads') return;
      var rows = signals[sigType] || [];
      var weight = SIGNAL_WEIGHTS[sigType] || 5;
      rows.forEach(function(row) {
        var name = row.account || row.full_name;
        if (!name) return;
        var key = name.toLowerCase();
        if (!accounts[key]) {
          accounts[key] = { name: name, score: 0, signalTypes: [], details: {}, sfdc: null, keywords: [], repId: rid, repName: seller.name };
        }
        var acc = accounts[key];
        if (acc.signalTypes.indexOf(sigType) === -1) {
          acc.signalTypes.push(sigType);
          acc.score += weight;
        }
        if (!acc.details[sigType]) acc.details[sigType] = row;
        if (!acc.sfdc && row.sfdc) acc.sfdc = row.sfdc;
        if (sigType.indexOf('intent_') === 0 && row.matched_keywords) {
          String(row.matched_keywords).split(',').forEach(function(k) {
            k = k.trim(); if (k && acc.keywords.indexOf(k) === -1) acc.keywords.push(k);
          });
        }
      });
    });
  });
  Object.keys(accounts).forEach(function(key) {
    var acc = accounts[key], sfdc = acc.sfdc;
    if (!sfdc) return;
    var days = sfdc.days_since_activity;
    if (sfdc.no_activity) acc.score += 30;
    else if (days != null) {
      if (days > 90) acc.score += 30;
      else if (days > 60) acc.score += 20;
      else if (days > 30) acc.score += 10;
    }
    if (!sfdc.has_deal && (sfdc.no_activity || (days && days > 30))) acc.score += 20;
    var titles = sfdc.engaged_contact_titles || [];
    var hasCsuite = titles.some(function(t) { return t && /(chief|\bceo\b|\bcto\b|\bcmo\b|\bcoo\b)/i.test(t); });
    var hasVP = titles.some(function(t) { return t && /(vice president|\bvp\s)/i.test(t); });
    if (hasCsuite) acc.score += 15; else if (hasVP) acc.score += 10;
  });
  return Object.values(accounts).sort(function(a, b) { return b.score - a.score; }).slice(0, limit);
}

function computeTeamTopPeople(repIds, limit) {
  limit = limit || 12;
  var seen = {};
  var all = [];
  repIds.forEach(function(rid) {
    var seller = DATA.sellers[rid];
    if (!seller) return;
    var people = computeTopPeople(seller.signals);
    people.forEach(function(p) {
      var key = (p.name + '::' + p.account).toLowerCase();
      if (!seen[key]) {
        seen[key] = true;
        all.push(Object.assign({}, p, { repId: rid, repName: seller.name, _signals: seller.signals }));
      }
    });
  });
  return all.sort(function(a, b) { return b.score - a.score; }).slice(0, limit);
}

function generateCoachSummary(coachData) {
  var repIds = coachData.reps;
  var parts = [];

  var mqaReps = repIds.filter(function(rid) {
    return DATA.sellers[rid] && (DATA.sellers[rid].summary.mqa_new || 0) > 0;
  });
  var totalMqa = mqaReps.reduce(function(s, rid) { return s + (DATA.sellers[rid].summary.mqa_new || 0); }, 0);
  if (totalMqa > 0) {
    var names = mqaReps.map(function(rid) { return DATA.sellers[rid].name.split(' ')[0]; });
    parts.push(totalMqa + ' new MQA account' + (totalMqa > 1 ? 's' : '') + ' this week \u2014 check in with ' + names.join(' and ') + ' first.');
  }

  var topAccts = computeTeamTopAccounts(repIds, 1);
  if (topAccts.length > 0) {
    var ta = topAccts[0];
    var why = buildAccountWhy(ta).split(' \u00B7 ')[0];
    parts.push(ta.name + ' (' + ta.repName.split(' ')[0] + '\u2019s account) is the hottest signal this week \u2014 ' + why.toLowerCase() + '.');
  }

  var coldRepNames = repIds.filter(function(rid) {
    var s = DATA.sellers[rid];
    if (!s) return false;
    var allRows = [];
    Object.keys(s.signals).forEach(function(t) { allRows = allRows.concat(s.signals[t] || []); });
    return allRows.some(function(r) { return r.sfdc && r.sfdc.days_since_activity && r.sfdc.days_since_activity > 90; });
  }).map(function(rid) { return DATA.sellers[rid].name.split(' ')[0]; });
  if (coldRepNames.length > 0 && parts.length < 3) {
    parts.push(coldRepNames.join(', ') + (coldRepNames.length === 1 ? ' has' : ' have') + ' accounts untouched 90+ days \u2014 worth a coaching conversation.');
  }

  return parts;
}

/* ═══════════════════════════════════════════
   Render: Coach
   ═══════════════════════════════════════════ */
function renderCoach(coachData) {
  var repIds = coachData.reps.filter(function(rid) { return !!DATA.sellers[rid]; });
  var m = DATA.meta;
  var name = coachData.name;
  var teamNames = coachData.teams || [];
  var initials = name.split(' ').map(function(w) { return w.charAt(0); }).join('').substring(0, 2).toUpperCase();

  // Aggregate stats
  var totalMqa = 0;
  var engagedPeopleUnique = {};
  var repsWithSignals = 0;
  repIds.forEach(function(rid) {
    var s = DATA.sellers[rid];
    if (!s) return;
    totalMqa += s.summary.mqa_new || 0;
    ['activity', 'new_people'].forEach(function(st) {
      (s.signals[st] || []).forEach(function(r) {
        if (r.full_name) engagedPeopleUnique[(r.full_name + '::' + (r.account || '')).toLowerCase()] = true;
      });
    });
    if (s.summary.total > 0) repsWithSignals++;
  });

  var teamTopAccounts = computeTeamTopAccounts(repIds, 10);
  var teamTopPeople = computeTeamTopPeople(repIds, 12);
  var engagedCount = Object.keys(engagedPeopleUnique).length;

  var html = '';

  // ── Header
  html += '<header><div class="hero-personal fade-in">';
  html += '<div style="text-align:center;width:100%;">';
  html += '<div class="avatar" aria-hidden="true" style="margin:0 auto 10px;">' + esc(initials) + '</div>';
  html += '<h1>' + esc(name) + '</h1>';
  html += '<div class="sub-personal" style="margin-top:4px;">' + esc(teamNames.join(' \u0026 ')) + ' \u00B7 Week of <strong>' + esc(m.week_of) + '</strong></div>';
  html += '<div style="margin-top:4px;"><a href="#" id="clearCoach" style="font-size:12px;color:var(--text-3);text-decoration:none;font-weight:500;">\u2190 Dashboard</a></div>';

  var heroStats = [
    { num: totalMqa,           label: 'New MQA'           },
    { num: teamTopAccounts.length, label: 'Top accounts'  },
    { num: engagedCount,       label: 'Engaged people'    },
    { num: repsWithSignals,    label: 'Reps with signals' },
  ].filter(function(s) { return s.num > 0; });

  if (heroStats.length > 0) {
    html += '<div style="display:grid;grid-template-columns:repeat(' + heroStats.length + ',1fr);gap:1px;background:var(--border);border:1px solid var(--border);border-radius:var(--radius-sm);overflow:hidden;margin-top:14px;">';
    heroStats.forEach(function(s) {
      html += '<div style="background:var(--bg-card);padding:10px 8px;text-align:center;">';
      html += '<div style="font-family:var(--font-serif);font-size:22px;font-weight:400;line-height:1;color:var(--text-1);">' + s.num + '</div>';
      html += '<div style="font-size:11px;color:var(--text-3);margin-top:3px;line-height:1.3;">' + esc(s.label) + '</div>';
      html += '</div>';
    });
    html += '</div>';
  }
  html += '</div></div></header>';

  html += '<div class="wrap" style="padding-top:24px;padding-bottom:48px;">';

  // ── This week summary
  var summaryLines = generateCoachSummary(coachData);
  if (summaryLines.length > 0) {
    html += '<div style="margin-bottom:28px;padding:16px 20px;background:var(--bg-card);border:1.5px solid var(--border);border-radius:var(--radius-sm);box-shadow:var(--shadow);">';
    html += '<div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:var(--text-3);margin-bottom:8px;">This week</div>';
    summaryLines.forEach(function(line, i) {
      html += '<p style="font-size:14px;color:var(--text-2);line-height:1.7;margin:0' + (i > 0 ? ';margin-top:6px' : '') + ';">' + esc(line) + '</p>';
    });
    html += '</div>';
  }

  // ── Hottest accounts across team
  if (teamTopAccounts.length > 0) {
    html += '<div class="top-section reveal">';
    html += '<div class="top-section-label">\uD83D\uDD25 Hottest accounts across your team</div>';
    teamTopAccounts.forEach(function(acc, i) {
      var cardId = 'coach-tacc-' + i;
      var rankCls = i < 3 ? ' r' + (i+1) : '';
      html += '<div class="priority-card" id="' + cardId + '">';
      html += '<div class="priority-card-header" data-card="' + cardId + '">';
      html += '<div class="priority-rank' + rankCls + '">' + (i+1) + '</div>';
      html += '<div class="priority-body">';
      html += '<div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:2px;">';
      html += '<div class="priority-name">' + esc(acc.name) + '</div>';
      html += '<span class="coach-rep-badge" data-seller="' + esc(acc.repId) + '">' + esc(acc.repName) + '</span>';
      html += '</div>';
      html += '<div class="priority-why">' + esc(buildAccountWhy(acc)) + '</div>';
      html += '<div class="priority-tags">';
      buildAccountTags(acc).forEach(function(t) { html += '<span class="badge ' + t.c + '" style="font-size:11px;padding:2px 8px;">' + esc(t.l) + '</span>'; });
      html += '</div>';
      html += '</div><div class="priority-expand">\u2304</div></div>';

      // Expanded detail
      var ad = (DATA.account_details || {})[acc.name.toLowerCase()] || {};
      html += '<div class="priority-detail">';
      var overview = ad.merchant_overview || ad.description;
      if (overview) {
        html += '<div style="font-size:12px;color:var(--text-2);line-height:1.7;margin-bottom:12px;padding:12px 16px;background:var(--bg);border-radius:8px;border-left:3px solid var(--accent-soft);">' + esc(overview) + '</div>';
      }
      var acctSuggestion = suggestAccountOutreach(acc);
      if (acctSuggestion.length > 0) {
        html += '<div style="margin-bottom:14px;padding:12px 14px;background:var(--accent-light);border-radius:8px;border-left:3px solid var(--accent);">';
        html += '<div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:var(--accent);margin-bottom:6px;">Outreach angle</div>';
        acctSuggestion.forEach(function(line) {
          html += '<div style="font-size:12px;color:var(--text-2);line-height:1.65;padding-left:10px;position:relative;margin-bottom:4px;">';
          html += '<span style="position:absolute;left:0;color:var(--accent);">\u203A</span>' + esc(line);
          html += '</div>';
        });
        html += '</div>';
      }
      if (ad.sfdc_url) {
        html += '<div style="display:flex;gap:12px;flex-wrap:wrap;margin-top:4px;">';
        html += '<a href="' + esc(ad.sfdc_url) + '" target="_blank" rel="noopener" style="font-size:12px;color:var(--accent2);text-decoration:none;font-weight:500;">View in SFDC \u2197</a>';
        html += '</div>';
      }
      // Recent news (coach view)
      var coachNews = (DATA.account_news || {})[acc.name.toLowerCase()] || [];
      if (coachNews.length > 0) {
        html += '<div style="margin-top:10px;padding:10px 12px;background:var(--bg);border-radius:8px;">';
        html += '<div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:var(--text-3);margin-bottom:6px;">\uD83D\uDCF0 Recent news</div>';
        html += '<div style="display:flex;flex-direction:column;gap:6px;">';
        coachNews.forEach(function(article) {
          html += '<div style="font-size:12px;">';
          html += '<a href="' + esc(article.url) + '" target="_blank" rel="noopener" style="color:var(--text-1);text-decoration:none;font-weight:500;line-height:1.5;">' + esc(article.title) + '</a>';
          html += '<div style="font-size:11px;color:var(--text-3);margin-top:1px;">';
          if (article.source) html += esc(article.source);
          if (article.source && article.date) html += ' \u00B7 ';
          if (article.date) html += esc(article.date);
          html += '</div></div>';
        });
        html += '</div></div>';
      }
      html += '<div class="coach-rep-drill" data-seller="' + esc(acc.repId) + '">View ' + esc(acc.repName) + '\u2019s full report \u2192</div>';
      html += '</div></div>';
    });
    html += '</div>';
  }

  // ── Top engaged people across team
  if (teamTopPeople.length > 0) {
    html += '<div class="top-section reveal">';
    html += '<div class="top-section-label">\uD83D\uDC65 Top engaged people across your team</div>';
    teamTopPeople.forEach(function(p, i) {
      var sc = p.sfdc_contact || {};
      html += '<div class="person-card">';
      html += '<div class="person-rank' + (i < 3 ? ' r'+(i+1) : '') + '">' + (i+1) + '</div>';
      html += '<div class="person-body">';
      html += '<div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:2px;">';
      html += '<div class="person-name">' + esc(p.name) + '</div>';
      html += '<span class="coach-rep-badge" data-seller="' + esc(p.repId) + '">' + esc(p.repName) + '</span>';
      html += '</div>';
      html += '<div class="person-meta">';
      if (p.title) html += esc(p.title);
      if (p.account) html += ' \u00B7 <strong>' + esc(p.account) + '</strong>';
      html += '</div>';
      if (p.engagements && p.engagements.length > 0) {
        html += '<div style="font-size:12px;color:var(--text-3);margin-top:3px;font-style:italic;">' + esc(p.engagements.slice(0, 2).join(' \u00B7 ')) + '</div>';
      }
      html += '<div class="person-tags" style="margin-top:6px;">';
      if (p.seniority && p.seniority !== 'IC') html += renderSeniority(p.seniority);
      if (p.isHvpAccount) html += '<span class="badge badge-hvp" style="font-size:10px;padding:2px 7px;">\u21A9 Previously CL</span>';
      if (p.accountHot && !p.isHvpAccount) html += '<span class="badge badge-mqa_new" style="font-size:10px;padding:2px 7px;">\u2605 Hot account</span>';
      if (sc.in_sfdc && sc.days_since_contact != null) {
        var dcls = sc.days_since_contact > 60 ? 'sfdc-cold' : sc.days_since_contact > 14 ? 'sfdc-warm' : 'sfdc-fresh';
        html += '<span class="sfdc-badge ' + dcls + '" style="font-size:10px;">' + esc(sc.days_since_contact === 0 ? 'Contacted today' : sc.days_since_contact + 'd since contacted') + '</span>';
      } else if (sc.in_sfdc === false) {
        html += '<span class="sfdc-badge" style="font-size:10px;background:var(--bg-muted);color:var(--text-3);">Not in SFDC</span>';
      }
      html += '</div>';
      var contactLinks = [];
      if (sc.email) contactLinks.push('<a href="mailto:' + esc(sc.email) + '" style="font-size:12px;color:var(--accent);text-decoration:none;font-weight:500;">\u2709 ' + esc(sc.email) + '</a>');
      if (sc.contact_url) contactLinks.push('<a href="' + esc(sc.contact_url) + '" target="_blank" rel="noopener" style="font-size:12px;color:var(--accent2);text-decoration:none;font-weight:500;">View in SFDC \u2197</a>');
      if (contactLinks.length > 0) html += '<div style="margin-top:6px;display:flex;gap:14px;flex-wrap:wrap;">' + contactLinks.join('') + '</div>';
      html += '</div></div>';
    });
    html += '</div>';
  }

  // ── Rep scorecards
  html += '<div class="top-section reveal">';
  html += '<div class="top-section-label">\uD83D\uDC64 Your team this week</div>';
  html += '<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:12px;margin-top:12px;">';
  var allTypes = Object.keys(DATA.signal_types).filter(function(t) { return t !== 'top_leads'; });
  repIds.forEach(function(rid) {
    var s = DATA.sellers[rid];
    if (!s) return;
    var repInitials = s.name.split(' ').map(function(w) { return w.charAt(0); }).join('').substring(0, 2).toUpperCase();
    var topAccts = computeTopAccounts(s.signals);
    var topAcct = topAccts.length > 0 ? topAccts[0] : null;
    var hasMqa = (s.summary.mqa_new || 0) > 0;
    html += '<div class="coach-rep-card" data-seller="' + esc(rid) + '" tabindex="0" role="button" aria-label="View ' + esc(s.name) + ' report">';
    html += '<div style="display:flex;align-items:center;gap:10px;margin-bottom:10px;">';
    html += '<div class="avatar" style="width:32px;height:32px;min-width:32px;font-size:13px;line-height:32px;' + (hasMqa ? 'background:var(--accent);color:#fff;' : '') + '">' + esc(repInitials) + '</div>';
    html += '<div><div style="font-weight:600;font-size:14px;color:var(--text-1);">' + esc(s.name) + '</div>';
    html += '<div style="font-size:11px;color:var(--text-3);">' + s.summary.total + ' signals this week</div></div>';
    html += '</div>';
    if (topAcct) {
      html += '<div style="font-size:12px;color:var(--text-2);margin-bottom:8px;padding:8px 10px;background:var(--bg-muted);border-radius:8px;">';
      html += '<span style="font-weight:600;">Top: </span>' + esc(topAcct.name);
      html += '<span style="color:var(--text-3);"> \u00B7 ' + esc(buildAccountWhy(topAcct).split(' \u00B7 ')[0]) + '</span>';
      html += '</div>';
    }
    html += '<div style="display:flex;flex-wrap:wrap;gap:4px;">';
    allTypes.forEach(function(t) {
      if ((s.summary[t] || 0) > 0) {
        html += '<span class="pill ' + (TYPE_CLS[t] || t) + '" style="font-size:10px;">' + s.summary[t] + ' ' + DATA.signal_types[t].short_label + '</span>';
      }
    });
    html += '</div></div>';
  });
  html += '</div></div>';

  // ── Archive
  html += '<div class="wrap section-wrap" style="margin-top:0;">';
  html += '<button id="coachArchiveToggle" style="background:none;border:none;cursor:pointer;font-family:var(--font);font-size:13px;color:var(--text-3);display:flex;align-items:center;gap:6px;padding:0;">';
  html += '<span id="coachArchiveArrow">\u25B6</span> Past weeks</button>';
  html += '<div id="coachArchiveGrid" style="display:none;margin-top:14px;"><div class="archive-grid">';
  WEEKS.forEach(function(w) {
    var isCurrent = w === CURRENT_WEEK;
    html += '<div class="archive-link' + (isCurrent ? ' current' : '') + '" data-week="' + esc(w) + '" tabindex="0" role="button">';
    html += '<span class="archive-week-label">Week of ' + esc(w) + '</span>';
    if (isCurrent) html += '<span class="archive-tag archive-tag-current">Current</span>';
    html += '</div>';
  });
  html += '</div></div>';
  html += '</div>';

  html += '<footer class="footer">CONFIDENTIAL \u2014 SHOPIFY INTERNAL USE ONLY \u00B7 Week of ' + esc(m.week_of) + '</footer>';
  html += '</div>';

  $('#coachView').innerHTML = html;

  // Wire up: back to dashboard
  var clearCoachEl = document.getElementById('clearCoach');
  if (clearCoachEl) clearCoachEl.addEventListener('click', function(e) { e.preventDefault(); CURRENT_COACH = null; showMaster(); });

  // Wire up: rep badge clicks → drill to seller
  document.querySelectorAll('#coachView .coach-rep-badge, #coachView .coach-rep-drill').forEach(function(el) {
    el.addEventListener('click', function(e) { e.stopPropagation(); var sid = this.dataset.seller; if (sid) showSeller(sid); });
  });

  // Wire up: rep scorecard clicks → drill to seller
  document.querySelectorAll('#coachView .coach-rep-card').forEach(function(el) {
    el.addEventListener('click', function() { var sid = this.dataset.seller; if (sid) showSeller(sid); });
    el.addEventListener('keydown', function(e) { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); var sid = this.dataset.seller; if (sid) showSeller(sid); }});
  });

  // Wire up: priority card expand/collapse
  document.querySelectorAll('#coachView .priority-card-header').forEach(function(hdr) {
    var fn = function() {
      var card = document.getElementById(this.dataset.card);
      if (card) card.classList.toggle('open');
    };
    hdr.addEventListener('click', fn);
    hdr.addEventListener('keydown', function(e) { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); fn.call(this, e); }});
  });

  // Wire up: archive toggle
  var archToggle = document.getElementById('coachArchiveToggle');
  if (archToggle) {
    archToggle.addEventListener('click', function() {
      var grid = document.getElementById('coachArchiveGrid');
      var arrow = document.getElementById('coachArchiveArrow');
      var open = grid.style.display !== 'none' && grid.style.display !== '';
      grid.style.display = open ? 'none' : 'block';
      if (arrow) arrow.textContent = open ? '\u25B6' : '\u25BC';
    });
  }

  // Wire up: archive week links
  document.querySelectorAll('#coachView .archive-link[data-week]').forEach(function(el) {
    el.addEventListener('click', function() { loadWeek(this.dataset.week, null); });
  });
}

/* ═══════════════════════════════════════════
   Render: Master
   ═══════════════════════════════════════════ */
function renderMaster() {
  var m = DATA.meta;
  var types = Object.keys(DATA.signal_types).filter(function(t) { return t !== 'top_leads'; });
  var sellers = DATA.sellers;
  var sellerIds = Object.keys(sellers);

  var totals = {};
  types.forEach(function(t) { totals[t] = 0; });
  sellerIds.forEach(function(sid) {
    types.forEach(function(t) { totals[t] += sellers[sid].summary[t] || 0; });
  });

  var firstName = getFirstName();
  var greetName = firstName ? ', ' + firstName : '';

  var html = '';
  var isArchive = CURRENT_WEEK !== LATEST_WEEK;
  if (isArchive) {
    html += '<div class="archive-banner"><strong>\uD83D\uDCC5 Viewing week of ' + esc(CURRENT_WEEK) + '</strong>';
    html += '<button class="archive-banner-btn" id="backToCurrentBtn">Back to current week</button></div>';
  }
  html += '<header><div class="hero-compact">';
  html += '<div class="hero-greeting fade-in">' + getGreeting() + greetName + '</div>';
  html += '<h1 class="fade-in fd2">Weekly Sales Insights</h1>';
  html += '<div class="sub fade-in fd3"><strong>' + m.total_sellers + ' sellers</strong> \u00B7 ' + m.sellers_with_signals + ' with signals \u00B7 Week of ' + esc(m.week_of) + '</div>';
  html += '</div></header>';

  html += '<div class="wrap section-wrap">';

  // Search for unidentified users
  if (!IDENTITY_ROLE) {
    html += '<div class="search-area"><div class="search-box">';
    html += '<svg width="18" height="18" viewBox="0 0 20 20" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true"><circle cx="9" cy="9" r="6"/><line x1="14" y1="14" x2="18" y2="18"/></svg>';
    html += '<input class="search-input" type="text" placeholder="Type your name here to find your report..." id="hubSearch" autocomplete="off" aria-label="Search for your name">';
    html += '</div><div class="search-results" id="searchResults" role="listbox" aria-live="polite"></div></div>';
  }

  // Region filter — only show if there are multiple regions in the data
  var allRegions = {};
  Object.keys(DATA.teams || {}).forEach(function(tn) {
    var r = (DATA.teams[tn].region || 'NA');
    allRegions[r] = true;
  });
  var regionList = Object.keys(allRegions).sort();
  var showRegionFilter = regionList.length > 1;

  if (showRegionFilter) {
    var savedRegion = localStorage.getItem('si_region') || 'all';
    html += '<div class="region-pills reveal" id="regionPills">';
    html += '<button class="region-pill' + (savedRegion === 'all' ? ' active' : '') + '" data-region="all">All Regions</button>';
    regionList.forEach(function(r) {
      html += '<button class="region-pill' + (savedRegion === r ? ' active' : '') + '" data-region="' + esc(r) + '">' + esc(r) + '</button>';
    });
    html += '</div>';
  }

  // Filters — simplified: group all intent_ types into one "Intent" button
  var intentTotal = types.filter(function(t) { return t.indexOf('intent_') === 0; }).reduce(function(sum, t) { return sum + (totals[t] || 0); }, 0);
  var nonIntentTypes = types.filter(function(t) { return t.indexOf('intent_') !== 0; });
  html += '<div class="filter-bar reveal" role="toolbar" aria-label="Signal type filters">';
  html += '<button class="filter-tag active" data-filter="all">All Signals</button>';
  nonIntentTypes.forEach(function(t) {
    if (totals[t] === 0) return;
    var st = DATA.signal_types[t];
    var extraCls = (t === 'mqa_new') ? ' filter-tag-mqa' : '';
    html += '<button class="filter-tag' + extraCls + '" data-filter="' + t + '">' + esc(st.short_label) + ' (' + totals[t] + ')</button>';
  });
  if (intentTotal > 0) {
    html += '<button class="filter-tag" data-filter="intent">Intent (' + intentTotal + ')</button>';
  }
  html += '</div>';

  // Notice for unidentified
  if (!IDENTITY_ROLE) {
    html += '<div class="notice"><strong>Don\'t see your name?</strong> That means none of your accounts had engagement signals this week. Check back next Monday.</div>';
  }

  // Top signals
  html += '<div style="margin-top:28px;"><div class="section-emoji reveal">\uD83D\uDD25</div>';
  html += '<div class="section-kicker reveal">Hottest this week</div>';
  html += '<div class="section-head reveal">This Week\u2019s Hottest Accounts</div>';
  html += '<div class="section-deck reveal">Strongest signals across all teams. If any of these are yours, act now.</div>';
  html += '<div class="top-scroll-row">';
  DATA.highlights.forEach(function(h, i) {
    var labelCls = 'rank-label-' + h.type;
    html += '<div class="top-card reveal">';
    html += '<div class="top-rank-wrap"><span class="top-rank ' + RANK_BG[i % RANK_BG.length] + '">' + (i+1) + '</span>';
    html += '<span class="top-rank-label ' + labelCls + '">' + esc((DATA.signal_types[h.type] && DATA.signal_types[h.type].short_label) || h.type) + '</span></div>';
    html += '<div class="top-title">' + esc(h.title) + '</div>';
    html += '<div class="top-meta">' + esc(h.subtitle) + '</div>';
    if (h.detail) html += '<div class="top-kw">' + esc(h.detail) + '</div>';
    if (h.seller_name && h.seller_id) {
      html += '<div class="top-owner" data-seller="' + esc(h.seller_id) + '" tabindex="0" role="link" aria-label="View ' + esc(h.seller_name) + ' report">\u2192 ' + esc(h.seller_name) + '</div>';
    }
    html += '</div>';
  });
  html += '</div></div></div>';

  // Sellers — grouped by team
  html += '<div class="wrap section-wrap">';
  html += '<div class="section-emoji reveal">\uD83D\uDC65</div>';
  html += '<div class="section-kicker reveal">Your team</div>';
  html += '<div class="section-head reveal">See What Each Seller Has This Week</div>';
  html += '<div class="section-deck reveal">Click any name to see their full personalized report.</div>';
  html += '<div id="sellersGrid">';

  var teams = DATA.teams || {};
  var teamNames = Object.keys(teams);

  function renderSellerCard(sid) {
    var s = sellers[sid];
    if (!s || s.summary.total === 0) return '';
    var typesPresent = types.filter(function(t) { return s.summary[t] > 0; }).join(' ');
    var sellerRegion = s.region || 'NA';

    // Compute the same 4 key stats as the seller page header
    var mqaCount  = s.summary.mqa_new || 0;
    var hvpCount  = s.summary.hvp || 0;
    var peopleCount = (s.summary.activity || 0) + (s.summary.new_people || 0);
    var intentCount = ['intent_agentic','intent_compete','intent_international','intent_marketing','intent_b2b']
      .reduce(function(sum, t) { return sum + (s.summary[t] || 0); }, 0);

    var stats = [];
    if (mqaCount > 0)     stats.push({ n: mqaCount,     l: 'New MQA',   cls: 'mqa_new' });
    if (hvpCount > 0)     stats.push({ n: hvpCount,     l: 'Prev. CL',  cls: 'hvp' });
    if (peopleCount > 0)  stats.push({ n: peopleCount,  l: 'Engaged',   cls: 'new_people' });
    if (intentCount > 0)  stats.push({ n: intentCount,  l: 'Intent',    cls: 'intent_compete' });

    var c = '';
    c += '<div class="seller-card reveal" data-seller="' + esc(sid) + '" data-name="' + esc(s.name.toLowerCase()) + '" data-types="' + typesPresent + '" data-region="' + esc(sellerRegion) + '" tabindex="0" role="button" aria-label="View ' + esc(s.name) + ' report">';
    var mqaDot = (mqaCount > 0 || s.summary.anz_high_intent > 0) ? '<span class="seller-mqa-indicator" title="Has hot accounts"></span>' : '';
    c += '<div class="seller-head"><span class="seller-name">' + esc(s.name) + mqaDot + '</span></div>';
    if (stats.length > 0) {
      c += '<div style="display:grid;grid-template-columns:repeat(' + Math.min(stats.length, 4) + ',1fr);gap:1px;background:var(--border);border:1px solid var(--border);border-radius:10px;overflow:hidden;margin-top:10px;">';
      stats.forEach(function(st) {
        c += '<div style="background:var(--bg-card);padding:7px 6px;text-align:center;">';
        c += '<div style="font-family:var(--font-serif);font-size:16px;color:var(--text-1);line-height:1;">' + st.n + '</div>';
        c += '<div style="font-size:10px;color:var(--text-3);margin-top:2px;line-height:1.2;">' + esc(st.l) + '</div>';
        c += '</div>';
      });
      c += '</div>';
    }
    c += '</div>';
    return c;
  }

  teamNames.forEach(function(tn) {
    var team = teams[tn];
    var teamRegion = team.region || 'NA';
    var teamSellers = (team.sellers || []).slice().sort(function(a, b) {
      return (sellers[a] ? sellers[a].name : '').localeCompare(sellers[b] ? sellers[b].name : '');
    });
    var hasSignals = teamSellers.some(function(sid) { return sellers[sid] && sellers[sid].summary.total > 0; });
    if (!hasSignals) return;

    // Team-level key stats
    var tMqa     = team.summary.mqa_new || 0;
    var tHvp     = team.summary.hvp || 0;
    var tPeople  = (team.summary.activity || 0) + (team.summary.new_people || 0);
    var tIntent  = ['intent_agentic','intent_compete','intent_international','intent_marketing','intent_b2b']
      .reduce(function(sum, t) { return sum + (team.summary[t] || 0); }, 0);
    var teamStats = [];
    if (tMqa > 0)    teamStats.push(tMqa + ' New MQA');
    if (tHvp > 0)    teamStats.push(tHvp + ' Prev. CL');
    if (tPeople > 0) teamStats.push(tPeople + ' engaged');
    if (tIntent > 0) teamStats.push(tIntent + ' intent');

    var coachSlug = team.lead ? nameToSlug(team.lead) : null;

    html += '<div class="team-group reveal" data-team="' + esc(tn) + '" data-region="' + esc(teamRegion) + '">';
    html += '<div class="team-header">';
    html += '<span class="team-name">' + esc(tn) + (showRegionFilter ? '<span class="region-tag">' + esc(teamRegion) + '</span>' : '') + '</span>';
    if (team.lead) {
      if (coachSlug) {
        html += '<span class="team-lead coach-link" data-coach="' + esc(coachSlug) + '" tabindex="0" role="link" title="View coach view">' + esc(team.lead) + ' \u2197</span>';
      } else {
        html += '<span class="team-lead">' + esc(team.lead) + '</span>';
      }
    }
    if (teamStats.length > 0) {
      html += '<span style="font-size:11px;color:var(--text-3);margin-left:auto;">' + teamStats.join(' \u00B7 ') + '</span>';
    }
    html += '</div>';
    html += '<div class="sellers-grid">';
    teamSellers.forEach(function(sid) { html += renderSellerCard(sid); });
    html += '</div></div>';
  });

  html += '</div>';
  html += '<div id="noResults" style="display:none;text-align:center;color:var(--text-3);padding:48px 0;font-size:15px;" aria-live="polite">No sellers match your search.</div>';
  html += '</div>';

  // Archive
  html += '<div class="wrap section-wrap">';
  html += '<div class="section-emoji reveal">\uD83D\uDCC5</div>';
  html += '<div class="section-kicker reveal">Weekly archive</div>';
  html += '<div class="section-head reveal">Browse Past Weeks</div>';
  html += '<div class="section-deck reveal">Each week is saved so you can always go back and review.</div>';
  html += '<div class="archive-grid reveal">';

  var isViewingLatest = CURRENT_WEEK === LATEST_WEEK;
  html += '<div class="archive-link' + (isViewingLatest ? ' current' : '') + '" data-week="' + esc(LATEST_WEEK) + '" tabindex="0" role="button" aria-label="Load current week">';
  html += '<div><span class="archive-week-label">Week of ' + esc(LATEST_WEEK) + '</span><span class="archive-tag archive-tag-current">Current</span></div>';
  if (isViewingLatest) {
    html += '<div class="archive-meta">' + m.total_sellers + ' sellers \u00B7 ' + m.sellers_with_signals + ' with signals</div>';
  }
  html += '</div>';

  var pastWeeks = WEEKS.filter(function(w) { return w !== LATEST_WEEK; });
  pastWeeks.forEach(function(w) {
    var isViewing = w === CURRENT_WEEK;
    html += '<div class="archive-link' + (isViewing ? ' viewing' : '') + '" data-week="' + esc(w) + '" tabindex="0" role="button" aria-label="Load week of ' + esc(w) + '">';
    html += '<div><span class="archive-week-label">Week of ' + esc(w) + '</span>';
    if (isViewing) html += '<span class="archive-tag archive-tag-viewing">Viewing</span>';
    html += '</div></div>';
  });

  if (pastWeeks.length === 0 && WEEKS.length <= 1) {
    html += '<div class="archive-empty"><span aria-hidden="true">\uD83D\uDCC5</span><span>No previous weeks yet \u2014 check back next Monday!</span></div>';
  }
  html += '</div></div>';



  html += '<footer class="wrap"><div class="footer">CONFIDENTIAL \u2014 SHOPIFY INTERNAL USE ONLY \u00B7 Week of ' + esc(m.week_of) + '</div></footer>';

  $('#masterView').innerHTML = html;
  bindMasterEvents();
}

/* ═══════════════════════════════════════════
   Render: Personal
   ═══════════════════════════════════════════ */
/* ═══════════════════════════════════════════
   Top Accounts — priority scoring + render
   ═══════════════════════════════════════════ */
var SIGNAL_WEIGHTS = {
  mqa_new:100, hvp:80, intent_compete:60, g2_intent:55, hvp_all:40,
  intent_agentic:35, intent_international:30,
  intent_marketing:25, intent_b2b:25,
  new_people:20, activity:15, all_mqa:10
};

function computeTopAccounts(signals) {
  var accounts = {};
  Object.keys(signals).forEach(function(sigType) {
    if (sigType === 'top_leads') return;
    var rows = signals[sigType] || [];
    var weight = SIGNAL_WEIGHTS[sigType] || 5;
    rows.forEach(function(row) {
      var name = row.account || row.full_name;
      if (!name) return;
      if (!accounts[name]) {
        accounts[name] = { name:name, score:0, signalTypes:[], details:{}, sfdc:null, keywords:[] };
      }
      var acc = accounts[name];
      if (acc.signalTypes.indexOf(sigType) === -1) {
        acc.signalTypes.push(sigType);
        acc.score += weight;
      }
      if (!acc.details[sigType]) acc.details[sigType] = row;
      if (!acc.sfdc && row.sfdc) acc.sfdc = row.sfdc;
      if (sigType.indexOf('intent_') === 0 && row.matched_keywords) {
        String(row.matched_keywords).split(',').forEach(function(k) {
          k = k.trim(); if (k && acc.keywords.indexOf(k) === -1) acc.keywords.push(k);
        });
      }
    });
  });

  // SFDC modifiers
  Object.keys(accounts).forEach(function(name) {
    var acc = accounts[name], sfdc = acc.sfdc;
    if (!sfdc) return;
    var days = sfdc.days_since_activity;
    if (sfdc.no_activity) acc.score += 30;
    else if (days != null) {
      if (days > 90) acc.score += 30;
      else if (days > 60) acc.score += 20;
      else if (days > 30) acc.score += 10;
    }
    if (!sfdc.has_deal && (sfdc.no_activity || (days && days > 30))) acc.score += 20;
    var titles = sfdc.engaged_contact_titles || [];
    var hasCsuite = titles.some(function(t) { return t && /(chief|\bceo\b|\bcto\b|\bcmo\b|\bcoo\b)/i.test(t); });
    var hasVP = titles.some(function(t) { return t && /(vice president|\bvp\s)/i.test(t); });
    if (hasCsuite) acc.score += 15; else if (hasVP) acc.score += 10;
  });

  return Object.values(accounts).sort(function(a,b){ return b.score - a.score; }).slice(0,5);
}

function buildAccountWhy(acc) {
  var parts = [], sfdc = acc.sfdc, st = acc.signalTypes;
  if (st.indexOf('mqa_new') !== -1) parts.push('New MQA this week');
  else if (st.indexOf('hvp') !== -1) parts.push('Previously CL \u2014 back on Plus pages');
  else if (st.indexOf('intent_compete') !== -1) parts.push('Evaluating competitors');
  else if (st.indexOf('hvp_all') !== -1) parts.push('Visiting high-value pages');
  else if (st.indexOf('intent_agentic') !== -1) parts.push('Agentic commerce interest');
  else if (st.indexOf('intent_international') !== -1) parts.push('International intent');
  else parts.push('Active signals this week');
  if (sfdc) {
    if (sfdc.has_deal && sfdc.open_opps && sfdc.open_opps[0]) {
      var o = sfdc.open_opps[0], d = sfdc.days_since_activity;
      parts.push((d && d > 30 ? 'deal stalled ' + d + 'd' : (o.stage || 'open deal')));
    } else if (!sfdc.has_deal) {
      if (sfdc.no_activity) parts.push('no deal \u00B7 no SFDC activity');
      else if (sfdc.days_since_activity > 60) parts.push('no deal \u00B7 cold ' + sfdc.days_since_activity + 'd');
      else parts.push('no open deal');
    }
    if (sfdc.engaged_contact_titles && sfdc.engaged_contact_titles[0]) {
      parts.push(sfdc.engaged_contact_titles[0] + ' engaged');
    }
  }
  return parts.join(' \u00B7 ');
}

function buildAccountTags(acc) {
  var tags = [], sfdc = acc.sfdc, st = acc.signalTypes;

  // 1. Closed Lost — only signal tag we keep
  if (st.indexOf('hvp') !== -1) tags.push({l:'\u21A9 Previously CL', c:'badge-hvp'});

  if (sfdc) {
    // 2. Days since contacted (or no activity)
    if (sfdc.no_activity) {
      tags.push({l:'No SFDC activity', c:'sfdc-cold'});
    } else if (sfdc.days_since_activity != null) {
      var d = sfdc.days_since_activity;
      var cls = d > 60 ? 'sfdc-cold' : d > 14 ? 'sfdc-warm' : 'sfdc-fresh';
      var label = d === 0 ? 'Contacted today' : d + ' days since contacted';
      tags.push({l:label, c:cls});
    }

    // 3. Contacts count
    if (sfdc.engaged_contact_count > 0) {
      tags.push({l:sfdc.engaged_contact_count + ' contact' + (sfdc.engaged_contact_count > 1 ? 's' : ''), c:'sfdc-contacts'});
    }
  }
  return tags;
}

function generateSellerSummary(seller, signals) {
  var parts = [];
  var m = DATA.meta;

  // Key counts
  var mqaNew = (signals.mqa_new || []);
  var hvpCL  = (signals.hvp || []);
  var hvpAll = (signals.hvp_all || []).filter(function(r){ return r.website && r.website.trim(); });
  var topAccts = computeTopAccounts(signals);
  var compete  = (signals.intent_compete || []);
  var agentic  = (signals.intent_agentic || []);

  // People — C-Suite / VP engaged
  var seniorPeople = [];
  ['activity','new_people'].forEach(function(st){
    (signals[st]||[]).forEach(function(r){
      if ((r.seniority === 'C-Suite' || r.seniority === 'VP') && r.full_name)
        seniorPeople.push(r);
    });
  });
  // Dedupe by name
  var seenNames = {};
  seniorPeople = seniorPeople.filter(function(p){
    if (seenNames[p.full_name]) return false;
    seenNames[p.full_name] = true; return true;
  });

  // Sentence 1 — headline signal
  if (mqaNew.length > 0) {
    var mqaNames = mqaNew.slice(0,2).map(function(r){ return r.account; });
    var s1 = mqaNew.length === 1
      ? mqaNames[0] + ' crossed the MQA threshold this week — your warmest account right now.'
      : mqaNew.length + ' accounts hit MQA this week including ' + mqaNames.join(' and ') + ' — these are your warmest leads.';
    parts.push(s1);
  } else if (hvpCL.length > 0) {
    var clNames = hvpCL.slice(0,2).map(function(r){ return r.account; });
    parts.push((hvpCL.length === 1 ? clNames[0] + ' is' : clNames.join(' and ') + ' are') + ' previously Closed Lost and back on Shopify Plus pages this week — re-engagement opportunity.');
  } else if (topAccts.length > 0) {
    var top = topAccts[0];
    var ad = (DATA.account_details||{})[top.name.toLowerCase()]||{};
    var platform = ad.ecomm_platform ? ' (currently on ' + ad.ecomm_platform + ')' : '';
    parts.push(top.name + platform + ' is your highest-priority account this week based on engagement signals and SFDC context.');
  }

  // Sentence 2 — context layer (people, platform, cold deals)
  if (seniorPeople.length > 0) {
    var sp = seniorPeople[0];
    var spAccount = sp.account ? ' at ' + sp.account : '';
    var coldNote = sp.sfdc_contact && sp.sfdc_contact.days_since_contact > 60
      ? ', not contacted in ' + sp.sfdc_contact.days_since_contact + ' days'
      : sp.sfdc_contact && !sp.sfdc_contact.in_sfdc ? ', not yet in SFDC' : '';
    var morePeople = seniorPeople.length > 1 ? ' and ' + (seniorPeople.length - 1) + ' other' + (seniorPeople.length > 2 ? 's' : '') : '';
    parts.push(sp.full_name + ' (' + sp.title + ')' + spAccount + morePeople + ' engaged with Shopify this week' + coldNote + '.');
  } else if (compete.length > 0) {
    var competeNames = compete.slice(0,2).map(function(r){ return r.account; });
    parts.push(compete.length + ' of your accounts ' + (compete.length === 1 ? 'is' : 'are') + ' evaluating competitors this week — ' + competeNames.join(' and ') + (compete.length > 2 ? ' among others' : '') + '.');
  } else if (agentic.length > 0) {
    parts.push(agentic.length + ' account' + (agentic.length > 1 ? 's are' : ' is') + ' researching AI and agentic commerce — a good angle for outreach this week.');
  }

  // Sentence 3 — what to do
  if (topAccts.length > 0) {
    var t = topAccts[0];
    var sfdc = t.sfdc || {};
    var dealCtx = sfdc.has_deal
      ? 'there\'s an open deal that needs attention'
      : sfdc.no_activity ? 'no SFDC activity on record — treat as a fresh open'
      : sfdc.days_since_activity > 60 ? 'cold for ' + sfdc.days_since_activity + ' days'
      : 'last touched ' + sfdc.days_since_activity + ' days ago';
    // Only add sentence 3 if we already have 2
    if (parts.length >= 2) {
      parts.push('Start with ' + t.name + ' — ' + dealCtx + '.');
    }
  }

  return parts.slice(0, 3);
}

function renderTopAccounts(signals) {
  var accounts = computeTopAccounts(signals);
  if (accounts.length === 0) return '';

  // Build a lookup: account name → activity/people rows (real names + what they did)
  var peopleLookup = {};
  ['activity', 'new_people'].forEach(function(st) {
    (signals[st] || []).forEach(function(row) {
      var key = (row.account || '').toLowerCase();
      if (!key) return;
      if (!peopleLookup[key]) peopleLookup[key] = [];
      // dedupe by full_name
      var already = peopleLookup[key].some(function(r) { return r.full_name === row.full_name; });
      if (!already) peopleLookup[key].push(row);
    });
  });


  var html = '<div class="top-section reveal">';
  html += '<div class="top-section-label">\uD83D\uDD25 Top accounts to act on this week</div>';

  accounts.forEach(function(acc, i) {
    var cardId = 'tacc-' + i;
    var rankCls = i < 3 ? ' r' + (i+1) : '';
    html += '<div class="priority-card" id="' + cardId + '">';
    html += '<div class="priority-card-header" data-card="' + cardId + '">';
    html += '<div class="priority-rank' + rankCls + '">' + (i+1) + '</div>';
    html += '<div class="priority-body">';
    html += '<div class="priority-name">' + esc(acc.name) + '</div>';
    html += '<div class="priority-why">' + esc(buildAccountWhy(acc)) + '</div>';
    html += '<div class="priority-tags">';
    buildAccountTags(acc).forEach(function(t) { html += '<span class="badge ' + t.c + '" style="font-size:11px;padding:2px 8px;">' + esc(t.l) + '</span>'; });
    html += '</div></div><div class="priority-expand">\u2304</div></div>';

    html += renderAccountCardExpanded(acc, peopleLookup);
    html += '</div>';
  });

  html += '</div>';
  return html;
}

/* ═══════════════════════════════════════════
   Shared expanded card detail renderer
   Used by Top Accounts and G2 Research sections
   ═══════════════════════════════════════════ */
function renderAccountCardExpanded(acc, peopleLookup) {
  var html = '';
  var ad = (DATA.account_details || {})[acc.name.toLowerCase()] || {};
  html += '<div class="priority-detail">';

  // Account overview / brief — richest available source
  var overview = ad.merchant_overview || ad.description;
  if (overview) {
    html += '<div style="font-size:12px;color:var(--text-2);line-height:1.7;margin-bottom:12px;padding:12px 16px;background:var(--bg);border-radius:8px;border-left:3px solid var(--accent-soft);">' + esc(overview) + '</div>';
  } else {
    var mqaRow = acc.details && acc.details.mqa_new;
    if (mqaRow && mqaRow.brief) html += '<div style="font-size:12px;color:var(--text-2);margin-bottom:12px;padding:12px 16px;background:var(--bg);border-radius:8px;">\uD83D\uDCA1 ' + esc(mqaRow.brief) + '</div>';
  }

  // Outreach angle
  var acctSuggestion = suggestAccountOutreach(acc);
  if (acctSuggestion.length > 0) {
    html += '<div style="margin-bottom:14px;padding:12px 14px;background:var(--accent-light);border-radius:8px;border-left:3px solid var(--accent);">';
    html += '<div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:var(--accent);margin-bottom:6px;">Outreach angle</div>';
    acctSuggestion.forEach(function(line) {
      html += '<div style="font-size:12px;color:var(--text-2);line-height:1.65;padding-left:10px;position:relative;margin-bottom:4px;">';
      html += '<span style="position:absolute;left:0;color:var(--accent);">\u203A</span>' + esc(line);
      html += '</div>';
    });
    html += '</div>';
  }

  // Account facts row
  var facts = [];
  if (ad.industry) facts.push(ad.industry);
  if (ad.city && ad.country) facts.push(ad.city + ', ' + ad.country);
  else if (ad.country) facts.push(ad.country);
  if (ad.employees) facts.push(parseInt(ad.employees).toLocaleString() + ' employees');
  if (ad.revenue_usd) {
    var rev = parseFloat(ad.revenue_usd);
    var revStr = rev >= 1e9 ? '$' + (rev/1e9).toFixed(1) + 'B' : rev >= 1e6 ? '$' + Math.round(rev/1e6) + 'M' : '$' + Math.round(rev).toLocaleString();
    facts.push(revStr + ' revenue');
  } else if (ad.annual_online_revenue) {
    facts.push(ad.annual_online_revenue + ' online revenue');
  }
  if (facts.length > 0) {
    html += '<div class="priority-detail-row"><span class="priority-detail-icon">\uD83C\uDFE2</span><span>' + esc(facts.join(' \u00B7 ')) + '</span></div>';
  }

  // Tech stack
  var tech = [];
  if (ad.ecomm_platform) tech.push('Ecomm: ' + ad.ecomm_platform);
  if (ad.pos_solution) tech.push('POS: ' + ad.pos_solution);
  var anyRow = (acc.details && (acc.details.mqa_new || acc.details.hvp_all || acc.details.hvp || acc.details.g2_intent)) || {};
  if (!ad.ecomm_platform && anyRow.platform) tech.push('Platform: ' + anyRow.platform);
  if (tech.length > 0) {
    html += '<div class="priority-detail-row"><span class="priority-detail-icon">\uD83D\uDDA5</span><span>' + esc(tech.join(' \u00B7 ')) + '</span></div>';
  }

  // Competitor contract end date — timing signal
  if (ad.competitor_contract_end) {
    html += '<div class="priority-detail-row"><span class="priority-detail-icon">\uD83D\uDCC5</span><span><span class="priority-detail-label">Competitor contract ends</span>' + esc(ad.competitor_contract_end) + ' — good timing for outreach</span></div>';
  }

  // Pages visited from Demandbase
  if (anyRow.pages_visited) {
    html += '<div class="priority-detail-row"><span class="priority-detail-icon">\uD83D\uDCC4</span><span><span class="priority-detail-label">Pages researched</span>' + esc(anyRow.pages_visited) + '</span></div>';
  }

  // Intent keywords
  if (acc.keywords && acc.keywords.length > 0) {
    html += '<div class="priority-detail-row"><span class="priority-detail-icon">\uD83C\uDFF7</span><span><span class="priority-detail-label">Intent keywords </span>';
    acc.keywords.slice(0,10).forEach(function(k){ html += '<span class="kw-chip">' + esc(k) + '</span>'; });
    html += '</span></div>';
  }

  // At-risk notes
  if (ad.why_at_risk) {
    html += '<div class="priority-detail-row"><span class="priority-detail-icon">\u26A0\uFE0F</span><span><span class="priority-detail-label">Risk note</span>' + esc(ad.why_at_risk) + '</span></div>';
  }

  // SFDC deal + activity
  var sfdc = acc.sfdc;
  if (sfdc) {
    if (sfdc.open_opps && sfdc.open_opps[0]) {
      var o = sfdc.open_opps[0];
      var oppStr = (o.stage || 'Open Deal') + (o.acv_str ? ' \u00B7 ' + o.acv_str : '') + (o.close_date ? ' \u00B7 closes ' + o.close_date : '');
      html += '<div class="priority-detail-row"><span class="priority-detail-icon">\uD83D\uDCCA</span><span><span class="priority-detail-label">Open deal</span>' + esc(oppStr) + '</span></div>';
    }
    if (sfdc.no_activity) {
      html += '<div class="priority-detail-row"><span class="priority-detail-icon">\u23F1</span><span><span class="priority-detail-label">Last SFDC activity</span>No activity on record</span></div>';
    } else if (sfdc.days_since_activity != null) {
      html += '<div class="priority-detail-row"><span class="priority-detail-icon">\u23F1</span><span><span class="priority-detail-label">Last SFDC activity</span>' + esc(sfdc.days_since_activity === 0 ? 'Today' : sfdc.days_since_activity + ' days ago') + '</span></div>';
    }
    if (sfdc.engaged_contacts && sfdc.engaged_contacts.length > 0) {
      html += '<div class="priority-detail-row" style="align-items:flex-start;"><span class="priority-detail-icon">\uD83D\uDC64</span><span style="flex:1;"><span class="priority-detail-label">SFDC contacts engaged (90d)</span>';
      html += '<div style="margin-top:5px;display:flex;flex-direction:column;gap:4px;">';
      sfdc.engaged_contacts.forEach(function(c) {
        html += '<div style="font-size:12px;color:var(--text-2);">';
        if (c.name) html += '<span style="color:var(--text-1);font-weight:500;">' + esc(c.name) + '</span>';
        if (c.name && c.title) html += ' \u00B7 ';
        if (c.title) html += esc(c.title);
        html += '</div>';
      });
      html += '</div></span></div>';
    } else if (sfdc.engaged_contact_titles && sfdc.engaged_contact_titles.length > 0) {
      html += '<div class="priority-detail-row"><span class="priority-detail-icon">\uD83D\uDC64</span><span><span class="priority-detail-label">SFDC contacts engaged (90d)</span>' + esc(sfdc.engaged_contact_titles.slice(0,3).join(', ')) + '</span></div>';
    }
  }

  // SFDC activity history for this account
  var acctActivities = (DATA.account_activities || {})[acc.name.toLowerCase()] || [];
  if (acctActivities.length > 0) {
    html += '<div class="priority-detail-row" style="align-items:flex-start;"><span class="priority-detail-icon">\uD83D\uDCCB</span><span style="flex:1;">';
    html += '<span class="priority-detail-label">Recent SFDC activity</span>';
    html += '<div style="margin-top:6px;display:flex;flex-direction:column;gap:5px;">';
    acctActivities.forEach(function(act) {
      var typeLabel = act.type || 'Activity';
      var typeColor = act.type === 'Email' ? 'var(--accent2)' : act.type === 'Call' ? 'var(--teal)' : 'var(--warm)';
      html += '<div style="display:flex;align-items:flex-start;gap:8px;font-size:12px;">';
      html += '<span style="font-weight:700;color:' + typeColor + ';white-space:nowrap;min-width:44px;">' + esc(typeLabel) + '</span>';
      html += '<span style="color:var(--text-2);flex:1;">';
      var contactLabel = act.contact_name || act.contact_title || '';
      if (contactLabel) html += '<span style="color:var(--text-1);font-weight:500;">' + esc(contactLabel) + '</span>';
      if (act.contact_name && act.contact_title) html += ' <span style="color:var(--text-3);">(' + esc(act.contact_title) + ')</span>';
      if (contactLabel) html += ' \u00B7 ';
      html += esc(act.subject || '');
      html += '</span>';
      if (act.date) html += '<span style="color:var(--text-3);white-space:nowrap;margin-left:6px;">' + esc(act.date) + '</span>';
      html += '</div>';
    });
    html += '</div></span></div>';
  }

  // Recent news
  var acctNews = (DATA.account_news || {})[acc.name.toLowerCase()] || [];
  if (acctNews.length > 0) {
    html += '<div class="priority-detail-row" style="align-items:flex-start;"><span class="priority-detail-icon">\uD83D\uDCF0</span><span style="flex:1;">';
    html += '<span class="priority-detail-label">Recent news</span>';
    html += '<div style="margin-top:6px;display:flex;flex-direction:column;gap:8px;">';
    acctNews.forEach(function(article) {
      html += '<div style="font-size:12px;">';
      html += '<a href="' + esc(article.url) + '" target="_blank" rel="noopener" style="color:var(--text-1);text-decoration:none;font-weight:500;line-height:1.5;">' + esc(article.title) + '</a>';
      html += '<div style="font-size:11px;color:var(--text-3);margin-top:2px;">';
      if (article.source) html += esc(article.source);
      if (article.source && article.date) html += ' \u00B7 ';
      if (article.date) html += esc(article.date);
      html += '</div></div>';
    });
    html += '</div></span></div>';
  }

  // People who engaged at this account (from Demandbase activity/new_people)
  if (peopleLookup) {
    var acctPeople = peopleLookup[acc.name.toLowerCase()] || [];
    if (acctPeople.length > 0) {
      html += '<div class="priority-detail-row" style="align-items:flex-start;"><span class="priority-detail-icon">\uD83D\uDC65</span><span style="flex:1;">';
      html += '<span class="priority-detail-label">People who engaged this week</span>';
      html += '<div style="margin-top:6px;display:flex;flex-direction:column;gap:6px;">';
      acctPeople.forEach(function(p) {
        html += '<div style="background:var(--bg-card);border:1px solid var(--border);border-radius:8px;padding:8px 12px;">';
        html += '<div style="font-weight:600;font-size:13px;color:var(--text-1);">' + esc(p.full_name || '—') + '</div>';
        html += '<div style="font-size:12px;color:var(--text-2);margin-top:2px;">' + esc(p.title || '') + '</div>';
        if (p.details) html += '<div style="font-size:11px;color:var(--text-3);margin-top:3px;font-style:italic;">' + esc(p.details) + '</div>';
        var sc = p.sfdc_contact || {};
        if (sc.in_sfdc && sc.days_since_contact != null) {
          var cls = sc.days_since_contact > 90 ? 'sfdc-cold' : sc.days_since_contact > 14 ? 'sfdc-warm' : 'sfdc-fresh';
          html += '<div style="margin-top:5px;"><span class="sfdc-badge ' + cls + '" style="font-size:10px;">In SFDC \u00B7 last touched ' + sc.days_since_contact + 'd ago</span></div>';
        } else if (sc.in_sfdc) {
          html += '<div style="margin-top:5px;"><span class="sfdc-badge sfdc-contacts" style="font-size:10px;">In SFDC</span></div>';
        } else if (sc.in_sfdc === false) {
          html += '<div style="margin-top:5px;"><span class="sfdc-badge" style="font-size:10px;background:var(--bg-muted);color:var(--text-3);">Not in SFDC yet</span></div>';
        }
        if (p.seniority && p.seniority !== 'IC') {
          html += '<span style="float:right;margin-top:-28px;">' + renderSeniority(p.seniority) + '</span>';
        }
        html += '</div>';
      });
      html += '</div></span></div>';
    }
  }

  // SFDC action links
  var links = [];
  if (ad.account_url) links.push('<a href="' + esc(ad.account_url) + '" target="_blank" rel="noopener" style="color:var(--accent);font-weight:600;font-size:12px;text-decoration:none;">View in SFDC \u2197</a>');
  if (ad.new_opportunity_url) links.push('<a href="' + esc(ad.new_opportunity_url) + '" target="_blank" rel="noopener" style="color:var(--accent2);font-weight:600;font-size:12px;text-decoration:none;">+ Create Opportunity</a>');
  if (links.length > 0) {
    html += '<div class="priority-detail-row" style="margin-top:8px;"><span class="priority-detail-icon">\uD83D\uDD17</span><span style="display:flex;gap:16px;flex-wrap:wrap;">' + links.join('') + '</span></div>';
  }

  html += '</div>';
  return html;
}

/* ═══════════════════════════════════════════
   Top People — ranked by seniority + SFDC
   ═══════════════════════════════════════════ */
var SENIORITY_RANK = { 'C-Suite':5, 'VP':4, 'Director':3, 'Manager':2, 'IC':1 };

function computeTopPeople(signals) {
  var seen = {};
  var people = [];

  // Build a set of HVP account names (Closed Lost accounts back on Shopify pages)
  var hvpAccounts = {};
  (signals.hvp || []).forEach(function(r) {
    if (r.account) hvpAccounts[r.account.toLowerCase()] = true;
  });

  // Collect from activity + new_people — these have real names
  ['activity', 'new_people'].forEach(function(sigType) {
    (signals[sigType] || []).forEach(function(row) {
      if (!row.full_name) return;
      // Group by person+account but collect all their engagement details
      var key = (row.full_name || '') + '::' + (row.account || '');
      if (!seen[key]) {
        seen[key] = true;
        var accountName = (row.account || '').toLowerCase();
        var acctBoost = 0;
        ['mqa_new','hvp','intent_compete','g2_intent','hvp_all','intent_agentic'].forEach(function(st) {
          if ((signals[st] || []).some(function(r2){ return (r2.account||'').toLowerCase() === accountName; })) {
            acctBoost += SIGNAL_WEIGHTS[st] || 0;
          }
        });
        var senRank = SENIORITY_RANK[row.seniority] || 0;
        var sc = row.sfdc_contact || {};
        var daysContact = sc.days_since_contact;
        var contactBonus = sc.in_sfdc ? (daysContact > 90 ? 30 : daysContact > 30 ? 15 : 5) : 0;
        people.push({
          name:        row.full_name,
          title:       row.title || '',
          account:     row.account || '',
          seniority:   row.seniority || 'IC',
          engagements: [],   // collect all their engagement events
          sfdc_contact: sc,
          isHvpAccount: !!hvpAccounts[accountName],
          accountHot:  acctBoost > 50,
          score:       senRank * 20 + acctBoost + contactBonus,
        });
      }
      // Add this engagement event to the person (even if they were already seen)
      var existing = people.find(function(p) {
        return p.name === row.full_name && p.account === (row.account || '');
      });
      if (existing && row.details) {
        var eventLabel = row.details + (row.category && row.category !== 'All Others' ? ' (' + row.category + ')' : '');
        if (existing.engagements.indexOf(eventLabel) === -1) existing.engagements.push(eventLabel);
      }
    });
  });

  return people.sort(function(a,b){ return b.score - a.score; });
}

function suggestAccountOutreach(acc) {
  var ad = (DATA.account_details || {})[acc.name.toLowerCase()] || {};
  var sfdc = acc.sfdc || {};
  var platform = (ad.ecomm_platform || '').toLowerCase();
  var industry = (ad.industry || '').toLowerCase();
  var st = acc.signalTypes;
  var keywords = acc.keywords || [];
  var parts = [];

  // 1. What's driving this — the opening frame
  if (st.indexOf('mqa_new') !== -1) {
    parts.push('Account crossed the MQA threshold this week — 200+ engagement points means active research is happening right now. Strike while intent is high.');
  } else if (st.indexOf('hvp') !== -1) {
    parts.push('This was a Closed Lost deal and they\'re back on Shopify Plus pages. Something has changed — a new budget cycle, a bad experience with their current platform, or internal pressure to switch. Re-engage with "what\'s changed since we last spoke."');
  } else if (st.indexOf('intent_compete') !== -1) {
    var competeKws = keywords.filter(function(k) {
      return /woocommerce|bigcommerce|sfcc|salesforce|sap|adobe|magento|commercetools|shopware|vtex/i.test(k);
    });
    if (competeKws.length > 0) {
      parts.push('Actively evaluating ' + competeKws.slice(0,2).join(' and ') + ' this week. They\'re in a platform evaluation — get in front of them before a shortlist is formed.');
    } else {
      parts.push('In an active platform evaluation this week. Get in front of them before a shortlist is formed.');
    }
  } else if (st.indexOf('intent_agentic') !== -1) {
    parts.push('Researching AI commerce and agentic buying this week. Lead with Shopify\'s AI-native checkout, Sidekick, and how Shopify is building for the next generation of commerce.');
  } else if (st.indexOf('intent_international') !== -1) {
    parts.push('Showing international expansion intent. Lead with Shopify Markets — multi-currency, localised checkout, duties and tax handling — and how quickly merchants go live in new markets.');
  } else if (st.indexOf('intent_b2b') !== -1) {
    parts.push('Researching B2B commerce solutions. Lead with Shopify B2B — company accounts, custom pricing, purchase orders, and net terms built natively into Shopify Plus.');
  } else if (st.indexOf('intent_marketing') !== -1) {
    parts.push('Researching marketing and growth tools. Lead with Shop Campaigns, Shop Pay\'s buyer network (150M+ buyers), and Shopify\'s native marketing integrations.');
  } else if (st.indexOf('hvp_all') !== -1) {
    parts.push('Visiting Shopify Plus and enterprise pages this week — they\'re actively researching. Good moment for a timely, relevant outreach.');
  }

  // 2. Platform angle
  if (/salesforce.*(commerce|cloud)|sfcc/i.test(platform)) {
    parts.push('On SFCC: lead with total cost of ownership (SFCC licensing alone is $500K–$2M+/yr), faster time to market, and Shopify\'s upgrade cycle vs Salesforce\'s release schedule.');
  } else if (/sap.*(commerce|hybris)/i.test(platform)) {
    parts.push('On SAP: lead with operational simplicity — SAP Commerce is complex to maintain. Shopify connects natively to SAP ERP via APIs so they keep the back-end they know.');
  } else if (/adobe|magento/i.test(platform)) {
    parts.push('On Adobe/Magento: lead with support reliability, lower upgrade costs, and the developer experience difference. Magento merchants are often frustrated with security patches and upgrade cycles.');
  } else if (/bigcommerce/i.test(platform)) {
    parts.push('On BigCommerce: competitive displacement play. Lead with Shopify Plus\'s checkout conversion rates, partner ecosystem, and enterprise scale.');
  } else if (/custom.*(build|platform)|bespoke/i.test(platform)) {
    parts.push('Custom build: they\'re carrying significant platform maintenance cost and developer burden. Lead with what their team could build on top of Shopify instead of maintaining infrastructure underneath it.');
  } else if (/shopify.*(plus|advanced)/i.test(platform)) {
    parts.push('Already on Shopify: expansion play — B2B, POS, Markets, or upgrading to Plus. Lead with what they\'re not using yet.');
  }

  // 3. Industry angle
  if (/apparel|fashion|clothing|accessori/i.test(industry)) {
    parts.push('Apparel vertical: lead with visual merchandising, size/variant handling, returns, and how Shopify powers brands like Allbirds, Supreme, and Gymshark.');
  } else if (/beauty|cosmetic|personal care/i.test(industry)) {
    parts.push('Beauty vertical: subscription commerce, loyalty programs, and DTC direct relationships — Shopify powers Kylie, Glossier, and hundreds of beauty DTC brands.');
  } else if (/food|beverage|cpg/i.test(industry)) {
    parts.push('CPG/Food & Bev: DTC + wholesale channel management, subscription, and Shopify Markets for international distribution.');
  } else if (/sport|outdoor|fitness/i.test(industry)) {
    parts.push('Sports/Outdoor: lead with POS for retail locations, B2B for team/wholesale sales, and the unified commerce story.');
  } else if (/health|pharma|medical|wellness/i.test(industry)) {
    parts.push('Health/Wellness: lead with compliance-ready checkout, subscription commerce, and DTC patient/consumer direct relationships.');
  } else if (/home|furnishing|interior/i.test(industry)) {
    parts.push('Home/Furniture: lead with large catalogue management, configurable products (Shopify Functions), and B2B trade accounts.');
  } else if (/tech|software|electronics/i.test(industry)) {
    parts.push('Tech/Electronics: lead with B2B, complex product configurations, and developer extensibility — they\'ll appreciate Shopify\'s API-first approach.');
  } else if (/luxury|jewellery|jewelry|watch/i.test(industry)) {
    parts.push('Luxury: lead with brand experience control, Hydrogen for custom storefronts, and how Shopify scales for high-AOV low-volume commerce.');
  }

  // 4. Deal + activity context — how to frame the outreach
  if (sfdc.has_deal && sfdc.open_opps && sfdc.open_opps[0]) {
    var opp = sfdc.open_opps[0];
    var d = sfdc.days_since_activity;
    if (d && d > 30) {
      parts.push('Open deal (' + (opp.stage || 'in progress') + ') stalled for ' + d + ' days. Use this week\'s engagement signal as a reason to re-engage — "I noticed your team has been active on our platform pages."');
    } else {
      parts.push('Open deal already in ' + (opp.stage || 'progress') + '. Use this signal to add urgency — they\'re still actively researching.');
    }
  } else if (!sfdc.has_deal) {
    if (sfdc.no_activity) {
      parts.push('No SFDC activity on record — likely never contacted. Treat as a cold open: personalise to their platform, their industry, and what\'s happening at their company this week.');
    } else if (sfdc.days_since_activity > 180) {
      parts.push('Cold for ' + sfdc.days_since_activity + ' days with no open deal. Reintroduce: a lot has changed at Shopify — use new capabilities as the reason to reconnect rather than referencing the old conversation.');
    } else if (sfdc.days_since_activity > 60) {
      parts.push('No open deal, cold for ' + sfdc.days_since_activity + ' days. Good net-new opportunity — this week\'s engagement is your reason to reach out.');
    }
  }

  // 5. Intent keyword hooks — specific things to reference
  var hookKws = keywords.filter(function(k) {
    return /pos terminal|point of sale|shop pay|checkout|b2b|cross border|ai chatbot|large language/i.test(k);
  }).slice(0, 2);
  if (hookKws.length > 0) {
    parts.push('Specific hooks from their research: ' + hookKws.join(', ') + ' — reference these directly in your outreach to show you know what they\'re looking at.');
  }

  return parts;
}

function suggestOutreach(person, signals) {
  var title = (person.title || '').toLowerCase();
  var accountLower = (person.account || '').toLowerCase();
  var ad = (DATA.account_details || {})[accountLower] || {};
  var platform = (ad.ecomm_platform || '').toLowerCase();
  var sc = person.sfdc_contact || {};
  var parts = [];

  // 1. Role angle — what this person cares about
  var t = person.title;
  if (/chief.*(exec|executive)|\bceo\b/i.test(t)) {
    parts.push('Lead with growth strategy and competitive positioning — they care about scale, speed, and staying ahead of competitors');
  } else if (/chief.*(financ|financial)|\bcfo\b/i.test(t)) {
    parts.push('Lead with total cost of ownership — platform licensing, payment processing fees, and Shop Pay conversion uplift (typically 10–20% checkout lift)');
  } else if (/chief.*(digit|dtc)|\bcdo\b|chief dtc/i.test(t)) {
    parts.push('Lead with digital experience — storefront performance, checkout conversion, and Shopify\'s composable frontend (Hydrogen/Headless)');
  } else if (/chief.*(tech|technolog)|\bcto\b/i.test(t)) {
    parts.push('Lead with API-first architecture, developer velocity, and extensibility via Shopify Functions and Apps');
  } else if (/chief.*(info|information)|\bcio\b/i.test(t)) {
    parts.push('Lead with infrastructure consolidation, platform reliability (99.99% uptime), and reduced maintenance overhead');
  } else if (/chief.*(market|marketing)|\bcmo\b/i.test(t)) {
    parts.push('Lead with Shop Pay, Shop Campaigns, and customer acquisition — Shopify\'s media network and conversion tools');
  } else if (/chief.*(operat|operating)|\bcoo\b/i.test(t)) {
    parts.push('Lead with operational efficiency — unified commerce, fulfillment integrations, and reduced platform complexity');
  } else if (/chief.*(revenue|commercial)|\bcro\b/i.test(t)) {
    parts.push('Lead with revenue growth levers — checkout conversion, Shop Pay, and channel expansion (B2B, POS, Markets)');
  } else if (/\bpresident\b|\bfounder\b|\bowner\b|\bco-founder\b/i.test(t)) {
    parts.push('Decision maker — lead with business outcomes: revenue growth, operational simplicity, and why leading merchants choose Shopify');
  } else if (/ecommerce|e-commerce|digital commerce|\bdtc\b|dtc ecom/i.test(t)) {
    parts.push('Lead with checkout performance, conversion tools, and platform flexibility — they own the P&L for the digital channel');
  } else if (/omnichannel|unified.*(commerce|retail)|retail.*tech|store.*tech/i.test(t)) {
    parts.push('Lead with POS, unified commerce, and real-time inventory — Shopify is the only platform built for both online and in-store at scale');
  } else if (/payment|treasury/i.test(t)) {
    parts.push('Lead with payment processing costs, Shop Pay conversion uplift, and financial reconciliation — they care about every basis point');
  } else if (/\bmarketing\b/i.test(t) && !/product marketing/i.test(t)) {
    parts.push('Lead with Shop Campaigns, Shop Pay, and Shopify\'s native marketing integrations — they care about ROAS and customer acquisition cost');
  } else if (/\bfinance\b|\bfinancial\b/i.test(t)) {
    parts.push('Lead with cost reduction — payment fees, platform licensing TCO, and Shop Pay conversion uplift');
  } else if (/\bit\b.*digital|digital.*\bit\b|vp it|head of it/i.test(t)) {
    parts.push('IT + digital hybrid — lead with platform reliability, integration capabilities, and reducing technical debt from legacy systems');
  } else if (/\btechnology\b|\bengineer\b|\barchitect\b|\bdevelop/i.test(t)) {
    parts.push('Lead with Shopify APIs, Functions, and developer experience — extensibility without forking the platform');
  } else if (/\bproduct\b/i.test(t)) {
    parts.push('Lead with Shopify Functions and speed of iteration — ship faster without waiting on a platform vendor');
  } else if (/loyalty|retention|crm|customer experience/i.test(t)) {
    parts.push('Lead with customer experience and retention — Shop Pay\'s one-tap checkout, loyalty integrations, and personalisation at scale');
  } else if (/creative|brand|content/i.test(t)) {
    parts.push('Lead with storefront quality — Hydrogen for custom frontends, Shopify\'s theme ecosystem, and brand experience at scale');
  } else if (/supply chain|\blogistic|\bfulfilment|\bfulfillment/i.test(t)) {
    parts.push('Lead with fulfillment integrations, Shopify Shipping, and real-time inventory across locations');
  } else if (/\boperations\b|\bstrateg/i.test(t)) {
    parts.push('Lead with operational efficiency and strategic growth — Shopify Plus removes platform complexity so they can focus on the business');
  } else if (/\bsales\b|\bbusiness dev|\bbiz dev/i.test(t)) {
    parts.push('Lead with Shopify B2B, wholesale, and channel expansion — they care about growing revenue through new channels');
  } else if (/sourcing|procurement|purchasing/i.test(t)) {
    parts.push('Lead with cost reduction and vendor consolidation — Shopify Plus reduces the number of platforms and vendors needed');
  } else if (/director|manager|head of|vp\b|vice president/i.test(t)) {
    parts.push('Mid-to-senior decision influencer — lead with what Shopify enables in their domain and how peers in their vertical use it');
  } else {
    parts.push('Tailor outreach to their account context — reference what their company is researching and lead with a relevant Shopify capability');
  }

  // 2. Platform angle — migration or expansion context
  if (/salesforce.*(commerce|cloud)|sfcc/i.test(platform)) {
    parts.push('Currently on SFCC — strong TCO and migration story, faster time to market vs Salesforce');
  } else if (/sap.*(commerce|hybris)/i.test(platform)) {
    parts.push('Currently on SAP — lead with operational simplicity and native SAP integration via APIs');
  } else if (/adobe|magento/i.test(platform)) {
    parts.push('Currently on Adobe/Magento — lead with support reliability, upgrade costs, and developer experience');
  } else if (/bigcommerce/i.test(platform)) {
    parts.push('Currently on BigCommerce — competitive displacement angle, lead with enterprise scale and checkout');
  } else if (/custom.*(build|platform)|bespoke/i.test(platform)) {
    parts.push('Custom build — lead with speed of iteration and reducing internal platform maintenance cost');
  } else if (/shopify.*(plus|advanced|basic)/i.test(platform)) {
    parts.push('Already on Shopify — focus on Plus upgrade, new features, or expansion into B2B/POS');
  }

  // 3. Signal angle — what their account is doing this week
  var inSignal = function(types) {
    return [].concat(types).some(function(st) {
      return (signals[st] || []).some(function(r) { return (r.account||'').toLowerCase() === accountLower; });
    });
  };
  if (inSignal('g2_intent')) {
    parts.push('Account is actively comparing vendors on G2 right now — they\'re in evaluation mode. Best time to reach out is this week, not next');
  } else if (inSignal('intent_compete')) {
    parts.push('Account is actively evaluating competitors this week — ideal time to get in front of them');
  } else if (inSignal('intent_agentic')) {
    parts.push('Account is researching AI commerce — lead with Shopify\'s AI-native checkout and Sidekick');
  } else if (inSignal('intent_international')) {
    parts.push('Account showing international expansion intent — lead with Shopify Markets and cross-border');
  } else if (inSignal('intent_b2b')) {
    parts.push('Account showing B2B intent — lead with Shopify B2B and wholesale features');
  } else if (inSignal('intent_marketing')) {
    parts.push('Account researching marketing and growth tools — lead with Shop Campaigns and Shop Pay');
  } else if (inSignal('mqa_new')) {
    parts.push('Account just hit MQA — high engagement, outreach window is open now');
  } else if (inSignal('hvp')) {
    parts.push('Closed Lost account back on Shopify Plus pages — re-engagement opportunity, reference what\'s changed');
  }

  // 4. Cold framing — how to open
  if (!sc.in_sfdc) {
    parts.push('Not yet in SFDC — first touch, personalise to their role and recent engagement');
  } else if (sc.days_since_contact > 180) {
    parts.push('Very cold (' + sc.days_since_contact + 'd) — reintroduce without referencing the gap, lead with something new');
  } else if (sc.days_since_contact > 90) {
    parts.push('Cold (' + sc.days_since_contact + 'd) — reference what\'s new at Shopify since you last spoke');
  } else if (sc.days_since_contact > 30) {
    parts.push('Warm (' + sc.days_since_contact + 'd since last touch) — follow up on prior conversation');
  }

  return parts;
}

function renderTopPeople(signals) {
  var people = computeTopPeople(signals);
  if (people.length === 0) return '';

  var html = '<div class="top-section reveal">';
  html += '<div class="top-section-label">\uD83D\uDC65 Top engaged people this week</div>';

  people.forEach(function(p, i) {
    var sc = p.sfdc_contact || {};
    var cardId = 'person-card-' + i;
    var rankCls = i < 3 ? ' r' + (i+1) : '';

    html += '<div class="priority-card" id="' + cardId + '">';
    // Collapsed header — rank + name + title + expand arrow
    html += '<div class="priority-card-header" data-card="' + cardId + '">';
    html += '<div class="priority-rank' + rankCls + '">' + (i+1) + '</div>';
    html += '<div class="priority-body">';
    html += '<div class="priority-name">' + esc(p.name) + '</div>';
    var metaParts = [];
    if (p.title) metaParts.push(esc(p.title));
    if (p.account) metaParts.push('<strong>' + esc(p.account) + '</strong>');
    if (metaParts.length) html += '<div class="priority-why">' + metaParts.join(' \u00B7 ') + '</div>';
    // Compact tags row in header
    html += '<div class="priority-tags">';
    if (p.seniority && p.seniority !== 'IC') html += renderSeniority(p.seniority);
    if (p.isHvpAccount) html += '<span class="badge badge-hvp" style="font-size:10px;padding:2px 7px;">\u21A9 Previously CL</span>';
    if (p.accountHot && !p.isHvpAccount) html += '<span class="badge badge-mqa_new" style="font-size:10px;padding:2px 7px;">\u2605 Hot account</span>';
    if (sc.in_sfdc && sc.days_since_contact != null) {
      var dcls = sc.days_since_contact > 60 ? 'sfdc-cold' : sc.days_since_contact > 14 ? 'sfdc-warm' : 'sfdc-fresh';
      var dlabel = sc.days_since_contact === 0 ? 'Contacted today' : sc.days_since_contact + 'd since contact';
      html += '<span class="sfdc-badge ' + dcls + '" style="font-size:10px;">' + esc(dlabel) + '</span>';
    } else if (sc.in_sfdc === false) {
      html += '<span class="sfdc-badge" style="font-size:10px;background:var(--bg-muted);color:var(--text-3);">Not in SFDC</span>';
    }
    html += '</div>';
    html += '</div>'; // priority-body
    html += '<div class="priority-expand">\u2304</div>';
    html += '</div>'; // priority-card-header

    // Expanded detail
    html += '<div class="priority-detail">';

    // What they engaged with
    if (p.engagements.length > 0) {
      html += '<div style="font-size:12px;color:var(--text-3);margin-bottom:10px;font-style:italic;line-height:1.6;">' + esc(p.engagements.slice(0,3).join(' \u00B7 ')) + '</div>';
    }

    // Outreach angle
    var suggestion = suggestOutreach(p, signals);
    if (suggestion.length > 0) {
      html += '<div style="margin-bottom:12px;padding:10px 12px;background:var(--accent-light);border-radius:8px;border-left:3px solid var(--accent);">';
      html += '<div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:var(--accent);margin-bottom:5px;">Outreach angle</div>';
      suggestion.forEach(function(line) {
        html += '<div style="font-size:12px;color:var(--text-2);line-height:1.6;padding-left:10px;position:relative;margin-bottom:3px;">';
        html += '<span style="position:absolute;left:0;color:var(--accent);">\u203A</span>' + esc(line);
        html += '</div>';
      });
      html += '</div>';
    }

    // Email + SFDC link
    var contactLinks = [];
    if (sc.email) contactLinks.push('<a href="mailto:' + esc(sc.email) + '" style="font-size:12px;color:var(--accent);text-decoration:none;font-weight:500;">\u2709 ' + esc(sc.email) + '</a>');
    if (sc.contact_url) contactLinks.push('<a href="' + esc(sc.contact_url) + '" target="_blank" rel="noopener" style="font-size:12px;color:var(--accent2);text-decoration:none;font-weight:500;">View in SFDC \u2197</a>');
    if (contactLinks.length > 0) {
      html += '<div style="display:flex;gap:14px;flex-wrap:wrap;">' + contactLinks.join('') + '</div>';
    }

    html += '</div>'; // priority-detail
    html += '</div>'; // priority-card
  });

  html += '</div>';
  return html;
}

function renderG2Section(signals) {
  var rows = (signals.g2_intent || []);
  if (!rows.length) return '';

  // Build people lookup from activity/new_people (same as Top Accounts)
  var peopleLookup = {};
  ['activity', 'new_people'].forEach(function(st) {
    (signals[st] || []).forEach(function(row) {
      var key = (row.account || '').toLowerCase();
      if (!key) return;
      if (!peopleLookup[key]) peopleLookup[key] = [];
      var already = peopleLookup[key].some(function(r) { return r.full_name === row.full_name; });
      if (!already) peopleLookup[key].push(row);
    });
  });

  // Sort: Grade A first, then by stage priority
  var stageOrder = { 'MQA': 0, 'SAL Opportunity': 1, 'Intent': 2, 'Engaged': 3, 'Closed-Won': 4 };
  var gradeOrder = { A: 0, B: 1, C: 2, D: 3 };
  rows = rows.slice().sort(function(a, b) {
    var ga = gradeOrder[a.grade] !== undefined ? gradeOrder[a.grade] : 9;
    var gb = gradeOrder[b.grade] !== undefined ? gradeOrder[b.grade] : 9;
    if (ga !== gb) return ga - gb;
    var sa = stageOrder[a.journey_stage] !== undefined ? stageOrder[a.journey_stage] : 9;
    var sb = stageOrder[b.journey_stage] !== undefined ? stageOrder[b.journey_stage] : 9;
    return sa - sb;
  });

  var gradeBg    = { A: '#d1fae5', B: '#fef3c7', C: '#dbeafe', D: '#f3f4f6' };
  var gradeColor = { A: '#065f46', B: '#92400e', C: '#1e3a5f', D: '#4b5563' };

  var html = '<div class="top-section reveal" id="section-g2_intent">';
  html += '<div class="top-section-label">🔍 Actively Researching on G2';
  html += ' <button class="help-btn" data-help="g2_intent" aria-label="What is G2 Research?" style="margin-left:6px;">?</button>';
  html += '</div>';

  rows.forEach(function(row, i) {
    var account  = row.account || '';
    var grade    = row.grade || '';
    var stage    = row.journey_stage || '';
    var priority = row.priority || '';
    var isCW     = (stage === 'Closed-Won');
    var cardId   = 'g2-' + i;

    // Build acc object in the same shape as computeTopAccounts outputs
    var acc = {
      name:        account,
      signalTypes: ['g2_intent'],
      sfdc:        row.sfdc || null,
      keywords:    [],
      details:     { g2_intent: row }
    };

    html += '<div class="priority-card" id="' + cardId + '">';
    html += '<div class="priority-card-header" data-card="' + cardId + '">';

    // Grade circle (replaces rank number)
    html += '<div class="priority-rank" style="background:' + (gradeBg[grade]||'#f3f4f6') + ';color:' + (gradeColor[grade]||'#4b5563') + ';font-size:13px;font-weight:800;">' + (grade || '?') + '</div>';

    html += '<div class="priority-body">';
    html += '<div class="priority-name">' + esc(account) + '</div>';

    // Why line — stage + G2 context
    var whyParts = [];
    if (isCW) {
      whyParts.push('Existing customer — researching on G2');
    } else if (stage === 'MQA') {
      whyParts.push('MQA account actively comparing vendors on G2');
    } else if (stage === 'SAL Opportunity') {
      whyParts.push('Active opportunity — researching on G2 now');
    } else if (stage === 'Intent') {
      whyParts.push('Showing intent and comparing vendors on G2');
    } else {
      whyParts.push('Actively comparing vendors on G2');
    }
    html += '<div class="priority-why">' + esc(whyParts[0]) + '</div>';

    // Tags — stage + priority fit segments
    html += '<div class="priority-tags">';
    if (isCW) {
      html += '<span class="badge" style="background:#d1fae5;color:#065f46;font-size:11px;padding:2px 8px;">\u2713 Customer</span>';
    } else if (stage) {
      html += '<span class="badge badge-g2_intent" style="font-size:11px;padding:2px 8px;">' + esc(stage) + '</span>';
    }
    if (priority) {
      priority.split(';').forEach(function(p) {
        p = p.trim(); if (!p) return;
        var isHigh = p.indexOf('High') !== -1;
        html += '<span class="badge" style="font-size:11px;padding:2px 8px;background:' + (isHigh ? '#fef3c7' : 'var(--bg-muted)') + ';color:' + (isHigh ? '#b45309' : 'var(--text-3)') + ';">' + esc(p) + '</span>';
      });
    }
    html += '</div>';
    html += '</div>'; // priority-body
    html += '<div class="priority-expand">\u2304</div>';
    html += '</div>'; // priority-card-header

    // SFDC badges inline (visible without expanding)
    if (acc.sfdc) {
      html += '<div style="padding:6px 16px 4px;">' + renderSfdcBadges(acc.sfdc) + '</div>';
    }

    // Expanded detail — same as Top Accounts
    html += renderAccountCardExpanded(acc, peopleLookup);

    html += '</div>'; // priority-card
  });

  html += '</div>'; // top-section
  return html;
}

function renderTopLeads(sellerId) {
  var seller = DATA.sellers[sellerId];
  var leads = seller && seller.signals && seller.signals.top_leads;
  if (!leads || leads.length === 0) return '';

  var LIMIT = 20;
  var total = leads.length;
  var intentCount = leads.filter(function(l) { return l.intent_active; }).length;
  var showAll = false;
  var sectionId = 'topLeadsSection';

  var html = '<div class="top-leads-section signal-section reveal" id="' + sectionId + '">';
  html += '<div class="top-leads-heading"><span class="badge badge-top_leads">Top Leads</span> Sales Nav Leads for Your Accounts <button class="help-btn" data-help="top_leads" aria-label="What are Top Leads?">?</button></div>';
  html += '<div class="top-leads-desc">Your highest-priority contacts from LinkedIn Sales Navigator, ranked by account fit.';
  if (intentCount > 0) html += ' <strong>' + intentCount + ' lead' + (intentCount !== 1 ? 's' : '') + '</strong> at accounts with active intent this week.';
  html += '</div>';

  html += '<div class="table-wrap"><table class="data-table"><thead><tr>';
  html += '<th>Name</th><th>Title</th><th>Company</th><th>City</th><th>Fit</th><th>Intent</th>';
  html += '</tr></thead><tbody id="topLeadsBody">';

  leads.forEach(function(lead, idx) {
    var rowCls = lead.intent_active ? ' class="intent-active-row"' : '';
    var hidden = idx >= LIMIT ? ' style="display:none" data-extra-lead="1"' : '';
    html += '<tr' + rowCls + hidden + '>';
    html += '<td>' + esc(lead.name) + '</td>';
    html += '<td>' + esc(lead.title) + '</td>';
    html += '<td>' + esc(lead.company) + '</td>';
    html += '<td>' + esc(lead.city) + '</td>';

    var fit = lead.fit_score || '';
    var fitShort = fit.replace(' Fit', '');
    var fitCls = 'fit-poor';
    if (fit === 'Excellent Fit') fitCls = 'fit-excellent';
    else if (fit === 'Good Fit') fitCls = 'fit-good';
    else if (fit === 'Potential Fit') fitCls = 'fit-potential';
    html += '<td><span class="fit-pill ' + fitCls + '">' + esc(fitShort) + '</span></td>';

    if (lead.intent_active) {
      var types = (lead.intent_types || []).map(function(t) {
        var st = DATA.signal_types[t];
        return st ? st.short_label : t;
      }).join(', ');
      html += '<td><span class="intent-badge">\uD83D\uDD25 Active</span></td>';
    } else {
      html += '<td><span class="intent-badge intent-badge-inactive">\u2014</span></td>';
    }
    html += '</tr>';
  });

  html += '</tbody></table></div>';

  if (total > LIMIT) {
    html += '<button class="leads-toggle" id="topLeadsToggle">Show all ' + total + ' leads</button>';
  }

  html += '<div class="top-leads-meta">Lead data from Sales Navigator. Intent flags refresh weekly from Demandbase signals.</div>';
  html += '</div>';
  return html;
}

function renderPersonal(sellerId) {
  var seller = DATA.sellers[sellerId];
  var sellerRegion = seller.region || 'NA';
  var types = Object.keys(DATA.signal_types).filter(function(t) {
    if (t === 'top_leads') return false;
    var isAnzType = t.indexOf('anz_') === 0;
    if (sellerRegion === 'ANZ') return isAnzType;
    return !isAnzType;
  });
  var m = DATA.meta;
  var firstName = seller.name.split(' ')[0];
  var initials = seller.name.split(' ').map(function(w) { return w.charAt(0); }).join('').substring(0, 2).toUpperCase();

  var nonZeroTypes = types.filter(function(t) { return (seller.summary[t] || 0) > 0; });
  var hotCount = nonZeroTypes.length > 0 ? seller.summary.total : 0;

  // Build stat pills
  var statDefs = types.filter(function(t) { return t !== 'top_leads' && (seller.summary[t] || 0) > 0; }).map(function(t) {
    var st = DATA.signal_types[t];
    return { key: t, num: seller.summary[t] || 0, label: st.short_label, cls: TYPE_CLS[t] || t };
  });

  var backLabel = (IDENTITY_ROLE === 'admin' || IDENTITY_ROLE === 'coach') ? '\u2190 Dashboard' : '\u2190 All sellers';

  var html = '';
  // Compact centered header: avatar + name + back link + week + signal pills
  html += '<header><div class="hero-personal fade-in">';
  html += '<div style="text-align:center;width:100%;">';
  html += '<div class="avatar" aria-hidden="true" style="margin:0 auto 10px;">' + esc(initials) + '</div>';
  html += '<h1>' + esc(seller.name) + '</h1>';
  html += '<div class="sub-personal" style="margin-top:4px;">Week of <strong>' + esc(m.week_of) + '</strong></div>';
  html += '<div style="margin-top:4px;"><a href="#" id="clearIdentity" style="font-size:12px;color:var(--text-3);text-decoration:none;font-weight:500;">' + backLabel + '</a></div>';

  // Compute stat counts
  var mqaNewCount = seller.summary.mqa_new || 0;
  var topAcctsCount = computeTopAccounts(seller.signals).length;
  var engagedPeopleUnique = {};
  ['activity','new_people'].forEach(function(st){
    (seller.signals[st]||[]).forEach(function(r){ if(r.full_name) engagedPeopleUnique[(r.full_name+'::'+r.account).toLowerCase()]=true; });
  });
  var engagedPeopleCount = Object.keys(engagedPeopleUnique).length;
  var hvpUnique = {};
  (seller.signals.hvp||[]).forEach(function(r){ if(r.account) hvpUnique[r.account.toLowerCase()]=true; });
  (seller.signals.hvp_all||[]).forEach(function(r){ if(r.account && r.website) hvpUnique[r.account.toLowerCase()]=true; });
  var hvpCount = Object.keys(hvpUnique).length;

  var heroStats = [
    { num: mqaNewCount,        label: 'New MQA',                    cls: 'mqa_new' },
    { num: topAcctsCount,      label: 'Top accounts this week',      cls: 'hvp_all' },
    { num: engagedPeopleCount, label: 'Engaged people',              cls: 'new_people' },
    { num: hvpCount,           label: 'Visiting high-value pages',   cls: 'all_mqa' },
  ].filter(function(s){ return s.num > 0; });

  if (heroStats.length > 0) {
    html += '<div style="display:grid;grid-template-columns:repeat(' + heroStats.length + ',1fr);gap:1px;background:var(--border);border:1px solid var(--border);border-radius:var(--radius-sm);overflow:hidden;margin-top:14px;">';
    heroStats.forEach(function(s) {
      html += '<div style="background:var(--bg-card);padding:10px 8px;text-align:center;">';
      html += '<div style="font-family:var(--font-serif);font-size:22px;font-weight:400;line-height:1;color:var(--text-1);">' + s.num + '</div>';
      html += '<div style="font-size:11px;color:var(--text-3);margin-top:3px;line-height:1.3;">' + esc(s.label) + '</div>';
      html += '</div>';
    });
    html += '</div>';
  }
  html += '</div></div></header>';

  html += '<div class="wrap" style="padding-top:24px;padding-bottom:48px;">';

  // Seller summary — 2-3 sentence contextual brief
  var summaryLines = generateSellerSummary(seller, seller.signals);
  if (summaryLines.length > 0) {
    html += '<div style="margin-bottom:28px;padding:16px 20px;background:var(--bg-card);border:1.5px solid var(--border);border-radius:var(--radius-sm);box-shadow:var(--shadow);">';
    html += '<div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:var(--text-3);margin-bottom:8px;">This week</div>';
    summaryLines.forEach(function(line, i) {
      html += '<p style="font-size:14px;color:var(--text-2);line-height:1.7;margin:0' + (i > 0 ? ';margin-top:6px' : '') + ';">' + esc(line) + '</p>';
    });
    html += '</div>';
  }

  // Top Accounts + Top People (new priority sections)
  html += renderTopAccounts(seller.signals);
  html += renderTopPeople(seller.signals);

  // G2 Research section
  html += renderG2Section(seller.signals);

  // Top Leads (Consumer team only)
  html += renderTopLeads(sellerId);

  // Signal sections — skip activity/new_people (Top People), hvp (merged into hvp_all), all intent + all_mqa (collapsed at bottom)
  var SKIP_TYPES = ['activity', 'new_people', 'hvp', 'intent_agentic', 'intent_compete', 'intent_international', 'intent_marketing', 'intent_b2b', 'all_mqa', 'g2_intent'];
  var hasAnyData = false;
  types.forEach(function(t) {
    if (SKIP_TYPES.indexOf(t) !== -1) return;

    // For hvp_all: merge in hvp rows (tagged as Closed Lost) at the top
    var rawSignals = seller.signals[t] || [];
    var signals = rawSignals;
    var lostOppAccounts = {};
    if (t === 'hvp_all') {
      var hvpRows = (seller.signals.hvp || []).map(function(r) {
        lostOppAccounts[(r.account || '').toLowerCase()] = true;
        return Object.assign({}, r, { _is_lost_opp: true });
      });
      var hvpAllFiltered = rawSignals.filter(function(r) {
        return !lostOppAccounts[(r.account || '').toLowerCase()];
      });
      signals = hvpRows.concat(hvpAllFiltered);
    }

    // Remove rows with no website URL (HVP only)
    if (t === 'hvp_all') {
      signals = signals.filter(function(r) { return r.website && r.website.trim() !== ''; });
    }

    // Remove accounts already in Top Accounts (HVP only)
    if (t === 'hvp_all') {
      var topAcctNames = computeTopAccounts(seller.signals).map(function(a) { return a.name.toLowerCase(); });
      signals = signals.filter(function(r) { return topAcctNames.indexOf((r.account||'').toLowerCase()) === -1; });
    }

    // Intent: only show confirmed accounts (have SFDC contacts OR appear in MQA/HVP/HVP_All)
    if (t.indexOf('intent_') === 0) {
      var strongSet = {};
      ['mqa_new','hvp','hvp_all'].forEach(function(st) {
        (seller.signals[st] || []).forEach(function(r) {
          if (r.account) strongSet[r.account.toLowerCase()] = true;
        });
      });
      signals = signals.filter(function(r) {
        var sfdc = r.sfdc || {};
        return sfdc.engaged_contact_count > 0 || strongSet[(r.account||'').toLowerCase()];
      });
    }

    if (!signals || signals.length === 0) return;
    hasAnyData = true;
    var st = DATA.signal_types[t];
    var cls = TYPE_CLS[t] || t;
    // For combined HVP section, use hvp_all columns as base
    var colSource = t === 'hvp_all' ? (seller.signals.hvp_all || []) : signals;
    var cols = filterVisibleCols(st.display_columns, colSource.length ? colSource : signals);
    var def = SIGNAL_DEFS[t];
    var isMqa = t === 'mqa_new';

    // Section heading — rename hvp_all when it includes lost opps
    var sectionLabel = st.label;
    var lostCount = (seller.signals.hvp || []).length;
    if (t === 'hvp_all' && lostCount > 0) {
      sectionLabel = 'Accounts Visiting High-Value Pages';
    }

    html += '<div class="signal-section' + (isMqa ? ' signal-section-mqa_new' : '') + ' reveal">';
    if (isMqa) html += '<div class="mqa-priority-banner">\u2B50 Priority \u2014 These accounts are warm and waiting for your outreach</div>';

    // HVP: card layout instead of table
    if (t === 'hvp_all') {
      html += '<div class="signal-heading">Accounts Visiting High-Value Pages <button class="help-btn" data-help="hvp_all" aria-label="What is High-Value Pages?">?</button></div>';
      html += '<div style="margin-top:12px;">';
      signals.forEach(function(row, i) {
        var isLostOpp = !!row._is_lost_opp || lostOppAccounts[(row.account || '').toLowerCase()];
        var sfdc = row.sfdc || {};
        var cardId = 'hvp-card-' + i;
        var rankCls = i < 3 ? ' r' + (i+1) : '';

        // Why sentence (shown inside detail)
        var why = isLostOpp
          ? 'Previously Closed Lost \u2014 back on Shopify Plus pages this week'
          : 'Visiting high-value Shopify pages this week';
        if (row.pages_visited) why += ' \u00B7 ' + row.pages_visited;

        // Tags — same badge style as Top Accounts
        var tagHtml = '';
        if (isLostOpp) tagHtml += '<span class="badge badge-hvp" style="font-size:11px;padding:2px 8px;">\u21A9 Previously CL</span> ';
        if (sfdc.no_activity) {
          tagHtml += '<span class="badge" style="font-size:11px;padding:2px 8px;background:#fee2e2;color:#991b1b;">Cold</span>';
        } else if (sfdc.days_since_activity != null) {
          var d = sfdc.days_since_activity;
          var badgeCl = d > 60 ? 'background:#fee2e2;color:#991b1b;' : d > 14 ? 'background:var(--warm-light);color:var(--warm);' : 'background:var(--accent-light);color:var(--accent);';
          tagHtml += '<span class="badge" style="font-size:11px;padding:2px 8px;' + badgeCl + '">Last touch ' + d + 'd</span> ';
          if (d > 60) tagHtml += '<span class="badge" style="font-size:11px;padding:2px 8px;background:#fee2e2;color:#991b1b;">Cold</span>';
        }

        // Card — identical structure to priority-card
        html += '<div class="priority-card" id="' + cardId + '">';
        html += '<div class="priority-card-header" data-card="' + cardId + '">';
        html += '<div class="priority-rank' + rankCls + '">' + (i+1) + '</div>';
        html += '<div class="priority-body">';
        html += '<div class="priority-name">' + esc(row.account) + '</div>';
        html += '<div class="priority-tags">' + tagHtml + '</div>';
        html += '</div><div class="priority-expand">\u2304</div>';
        html += '</div>';

        // Expanded detail — identical structure to priority-card detail
        html += '<div class="priority-detail">';

        // Why at top
        html += '<div style="font-size:13px;color:var(--text-2);margin-bottom:12px;line-height:1.6;">' + esc(why) + '</div>';

        // Account overview
        var ad = (DATA.account_details || {})[row.account.toLowerCase()] || {};
        var overview = ad.merchant_overview || ad.description;
        if (overview) {
          html += '<div style="font-size:12px;color:var(--text-2);line-height:1.7;margin-bottom:12px;padding:12px 16px;background:var(--bg);border-radius:8px;border-left:3px solid var(--accent-soft);">' + esc(overview) + '</div>';
        }

        // Outreach angle
        var hvpAcc = { name: row.account, signalTypes: isLostOpp ? ['hvp','hvp_all'] : ['hvp_all'], sfdc: sfdc, keywords: (row.keywords||'').split(',').map(function(k){return k.trim();}).filter(Boolean) };
        var angle = suggestAccountOutreach(hvpAcc);
        if (angle.length > 0) {
          html += '<div style="margin-bottom:14px;padding:12px 14px;background:var(--accent-light);border-radius:8px;border-left:3px solid var(--accent);">';
          html += '<div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:var(--accent);margin-bottom:6px;">Outreach angle</div>';
          angle.forEach(function(line) {
            html += '<div style="font-size:12px;color:var(--text-2);line-height:1.65;padding-left:10px;position:relative;margin-bottom:4px;"><span style="position:absolute;left:0;color:var(--accent);">\u203A</span>' + esc(line) + '</div>';
          });
          html += '</div>';
        }

        // Facts
        var facts = [];
        if (ad.industry) facts.push(ad.industry);
        if (ad.city && ad.country) facts.push(ad.city + ', ' + ad.country);
        else if (ad.country) facts.push(ad.country);
        if (ad.revenue_usd) { var rev=parseFloat(ad.revenue_usd); if(rev>=1e9) facts.push('$'+(rev/1e9).toFixed(1)+'B'); else if(rev>=1e6) facts.push('$'+Math.round(rev/1e6)+'M'); }
        if (facts.filter(Boolean).length) html += '<div class="priority-detail-row"><span class="priority-detail-icon">\uD83C\uDFE2</span><span>' + esc(facts.filter(Boolean).join(' \u00B7 ')) + '</span></div>';

        var platform = ad.ecomm_platform || row.platform;
        if (platform) html += '<div class="priority-detail-row"><span class="priority-detail-icon">\uD83D\uDDA5</span><span><span class="priority-detail-label">Platform</span>' + esc(platform) + '</span></div>';
        if (row.pages_visited) html += '<div class="priority-detail-row"><span class="priority-detail-icon">\uD83D\uDCC4</span><span><span class="priority-detail-label">Pages researched</span>' + esc(row.pages_visited) + '</span></div>';
        if (row.keywords) {
          var kws = row.keywords.split(',').map(function(k){return k.trim();}).filter(Boolean);
          if (kws.length) { html += '<div class="priority-detail-row"><span class="priority-detail-icon">\uD83C\uDFF7</span><span>'; kws.forEach(function(k){ html += '<span class="kw-chip">'+esc(k)+'</span>'; }); html += '</span></div>'; }
        }
        if (sfdc.open_opps && sfdc.open_opps[0]) {
          var o = sfdc.open_opps[0];
          html += '<div class="priority-detail-row"><span class="priority-detail-icon">\uD83D\uDCCA</span><span><span class="priority-detail-label">Open deal</span>' + esc((o.stage||'Open Deal')+(o.acv_str?' \u00B7 '+o.acv_str:'')) + '</span></div>';
        }
        if (sfdc.engaged_contacts && sfdc.engaged_contacts.length > 0) {
          html += '<div class="priority-detail-row" style="align-items:flex-start;"><span class="priority-detail-icon">\uD83D\uDC64</span><span><span class="priority-detail-label">Contacts (90d)</span>';
          sfdc.engaged_contacts.forEach(function(c){ html += '<div style="font-size:12px;">'+(c.name?'<strong>'+esc(c.name)+'</strong> \u00B7 ':'')+esc(c.title||'')+'</div>'; });
          html += '</span></div>';
        }
        var acctActs = (DATA.account_activities||{})[(row.account||'').toLowerCase()]||[];
        if (acctActs.length > 0) {
          var last = acctActs[0];
          var who = last.contact_name || last.contact_title || '';
          var what = (last.subject||'').replace(/^(Email:|Call:|Meeting:)\s*/i,'');
          html += '<div class="priority-detail-row"><span class="priority-detail-icon">\uD83D\uDCCB</span><span><span class="priority-detail-label">Last contact</span>' + esc([who,what,last.date].filter(Boolean).join(' \u00B7 ')) + '</span></div>';
        }

        // HVP People — people from this account visiting high-value pages (2+ engagement pts)
        var hvpPeople = (DATA.hvp_people_by_account || {})[(row.account||'').toLowerCase()] || [];
        if (hvpPeople.length > 0) {
          html += '<div class="priority-detail-row" style="align-items:flex-start;margin-top:10px;"><span class="priority-detail-icon">\uD83D\uDC65</span><span style="flex:1;">';
          html += '<span class="priority-detail-label" style="display:block;margin-bottom:6px;">People visiting these pages</span>';
          hvpPeople.forEach(function(p) {
            var senBadge = p.seniority && p.seniority !== 'other' ? '<span style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.05em;padding:1px 6px;border-radius:8px;margin-left:6px;background:var(--warm-light);color:var(--warm);">'+esc(p.seniority)+'</span>' : '';
            html += '<div style="display:flex;justify-content:space-between;align-items:flex-start;padding:7px 10px;margin-bottom:5px;background:var(--bg);border-radius:8px;border:1px solid var(--border-soft);">';
            html += '<div style="flex:1;min-width:0;">';
            html += '<div style="font-size:12px;font-weight:600;color:var(--text-1);">' + esc(p.full_name||'Unknown') + senBadge + '</div>';
            if (p.title) html += '<div style="font-size:11px;color:var(--text-3);margin-top:1px;">' + esc(p.title) + '</div>';
            if (p.email) html += '<div style="margin-top:4px;"><a href="mailto:'+esc(p.email)+'" style="font-size:11px;color:var(--accent);text-decoration:none;">\u2709 '+esc(p.email)+'</a></div>';
            html += '</div>';
            html += '<div style="font-size:11px;color:var(--text-3);white-space:nowrap;margin-left:10px;padding-top:2px;">' + p.engagement_7d.toFixed(p.engagement_7d % 1 === 0 ? 0 : 2) + ' pts</div>';
            html += '</div>';
          });
          html += '</span></div>';
        }

        if (ad.account_url || ad.new_opportunity_url) {
          html += '<div class="priority-detail-row" style="margin-top:6px;"><span class="priority-detail-icon">\uD83D\uDD17</span><span style="display:flex;gap:14px;flex-wrap:wrap;">';
          if (ad.account_url) html += '<a href="'+esc(ad.account_url)+'" target="_blank" rel="noopener" style="font-size:12px;color:var(--accent);font-weight:600;text-decoration:none;">View in SFDC \u2197</a>';
          if (ad.new_opportunity_url) html += '<a href="'+esc(ad.new_opportunity_url)+'" target="_blank" rel="noopener" style="font-size:12px;color:var(--accent2);font-weight:600;text-decoration:none;">+ Create Opportunity</a>';
          html += '</span></div>';
        }
        html += '</div></div>';
      });
      html += '</div></div>';

    } else {
      // All other signal types — keep table layout
      html += '<div class="signal-heading"><span class="badge badge-' + cls + '">' + esc(st.short_label) + '</span> ' + esc(sectionLabel) + ' <button class="help-btn" data-help="' + t + '" aria-label="What is ' + esc(st.short_label) + '?">?</button></div>';

      html += '<div class="table-wrap"><table class="data-table"><thead><tr>';
      cols.forEach(function(c) { html += '<th>' + esc(c.label) + '</th>'; });
      html += '</tr></thead><tbody>';

      signals.forEach(function(row) {
        html += '<tr>';
        cols.forEach(function(c) {
          if (c.key === 'seniority') {
            html += '<td>' + renderSeniority(row[c.key]) + '</td>';
          } else if ((c.key === 'signals' || c.key === 'high_intent_keywords' || c.key === 'intent_sets' || c.key === 'competitive_keywords') && t.indexOf('intent_') === 0 && row[c.key]) {
            var chips = String(row[c.key]).split(',').map(function(kw) {
              var k = kw.trim();
              return k ? '<span style="display:inline-block;font-size:11px;background:var(--bg-muted);border:1px solid var(--border);border-radius:10px;padding:1px 8px;margin:2px 2px 2px 0;white-space:nowrap;color:var(--text-2);">' + esc(k) + '</span>' : '';
            }).join('');
            html += '<td style="white-space:normal;">' + chips + '</td>';
          } else {
            html += '<td>' + esc(row[c.key]) + '</td>';
          }
        });
        html += '</tr>';

        // MQA brief + SFDC badges
        if (t === 'mqa_new' && (row.brief || row.sfdc)) {
          html += '<tr class="row-explain"><td colspan="' + cols.length + '"><div class="row-explain-inner">';
          if (row.brief) html += '<span style="color:var(--text-2);font-style:normal;">\uD83D\uDCA1 ' + esc(row.brief) + '</span>';
          html += renderSfdcBadges(row.sfdc);
          html += '</div></td></tr>';
        }
        // Intent SFDC badges
        if (t.indexOf('intent_') === 0 && row.sfdc) {
          html += '<tr class="row-explain"><td colspan="' + cols.length + '"><div class="row-explain-inner">';
          html += renderSfdcBadges(row.sfdc);
          html += '</div></td></tr>';
        }
        // Activity row explanation
        if (t === 'activity' && row.category) {
          var actExplain = '';
          var cat = (row.category || '').toLowerCase();
          if (cat.indexOf('high intent') !== -1) actExplain = 'They submitted a high-intent contact form \u2014 this is a strong buying signal.';
          else if (cat.indexOf('event') !== -1) actExplain = 'They attended or engaged with an event \u2014 a good reason to follow up.';
          if (actExplain) {
            html += '<tr class="row-explain"><td colspan="' + cols.length + '"><div class="row-explain-inner"><span>' + esc(actExplain) + '</span></div></td></tr>';
          }
        }
      });

      html += '</tbody></table></div></div>';
    }
  });

  if (!hasAnyData) {
    html += '<div style="text-align:center;color:var(--text-3);padding:48px 0;font-size:15px;">No engagement signals for your accounts this week.</div>';
  }

  // Collapsed reference tables — All MQA + AI Intent + Compete Intent
  var collapsedSections = [
    {
      key: 'all_mqa',
      label: 'All Accounts at MQA Status',
      color: 'var(--accent)',
      bg: 'var(--accent-light)',
      cols: [
        { key: 'account', label: 'Account' },
        { key: 'territory', label: 'Territory' },
        { key: 'industry', label: 'Industry' },
        { key: 'revenue', label: 'Revenue' },
        { key: 'platform', label: 'Platform' },
        { key: 'pages_visited', label: 'Pages Visited' },
      ],
      rowFn: function(row) {
        return [
          '<td style="font-weight:500;">' + esc(row.account||'') + '</td>',
          '<td>' + esc(row.territory||'') + '</td>',
          '<td>' + esc(row.industry||'') + '</td>',
          '<td>' + esc(row.revenue||'') + '</td>',
          '<td>' + esc(row.platform||'') + '</td>',
          '<td>' + esc(row.pages_visited||'') + '</td>',
        ].join('');
      }
    },
    {
      key: 'intent_agentic',
      label: 'AI & Agentic Commerce Intent',
      color: '#7c3aed',
      bg: '#ede9fe',
      cols: [
        { key: 'account', label: 'Account' },
        { key: 'journey_stage', label: 'Stage' },
        { key: 'engagement_3mo', label: 'Engagement (3mo)' },
        { key: 'matched_keywords', label: 'Keywords' },
      ],
      rowFn: function(row) {
        var kws = (row.matched_keywords||'').split(',').map(function(k){return k.trim();}).filter(Boolean);
        var kwHtml = kws.map(function(k){ return '<span style="display:inline-block;font-size:11px;background:var(--bg-muted);border:1px solid var(--border);border-radius:8px;padding:1px 7px;margin:1px;white-space:nowrap;color:var(--text-2);">'+esc(k)+'</span>'; }).join('');
        var eng = row.engagement_3mo ? parseFloat(row.engagement_3mo).toLocaleString('en-US',{maximumFractionDigits:0}) : '';
        return '<td style="font-weight:500;">'+esc(row.account||'')+'</td><td>'+esc(row.journey_stage||'')+'</td><td>'+esc(eng)+'</td><td style="white-space:normal;">'+kwHtml+'</td>';
      }
    },
    {
      key: 'intent_compete',
      label: 'Compete Intent',
      color: '#b91c1c',
      bg: '#fee2e2',
      cols: [
        { key: 'account', label: 'Account' },
        { key: 'journey_stage', label: 'Stage' },
        { key: 'engagement_3mo', label: 'Engagement (3mo)' },
        { key: 'matched_keywords', label: 'Keywords' },
      ],
      rowFn: function(row) {
        var kws = (row.matched_keywords||'').split(',').map(function(k){return k.trim();}).filter(Boolean);
        var kwHtml = kws.map(function(k){ return '<span style="display:inline-block;font-size:11px;background:var(--bg-muted);border:1px solid var(--border);border-radius:8px;padding:1px 7px;margin:1px;white-space:nowrap;color:var(--text-2);">'+esc(k)+'</span>'; }).join('');
        var eng = row.engagement_3mo ? parseFloat(row.engagement_3mo).toLocaleString('en-US',{maximumFractionDigits:0}) : '';
        return '<td style="font-weight:500;">'+esc(row.account||'')+'</td><td>'+esc(row.journey_stage||'')+'</td><td>'+esc(eng)+'</td><td style="white-space:normal;">'+kwHtml+'</td>';
      }
    },
  ];

  var hasCollapsed = collapsedSections.some(function(cs) { return (seller.signals[cs.key]||[]).length > 0; });
  if (hasCollapsed) {
    html += '<div class="section-label" style="margin-top:40px;">Accounts with Intent</div>';
  }
  collapsedSections.forEach(function(cs, ci) {
    var allRows = seller.signals[cs.key] || [];
    if (allRows.length === 0) return;
    var toggleId = 'collapsed-toggle-' + ci;
    var gridId   = 'collapsed-grid-' + ci;
    html += '<div style="margin-bottom:10px;border:1px solid var(--border);border-radius:var(--radius-sm);overflow:hidden;">';
    html += '<button id="' + toggleId + '" style="width:100%;display:flex;align-items:center;justify-content:space-between;padding:13px 18px;background:var(--bg-card);border:none;cursor:pointer;font-family:var(--font);text-align:left;">';
    html += '<span style="display:flex;align-items:center;gap:8px;">';
    html += '<span style="font-size:12px;font-weight:700;padding:2px 9px;border-radius:10px;background:' + cs.bg + ';color:' + cs.color + ';">' + esc(cs.label) + '</span>';
    html += '<span style="font-size:13px;color:var(--text-2);font-weight:500;">' + allRows.length + ' accounts</span>';
    html += '</span>';
    html += '<span id="' + toggleId + '-arrow" style="font-size:12px;color:var(--text-3);">\u25B6</span>';
    html += '</button>';
    html += '<div id="' + gridId + '" style="display:none;">';
    html += '<div class="table-wrap"><table class="data-table"><thead><tr>';
    cs.cols.forEach(function(c) { html += '<th>' + esc(c.label) + '</th>'; });
    html += '</tr></thead><tbody>';
    allRows.forEach(function(row) { html += '<tr>' + cs.rowFn(row) + '</tr>'; });
    html += '</tbody></table></div></div></div>';
  });

  // Archive browser
  var isArchive = CURRENT_WEEK !== LATEST_WEEK;
  if (isArchive) {
    html += '<div class="archive-banner"><strong>\uD83D\uDCC5 Viewing week of ' + esc(CURRENT_WEEK) + '</strong>';
    html += '<button class="archive-banner-btn" id="personalBackToCurrentBtn">Back to current week</button></div>';
  }
  // Archive — collapsed by default
  html += '<div style="margin-top:32px;border-top:1px solid var(--border);padding-top:20px;">';
  html += '<button id="personalArchiveToggle" style="background:none;border:none;cursor:pointer;font-family:var(--font);font-size:13px;color:var(--text-3);display:flex;align-items:center;gap:6px;padding:0;">';
  html += '<span id="personalArchiveArrow">\u25B6</span> Past weeks</button>';
  html += '<div id="personalArchiveGrid" style="display:none;margin-top:14px;"><div class="archive-grid">';

  var isViewingLatest = CURRENT_WEEK === LATEST_WEEK;
  html += '<div class="archive-link' + (isViewingLatest ? ' current' : '') + '" data-week="' + esc(LATEST_WEEK) + '" tabindex="0" role="button" aria-label="Load current week">';
  html += '<div><span class="archive-week-label">Week of ' + esc(LATEST_WEEK) + '</span><span class="archive-tag archive-tag-current">Current</span></div>';
  html += '</div>';

  var pastWeeks = WEEKS.filter(function(w) { return w !== LATEST_WEEK; });
  pastWeeks.forEach(function(w) {
    var isViewing = w === CURRENT_WEEK;
    html += '<div class="archive-link' + (isViewing ? ' viewing' : '') + '" data-week="' + esc(w) + '" tabindex="0" role="button" aria-label="Load week of ' + esc(w) + '">';
    html += '<div><span class="archive-week-label">Week of ' + esc(w) + '</span>';
    if (isViewing) html += '<span class="archive-tag archive-tag-viewing">Viewing</span>';
    html += '</div></div>';
  });

  if (pastWeeks.length === 0 && WEEKS.length <= 1) {
    html += '<div class="archive-empty"><span aria-hidden="true">\uD83D\uDCC5</span><span>No previous weeks yet \u2014 check back next Monday!</span></div>';
  }
  html += '</div></div></div>';

  html += '<footer class="footer">CONFIDENTIAL \u2014 SHOPIFY INTERNAL USE ONLY \u00B7 Week of ' + esc(m.week_of) + '</footer>';
  html += '</div>';

  $('#personalView').innerHTML = html;
  $('#clearIdentity').addEventListener('click', function(e) { e.preventDefault(); clearSeller(); if (CURRENT_COACH) { showCoach(CURRENT_COACH); } else { showMaster(); } });
  bindHelpButtons();

  // Bind Top Account card expand/collapse (priority-card style)
  $$('.priority-card-header').forEach(function(header) {
    var fn = function() {
      var cardId = this.dataset.card;
      var card = $('#' + cardId);
      if (card) card.classList.toggle('open');
    };
    header.addEventListener('click', fn);
    header.addEventListener('keydown', function(e) { if (e.key==='Enter'||e.key===' '){e.preventDefault();fn.call(this,e);} });
  });

  // Bind HVP + account-card style expand/collapse
  $$('.account-card-header').forEach(function(header) {
    var fn = function() {
      var card = this.closest('.account-card');
      if (!card) return;
      var isExpanded = card.classList.contains('expanded');
      card.classList.toggle('expanded', !isExpanded);
      this.setAttribute('aria-expanded', String(!isExpanded));
    };
    header.addEventListener('click', fn);
    header.addEventListener('keydown', function(e) { if (e.key==='Enter'||e.key===' '){e.preventDefault();fn.call(this,e);} });
  });

  // Bind collapsed section toggles (all_mqa + intent cadence tables)
  [0, 1, 2].forEach(function(ci) {
    var btn = $('#collapsed-toggle-' + ci);
    var grid = $('#collapsed-grid-' + ci);
    var arrow = $('#collapsed-toggle-' + ci + '-arrow');
    if (!btn || !grid) return;
    btn.addEventListener('click', function() {
      var open = grid.style.display !== 'none';
      grid.style.display = open ? 'none' : 'block';
      if (arrow) arrow.textContent = open ? '\u25B6' : '\u25BC';
    });
  });

  // Bind Top Leads toggle
  var toggleBtn = $('#topLeadsToggle');
  if (toggleBtn) {
    toggleBtn.addEventListener('click', function() {
      var extras = $$('[data-extra-lead]');
      var expanded = extras.length > 0 && extras[0].style.display !== 'none';
      extras.forEach(function(tr) { tr.style.display = expanded ? 'none' : ''; });
      this.textContent = expanded ? 'Show all ' + $$('#topLeadsBody tr').length + ' leads' : 'Show top 20 only';
    });
  }

  // Bind archive toggle
  var archiveToggle = $('#personalArchiveToggle');
  var archiveGrid = $('#personalArchiveGrid');
  var archiveArrow = $('#personalArchiveArrow');
  if (archiveToggle && archiveGrid) {
    archiveToggle.addEventListener('click', function() {
      var open = archiveGrid.style.display !== 'none';
      archiveGrid.style.display = open ? 'none' : 'block';
      archiveArrow.textContent = open ? '\u25B6' : '\u25BC';
    });
  }

  // Bind archive events in personal view
  $$('#personalView .archive-link').forEach(function(el) {
    var handler = function() { var w = this.dataset.week; if (w) loadWeek(w, sellerId); };
    el.addEventListener('click', handler);
    activateOnKey(el, handler);
  });
  var personalBackBtn = $('#personalBackToCurrentBtn');
  if (personalBackBtn) {
    personalBackBtn.addEventListener('click', function() { loadWeek(LATEST_WEEK, sellerId); });
  }
}

/* ═══════════════════════════════════════════
   Events
   ═══════════════════════════════════════════ */
function activateOnKey(el, fn) {
  el.addEventListener('keydown', function(e) {
    if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); fn.call(this, e); }
  });
}

function bindMasterEvents() {
  $$('.seller-card').forEach(function(c) {
    var handler = function() { var sid = this.dataset.seller; if (sid) showSeller(sid); };
    c.addEventListener('click', handler);
    activateOnKey(c, handler);
  });
  $$('.top-owner').forEach(function(el) {
    var handler = function() { var sid = this.dataset.seller; if (sid) showSeller(sid); };
    el.addEventListener('click', handler);
    activateOnKey(el, handler);
  });
  $$('.coach-link').forEach(function(el) {
    var handler = function() { var slug = this.dataset.coach; if (slug) showCoach(slug); };
    el.addEventListener('click', handler);
    activateOnKey(el, handler);
  });
  $$('.archive-link').forEach(function(el) {
    var handler = function() { var w = this.dataset.week; if (w) loadWeek(w); };
    el.addEventListener('click', handler);
    activateOnKey(el, handler);
  });
  $$('.filter-tag').forEach(function(btn) {
    btn.addEventListener('click', function(e) {
      if (e.target.classList.contains('help-btn')) return;
      $$('.filter-tag').forEach(function(b) { b.classList.remove('active'); });
      this.classList.add('active');
      ACTIVE_FILTER = this.dataset.filter || 'all';
      var searchVal = $('#hubSearch') ? $('#hubSearch').value.toLowerCase().trim() : '';
      applyCardFilters(searchVal);
    });
  });
  // Region filter pills
  $$('.region-pill').forEach(function(btn) {
    btn.addEventListener('click', function() {
      $$('.region-pill').forEach(function(b) { b.classList.remove('active'); });
      this.classList.add('active');
      ACTIVE_REGION = this.dataset.region || 'all';
      localStorage.setItem('si_region', ACTIVE_REGION);
      var searchVal = $('#hubSearch') ? $('#hubSearch').value.toLowerCase().trim() : '';
      applyCardFilters(searchVal);
    });
  });
  // Restore saved region on load
  var savedRegion = localStorage.getItem('si_region') || 'all';
  if (savedRegion !== 'all') {
    ACTIVE_REGION = savedRegion;
    var searchVal = $('#hubSearch') ? $('#hubSearch').value.toLowerCase().trim() : '';
    applyCardFilters(searchVal);
  }
  var backBtn = $('#backToCurrentBtn');
  if (backBtn) {
    backBtn.addEventListener('click', function() { loadWeek(LATEST_WEEK); });
  }
  initSearch();
  bindHelpButtons();
}

function bindHelpButtons() {
  $$('.help-btn').forEach(function(btn) {
    btn.addEventListener('click', function(e) { e.stopPropagation(); e.preventDefault(); openModal(this.dataset.help); });
  });
}

function initSearch() {
  var input = $('#hubSearch');
  var results = $('#searchResults');
  if (!input || !results) return;
  var sellerList = Object.keys(DATA.sellers).map(function(sid) {
    var s = DATA.sellers[sid];
    var pills = Object.keys(DATA.signal_types).filter(function(t) { return s.summary[t] > 0; });
    return { id: sid, name: s.name, pills: pills };
  });
  input.addEventListener('input', function() {
    var q = this.value.toLowerCase().trim();
    if (!q) { results.classList.remove('active'); results.innerHTML = ''; applyCardFilters(''); return; }
    window.scrollTo({ top: 0, behavior: 'smooth' });
    var matches = sellerList.filter(function(s) { return s.name.toLowerCase().indexOf(q) !== -1; });
    if (matches.length > 0) {
      results.innerHTML = matches.map(function(s) {
        var pillsHtml = s.pills.map(function(t) {
          return '<span class="pill ' + pillCls(t) + '" style="font-size:11px;padding:2px 8px;">' + DATA.signal_types[t].short_label + '</span>';
        }).join('');
        return '<div class="sr-item" data-seller="' + esc(s.id) + '"><span class="sr-name">' + esc(s.name) + '</span><span class="sr-pills">' + pillsHtml + '</span></div>';
      }).join('');
    } else {
      results.innerHTML = '<div class="sr-empty">No sellers match \u201C' + esc(q) + '\u201D</div>';
    }
    results.classList.add('active');
    applyCardFilters(q);
  });
  input.addEventListener('focus', function() { if (this.value.trim()) this.dispatchEvent(new Event('input')); });
  results.addEventListener('click', function(e) {
    var item = e.target.closest('.sr-item');
    if (item && item.dataset.seller) showSeller(item.dataset.seller);
  });
  document.addEventListener('click', function(e) {
    if (!input.contains(e.target) && !results.contains(e.target)) results.classList.remove('active');
  });
}

function applyCardFilters(q) {
  var visible = 0;
  $$('.seller-card').forEach(function(card) {
    var nameMatch = !q || card.dataset.name.indexOf(q) !== -1;
    var typeMatch = ACTIVE_FILTER === 'all' ||
      (ACTIVE_FILTER === 'intent' && /intent_/.test(card.dataset.types)) ||
      (ACTIVE_FILTER !== 'intent' && card.dataset.types.indexOf(ACTIVE_FILTER) !== -1);
    var regionMatch = ACTIVE_REGION === 'all' || card.dataset.region === ACTIVE_REGION;
    var show = nameMatch && typeMatch && regionMatch;
    card.classList.toggle('hidden', !show);
    if (show) visible++;
  });
  $$('.team-group').forEach(function(group) {
    var regionMatch = ACTIVE_REGION === 'all' || group.dataset.region === ACTIVE_REGION;
    if (!regionMatch) { group.classList.add('hidden'); return; }
    var hasVisible = group.querySelectorAll('.seller-card:not(.hidden)').length > 0;
    group.classList.toggle('hidden', !hasVisible);
  });
  var nr = $('#noResults');
  if (nr) nr.style.display = visible === 0 ? 'block' : 'none';
}

function loadWeek(week, sellerId) {
  var isLatest = !week || week === LATEST_WEEK;
  var url = isLatest ? 'data/current-v2.json' : 'data/' + week + '.json';

  var activeView = sellerId ? '#personalView' : '#masterView';
  $(activeView).style.opacity = '0.4';
  $(activeView).style.pointerEvents = 'none';
  window.scrollTo({ top: 0, behavior: 'smooth' });

  fetch(url + '?v=' + Date.now())
    .then(function(r) { return r.json(); })
    .then(function(data) {
      DATA = data;
      CURRENT_WEEK = data.meta.week_of;
      document.title = 'Weekly Sales Insights \u2014 ' + CURRENT_WEEK;
      var params = new URLSearchParams(location.search);
      if (isLatest) { params.delete('week'); } else { params.set('week', week); }

      if (sellerId && DATA.sellers[sellerId]) {
        params.set('seller', sellerId);
        var qs = params.toString();
        history.pushState({ seller: sellerId }, '', location.pathname + (qs ? '?' + qs : ''));
        CURRENT_SELLER = sellerId;
        renderPersonal(sellerId);
        $(activeView).style.opacity = '';
        $(activeView).style.pointerEvents = '';
        initReveal();
      } else {
        params.delete('seller');
        var qs = params.toString();
        history.pushState(null, '', location.pathname + (qs ? '?' + qs : ''));
        CURRENT_SELLER = null;
        showMaster();
        $(activeView).style.opacity = '';
        $(activeView).style.pointerEvents = '';
      }
    })
    .catch(function() {
      alert('Could not load data for week ' + week);
      $(activeView).style.opacity = '';
      $(activeView).style.pointerEvents = '';
    });
}

function initReveal() {
  function check() {
    document.querySelectorAll('.reveal:not(.visible)').forEach(function(el) {
      if (el.getBoundingClientRect().top < window.innerHeight * 0.95) el.classList.add('visible');
    });
  }
  check(); setTimeout(check, 100); setTimeout(check, 400);
  window.removeEventListener('scroll', scrollRevealHandler);
  window.addEventListener('scroll', scrollRevealHandler, { passive: true });
}
function scrollRevealHandler() {
  document.querySelectorAll('.reveal:not(.visible)').forEach(function(el) {
    if (el.getBoundingClientRect().top < window.innerHeight * 0.95) el.classList.add('visible');
  });
}

window.addEventListener('scroll', function() {
  var ring = document.getElementById('progressRing');
  if (!ring) return;
  var pct = window.scrollY / (document.documentElement.scrollHeight - window.innerHeight);
  pct = Math.min(Math.max(pct, 0), 1);
  ring.style.strokeDashoffset = 113 - (113 * pct);
}, { passive: true });

$('#themeToggle').addEventListener('click', function() {
  document.documentElement.classList.toggle('dark');
  localStorage.setItem('theme', document.documentElement.classList.contains('dark') ? 'dark' : 'light');
});

$('#modalClose').addEventListener('click', closeModal);
$('#modalOverlay').addEventListener('click', function(e) { if (e.target === this) closeModal(); });
document.addEventListener('keydown', function(e) {
  if (e.key === 'Escape') closeModal();
  if (e.key === 'Tab' && $('#modalOverlay').classList.contains('open')) {
    var focusable = $('#modalBox').querySelectorAll('button, [href], [tabindex]:not([tabindex="-1"])');
    if (focusable.length === 0) return;
    var first = focusable[0];
    var last = focusable[focusable.length - 1];
    if (e.shiftKey) {
      if (document.activeElement === first) { e.preventDefault(); last.focus(); }
    } else {
      if (document.activeElement === last) { e.preventDefault(); first.focus(); }
    }
  }
});

/* ═══════════════════════════════════════════
   Init
   ═══════════════════════════════════════════ */
function init() {
  var params = new URLSearchParams(location.search);
  var weekParam = params.get('week');
  var dataUrl = weekParam ? 'data/' + weekParam + '.json' : 'data/current-v2.json';
  var bust = '?v=' + Date.now();

  Promise.all([
    fetch(dataUrl + bust).then(function(r) { return r.json(); }),
    fetch('data/weeks.json' + bust).then(function(r) { return r.json(); }).catch(function() { return []; }),
    fetchIAPEmail()
  ]).then(function(results) {
    DATA = results[0];
    WEEKS = results[1];
    IAP_EMAIL = results[2];
    CURRENT_WEEK = DATA.meta.week_of;
    if (!LATEST_WEEK) LATEST_WEEK = WEEKS.length > 0 ? WEEKS[0] : CURRENT_WEEK;
    document.title = 'Weekly Sales Insights \u2014 ' + CURRENT_WEEK;

    if (IAP_EMAIL) resolveRole(IAP_EMAIL);
    console.log('[Identity] Routing \u2014 role:', IDENTITY_ROLE, '| email:', IAP_EMAIL);

    $('#loadingView').style.display = 'none';

    var params = new URLSearchParams(location.search);
    var coachParam = params.get('coach');
    if (coachParam && getCoachBySlug(coachParam)) {
      showCoach(coachParam);
    } else if (IDENTITY_ROLE === 'coach' && IDENTITY_DATA && !params.get('seller')) {
      showCoach(nameToSlug(IDENTITY_DATA.name));
    } else {
      var sellerId = resolveIdentity();
      if (sellerId) { showSeller(sellerId); }
      else { showMaster(); }
    }
  }).catch(function(err) {
    $('#loadingView').innerHTML = '<p style="color:var(--rose);">Failed to load data. Check that data/current-v2.json exists.</p>';
    console.error(err);
  });
}

init();

})();
