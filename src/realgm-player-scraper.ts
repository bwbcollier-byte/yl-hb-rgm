import * as dotenv from 'dotenv';
dotenv.config();

import { supabase } from './supabase';
import * as cheerio from 'cheerio';

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const BASE_URL       = 'https://basketball.realgm.com';
const SOCIAL_TYPES   = (process.env.SOCIAL_TYPES   || 'REALGM,realgm').split(',').map(s => s.trim());
const SPORT_LABEL    =  process.env.SPORT_LABEL    || 'NBA';
const RAPIDAPI_KEY   =  process.env.RAPIDAPI_KEY!;
const MAX_PLAYERS    = parseInt(process.env.MAX_PLAYERS    || '0');   // 0 = all
const PLAYER_OFFSET  = parseInt(process.env.PLAYER_OFFSET  || '0');   // skip first N players (for batching)
const MAX_NEWS_PAGES = parseInt(process.env.MAX_NEWS_PAGES || '5');   // per player, 0 = all
const WORKFLOW_ID    = process.env.WORKFLOW_ID ? parseInt(process.env.WORKFLOW_ID) : null;
const FETCH_DELAY    = 2000; // ms between proxy requests

if (!RAPIDAPI_KEY) {
    console.error('❌ Missing RAPIDAPI_KEY');
    process.exit(1);
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface PlayerBio {
    headshot:      string | null;
    highlightBox:  string | null;  // yellow box (achievements)
    bioNarrative:  string | null;  // prose paragraphs below structured fields
    birthDate:     string | null;  // "Dec 30, 1984"
    birthLocation: string | null;  // "Akron, Ohio"
    nationality:   string | null;  // "United States"
    heightCm:      number | null;
    agentName:     string | null;  // "Rich Paul"
    agentHref:     string | null;  // "/info/agent_clients/Rich-Paul/258"
}

interface AgentInfo {
    name:        string;
    firstName:   string | null;
    lastName:    string | null;
    companyName: string | null;
    website:     string | null;
}

interface NewsArticle {
    title:      string;
    link:       string;   // absolute wiretap URL
    dateStr:    string | null;
    published:  string | null;
    imgSrc:     string | null;
    body:       string | null;
    sourceName: string | null;
    sourceHref: string | null;
    playerIds:  string[];
    playerUrls: string[];
}

// ---------------------------------------------------------------------------
// Proxy fetch (same strategy as realgm-news-scraper)
// ---------------------------------------------------------------------------

async function fetchViaProxy(url: string): Promise<string | null> {
    await new Promise(r => setTimeout(r, FETCH_DELAY));

    const attempts = [
        { js: true,  delay: 0    },
        { js: true,  delay: 3000 },
        { js: false, delay: 5000 },
    ];

    for (const { js, delay } of attempts) {
        if (delay > 0) await new Promise(r => setTimeout(r, delay));
        const proxyUrl = `https://proxycrawl-crawling.p.rapidapi.com/?url=${encodeURIComponent(url)}${js ? '&javascript=true' : ''}`;
        try {
            const res = await fetch(proxyUrl, {
                headers: {
                    'x-rapidapi-key':  RAPIDAPI_KEY,
                    'x-rapidapi-host': 'proxycrawl-crawling.p.rapidapi.com',
                },
            });
            if (res.ok) return await res.text();
            console.warn(`    [HTTP ${res.status}${js ? ' js' : ''}] retrying...`);
        } catch (e: any) {
            console.warn(`    [FETCH ERR] ${url}: ${e.message}`);
        }
    }

    console.warn(`    [FAILED] all attempts exhausted for ${url}`);
    return null;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function parseRealGMDate(raw: string | null): string | null {
    if (!raw) return null;
    try {
        const d = new Date(raw.trim().replace(/\s+/g, ' '));
        return isNaN(d.valueOf()) ? null : d.toISOString();
    } catch { return null; }
}

function extractPlayerId(href: string): string | null {
    const m = href.match(/\/player\/[^/]+\/[^/]+\/(\d+)/i);
    return m ? m[1] : null;
}

function parseSourceCredit(raw: string | null): { publication: string | null; author: string | null } {
    if (!raw) return { publication: null, author: null };
    const slashIdx = raw.lastIndexOf('/');
    if (slashIdx === -1) return { publication: raw.trim() || null, author: null };
    const authorPart = raw.substring(0, slashIdx).trim();
    const pubPart    = raw.substring(slashIdx + 1).trim();
    const author = authorPart
        ? authorPart.split(',').map(s => s.trim()).filter(Boolean).join(' & ')
        : null;
    return { publication: pubPart || null, author: author || null };
}

async function logWorkflowRun(status: string, durationSecs?: number, lastError?: string) {
    if (!WORKFLOW_ID) return;
    try {
        await supabase.rpc('log_workflow_run', {
            p_workflow_id:   WORKFLOW_ID,
            p_status:        status,
            p_duration_secs: durationSecs ?? null,
            p_last_error:    lastError    ?? null,
        });
    } catch (_) { /* non-fatal */ }
}

// ---------------------------------------------------------------------------
// Parse Bio page
// ---------------------------------------------------------------------------

function parseBioPage(html: string): PlayerBio {
    const $ = cheerio.load(html);

    // Headshot image
    const headshotRaw = $('.player_profile_headshot img').first().attr('src') || null;
    const headshot = headshotRaw
        ? (headshotRaw.startsWith('http') ? headshotRaw : `${BASE_URL}${headshotRaw}`)
        : null;

    // Yellow highlight box (achievements summary)
    const highlightBox = $('.profile-highlight-box').first().text().trim() || null;

    // Structured fields — both the card section and the bio section share the same <p><strong>Key:</strong> value</p> pattern
    let birthDate:     string | null = null;
    let birthLocation: string | null = null;
    let nationality:   string | null = null;
    let heightCm:      number | null = null;
    let agentName:     string | null = null;
    let agentHref:     string | null = null;

    $('p').each((_, el) => {
        const $p   = $(el);
        const key  = $p.find('strong').first().text().replace(':', '').trim();
        const $a   = $p.find('a').first();
        const text = $p.text();

        switch (key) {
            case 'Born':
                // Link text = "Dec 30, 1984"
                if ($a.length) birthDate = $a.text().trim() || null;
                break;
            case 'Birthplace/Hometown':
            case 'Hometown':
                birthLocation = $a.length
                    ? $a.text().trim()
                    : text.replace(/^(Birthplace\/Hometown|Hometown):\s*/i, '').trim();
                birthLocation = birthLocation || null;
                break;
            case 'Nationality':
                nationality = $a.length
                    ? $a.text().trim()
                    : text.replace(/^Nationality:\s*/i, '').trim();
                nationality = nationality || null;
                break;
            case 'Height': {
                const m = text.match(/\((\d+)cm\)/);
                if (m) heightCm = parseInt(m[1]);
                break;
            }
            case 'Agent':
                agentName = $a.text().trim() || null;
                agentHref = $a.attr('href') || null;
                break;
        }
    });

    // Narrative bio paragraphs — any <p> after the bio h1 that has no <strong> child
    // (these are the prose paragraphs describing the player's career)
    const bioNarrativeParts: string[] = [];
    let inBioSection = false;
    $('h1, h2, h3, p').each((_, el) => {
        const tag  = el.tagName?.toLowerCase();
        const text = $(el).text().trim();
        if (tag === 'h1' && text.includes('Bio')) { inBioSection = true; return; }
        if (inBioSection && tag === 'p' && !$(el).find('strong').length && text.length > 40) {
            bioNarrativeParts.push(text);
        }
        // Stop at next major section
        if (inBioSection && (tag === 'h2' || tag === 'h3') && !text.includes('Bio')) inBioSection = false;
    });
    const bioNarrative = bioNarrativeParts.length > 0 ? bioNarrativeParts.join('\n\n') : null;

    return { headshot, highlightBox, bioNarrative, birthDate, birthLocation, nationality, heightCm, agentName, agentHref };
}

// ---------------------------------------------------------------------------
// Parse agent page for company / website
// ---------------------------------------------------------------------------

function parseAgentPage(html: string, agentName: string): AgentInfo {
    const $ = cheerio.load(html);

    const parts     = agentName.trim().split(/\s+/);
    const firstName = parts[0] || null;
    const lastName  = parts.slice(1).join(' ') || null;

    let companyName: string | null = null;
    let website:     string | null = null;

    $('table tr').each((_, row) => {
        const key = $(row).find('th').text().trim().replace(':', '');
        const $td = $(row).find('td');
        if (key === 'Company') companyName = $td.text().trim() || null;
        if (key === 'Website') website = $td.find('a').attr('href') || $td.text().trim() || null;
    });

    return { name: agentName, firstName, lastName, companyName, website };
}

// ---------------------------------------------------------------------------
// Parse news page (works for both global and player-specific pages)
// ---------------------------------------------------------------------------

interface NewsPageResult {
    articles: NewsArticle[];
    nextUrl:  string | null;
}

function parseNewsPage(html: string, contextPlayerUrl?: string): NewsPageResult {
    const $ = cheerio.load(html);
    const articles: NewsArticle[] = [];

    $('div.article.clearfix').each((_, el) => {
        const $el = $(el);

        const $titleLink = $el.find('a.article-title').first();
        const title      = $titleLink.text().trim();
        const href       = $titleLink.attr('href') || '';
        if (!title || !href) return;

        const link = href.startsWith('http') ? href : `${BASE_URL}${href}`;

        const dateStr   = $el.find('p.author-details').first().text().trim() || null;
        const published = parseRealGMDate(dateStr);

        const imgAttr = $el.find('img.article-img').first().attr('src') || null;
        const imgSrc  = imgAttr
            ? (imgAttr.startsWith('http') ? imgAttr : `${BASE_URL}${imgAttr}`)
            : null;

        const bodyParagraphs: string[] = [];
        $el.find('div.article-body p').each((_, p) => {
            const t = $(p).text().trim();
            if (t) bodyParagraphs.push(t);
        });
        const body = bodyParagraphs.length > 0 ? bodyParagraphs.join('\n\n') : null;

        const $sourceLink = $el.find('p.article-source a').first();
        const sourceName  = $sourceLink.text().trim() || null;
        const sourceHref  = $sourceLink.attr('href') || null;

        // Player links from both tags section and article body
        const playerIdSet:  Set<string> = new Set();
        const playerUrlSet: Set<string> = new Set();

        const collectPlayerLinks = (selector: string) => {
            $el.find(selector).each((_, a) => {
                const h  = $(a).attr('href') || '';
                const id = extractPlayerId(h);
                if (!id || playerIdSet.has(id)) return;
                playerIdSet.add(id);
                playerUrlSet.add(h.startsWith('http') ? h : `${BASE_URL}${h}`);
            });
        };
        collectPlayerLinks('p.tags a[href*="/player/"]');
        collectPlayerLinks('div.article-body a[href*="/player/"]');

        // Also add the context player's URL if this is a player-specific page
        if (contextPlayerUrl) {
            const ctxId = extractPlayerId(contextPlayerUrl);
            if (ctxId && !playerIdSet.has(ctxId)) {
                playerIdSet.add(ctxId);
                playerUrlSet.add(contextPlayerUrl);
            }
        }

        articles.push({
            title, link, dateStr, published, imgSrc, body,
            sourceName, sourceHref,
            playerIds:  [...playerIdSet],
            playerUrls: [...playerUrlSet],
        });
    });

    // Pagination: follow the highest /older/ number link
    let maxOlderN = 0;
    let olderHref: string | null = null;
    $('a[href*="/older/"]').each((_, a) => {
        const h = $(a).attr('href') || '';
        const m = h.match(/\/older\/(\d+)/);
        if (m && parseInt(m[1]) > maxOlderN) { maxOlderN = parseInt(m[1]); olderHref = h; }
    });
    const nextUrl = olderHref ? (olderHref.startsWith('http') ? olderHref : `${BASE_URL}${olderHref}`) : null;

    return { articles, nextUrl };
}

// ---------------------------------------------------------------------------
// Resolve player URLs → talent UUIDs (with retry)
// ---------------------------------------------------------------------------

async function resolvePlayerUrlsToUuids(playerUrls: string[]): Promise<Record<string, string>> {
    if (playerUrls.length === 0) return {};

    let data:      any[] | null = null;
    let lastError: any          = null;

    for (let attempt = 1; attempt <= 3; attempt++) {
        if (attempt > 1) await new Promise(r => setTimeout(r, 5000));
        const { data: rows, error } = await supabase
            .from('hb_socials')
            .select('social_url, linked_talent')
            .not('linked_talent', 'is', null)
            .in('type', SOCIAL_TYPES)
            .in('social_url', playerUrls);
        if (!error) { data = rows; lastError = null; break; }
        lastError = error;
        console.warn(`    [WARN] hb_socials lookup attempt ${attempt}: ${error.message}`);
    }

    if (lastError) { console.warn(`    [WARN] hb_socials lookup failed`); return {}; }

    const urlToUuid: Record<string, string> = {};
    for (const row of (data || [])) {
        if (row.social_url && row.linked_talent) urlToUuid[row.social_url] = row.linked_talent;
    }
    return urlToUuid;
}

// ---------------------------------------------------------------------------
// Insert news articles (reused from realgm-news-scraper logic)
// ---------------------------------------------------------------------------

async function insertNewsArticles(articles: NewsArticle[], playerSocialUrl: string, playerUuid: string): Promise<{ inserted: number; failed: number }> {
    if (articles.length === 0) return { inserted: 0, failed: 0 };

    // De-dupe against DB
    const allLinksToCheck = [
        ...articles.map(a => a.sourceHref).filter(Boolean) as string[],
        ...articles.map(a => a.link),
    ];
    const { data: existing } = await supabase
        .from('news')
        .select('source_link')
        .in('source_link', allLinksToCheck);

    const existingLinks = new Set((existing || []).map((e: any) => e.source_link));
    const newArticles = articles.filter(a =>
        !existingLinks.has(a.sourceHref || '') &&
        !existingLinks.has(a.link)
    );

    if (newArticles.length === 0) return { inserted: 0, failed: 0 };

    // Batch-resolve player URLs to UUIDs
    const allUrls = [...new Set(newArticles.flatMap(a => a.playerUrls))];
    const urlToUuid = await resolvePlayerUrlsToUuids(allUrls);
    // Always ensure the profile player is included
    urlToUuid[playerSocialUrl] = playerUuid;

    let inserted = 0, failed = 0;

    for (const article of newArticles) {
        const talentUuids = [...new Set(
            article.playerUrls.map(u => urlToUuid[u]).filter(Boolean)
        )];
        // Always tag the profile player
        if (!talentUuids.includes(playerUuid)) talentUuids.push(playerUuid);

        const { publication, author } = parseSourceCredit(article.sourceName);
        const sourceName = publication
            ? (author ? `${publication} — ${author}` : publication)
            : 'RealGM Wiretap';
        const sourceLink = article.sourceHref || article.link;

        const notes: string[] = [];
        if (!article.imgSrc) notes.push('image:no_thumbnail');
        notes.push(`realgm_wiretap:${article.link}`);

        const record = {
            article_title:     article.title,
            article_heading:   null,
            article:           article.body,
            source_name:       sourceName,
            source_link:       sourceLink,
            image_primary:     article.imgSrc,
            published:         article.published,
            status:            'in progress',
            public_visible:    true,
            tagged_talent:     talentUuids,
            tagged_media:      [],
            linked_talent_ids: article.playerIds,
            linked_media_ids:  [],
            internal_notes:    notes,
        };

        let insertError: any = null;
        for (let attempt = 1; attempt <= 2; attempt++) {
            const { error } = await supabase.from('news').insert(record);
            if (!error) { inserted++; insertError = null; break; }
            insertError = error;
            if (error.message.includes('timeout') && attempt === 1) {
                await new Promise(r => setTimeout(r, 2000));
            } else break;
        }
        if (insertError) {
            if (insertError.message.includes('duplicate key') || insertError.message.includes('unique constraint')) {
                // Already in DB — fine
            } else {
                console.warn(`    [ERR] insert: ${insertError.message}`);
                failed++;
            }
        }
    }

    return { inserted, failed };
}

// ---------------------------------------------------------------------------
// Upsert agent contact + company, return { contactId, companyId }
// ---------------------------------------------------------------------------

async function upsertAgentContact(agentInfo: AgentInfo): Promise<{ contactId: string | null; companyId: string | null }> {
    let contactId: string | null = null;
    let companyId: string | null = null;

    // -- Contact --
    const { data: existing } = await supabase
        .from('hb_contacts')
        .select('id')
        .ilike('name_full', agentInfo.name)
        .limit(1);

    if (existing && existing.length > 0) {
        contactId = existing[0].id;
        console.log(`    Contact found: ${agentInfo.name} (${contactId})`);
    } else {
        const { data: inserted, error } = await supabase
            .from('hb_contacts')
            .insert({
                name_full:   agentInfo.name,
                first_name:  agentInfo.firstName,
                last_name:   agentInfo.lastName,
                company_name: agentInfo.companyName,
                soc_website: agentInfo.website,
                status:      'active',
            })
            .select('id')
            .single();
        if (error) {
            console.warn(`    [WARN] insert contact: ${error.message}`);
        } else {
            contactId = inserted.id;
            console.log(`    Contact created: ${agentInfo.name} (${contactId})`);
        }
    }

    // -- Company --
    if (agentInfo.companyName) {
        const { data: existingCo } = await supabase
            .from('hb_companies')
            .select('id')
            .ilike('name', agentInfo.companyName)
            .limit(1);

        if (existingCo && existingCo.length > 0) {
            companyId = existingCo[0].id;
            console.log(`    Company found: ${agentInfo.companyName} (${companyId})`);
        } else {
            const { data: insertedCo, error: coErr } = await supabase
                .from('hb_companies')
                .insert({
                    name:        agentInfo.companyName,
                    soc_website: agentInfo.website,
                    status:      'active',
                })
                .select('id')
                .single();
            if (coErr) {
                console.warn(`    [WARN] insert company: ${coErr.message}`);
            } else {
                companyId = insertedCo.id;
                console.log(`    Company created: ${agentInfo.companyName} (${companyId})`);
            }
        }
    }

    return { contactId, companyId };
}

// ---------------------------------------------------------------------------
// Add a UUID to a uuid[] column (only if not already present)
// ---------------------------------------------------------------------------

async function addToUuidArray(table: string, rowId: string, column: string, uuid: string): Promise<void> {
    for (let attempt = 1; attempt <= 3; attempt++) {
        if (attempt > 1) await new Promise(r => setTimeout(r, 3000));
        const { data: row, error: fetchErr } = await supabase
            .from(table).select(column).eq('id', rowId).single();
        if (fetchErr) {
            console.warn(`    [WARN] fetch ${table}.${column} attempt ${attempt}: ${fetchErr.message}`);
            continue;
        }
        const existing: string[] = row?.[column] ?? [];
        if (existing.includes(uuid)) return; // already there
        const { error: updErr } = await supabase
            .from(table).update({ [column]: [...existing, uuid] }).eq('id', rowId);
        if (!updErr) return;
        console.warn(`    [WARN] update ${table}.${column} attempt ${attempt}: ${updErr.message}`);
    }
}

// ---------------------------------------------------------------------------
// Main — process each REALGM player
// ---------------------------------------------------------------------------

async function scrapePlayer(social: {
    id:            string;
    social_url:    string;
    linked_talent: string;
    name:          string | null;
}): Promise<void> {
    const { id: socialId, social_url, linked_talent: talentId, name } = social;

    // Derive Bio and News URLs from social_url
    // social_url: https://basketball.realgm.com/player/LeBron-James/Summary/250
    const bioUrl  = social_url.replace('/Summary/', '/Bio/');
    const newsUrl = social_url.replace('/Summary/', '/News/');

    console.log(`\n--- ${name || social_url} ---`);
    console.log(`  Bio:  ${bioUrl}`);

    // -----------------------------------------------------------------------
    // 1. Fetch & parse Bio page
    // -----------------------------------------------------------------------
    const bioHtml = await fetchViaProxy(bioUrl);
    if (!bioHtml) {
        console.warn(`  [SKIP] could not fetch bio page`);
        return;
    }

    const bio = parseBioPage(bioHtml);

    // Build description: highlight box + narrative (if any)
    const descriptionParts = [bio.highlightBox, bio.bioNarrative].filter(Boolean);
    const description = descriptionParts.length > 0 ? descriptionParts.join('\n\n') : null;

    // -----------------------------------------------------------------------
    // 2. Update hb_socials
    // -----------------------------------------------------------------------
    const now = new Date().toISOString();
    const { error: socialErr } = await supabase
        .from('hb_socials')
        .update({
            description,
            image:      bio.headshot,
            last_check: now,
            updated_at: now,
        })
        .eq('id', socialId);
    if (socialErr) console.warn(`  [WARN] update hb_socials: ${socialErr.message}`);
    else console.log(`  hb_socials updated (desc: ${description ? 'yes' : 'no'}, image: ${bio.headshot ? 'yes' : 'no'})`);

    // -----------------------------------------------------------------------
    // 3. Update hb_talent (only empty columns)
    // -----------------------------------------------------------------------
    const { data: talent, error: talentFetchErr } = await supabase
        .from('hb_talent')
        .select('birth_location, birth_country, date_birthdate, stats_height, birth_country_link')
        .eq('id', talentId)
        .single();

    if (talentFetchErr) {
        console.warn(`  [WARN] fetch hb_talent: ${talentFetchErr.message}`);
    } else {
        const talentUpdate: Record<string, any> = {};

        if (!talent.birth_location && bio.birthLocation)
            talentUpdate.birth_location = bio.birthLocation;

        if (!talent.birth_country && bio.nationality)
            talentUpdate.birth_country = bio.nationality;

        if (!talent.date_birthdate && bio.birthDate) {
            const parsed = parseRealGMDate(bio.birthDate);
            if (parsed) talentUpdate.date_birthdate = parsed.substring(0, 10); // date only
        }

        if (!talent.stats_height && bio.heightCm)
            talentUpdate.stats_height = bio.heightCm;

        // birth_country_link — look up countries table
        if (!talent.birth_country_link && bio.nationality) {
            for (let attempt = 1; attempt <= 3; attempt++) {
                if (attempt > 1) await new Promise(r => setTimeout(r, 3000));
                const { data: countries, error: cErr } = await supabase
                    .from('countries')
                    .select('id, name')
                    .ilike('name', bio.nationality)
                    .limit(1);
                if (!cErr && countries && countries.length > 0) {
                    talentUpdate.birth_country_link = countries[0].id;
                    break;
                }
                if (cErr) console.warn(`    [WARN] countries lookup attempt ${attempt}: ${cErr.message}`);
            }
        }

        if (Object.keys(talentUpdate).length > 0) {
            const { error: talentUpdErr } = await supabase
                .from('hb_talent')
                .update(talentUpdate)
                .eq('id', talentId);
            if (talentUpdErr) console.warn(`  [WARN] update hb_talent: ${talentUpdErr.message}`);
            else console.log(`  hb_talent updated: ${Object.keys(talentUpdate).join(', ')}`);
        } else {
            console.log(`  hb_talent: all target fields already populated`);
        }
    }

    // -----------------------------------------------------------------------
    // 4. Handle agent
    // -----------------------------------------------------------------------
    if (bio.agentName && bio.agentHref) {
        console.log(`  Agent: ${bio.agentName}`);

        // Fetch agent page for company/website
        const agentPageUrl = bio.agentHref.startsWith('http')
            ? bio.agentHref
            : `${BASE_URL}${bio.agentHref}`;

        let agentInfo: AgentInfo = {
            name:        bio.agentName,
            firstName:   bio.agentName.split(' ')[0] || null,
            lastName:    bio.agentName.split(' ').slice(1).join(' ') || null,
            companyName: null,
            website:     null,
        };

        const agentHtml = await fetchViaProxy(agentPageUrl);
        if (agentHtml) agentInfo = parseAgentPage(agentHtml, bio.agentName);

        const { contactId, companyId } = await upsertAgentContact(agentInfo);

        // Update hb_talent arrays (agent contact + company)
        if (contactId) {
            await addToUuidArray('hb_talent', talentId, 'contacts_all',        contactId);
            await addToUuidArray('hb_talent', talentId, 'agenct_contacts',     contactId);
        }
        if (companyId) {
            await addToUuidArray('hb_talent', talentId, 'companies_all',       companyId);
            await addToUuidArray('hb_talent', talentId, 'agenct_companies',    companyId);
        }
    }

    // -----------------------------------------------------------------------
    // 5. Scrape player news page
    // -----------------------------------------------------------------------
    console.log(`  News: ${newsUrl}`);

    const allArticles: NewsArticle[] = [];
    const seenLinks   = new Set<string>();

    let currentUrl: string | null = newsUrl;
    let newsPage = 0;
    while (currentUrl && (MAX_NEWS_PAGES === 0 || newsPage < MAX_NEWS_PAGES)) {
        newsPage++;
        const html = await fetchViaProxy(currentUrl);
        if (!html) break;

        const { articles, nextUrl } = parseNewsPage(html, social_url);
        const newArts = articles.filter(a => !seenLinks.has(a.link));
        articles.forEach(a => seenLinks.add(a.link));
        allArticles.push(...newArts);

        console.log(`    News page ${newsPage}: ${articles.length} articles, ${newArts.length} new`);
        if (!nextUrl) break;
        currentUrl = nextUrl;
    }

    const { inserted, failed } = await insertNewsArticles(allArticles, social_url, talentId);
    console.log(`  News: ${allArticles.length} articles found | Inserted: ${inserted} | Failed: ${failed}`);
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

async function run(): Promise<void> {
    const startTime = Date.now();
    console.log(`=== RealGM ${SPORT_LABEL} Player Scraper ===`);
    console.log(`MAX_PLAYERS=${MAX_PLAYERS || 'all'}, MAX_NEWS_PAGES=${MAX_NEWS_PAGES || 'all'}\n`);

    await logWorkflowRun('running');

    // Load all REALGM socials that are linked to a talent profile (paginated to bypass 1000-row cap)
    let socials: any[] = [];
    let from = 0;
    const PAGE = 1000;
    while (true) {
        let loaded = false;
        for (let attempt = 1; attempt <= 3; attempt++) {
            if (attempt > 1) await new Promise(r => setTimeout(r, 3000));
            const { data, error } = await supabase
                .from('hb_socials')
                .select('id, social_url, linked_talent, name')
                .in('type', SOCIAL_TYPES)
                .not('linked_talent', 'is', null)
                .not('social_url', 'is', null)
                .like('social_url', `${BASE_URL}/player/%/Summary/%`)
                .order('name')
                .range(from, from + PAGE - 1);
            if (error) { console.warn(`hb_socials load attempt ${attempt}: ${error.message}`); continue; }
            socials.push(...(data || []));
            if ((data?.length ?? 0) < PAGE) { from = -1; } else { from += PAGE; }
            loaded = true;
            break;
        }
        if (!loaded || from < 0) break;
    }

    const batch   = PLAYER_OFFSET > 0 ? socials.slice(PLAYER_OFFSET) : socials;
    const limit   = MAX_PLAYERS > 0 ? MAX_PLAYERS : batch.length;
    console.log(`Found ${socials.length} REALGM players, offset=${PLAYER_OFFSET}, processing ${limit}\n`);

    let totalInserted = 0, totalFailed = 0;
    for (let i = 0; i < limit; i++) {
        const social = batch[i];
        try {
            await scrapePlayer(social);
        } catch (e: any) {
            console.error(`  [ERR] ${social.name}: ${e.message}`);
            totalFailed++;
        }
    }

    const durationSecs = Math.round((Date.now() - startTime) / 1000);
    console.log(`\n=== Done in ${durationSecs}s ===`);

    await logWorkflowRun(
        totalFailed > 0 ? 'partial' : 'success',
        durationSecs,
        totalFailed > 0 ? `${totalFailed} players failed` : undefined,
    );
}

run().catch(async (e) => {
    console.error('Fatal:', e);
    await logWorkflowRun('failure', 0, e.message);
    process.exit(1);
});
