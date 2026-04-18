import * as dotenv from 'dotenv';
dotenv.config();

import { supabase } from './supabase';
import * as cheerio from 'cheerio';

const MAX_PAGES   = parseInt(process.env.MAX_PAGES  || '0');   // 0 = all pages
const WORKFLOW_ID = process.env.WORKFLOW_ID ? parseInt(process.env.WORKFLOW_ID) : null;
const RAPIDAPI_KEY = process.env.RAPIDAPI_KEY!;

if (!RAPIDAPI_KEY) {
    console.error('❌ Missing RAPIDAPI_KEY');
    process.exit(1);
}

const BASE_URL    = 'https://basketball.realgm.com';
const NEWS_URL    = `${BASE_URL}/nba/news`;
const FETCH_DELAY = 1500; // ms — polite crawling through proxy

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface Article {
    title:       string;
    link:        string;       // absolute wiretap URL — used as source_link key
    dateStr:     string | null;
    published:   string | null; // ISO
    imgSrc:      string | null;
    body:        string | null;
    sourceName:  string | null; // original publication, e.g. "The Athletic"
    sourceHref:  string | null; // external article URL
    playerIds:   string[];      // RealGM player IDs from Tags section
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function fetchViaProxy(url: string): Promise<string | null> {
    await new Promise(r => setTimeout(r, FETCH_DELAY));
    const proxyUrl = `https://proxycrawl-crawling.p.rapidapi.com/?url=${encodeURIComponent(url)}`;
    try {
        const res = await fetch(proxyUrl, {
            headers: {
                'x-rapidapi-key':  RAPIDAPI_KEY,
                'x-rapidapi-host': 'proxycrawl-crawling.p.rapidapi.com',
            },
        });
        if (!res.ok) {
            // Retry once with javascript=true on 5xx
            if (res.status >= 500) {
                console.warn(`  [HTTP ${res.status}] retrying with JS rendering...`);
                await new Promise(r => setTimeout(r, 2000));
                const res2 = await fetch(
                    `https://proxycrawl-crawling.p.rapidapi.com/?url=${encodeURIComponent(url)}&javascript=true`,
                    { headers: { 'x-rapidapi-key': RAPIDAPI_KEY, 'x-rapidapi-host': 'proxycrawl-crawling.p.rapidapi.com' } }
                );
                if (!res2.ok) {
                    console.warn(`  [HTTP ${res2.status}] ${url}`);
                    return null;
                }
                return await res2.text();
            }
            console.warn(`  [HTTP ${res.status}] ${url}`);
            return null;
        }
        return await res.text();
    } catch (e: any) {
        console.warn(`  [FETCH ERR] ${url}: ${e.message}`);
        return null;
    }
}

// Parse a RealGM date string like "Apr 17, 2026 8:43 AM" → ISO
function parseRealGMDate(raw: string | null): string | null {
    if (!raw) return null;
    try {
        const cleaned = raw.trim().replace(/\s+/g, ' ');
        const d = new Date(cleaned);
        return isNaN(d.valueOf()) ? null : d.toISOString();
    } catch { return null; }
}

// Extract RealGM player ID from a URL like /player/LeBron-James/Summary/250 → "250"
function extractPlayerId(href: string): string | null {
    const m = href.match(/\/player\/[^/]+\/[^/]+\/(\d+)/i);
    return m ? m[1] : null;
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
// Parse news listing page → articles (listing already contains full content)
// ---------------------------------------------------------------------------
// HTML structure per article:
//   <div class='article clearfix'>
//     <a href='/wiretap/ID/Slug' class='article-title'>Title</a>
//     <p class="author-details">Apr 17, 2026 8:43 AM</p>
//     <div class="lead-photo"><a href="..."><img src="..." class="article-img"></a></div>
//     <div class="article-content content">
//       <div class="article-body"><p>...</p></div>
//       <p class="article-source"><a href="external">Source Name</a></p>
//       <p class='tags'><span>Tags:</span> <a href="/player/Name/Summary/ID">Name</a> ...</p>
//     </div>
//   </div>

async function fetchNewsPage(pageUrl: string): Promise<Article[]> {
    console.log(`  Fetching: ${pageUrl}`);
    const html = await fetchViaProxy(pageUrl);
    if (!html) return [];

    const $        = cheerio.load(html);
    const articles: Article[] = [];

    $('div.article.clearfix').each((_, el) => {
        const $el = $(el);

        const $titleLink = $el.find('a.article-title').first();
        const title      = $titleLink.text().trim();
        const href       = $titleLink.attr('href') || '';
        if (!title || !href) return;

        const link = href.startsWith('http') ? href : `${BASE_URL}${href}`;

        // Date
        const dateStr  = $el.find('p.author-details').first().text().trim() || null;
        const published = parseRealGMDate(dateStr);

        // Image
        const imgAttr = $el.find('img.article-img').first().attr('src') || null;
        const imgSrc  = imgAttr
            ? (imgAttr.startsWith('http') ? imgAttr : `${BASE_URL}${imgAttr}`)
            : null;

        // Article body — join all paragraph texts
        const bodyParagraphs: string[] = [];
        $el.find('div.article-body p').each((_, p) => {
            const t = $(p).text().trim();
            if (t) bodyParagraphs.push(t);
        });
        const body = bodyParagraphs.length > 0 ? bodyParagraphs.join('\n\n') : null;

        // Original source credit (from p.article-source a)
        const $sourceLink = $el.find('p.article-source a').first();
        const sourceName  = $sourceLink.text().trim() || null;
        const sourceHref  = $sourceLink.attr('href') || null;

        // Player IDs from Tags section
        const playerIds: string[] = [];
        $el.find('p.tags a[href*="/player/"]').each((_, a) => {
            const id = extractPlayerId($(a).attr('href') || '');
            if (id) playerIds.push(id);
        });

        articles.push({ title, link, dateStr, published, imgSrc, body, sourceName, sourceHref, playerIds });
    });

    return articles;
}

// ---------------------------------------------------------------------------
// Resolve player IDs → hb_talent UUIDs via hb_socials (type='realgm')
// ---------------------------------------------------------------------------

async function resolvePlayerIdsToUuids(playerIds: string[]): Promise<Record<string, string>> {
    if (playerIds.length === 0) return {};

    const { data, error } = await supabase
        .from('hb_socials')
        .select('identifier, linked_talent')
        .not('linked_talent', 'is', null)
        .eq('type', 'realgm')
        .in('identifier', playerIds);

    if (error) {
        console.warn(`  [WARN] hb_socials lookup: ${error.message}`);
        return {};
    }

    const idToUuid: Record<string, string> = {};
    for (const row of (data || [])) {
        if (row.identifier && row.linked_talent) {
            idToUuid[row.identifier] = row.linked_talent;
        }
    }
    return idToUuid;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function scrapeNews(): Promise<void> {
    const startTime = Date.now();
    console.log('=== RealGM NBA News Scraper ===');
    console.log(`Checking ${MAX_PAGES === 0 ? 'all' : MAX_PAGES} news page(s)\n`);

    await logWorkflowRun('running');

    // -- Collect articles from listing pages --
    const allArticles: Article[] = [];
    for (let page = 1; MAX_PAGES === 0 || page <= MAX_PAGES; page++) {
        const pageUrl = page === 1 ? NEWS_URL : `${NEWS_URL}?page=${page}`;
        console.log(`Fetching news page ${page}: ${pageUrl}`);
        const arts = await fetchNewsPage(pageUrl);
        console.log(`  Found ${arts.length} article(s)`);
        if (arts.length === 0) break;
        allArticles.push(...arts);
        // Stop paginating if clearly at end
        if (arts.length < 5) break;
    }

    console.log(`\nTotal articles found: ${allArticles.length}`);
    if (allArticles.length === 0) {
        console.warn('  No articles found — check selector or proxy.');
        await logWorkflowRun('success', 0);
        return;
    }

    // -- De-dupe against news table by source_link --
    const { data: existing } = await supabase
        .from('news')
        .select('source_link')
        .in('source_link', allArticles.map(a => a.link));

    const existingLinks = new Set((existing || []).map(e => e.source_link));
    const newArticles = allArticles.filter(a => !existingLinks.has(a.link));

    console.log(`Already in DB: ${allArticles.length - newArticles.length} | New to process: ${newArticles.length}\n`);

    if (newArticles.length === 0) {
        console.log('Nothing new — all articles already in the database.');
        await logWorkflowRun('success', Math.round((Date.now() - startTime) / 1000));
        return;
    }

    // -- Resolve and insert each new article --
    let insertedCount = 0;
    let failedCount   = 0;

    // Batch-resolve all player IDs at once (one DB round-trip)
    const allPlayerIds = [...new Set(newArticles.flatMap(a => a.playerIds))];
    const idToUuid = await resolvePlayerIdsToUuids(allPlayerIds);
    console.log(`Resolved ${Object.keys(idToUuid).length}/${allPlayerIds.length} player IDs to talent UUIDs\n`);

    for (const article of newArticles) {
        const talentUuids = [...new Set(article.playerIds.map(id => idToUuid[id]).filter(Boolean))];

        console.log(`[INS] ${article.title}`);
        console.log(`  players: [${article.playerIds.join(', ')}] → ${talentUuids.length} UUID(s)`);

        const notes: string[] = [];
        if (!article.imgSrc) notes.push('image:no_thumbnail');
        if (article.sourceHref) notes.push(`original_source:${article.sourceHref}`);

        const record = {
            article_title:     article.title,
            article_heading:   null,
            article:           article.body,
            source_name:       article.sourceName ? `RealGM Wiretap — ${article.sourceName}` : 'RealGM Wiretap',
            source_link:       article.link,
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

        // Insert with one retry on statement timeout
        let insertError: any = null;
        for (let attempt = 1; attempt <= 2; attempt++) {
            const { error } = await supabase.from('news').insert(record);
            if (!error) { insertedCount++; insertError = null; break; }
            insertError = error;
            if (error.message.includes('timeout') && attempt === 1) {
                await new Promise(r => setTimeout(r, 2000));
            } else {
                break;
            }
        }
        if (insertError) {
            console.error(`  [ERR] ${insertError.message}`);
            failedCount++;
        }
    }

    const durationSecs = Math.round((Date.now() - startTime) / 1000);
    console.log(`\n=== Done in ${durationSecs}s | Inserted: ${insertedCount} | Failed: ${failedCount} ===`);

    await logWorkflowRun(
        failedCount > 0 ? 'partial' : 'success',
        durationSecs,
        failedCount > 0 ? `${failedCount} inserts failed` : undefined,
    );
}

scrapeNews().catch(async (e) => {
    console.error('Fatal:', e);
    await logWorkflowRun('failure', 0, e.message);
    process.exit(1);
});
