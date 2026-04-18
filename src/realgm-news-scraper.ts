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

// Sport-specific config — set via env vars in each workflow
const BASE_URL     = (process.env.BASE_URL   || 'https://basketball.realgm.com').replace(/\/$/, '');
const NEWS_PATH    =  process.env.NEWS_PATH   || '/nba/news';
const SPORT_LABEL  =  process.env.SPORT_LABEL || 'NBA';
const SOCIAL_TYPES = (process.env.SOCIAL_TYPES || 'realgm,REALGM').split(',').map(s => s.trim());

const NEWS_URL    = `${BASE_URL}${NEWS_PATH}`;
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
    playerIds:   string[];      // RealGM player IDs extracted from tag URLs
    playerUrls:  string[];      // full absolute RealGM player URLs (for exact social_url lookup)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function fetchViaProxy(url: string): Promise<string | null> {
    await new Promise(r => setTimeout(r, FETCH_DELAY));

    // JS rendering is more reliable against RealGM's bot detection.
    // Try up to 3 times with increasing back-off delays.
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
            console.warn(`  [HTTP ${res.status}${js ? ' js' : ''}] retrying...`);
        } catch (e: any) {
            console.warn(`  [FETCH ERR] ${url}: ${e.message}`);
        }
    }

    console.warn(`  [FAILED] all attempts exhausted for ${url}`);
    return null;
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

// Parse "Dan Woike, Sam Amick/The Athletic" → { publication: "The Athletic", author: "Dan Woike & Sam Amick" }
// If no "/" present the whole string is the publication with no separate author.
function parseSourceCredit(raw: string | null): { publication: string | null; author: string | null } {
    if (!raw) return { publication: null, author: null };
    const slashIdx = raw.lastIndexOf('/');
    if (slashIdx === -1) return { publication: raw.trim() || null, author: null };
    const authorPart = raw.substring(0, slashIdx).trim();
    const pubPart    = raw.substring(slashIdx + 1).trim();
    const author     = authorPart
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

interface NewsPageResult {
    articles: Article[];
    nextUrl:  string | null;  // "Older Wiretaps" link, or null if last page
}

async function fetchNewsPage(pageUrl: string): Promise<NewsPageResult> {
    console.log(`  Fetching: ${pageUrl}`);
    const html = await fetchViaProxy(pageUrl);
    if (!html) return { articles: [], nextUrl: null };

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

        // Player IDs + full URLs from Tags section AND article body links.
        // Some listing-page articles have no <p class="tags"> player links even when
        // a player is clearly tagged — so we harvest from both sources and deduplicate.
        const playerIdSet:  Set<string> = new Set();
        const playerUrlSet: Set<string> = new Set();

        const collectPlayerLinks = (selector: string) => {
            $el.find(selector).each((_, a) => {
                const h = $(a).attr('href') || '';
                const id = extractPlayerId(h);
                if (!id) return;
                if (playerIdSet.has(id)) return; // already seen
                playerIdSet.add(id);
                const fullUrl = h.startsWith('http') ? h : `${BASE_URL}${h}`;
                playerUrlSet.add(fullUrl);
            });
        };

        collectPlayerLinks('p.tags a[href*="/player/"]');
        collectPlayerLinks('div.article-body a[href*="/player/"]');

        const playerIds  = [...playerIdSet];
        const playerUrls = [...playerUrlSet];

        articles.push({ title, link, dateStr, published, imgSrc, body, sourceName, sourceHref, playerIds, playerUrls });
    });

    // Find "Older Wiretaps" link for next page — href="/nba/news/older/N"
    const olderHref = $('a[href*="/older/"]').first().attr('href') || null;
    const nextUrl   = olderHref ? (olderHref.startsWith('http') ? olderHref : `${BASE_URL}${olderHref}`) : null;

    return { articles, nextUrl };
}

// ---------------------------------------------------------------------------
// Resolve player IDs → hb_talent UUIDs via hb_socials (type='realgm')
// ---------------------------------------------------------------------------

// Resolve player URLs → hb_talent UUIDs via exact social_url match in hb_socials.
// playerUrls are the full absolute URLs from article tags, e.g.:
//   https://basketball.realgm.com/player/LeBron-James/Summary/250
// These match exactly what is stored in hb_socials.social_url.
async function resolvePlayerUrlsToUuids(playerUrls: string[]): Promise<Record<string, string>> {
    if (playerUrls.length === 0) return {};

    let data: any[] | null = null;
    let lastError: any = null;

    for (let attempt = 1; attempt <= 3; attempt++) {
        if (attempt > 1) await new Promise(r => setTimeout(r, 3000)); // wait for DB warm-up
        const { data: rows, error } = await supabase
            .from('hb_socials')
            .select('social_url, linked_talent')
            .not('linked_talent', 'is', null)
            .in('type', SOCIAL_TYPES)
            .in('social_url', playerUrls);

        if (!error) { data = rows; lastError = null; break; }
        lastError = error;
        console.warn(`  [WARN] hb_socials lookup attempt ${attempt}: ${error.message}`);
    }

    if (lastError) {
        console.warn(`  [WARN] hb_socials lookup failed after retries: ${lastError.message}`);
        return {};
    }

    const urlToUuid: Record<string, string> = {};
    for (const row of (data || [])) {
        if (row.social_url && row.linked_talent) {
            urlToUuid[row.social_url] = row.linked_talent;
        }
    }
    return urlToUuid;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function scrapeNews(): Promise<void> {
    const startTime = Date.now();
    console.log(`=== RealGM ${SPORT_LABEL} News Scraper ===`);
    console.log(`Checking ${MAX_PAGES === 0 ? 'all' : MAX_PAGES} news page(s)\n`);

    await logWorkflowRun('running');

    // -- Collect articles from listing pages --
    // Follow "Older Wiretaps" links (href="/nba/news/older/N") for pagination.
    // Stop when: no nextUrl found, MAX_PAGES reached, or a page adds 0 new articles.
    const allArticlesDeduped: Article[] = [];
    const seenLinks = new Set<string>();

    let currentUrl: string | null = NEWS_URL;
    let page = 0;
    while (currentUrl && (MAX_PAGES === 0 || page < MAX_PAGES)) {
        page++;
        console.log(`Fetching news page ${page}: ${currentUrl}`);
        const { articles: arts, nextUrl } = await fetchNewsPage(currentUrl);
        const newArts = arts.filter(a => !seenLinks.has(a.link));
        arts.forEach(a => seenLinks.add(a.link));
        console.log(`  Found ${arts.length} article(s), ${newArts.length} new`);
        allArticlesDeduped.push(...newArts);
        if (!nextUrl) { console.log('  No older wiretaps link — reached last page'); break; }
        currentUrl = nextUrl;
    }

    console.log(`\nTotal unique articles: ${allArticlesDeduped.length}`);
    if (allArticlesDeduped.length === 0) {
        console.warn('  No articles found — check selector or proxy.');
        await logWorkflowRun('success', 0);
        return;
    }

    // -- De-dupe against news table --
    // Check both the external source URL (new schema) and the wiretap URL (old schema / fallback)
    // so already-inserted records are correctly skipped regardless of which URL was stored.
    const allLinksToCheck = [
        ...allArticlesDeduped.map(a => a.sourceHref).filter(Boolean) as string[],
        ...allArticlesDeduped.map(a => a.link),
    ];
    const { data: existing } = await supabase
        .from('news')
        .select('source_link')
        .in('source_link', allLinksToCheck);

    const existingLinks = new Set((existing || []).map(e => e.source_link));
    const newArticles = allArticlesDeduped.filter(a =>
        !existingLinks.has(a.sourceHref || '') &&
        !existingLinks.has(a.link)
    );

    console.log(`Already in DB: ${allArticlesDeduped.length - newArticles.length} | New to process: ${newArticles.length}\n`);

    if (newArticles.length === 0) {
        console.log('Nothing new — all articles already in the database.');
        await logWorkflowRun('success', Math.round((Date.now() - startTime) / 1000));
        return;
    }

    // -- Resolve and insert each new article --
    let insertedCount = 0;
    let failedCount   = 0;

    // Batch-resolve all player URLs at once (one DB round-trip, exact social_url match)
    const allPlayerUrls = [...new Set(newArticles.flatMap(a => a.playerUrls))];
    const urlToUuid = await resolvePlayerUrlsToUuids(allPlayerUrls);
    console.log(`Resolved ${Object.keys(urlToUuid).length}/${allPlayerUrls.length} player URLs to talent UUIDs\n`);

    for (const article of newArticles) {
        const talentUuids = [...new Set(article.playerUrls.map(url => urlToUuid[url]).filter(Boolean))];

        console.log(`[INS] ${article.title}`);
        console.log(`  players: [${article.playerIds.join(', ')}] → ${talentUuids.length} UUID(s) resolved`);

        const { publication, author } = parseSourceCredit(article.sourceName);
        const sourceName = publication
            ? (author ? `${publication} — ${author}` : publication)
            : 'RealGM Wiretap';
        const sourceLink = article.sourceHref || article.link; // prefer external URL

        const notes: string[] = [];
        if (!article.imgSrc) notes.push('image:no_thumbnail');
        notes.push(`realgm_wiretap:${article.link}`); // always store wiretap URL for reference

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
            // Duplicate = already in DB from a previous run, not a real error
            if (insertError.message.includes('duplicate key') || insertError.message.includes('unique constraint')) {
                console.log(`  [SKIP] already exists`);
            } else {
                console.error(`  [ERR] ${insertError.message}`);
                failedCount++;
            }
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
