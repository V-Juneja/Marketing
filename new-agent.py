#!/usr/bin/env python3
"""
Arlo Lead Generation Agent
Autonomous Reddit lead finder and DM sender
- Runs every 4 hours to find leads
- Drafts DMs using MiMo v2.5
- Sends DMs via Puppeteer (2 per hour max)
- First-run login flow
"""

import json
import os
import sys
import time
import random
import asyncio
import signal
import re
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote
from typing import List, Dict, Optional, Tuple
import requests

# Apify configuration for spry_wholemeal/reddit-scraper
# Key rotator - exhausted/rate-limited keys are moved to the back automatically
APIFY_API_KEYS = [
    "apify_api_f1n7JFUVOZotji1ibHRaWNCpHBZSJb3Huv46",
    "apify_api_aIZDtajg604ajUoEubKcD5pSQYV7Ya2sLzdY",
    "apify_api_7F89LkDiqZPb0WdrHqpCfHcUgLdhpO0TLUC3",
    "apify_api_ac2oe1kmqJVhfq4P5MbPqVUX9lv4IF3xwFvN",
    "apify_api_eSts5hfr3lPX6EXDhbu2vCgVnEj15e01AGJB",
]
_apify_key_index = 0  # Points to the currently active key

def get_apify_key() -> str:
    return APIFY_API_KEYS[_apify_key_index]

def rotate_apify_key(reason: str = "") -> bool:
    """Rotate to the next Apify key. Returns False if all keys are exhausted."""
    global _apify_key_index
    old_key = APIFY_API_KEYS[_apify_key_index][:20] + "..."
    _apify_key_index += 1
    if _apify_key_index >= len(APIFY_API_KEYS):
        _apify_key_index = 0  # Wrap around and try again from the top
        log(f"[KEY ROTATOR] All {len(APIFY_API_KEYS)} keys cycled. Wrapping back to key 0. Reason: {reason}")
        return False
    new_key = APIFY_API_KEYS[_apify_key_index][:20] + "..."
    log(f"[KEY ROTATOR] Rotated key: {old_key} -> {new_key} (key {_apify_key_index+1}/{len(APIFY_API_KEYS)}). Reason: {reason}")
    return True

APIFY_API_KEY = property(get_apify_key)  # kept for reference; use get_apify_key() in code
APIFY_ACTOR_ID = "spry_wholemeal~reddit-scraper"
APIFY_RUN_URL = f"https://api.apify.com/v2/actors/{APIFY_ACTOR_ID}/run-sync-get-dataset-items"

# Constants
MIMO_API_KEY = "sk-skc4e9x7xt3thrcf25p7x39asd16hn6ubf0tughkrik2pzdk"
MIMO_API_URL = "https://api.xiaomimimo.com/v1/chat/completions"
REDDIT_COOKIES_FILE = Path("cookies/reddit_cookies.json")
LEADS_FILE = Path("leads.json")
SEEN_FILE = Path("seen_posts.json")
CONFIG_FILE = Path("config.json")
LOG_FILE = Path("agent.log")
FIRST_RUN_FILE = Path(".first_run_complete")
SENT_USERS_FILE = Path("sent_users.json")  # Permanent record of all users we've sent DMs to
RATE_LIMITED_FILE = Path("rate_limited_subs.json")  # Track subreddits that got rate limited

# Rate limiting - DISABLED for continuous sending
MAX_DMS_PER_HOUR = 999
DM_COOLDOWN_SECONDS = 0  # No cooldown between DMs
SCAN_INTERVAL_HOURS = 8

# Enhanced Subreddits for more volume - 195+ subreddits
SUBREDDITS = [
    # SaaS & Startups (Core targets)
    "startups", "Entrepreneur", "SaaS", "indiehackers", "smallbusiness",
    "webdev", "programming", "SideProject", "roastmystartup",
    "startup", "business", "marketing", "growthhacking", "startupsociety",
    "EntrepreneurRideAlong", "juststart", "businessideas", "passive_income",
    "startups_", "founders", "startupresources", "startupjobs",
    
    # E-commerce (High priority - store owners need conversion help)
    "ecommerce", "shopify", "Etsy", "AmazonSeller", "ecommerce_entrepreneurs",
    "dropship", "woocommerce", "Magento", "BigCommerce", "dropshipping",
    "printondemand", "FBA", "ShopifySeller", "EtsySellers",
    "ecommerce_ideas", "shopifystores", "amazonsellercentral", "fba_",
    "onlineselling", "reselling", "flipping", "retailarbitrage",
    "shopifyapps", "shopifydevelopement", "woocommercehelp",
    "dropshipping_newbies", "dropshipping_tips", "printondemandhustle",
    
    # Founders & Business Owners
    "fatFIRE", "smallbusinessowners", "businessowners", "startabusiness",
    "begginerentrepreneur", "solopreneur", "freelance", "freelancers",
    "WorkOnline", "digitalnomad", "remotework", "workfromhome",
    "selfemployed", "smallbusinesses", "localbusiness", "servicebusiness",
    
    # Web Development & Technical
    "web_design", "userexperience", "productmanagement",
    "conversionrateoptimization", "analytics", "googleanalytics",
    "javascript", "reactjs", "nextjs", "vuejs",
    "webdevelopment", "web_development", "Frontend", "Fullstack",
    "css", "webdevforbeginners", "learntocode", "coding",
    "devops", "webhosting", "Wordpress", "Drupal", "Joomla",
    "webflow", "squarespace", "wix", "weebly", "carrd",
    "bubbleio", "nocode", "lowcode", "flutter", "reactnative",
    
    # UX/UI & Design
    "UI_Design", "UXDesign", "design_critiques", "web_design_critiques",
    "conversiondesign", "landing_page_design", "designsystems",
    "productdesign", "interfacedesign", "webdevdesigners",
    
    # Marketing & Sales (Lead gen targets)
    "PPC", "SEO", "digital_marketing", "sales", "leadgeneration",
    "copywriting", "landingpage", "cro", "emailmarketing",
    "advertising", "FacebookAds", "GoogleAds", "socialmedia",
    "contentmarketing", "marketingdigital", "marketingstrategy",
    "onlinemarketing", "internetmarketing", "affiliatemarketing",
    "marketingautomation", "sales_funnels", "clickfunnels",
    "coldcalling", "coldoutreach", "prospecting", "b2bsales",
    
    # Support & Customer Success
    "customersupport", "customersuccess", "UserTesting",
    "CX", "customerexperience", "helpdesk", "zendesk",
    
    # Specific pain/problem subreddits
    "Vent", "work", "careerguidance", "AskReddit", "small_business_ideas",
    "stressed", "overwhelmed", "productivity", "time_management",
    
    # B2B & Enterprise Sales
    "b2b", "b2bmarketing", "outbound", "accountbasedmarketing",
    "enterprisesales", "saasmarketing", "techsales",
    
    # Platform-specific communities
    "Wordpress", "WordpressPlugins", "WordpressThemes",
    "shopifyapps", "shopifythemes", "shopifyexperts",
    "stripe", "paypal", "paymentprocessing",
    "cloudflare", "hosting", "webhosting", "vps",
    
    # AI & Automation communities (tech-forward users)
    "LocalLLaMA", "OpenAI", "ChatGPT", "artificial", "MachineLearning",
    "artificialintelligence", "automation", "zapier", "make",
    "aitools", "ai_automation", "chatbots", "AI_Agents",
    
    # Productivity & Business Tools
    "Notion", "Airtable", "asana", "mondaydotcom", "trello",
    "slack", "discordapp", "telegram", "crm", "salesforce",
    "hubspot", "mailchimp", "klaviyo", "convertkit",
    
    # === 50+ NEW SUBREDDITS - Indie Startups & High-MRR Founders ===
    
    # High-MRR / Successful Founder Communities
    "SaaSFounders", "SaaSFounders", "foundersin30", "founderscircle",
    "startupfounders", "techfounders", "bootstrapped", "bootstrappedfounders",
    "indiefounders", "indiehackers", "makers", "wiplog",
    "revenuegoals", "profitfirst", "mrr", "monthlyrecurringrevenue",
    "startupmetrics", "growthmetrics", "kpis", "saasmetrics",
    "exitwealth", "exits", "acquisitions", "sellingyourbusiness",
    "entrepreneurwealth", "wealthbuilding", "financialindependence",
    
    # Indie / Bootstrapped / Maker Communities
    "indiedev", "indiegamedev", "indiemakers", "indiehacker",
    "buildinpublic", "buildinginpublic", "openstartup", "transparentbusiness",
    "100DaysOfCode", "100DaysOfNoCode", "100DaysOfBusiness", "the100dayproject",
    "weekendproject", "sidehustle", "nightsandweekends", "eveningstartup",
    "microsaas", "minisaas", "nanosass", "paas", "saasbyexample",
    "nocodeapps", "lowcodeapps", "bubble", "flutterflow", "adalo",
    
    # Bigger Company / Scale-up Founders
    "scalers", "scaleups", "latestagestartups", "seriesa", "seriesb",
    "venturecapital", "vc", "angelinvestors", "seedstartups",
    "ceos", "founderceo", "cto", "cmo", "cofounder", "cofounders",
    "startupcto", "technicalfounder", "nontechnicalfounder",
    "productledgrowth", "plg", "productled", "salesled", 
    "enterprise_saas", "b2bsaas", "verticalsaas", "APIs",
    
    # Popular Entrepreneur / Business (High Volume)
    "entrepreneurs", "entrepreneurship", "businessowner", 
    "businessowners", "business_strategy", "businessadvice",
    "smallbiz", "smallbusinessowner", "smallbusinessadvice",
    "startupcommunity", "startuplife", "startupgrind", "startupculture",
    "hustle", "grind", "garyvee", "patflynn", "smartpassiveincome",
    "fastlaneforum", "millionaire", "entrepreneurmindset", "success",
    
    # Agency & Service Business Owners (High MRR potential)
    "agency", "marketingagency", "digitalagency", "webagency",
    "seoagency", "ppcagency", "socialmediaagency", "contentagency",
    "consulting", "consultants", "managementconsulting", "itconsulting",
    "freelancetobusiness", "freelancetoagency", "agencylife",
    "webdevagency", "designagency", "brandingagency", "creativeagency",
    "devshop", "softwarehouse", "appdevelopment", "mobileappagency",
    
    # Sales & Revenue Growth (Decision makers)
    "salesperformance", "salestips", "salestraining", "salesstrategy",
    "revenuegrowth", "revenueoperations", "revops", "salesops",
    "salesmanagement", "salesleader", "salesdirector", "vpofsales",
    "accountexecutive", "ae", "salesdevelopment", "sdr", "bdr",
    "closers", "high_ticket", "highticketclosing", "highticketsales",
    "enterprise", "enterprisesoftware", "softwaresales", 
    
    # Product & Growth Teams
    "productmanager", "productmanagers", "productmgmt", 
    "growth", "growthmarketing", "growthteam", "growthhacker",
    "useracquisition", "customeracquisition", "acquisition",
    "retention", "churn", "churnrate", "churnreduction",
    "onboarding", "useronboarding", "productonboarding",
    "activation", "engagement", "productanalytics", "amplitude", 
    "mixpanel", "heap", "clevertap", "optimizely",
]

# Enhanced Keywords - Tier 1 (High Intent, 4 points)
TIER_1_KEYWORDS = [
    # Conversion & Sales Pain
    "not converting", "low conversion", "conversion rate is", "no conversions",
    "traffic but no sales", "visitors but no sales", "clicks but no sales",
    "cart abandonment", "abandoned cart", "checkout abandonment",
    "people add to cart but don't buy", "adding to cart but not buying",
    "bounce rate is high", "high bounce rate", "users leaving",
    "people visit but don't buy", "window shoppers", "no one buys",
    "sales are down", "revenue dropped", "income dropped",
    "not making sales", "zero sales", "no revenue",
    
    # Support Overload
    "drowning in support", "too many support tickets", "support emails",
    "customer service overwhelmed", "cant keep up with emails",
    "answering same questions", "repetitive support questions",
    "support queue", "backlog of tickets", "support is swamped",
    
    # User Confusion
    "users confused", "customers don't understand", "people don't get it",
    "too complicated for users", "users are lost", "onboarding confusion",
    "nobody completes signup", "people drop off during signup",
    "users dont finish onboarding", "signup abandonment",
    "activation rate low", "users not activating", "ghost signups",
    
    # Churn & Retention
    "churn rate", "customers leaving", "cancelling subscriptions",
    "not retaining customers", "retention is bad", "users churning",
    "free trial not converting", "trial users don't convert",
    "freemium users wont pay", "cant convert free users",
    
    # Specific Metrics Pain
    "conversion rate under 1%", "conversion rate under 2%",
    "ctr is low", "click through rate sucks", "low ctr",
    "engagement is down", "time on site is low",
    
    # Business Struggles
    "struggling to get customers", "cant find customers",
    "need more customers", "how to get first customer",
    "how to get more sales", "how to increase revenue",
    "marketing not working", "ads not converting",
    "facebook ads not working", "google ads waste of money",
]

# Tier 2 Keywords (2 points)
TIER_2_KEYWORDS = [
    # General Pain Indicators
    "frustrated", "struggling with", "stuck on", "need help with",
    "what am i doing wrong", "tried everything", "losing money",
    "wasting money", "burning cash", "roi is negative",
    "not profitable", "losing customers", "cant scale",
    
    # Questions That Indicate Need
    "how do i improve", "how can i get more", "tips for increasing",
    "best way to", "how to optimize", "how to fix",
    "recommendations for", "suggestions for", "advice on",
    
    # Specific Scenarios
    "email capture not working", "popup not converting",
    "live chat too expensive", "intercom alternative",
    "zendesk too expensive", "crisp chat alternative",
    "chatbot doesn't work", "ai chatbot sucks",
    
    # Growth Challenges
    "growth stalled", "plateaued", "cant grow",
    "marketing budget low", "no budget for marketing",
    "bootstrap marketing", "cheap marketing ideas",
    "free ways to get customers", "zero budget marketing",
    
    # Product/Market Fit
    "product market fit", "finding product market fit",
    "is my product good", "will people buy this",
    "market validation", "validate my idea",
    
    # Technical but Business-Related
    "seo not working", "organic traffic low",
    "google ranking dropped", "lost ranking",
    "search traffic down", "impressions but no clicks",
]

# Seeker Signals - Must have one to qualify
SEEKER_SIGNALS = [
    "how do i", "how can i", "how to", "how should i",
    "need help", "please help", "help me", "advice needed",
    "struggling with", "stuck on", "can't figure out",
    "what am i doing wrong", "why is my", "why won't",
    "looking for", "searching for", "any recommendations",
    "has anyone tried", "has anyone used", "would appreciate",
    "tips appreciated", "advice appreciated", "feedback welcome",
    "thoughts?", "suggestions?", "recommendations?",
    "frustrated", "desperate", "at my wit's end",
    "losing hope", "about to give up", "ready to quit",
    "any ideas", "open to suggestions", "what would you do",
    "how did you", "success stories", "case studies",
    "real examples", "working solutions", "proven methods",
]

# Content Disqualifiers - Skip these
DISQUALIFIERS = [
    "hiring", "job posting", "we are looking for", "join our team",
    "promotion", "sponsored", "affiliate link", "referral code",
    "i'm selling", "for sale", "buy my", "check out my product",
    "shameless plug", "self promotion", "my startup",
    "launched today", "just launched", "show hn",
    "looking for cofounder", "seeking cofounder", "co-founder needed",
    "we're building", "we are building", "our startup",
    "feedback on my", "review my", "critique my",
]

# Score threshold
SCORE_MINIMUM = 3

# Product Description (No internal lingo, focus on outcomes)
PRODUCT_DESCRIPTION = """You are a founder writing a short, direct cold DM to someone who posted about a website/business problem. You sound like a real person, not a sales bot.

FULL PRODUCT BREAKDOWN (internal knowledge - never repeat these terms verbatim):

The product is an embeddable web agent that website owners add with a single script tag. Once installed, it reads and understands the website's content, offerings, and layout automatically. It then monitors visitor behavior in real time. When it detects a visitor who seems stuck, confused, hesitating, or about to leave (bouncing), it proactively engages them with a small unobtrusive card/popup offering help. Unlike basic chatbots, this agent can actually take actions on the page - it can click buttons, fill out forms, navigate between pages, and walk users through multi-step processes like checkout flows or signups. It answers questions about the site's content based on what it learned. It works on any website - ecommerce stores, SaaS apps, service businesses, landing pages. No technical configuration needed beyond pasting the script tag. It runs 24/7 without human monitoring. Sites using it have seen up to a 40% lift in conversions. It has a free tier to try, and a discounted plan available after the trial if they see results.

Key outcomes: reduces bounce rate, cuts support ticket volume, increases free-to-paid conversion, guides visitors through checkout/signup, recovers abandoning carts.

YOUR WRITING STYLE:
- Write like you're the 3rd cold message someone sent that day - calm, brief, no desperation
- Lead with something specific from their post (their exact problem, not a summary)
- Short punchy sentences. No fluff. No filler.
- End with a concrete, low-commitment ask
- Assume mutual respect, not seeking approval
- Describe the product naturally as "a script that catches stuck visitors and helps them convert" or similar casual phrasing. Never say "embeddable web agent" or "AI agent" or "smart assistant" in the DM.

ABSOLUTE RULES - THESE ARE NON-NEGOTIABLE:
1. NEVER include the recipient's username or "Hey u/username" or any greeting line with their name. Just start with the hook.
2. NEVER use exclamation points
3. NEVER use these phrases: "I hope this finds you well", "just reaching out", "I'd love to", "wondering if", "wanted to see if", "wanted to reach out", "I've been working on something that might help"
4. NEVER use generic flattery: "impressive", "love what you're doing", "great post", "resonated"
5. NEVER mention: Arlo, triggers, ANS scoring, fallback chains, nudge engines, behavioral detection, AI agent, smart assistant, embeddable web agent
6. ALWAYS mention the 40% conversion lift naturally (e.g. "sites using it see ~40% conversion lift")
7. ALWAYS mention it's free to try
8. ALWAYS mention discounted plan available after trial
9. NO em-dashes. Use commas or periods instead.
10. Max 250 words total
11. No bullet points, no lists, no markdown
12. No signature, no sign-off
13. Sound like a peer already in their world, not a vendor breaking in
14. First line must reference their specific problem directly
15. Describe the product in plain terms - what it does for the user, not what it is technically

HOW TO USE THEIR SPECIFIC CONTEXT:
When they mention what they sell/build, reference it. For example:
- If they run a Shopify jewelry store with cart abandonment, mention "catches people browsing your jewelry about to abandon cart and guides them through checkout"
- If they have a SaaS analytics tool with low trial conversion, mention "helps trial users who seem confused during setup and nudges them toward activation"
- If they have a PDF template site with low conversions, mention "catches visitors hesitating on your templates and walks them through the purchase"

BASE TEMPLATES (use as structural guide, but HEAVILY CUSTOMIZE with their specific context):

Ecommerce template structure:
- Hook: Reference their specific store/product problem
- Product: "I built an AI agent that runs on your store with a single script tag. There's no setup and nothing to train — it reads your store itself and knows what you sell from day one."
- What it does: "It sits there as a support chat for shoppers who have questions, pops up with a small card when someone looks like they're about to leave, and can actually do things on the page like click buttons and fill out forms on behalf of your customers."
- Sales layer: "You can also tune it to act more like a built-in sales agent, guiding people toward checkout in a way that feels helpful rather than aggressive."
- Results: "We've seen it lift conversions by around 40% on stores it's already running on."
- Offer: "Free right now — I just want honest feedback from real stores. If it performs, there's a discounted plan whenever you're ready."
- Ask: "Worth dropping on your store for a week?"

SaaS template structure:
- Hook: Reference their specific app/onboarding problem
- Product: "I built an AI agent that embeds in your app with one script tag. Nothing to configure, nothing to train — it figures out what your product does on its own."
- What it does: "It handles support questions, pops up a small card when a user looks confused or stuck, and can actually interact with your UI — clicking buttons, filling forms, completing flows on the user's behalf."
- Sales layer: "You can also set it up to act more like a built-in sales layer, converting free users or nudging people toward upgrades naturally."
- Results: "We've already seen around 40% conversion lifts on the products it's running on."
- Offer: "It's free right now and I'm looking for real products to test it on. If it moves the needle for you, there's a discounted plan ready when you want it."
- Ask: "Worth a try?"

Founder/General template structure:
- Hook: Reference their specific website/business problem
- Product: "I built an AI agent that lives on your website. One script tag and it's running — zero setup, zero training. It reads your site on its own and immediately understands what you do."
- What it does: "It works as a support chat, a sales agent, and a hands-on helper all at once. If a visitor has a question, it answers. If someone looks stuck, a small card pops up offering to help. And it doesn't just talk — it actually does things: clicks buttons, fills out forms, walks users through flows on their behalf."
- Sales layer: "You can also customize it to act more like a built-in sales agent, nudging visitors toward a purchase or signup in a way that feels natural rather than pushy."
- Results: "We've already seen it boost conversions by around 40% on sites it's been running on."
- Offer: "Completely free right now. I just want real sites to run it on and honest feedback. If it performs, there's a discounted plan available whenever you want to keep it."
- Ask: "Would you be open to trying it out?"

REQUIRED PRODUCT DESCRIPTION PHRASE:
You MUST include this exact explanation of the product's capabilities in every DM (reword naturally to fit the context):
"It reads your site content on its own, so no setup. It can click buttons, fill forms, and guide users through your platform's features in real time."

CRITICAL CUSTOMIZATION RULES:
1. Use the template STRUCTURE but replace ALL generic examples with their SPECIFIC situation
2. If they mention "my Shopify candle store," say "your candle store" not "your store"
3. If they mention "PDF form tool," say "catches visitors trying your PDF tools" not generic "catches visitors"
4. Reference their exact problem: if they say "users abandon at checkout," mention that specifically
5. The templates above are STRUCTURAL GUIDES only - your output should sound completely customized to their post
6. ALWAYS include the REQUIRED PRODUCT DESCRIPTION PHRASE above - this is non-negotiable

Write the DM now. Max 250 words. No greeting line. Use template structure but make it sound like you read their specific post and are responding to THEIR exact situation."""


def log(message: str):
    """Log to console and file"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {message}"
    # Safe print - replace unencodable chars for the console
    try:
        print(line)
    except UnicodeEncodeError:
        print(line.encode("ascii", errors="replace").decode("ascii"))
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def load_json(filepath: Path) -> dict:
    """Load JSON file or return empty dict"""
    if filepath.exists():
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {}
    return {}


def save_json(filepath: Path, data: dict):
    """Save data to JSON file"""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def load_seen_posts() -> set:
    """Load set of seen post IDs"""
    data = load_json(SEEN_FILE)
    # Handle both old list format and new dict format
    if isinstance(data, list):
        return set(data)
    return set(data.get("ids", []))


def save_seen_posts(seen: set):
    """Save seen post IDs"""
    save_json(SEEN_FILE, {"ids": list(seen), "updated": datetime.now().isoformat()})


def load_leads() -> List[Dict]:
    """Load leads from JSON"""
    data = load_json(LEADS_FILE)
    return data.get("leads", [])


def save_leads(leads: List[Dict]):
    """Save leads to JSON"""
    save_json(LEADS_FILE, {"leads": leads, "updated": datetime.now().isoformat()})


def load_sent_users() -> set:
    """Load set of usernames we've already sent DMs to (permanent record)"""
    data = load_json(SENT_USERS_FILE)
    users = set(data.get("users", []))
    # Also sync from leads file for backwards compatibility
    leads = load_leads()
    for lead in leads:
        if lead.get("status") == "sent" and lead.get("username"):
            users.add(lead.get("username"))
    return users


def save_sent_user(username: str):
    """Add a username to the permanent sent users record"""
    users = load_sent_users()
    users.add(username)
    save_json(SENT_USERS_FILE, {
        "users": list(users),
        "updated": datetime.now().isoformat(),
        "total_sent": len(users)
    })
    log(f"[SENT_USERS] Added u/{username} to permanent sent record (total: {len(users)})")


def load_rate_limited_subs() -> dict:
    """Load subreddits that got rate limited with their last attempt timestamp"""
    data = load_json(RATE_LIMITED_FILE)
    return data.get("subs", {})


def save_rate_limited_sub(subreddit: str):
    """Mark a subreddit as rate limited (so it gets priority next scan)"""
    subs = load_rate_limited_subs()
    subs[subreddit] = {
        "last_rate_limited": datetime.now().isoformat(),
        "count": subs.get(subreddit, {}).get("count", 0) + 1
    }
    save_json(RATE_LIMITED_FILE, {"subs": subs, "updated": datetime.now().isoformat()})
    log(f"[RATE_LIMIT] Marked r/{subreddit} for priority scan next cycle (rate limited {subs[subreddit]['count']} times)")


def clear_rate_limited_sub(subreddit: str):
    """Clear a subreddit from rate limited list after successful fetch"""
    subs = load_rate_limited_subs()
    if subreddit in subs:
        del subs[subreddit]
        save_json(RATE_LIMITED_FILE, {"subs": subs, "updated": datetime.now().isoformat()})
        log(f"[RATE_LIMIT] Cleared r/{subreddit} from rate limited list (fetch succeeded)")


def get_subreddits_priority_order() -> List[str]:
    """Return subreddits sorted by priority: rate-limited first, then regular"""
    rate_limited = load_rate_limited_subs()
    # Sort by count (most rate limited first), then by last rate limited time
    priority_subs = sorted(
        rate_limited.keys(),
        key=lambda s: (-rate_limited[s].get("count", 0), rate_limited[s].get("last_rate_limited", "")),
        reverse=False
    )
    # Add remaining subs that aren't rate limited
    regular_subs = [s for s in SUBREDDITS if s not in rate_limited]
    return priority_subs + regular_subs


def load_config() -> dict:
    """Load or create config"""
    default = {
        "dm_count_last_hour": 0,
        "last_dm_time": None,
        "hourly_window_start": datetime.now().isoformat(),
        "total_dms_sent": 0,
    }
    data = load_json(CONFIG_FILE)
    for k, v in default.items():
        if k not in data:
            data[k] = v
    return data


def save_config(config: dict):
    """Save config"""
    save_json(CONFIG_FILE, config)


def reset_hourly_limit_if_needed(config: dict) -> dict:
    """Reset hourly counter if hour has passed"""
    if config.get("hourly_window_start"):
        window_start = datetime.fromisoformat(config["hourly_window_start"])
        if datetime.now() - window_start >= timedelta(hours=1):
            config["dm_count_last_hour"] = 0
            config["hourly_window_start"] = datetime.now().isoformat()
            log("Hourly DM limit reset")
    return config


def can_send_dm() -> bool:
    """Check if we can send a DM based on rate limits"""
    config = load_config()
    config = reset_hourly_limit_if_needed(config)
    
    # Check hourly limit
    if config.get("dm_count_last_hour", 0) >= MAX_DMS_PER_HOUR:
        return False
    
    # Check cooldown
    last_dm = config.get("last_dm_time")
    if last_dm:
        last_time = datetime.fromisoformat(last_dm)
        seconds_since = (datetime.now() - last_time).total_seconds()
        if seconds_since < DM_COOLDOWN_SECONDS:
            return False
    
    return True


def record_dm_sent():
    """Record that a DM was sent"""
    config = load_config()
    config = reset_hourly_limit_if_needed(config)
    config["dm_count_last_hour"] = config.get("dm_count_last_hour", 0) + 1
    config["last_dm_time"] = datetime.now().isoformat()
    config["total_dms_sent"] = config.get("total_dms_sent", 0) + 1
    save_config(config)


def _normalize_apify_post(item: Dict) -> Dict:
    """Convert an Apify spry_wholemeal/reddit-scraper result item into the same
    shape as a Reddit JSON API post child so all downstream code works unchanged.

    Actual Apify field names (confirmed from live response):
      post_id, title, text, author, subreddit, permalink, url,
      num_comments, score, created_utc_ts (unix timestamp float)
    """
    # created_utc_ts is a unix timestamp (float); fall back to ISO string
    created_utc = item.get("created_utc_ts") or item.get("created_utc") or 0
    if not created_utc:
        created_raw = item.get("created_utc_iso") or item.get("createdAt") or item.get("created_at") or ""
        try:
            created_utc = datetime.fromisoformat(
                created_raw.replace("Z", "+00:00")
            ).timestamp() if created_raw else 0
        except Exception:
            created_utc = 0

    # Body text field is 'text' in this actor (not selftext/body)
    body = (
        item.get("text")
        or item.get("selftext")
        or item.get("body")
        or ""
    )

    return {
        "data": {
            "id":           item.get("post_id") or item.get("id", ""),
            "title":        item.get("title", ""),
            "selftext":     body,
            "author":       item.get("author", ""),
            "subreddit":    item.get("subreddit", ""),
            "permalink":    item.get("permalink", ""),
            "url":          item.get("url", ""),
            "num_comments": item.get("num_comments") or item.get("numComments") or 0,
            "score":        item.get("score", 0),
            "created_utc":  float(created_utc) if created_utc else 0,
        }
    }


def fetch_reddit_posts_apify(subreddit: str, query: str, limit: int = 25) -> List[Dict]:
    """Fetch posts via Apify spry_wholemeal/reddit-scraper (Search mode).

    Runs the actor synchronously and returns a list of normalised post dicts
    that match the shape produced by the old Reddit JSON API helper, so all
    downstream scoring / categorising / DM logic works without modification.
    """
    actor_input = {
        "mode": "search",
        "searchTargets": [
            {
                "query": query,
                "searchSort": "new",
                "timeframe": "week",
                "restrictToSubreddit": subreddit,
                "maxResults": limit,
            }
        ],
        "includeCommentsMode": "none",
        "includeNsfw": False,
        "proxyConfiguration": {
            "useApifyProxy": True,
            "apifyProxyGroups": ["RESIDENTIAL"],
        },
    }

    # Retry loop - tries every key before giving up
    keys_tried = 0
    while keys_tried < len(APIFY_API_KEYS):
        headers = {
            "Authorization": f"Bearer {get_apify_key()}",
            "Content-Type": "application/json",
        }

        try:
            resp = requests.post(
                APIFY_RUN_URL,
                headers=headers,
                json=actor_input,
                timeout=300,
            )

            # 429 = Apify rate limit (per-key), rotate and retry
            if resp.status_code == 429:
                log(f"[APIFY 429] r/{subreddit} query='{query}' - key quota hit, rotating...")
                rotate_apify_key("429 rate limit")
                keys_tried += 1
                save_rate_limited_sub(subreddit)
                continue

            # 401/403 = key invalid/exhausted, rotate and retry
            if resp.status_code in (401, 403):
                log(f"[APIFY {resp.status_code}] r/{subreddit} - key auth failed, rotating...")
                rotate_apify_key(f"HTTP {resp.status_code} auth error")
                keys_tried += 1
                continue

            if resp.status_code not in (200, 201):
                log(f"[APIFY ERROR] r/{subreddit} HTTP {resp.status_code}: {resp.text[:200]}")
                return []

            # Success
            clear_rate_limited_sub(subreddit)
            items = resp.json()
            if not isinstance(items, list):
                log(f"[APIFY] Unexpected response format: {str(items)[:200]}")
                return []

            posts = [_normalize_apify_post(item) for item in items if isinstance(item, dict)]
            log(f"[APIFY] r/{subreddit} query='{query}' -> {len(posts)} posts (key {_apify_key_index+1}/{len(APIFY_API_KEYS)})")
            return posts

        except requests.Timeout:
            log(f"[APIFY TIMEOUT] r/{subreddit} query='{query}' timed out - rotating key...")
            rotate_apify_key("timeout")
            keys_tried += 1
            save_rate_limited_sub(subreddit)
            continue
        except Exception as e:
            log(f"[APIFY ERROR] r/{subreddit} query='{query}': {e}")
            return []

    log(f"[APIFY] All {len(APIFY_API_KEYS)} keys exhausted for r/{subreddit} query='{query}'. Giving up.")
    return []


def fetch_reddit_posts(subreddit: str, query: str, limit: int = 25) -> List[Dict]:
    """Fetch posts - delegates to Apify reddit-scraper actor."""
    return fetch_reddit_posts_apify(subreddit, query, limit)


def score_post(post: Dict) -> Tuple[int, List[str]]:
    """Score a post based on keywords and signals"""
    d = post.get("data", {})
    title = (d.get("title") or "").lower()
    body = (d.get("selftext") or "").lower()
    combined = f"{title} {body}"
    
    score = 0
    matched_keywords = []
    
    # Check disqualifiers first
    if any(dq in combined for dq in DISQUALIFIERS):
        return 0, []
    
    # Score Tier 1 keywords (4 pts each)
    for kw in TIER_1_KEYWORDS:
        if kw in combined:
            score += 4
            matched_keywords.append(kw)
    
    # Score Tier 2 keywords (2 pts each)
    for kw in TIER_2_KEYWORDS:
        if kw in combined and kw not in matched_keywords:
            score += 2
            matched_keywords.append(kw)
    
    # If no keyword matched at all, still give base score of 2 since
    # Apify already filtered for relevance via the search query itself
    if score == 0 and len(combined) > 50:
        score = 2
    
    # Seeker signals add bonus points (no longer a hard gate)
    has_seeker_signal = any(signal in combined for signal in SEEKER_SIGNALS)
    if has_seeker_signal:
        score += 2
    
    # Question in title is a strong seeker signal
    if "?" in title:
        score += 2
    
    if d.get("num_comments", 0) < 10:
        score += 1  # Low engagement = more likely to respond
    
    # Recency bonus
    created_utc = d.get("created_utc", 0)
    hours_ago = (datetime.now().timestamp() - created_utc) / 3600
    if hours_ago < 24:
        score += 2
    elif hours_ago < 48:
        score += 1
    
    return score, matched_keywords


def categorize_post(post: Dict) -> str:
    """Categorize post as ecommerce, saas, or founder"""
    d = post.get("data", {})
    title = (d.get("title") or "").lower()
    body = (d.get("selftext") or "").lower()
    subreddit = d.get("subreddit", "").lower()
    combined = f"{title} {body}"
    
    # Ecommerce indicators
    ecommerce_terms = [
        "shopify", "woocommerce", "etsy", "amazon", "store", "product",
        "cart", "checkout", "payment", "order", "shipping", "inventory",
        "customer", "buyer", "purchase", "refund", "shop"
    ]
    
    # SaaS indicators
    saas_terms = [
        "saas", "app", "platform", "software", "subscription", "trial",
        "signup", "onboarding", "activation", "user", "dashboard",
        "integration", "api", "feature", "plan"
    ]
    
    ecommerce_score = sum(1 for term in ecommerce_terms if term in combined)
    saas_score = sum(1 for term in saas_terms if term in combined)
    
    if subreddit in ["ecommerce", "shopify", "etsy", "amazonseller"]:
        ecommerce_score += 2
    if subreddit in ["saas", "startups", "indiehackers"]:
        saas_score += 2
    
    if ecommerce_score > saas_score:
        return "ecommerce"
    elif saas_score > ecommerce_score:
        return "saas"
    else:
        return "founder"


def ai_quality_check(post: Dict, category: str) -> bool:
    """Use AI to judge whether this person actually needs the product. Returns True if they do."""
    d = post.get("data", {})
    title = d.get("title", "")
    body = d.get("selftext", "") or ""
    subreddit = d.get("subreddit", "")
    body_truncated = body[:1500] if len(body) > 1500 else body
    
    check_prompt = f"""You are evaluating whether a Reddit poster would genuinely benefit from this product.

PRODUCT: An embeddable web agent that goes on any website with one script tag. It monitors visitors, catches ones who are stuck/confused/about to bounce, and proactively helps them convert by answering questions, clicking buttons, filling forms, and guiding them through checkout or signup. Sites see ~40% conversion lift. Reduces bounce rate, cuts support tickets, increases free-to-paid conversions.

POST TO EVALUATE:
Subreddit: r/{subreddit}
Title: {title}
Content: {body_truncated}
Category: {category}

Does this person actually need this product? Consider:
- Do they have a website, store, or app where visitors convert (buy, sign up, subscribe)?
- Are they struggling with conversion rates, bounce rates, cart abandonment, support load, or onboarding?
- Would adding something that catches stuck visitors and helps them convert directly solve their stated problem?
- Are they asking about getting traffic to convert, visitors not buying, users not signing up, or similar on-site conversion problems?

Say YES if they have ANY website/app conversion problem - even if they're also asking about ads or marketing. The product helps with on-site conversion regardless of traffic source. Only say NO if their problem is completely unrelated (e.g. looking for a job, asking about accounting software, seeking legal advice, purely technical coding question with no business angle).

Reply with exactly one word: YES or NO"""

    try:
        headers = {
            "api-key": MIMO_API_KEY,
            "Content-Type": "application/json"
        }
        
        body_payload = {
            "model": "mimo-v2.5",
            "messages": [
                {"role": "user", "content": check_prompt}
            ],
            "max_completion_tokens": 10,
            "temperature": 0.1,
            "top_p": 0.95,
            "stream": False,
            "thinking": {"type": "disabled"}
        }
        
        resp = requests.post(MIMO_API_URL, headers=headers, json=body_payload, timeout=30)
        if resp.status_code != 200:
            log(f"[QUALITY CHECK] API error: {resp.status_code}, defaulting to YES")
            return True
        
        data = resp.json()
        answer = data.get("choices", [{}])[0].get("message", {}).get("content", "").strip().upper()
        
        if "NO" in answer:
            log(f"[QUALITY CHECK] AI says NO - this person doesn't need the product. Skipping.")
            return False
        else:
            log(f"[QUALITY CHECK] AI says YES - this person needs the product. Proceeding.")
            return True
            
    except Exception as e:
        log(f"[QUALITY CHECK] Error: {e}, defaulting to YES")
        return True


def draft_dm_with_mimo(post: Dict, category: str) -> str:
    """Draft a personalized DM using MiMo v2.5 with full post context"""
    d = post.get("data", {})
    
    # Extract ALL available post data for maximum context
    title = d.get("title", "")
    body = d.get("selftext", "") or ""
    subreddit = d.get("subreddit", "")
    author = d.get("author", "")
    permalink = d.get("permalink", "")
    full_url = f"https://reddit.com{permalink}" if permalink else ""
    num_comments = d.get("num_comments", 0)
    created_utc = d.get("created_utc", 0)
    
    # Include up to 2000 chars of body for full context
    body_truncated = body[:2000] if len(body) > 2000 else body
    
    user_prompt = f"""FULL POST CONTEXT:
Username: u/{author}
Subreddit: r/{subreddit}
Post URL: {full_url}
Comments on post: {num_comments}
Post Title: {title}

FULL POST CONTENT:
{body_truncated}

Detected Category: {category}

TASK: Draft a personalized, conversational DM for this person based on their FULL post content above. 

The DM should:
1. Reference their SPECIFIC situation mentioned in their post (be detailed, mention what they actually wrote)
2. Offer the embeddable web agent as a potential solution to their exact problem
3. Ask if they'd be interested in trying it free
4. Follow all the rules in the system prompt (40% conversion lift, no jargon, no product name, etc.)

Write the DM now:"""
    
    username = author
    log(f"[AI DRAFTING] Starting DM draft for u/{username}...")
    log(f"[AI DRAFTING] Post from r/{subreddit}: {title[:80]}...")
    
    try:
        headers = {
            "api-key": MIMO_API_KEY,
            "Content-Type": "application/json"
        }
        
        body_payload = {
            "model": "mimo-v2.5",
            "messages": [
                {"role": "system", "content": PRODUCT_DESCRIPTION},
                {"role": "user", "content": user_prompt}
            ],
            "max_completion_tokens": 512,
            "temperature": 0.7,
            "top_p": 0.95,
            "stream": False,
            "thinking": {"type": "disabled"}
        }
        
        log(f"[AI DRAFTING] Calling MiMo API for u/{username}...")
        resp = requests.post(MIMO_API_URL, headers=headers, json=body_payload, timeout=60)
        if resp.status_code != 200:
            log(f"[AI DRAFTING] MiMo API error: {resp.status_code}")
            log(f"[AI DRAFTING] Response: {resp.text[:200]}")
            return ""
        
        data = resp.json()
        dm_text = data.get("choices", [{}])[0].get("message", {}).get("content", "").strip()
        
        # Post-process to ensure compliance
        dm_text = dm_text.replace("—", "-").replace("–", "-")  # No em-dashes
        dm_text = dm_text.replace("Arlo", "this tool")  # Remove product name
        
        # Log the drafted DM
        log(f"[AI DRAFTED DM for u/{username}]:")
        log("-" * 50)
        for line in dm_text.split('\n'):
            log(f"  {line}")
        log("-" * 50)
        log(f"[AI DRAFTING] DM draft complete for u/{username} ({len(dm_text)} chars)")
        
        return dm_text
    except Exception as e:
        log(f"[AI DRAFTING] Error drafting DM for u/{username}: {e}")
        return ""


def build_dm_url(username: str, subject: str, message: str) -> str:
    """Build Reddit compose URL"""
    encoded_subject = quote(subject[:100])  # Subject limit
    encoded_message = quote(message[:10000])  # Message limit
    return f"https://www.reddit.com/message/compose/?to={username}&subject={encoded_subject}&message={encoded_message}"


def scan_for_leads() -> List[Dict]:
    """Scan Reddit for new leads - every fetched post goes through LLM judge,
    anything approved gets a DM drafted and sent immediately (no score gate)."""
    log("=" * 50)
    log("Starting lead scan... [LLM-FIRST MODE: score gate bypassed]")
    
    seen = load_seen_posts()
    existing_leads = load_leads()
    existing_ids = {lead.get("post_id") for lead in existing_leads}
    sent_users = load_sent_users()  # Load permanent sent users record
    new_leads = []
    
    # Get priority order: rate-limited subs first
    priority_subs = get_subreddits_priority_order()
    rate_limited_count = len(load_rate_limited_subs())
    log(f"[PRIORITY SCAN] {rate_limited_count} rate-limited subreddits will be scanned first")
    
    # Search queries used as Reddit search terms via Apify
    search_queries = [
        "conversion rate",
        "not converting",
        "traffic no sales",
        "cart abandonment",
        "low sales",
        "support tickets overwhelmed",
        "users confused onboarding",
        "churn rate",
        "free trial not converting",
        "bounce rate high",
    ]
    
    all_posts = []
    
    # Search in each subreddit using priority order
    subs_to_scan = priority_subs[:30]  # Scan 30 subs per cycle
    for subreddit in subs_to_scan:
        for query in search_queries[:6]:  # 6 queries per sub
            posts = fetch_reddit_posts(subreddit, query, limit=15)
            all_posts.extend(posts)
            time.sleep(0.3)  # Small delay between requests
    
    log(f"Fetched {len(all_posts)} total posts")
    
    # Dedupe by post ID - no score filter, everything goes to the LLM judge
    unseen_posts = []
    for post in all_posts:
        post_id = post.get("data", {}).get("id")
        if not post_id or post_id in seen or post_id in existing_ids:
            continue
        seen.add(post_id)
        unseen_posts.append(post)

    log(f"[LLM-FIRST] {len(unseen_posts)} unseen posts -> sending ALL to LLM judge (no score gate)")

    for post in unseen_posts:
        d = post.get("data", {})
        username = d.get("author")

        # Skip bots / deleted accounts
        if not username or username in ("[deleted]", "AutoModerator"):
            continue

        # Dedupe: skip users we've already DM'd
        if username in sent_users:
            log(f"[DEDUPE SCAN] Skipping u/{username} - already sent DM to this user")
            continue

        # Score still computed for logging/dashboard only - does NOT gate anything
        score, keywords = score_post(post)
        category = categorize_post(post)

        # ── LLM JUDGE: only gate that matters ────────────────────────────────
        log(f"[LLM JUDGE] Evaluating: {d.get('title', '')[:60]}...")
        if not ai_quality_check(post, category):
            log(f"[LLM JUDGE] NO - skipping u/{username}")
            continue
        log(f"[LLM JUDGE] YES - drafting & sending DM to u/{username}")
        # ─────────────────────────────────────────────────────────────────────

        # Draft DM
        dm_text = draft_dm_with_mimo(post, category)
        if not dm_text:
            log(f"[DRAFT FAILED] Skipping u/{username}")
            continue

        subjects = {
            "ecommerce": "Quick question about your store",
            "saas": "Thought this might help",
            "founder": "Saw your post and wanted to reach out"
        }
        subject = subjects.get(category, "Quick question")

        lead = {
            "id": f"lead_{int(time.time())}_{random.randint(1000,9999)}",
            "post_id": d.get("id"),
            "username": username,
            "subreddit": d.get("subreddit"),
            "post_title": d.get("title"),
            "post_url": f"https://reddit.com{d.get('permalink', '')}",
            "post_preview": d.get("selftext", "")[:200],
            "category": category,
            "dm_text": dm_text,
            "dm_subject": subject,
            "dm_url": build_dm_url(username, subject, dm_text),
            "score": score,
            "keywords": keywords,
            "num_comments": d.get("num_comments", 0),
            "scraped_at": datetime.now().isoformat(),
            "status": "pending",
            "sent_at": None
        }

        # ── SEND IMMEDIATELY (bypass all other gates) ─────────────────────
        log(f"[SENDING] Sending DM immediately to u/{username}...")
        if send_dm_with_puppeteer(lead, bypass_rate_limit=True):
            lead["status"] = "sent"
            lead["sent_at"] = datetime.now().isoformat()
            save_sent_user(username)
            sent_users.add(username)
            log(f"[SENT] DM delivered to u/{username}")
        else:
            log(f"[SEND FAILED] Could not deliver to u/{username}, saving as pending for retry")
        # ─────────────────────────────────────────────────────────────────────

        new_leads.append(lead)
        log(f"Processed lead: u/{username} (score: {score}, status: {lead['status']})")

        # Small delay between sends so we don't hammer Reddit
        time.sleep(random.randint(5, 15))

    # Save results
    save_seen_posts(seen)
    if new_leads:
        all_leads = existing_leads + new_leads
        save_leads(all_leads)
        log(f"Saved {len(new_leads)} new leads ({sum(1 for l in new_leads if l['status']=='sent')} sent)")

    return new_leads


def check_first_run():
    """Check if this is first run and setup if needed"""
    if not FIRST_RUN_FILE.exists():
        log("=" * 50)
        log("FIRST RUN DETECTED")
        log("=" * 50)
        log("Opening browser for Reddit login...")
        log("Please log into Reddit in the browser window.")
        log("After logging in, close the browser and press ENTER to continue.")
        
        # Open browser for login
        try:
            from playwright.sync_api import sync_playwright
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=False)
                context = browser.new_context()
                page = context.new_page()
                page.goto("https://www.reddit.com/login")
                
                log("Browser opened. Please log in manually.")
                input("Press ENTER after you've logged in and closed the browser...")
                
                # Save cookies
                cookies = context.cookies()
                REDDIT_COOKIES_FILE.parent.mkdir(parents=True, exist_ok=True)
                with open(REDDIT_COOKIES_FILE, "w") as f:
                    json.dump(cookies, f)
                
                browser.close()
                log("Cookies saved successfully!")
        except ImportError:
            log("Playwright not installed. Installing...")
            os.system(f"{sys.executable} -m pip install playwright")
            os.system(f"{sys.executable} -m playwright install chromium")
            log("Please restart the agent after installation.")
            sys.exit(1)
        except Exception as e:
            log(f"Error during first run: {e}")
            return False
        
        # Mark first run complete
        FIRST_RUN_FILE.touch()
        log("First run setup complete!")
        return True
    return True


def send_chat_request_browser(lead: Dict) -> bool:
    """Send a Reddit chat request via browser for users who have DMs disabled."""
    username = lead.get("username")
    dm_text = lead.get("dm_text", "")
    if not username or not dm_text:
        return False

    log(f"[CHAT REQUEST] Opening browser to send chat request to u/{username}...")
    chat_url = f"https://www.reddit.com/user/{username}"

    try:
        from playwright.sync_api import sync_playwright

        REDDIT_PROFILE_DIR = Path("chrome_reddit_profile")
        REDDIT_PROFILE_DIR.mkdir(exist_ok=True)

        with sync_playwright() as p:
            context = p.chromium.launch_persistent_context(
                str(REDDIT_PROFILE_DIR),
                headless=False,
                channel="chrome",
                args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/125.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1366, "height": 768},
            )

            if REDDIT_COOKIES_FILE.exists():
                with open(REDDIT_COOKIES_FILE, "r") as f:
                    context.add_cookies(json.load(f))

            page = context.new_page()

            # Go to the user's profile page
            log(f"[CHAT REQUEST] Navigating to u/{username} profile...")
            page.goto(chat_url, timeout=30000, wait_until="domcontentloaded")
            page.wait_for_timeout(3000)

            if "login" in page.url:
                log("[CHAT REQUEST] Not logged in.")
                context.close()
                return False

            # Click the Chat button on their profile
            chat_btn_selectors = [
                "button:has-text('Chat')",
                "a:has-text('Chat')",
                "[data-testid='chat-button']",
                "button[aria-label*='chat' i]",
                "button[aria-label*='Chat']",
            ]
            chat_btn = None
            for sel in chat_btn_selectors:
                try:
                    loc = page.locator(sel).first
                    if loc.count() > 0 and loc.is_visible():
                        chat_btn = loc
                        log(f"[CHAT REQUEST] Found chat button: {sel}")
                        break
                except:
                    continue

            if not chat_btn:
                log(f"[CHAT REQUEST] Could not find Chat button on u/{username} profile. Skipping.")
                context.close()
                record_dm_sent()  # mark as done so we don't retry forever
                return True

            chat_btn.click()
            page.wait_for_timeout(3000)

            # Type the message into the chat input
            chat_input_selectors = [
                "[data-testid='chat-input']",
                "div[contenteditable='true']",
                "textarea[placeholder*='message' i]",
                "div[role='textbox']",
            ]
            chat_input = None
            for sel in chat_input_selectors:
                try:
                    loc = page.locator(sel).first
                    if loc.count() > 0 and loc.is_visible():
                        chat_input = loc
                        log(f"[CHAT REQUEST] Found chat input: {sel}")
                        break
                except:
                    continue

            if not chat_input:
                log(f"[CHAT REQUEST] Could not find chat input for u/{username}.")
                context.close()
                return False

            # Type message with human-like pacing
            chat_input.click()
            page.wait_for_timeout(500)
            chat_input.type(dm_text[:1000], delay=20)
            page.wait_for_timeout(1000)

            # Hit Enter or find send button
            try:
                page.keyboard.press("Enter")
            except:
                pass
            page.wait_for_timeout(3000)

            log(f"[SUCCESS] Chat request sent to u/{username}")
            record_dm_sent()
            # Save updated cookies
            cookies = context.cookies()
            with open(REDDIT_COOKIES_FILE, "w") as f:
                json.dump(cookies, f)
            context.close()
            return True

    except Exception as e:
        log(f"[CHAT REQUEST ERROR] u/{username}: {e}")
        import traceback
        log(traceback.format_exc())
        return False


def send_dm_with_puppeteer(lead: Dict, bypass_rate_limit: bool = False) -> bool:
    """Send DM using Playwright with correct button selector and headless mode"""
    if not bypass_rate_limit and not can_send_dm():
        log("Rate limit: Cannot send DM yet")
        return False
    
    username = lead.get("username")
    dm_url = lead.get("dm_url")
    dm_text = lead.get("dm_text", "")
    
    if not username or not dm_url:
        log("Invalid lead data")
        return False
    
    # Log the DM we're about to send
    log(f"=" * 50)
    log(f"[SENDING DM to u/{username}]")
    log(f"[DM SUBJECT]: {lead.get('dm_subject', 'Quick question')}")
    log(f"[DM CONTENT]:")
    for line in dm_text.split('\n'):
        log(f"  {line}")
    log(f"=" * 50)
    log(f"Sending DM via Reddit OAuth API (no browser)...")

    try:
        # ── Reddit OAuth API send - no browser, no bot detection ────────────
        cookies_data = {}
        if REDDIT_COOKIES_FILE.exists():
            with open(REDDIT_COOKIES_FILE, "r") as f:
                raw_cookies = json.load(f)
            for c in raw_cookies:
                cookies_data[c["name"]] = c["value"]

        token_v2 = cookies_data.get("token_v2", "")
        if not token_v2:
            log("[API SEND] No token_v2 cookie found.")
            log("[API SEND] >> Run: python agent.py  then pick option 6 to re-login <<")
            return False

        # Quick pre-check: verify token is still valid before trying to send
        _check_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "Authorization": f"Bearer {token_v2}",
        }
        _check_session = requests.Session()
        _check_session.cookies.update(cookies_data)
        _pre = _check_session.get("https://oauth.reddit.com/api/v1/me", headers=_check_headers, timeout=10)
        if _pre.status_code != 200:
            log(f"[API SEND] Token expired or invalid (status {_pre.status_code}).")
            log("[API SEND] >> Run: python agent.py  then pick option 6 to re-login <<")
            return False
        log(f"[API SEND] Token valid, logged in as u/{_pre.json().get('name', '?')}")

        session = requests.Session()
        for name, value in cookies_data.items():
            session.cookies.set(name, value, domain=".reddit.com")

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/125.0.0.0 Safari/537.36"
            ),
            "Authorization": f"Bearer {token_v2}",
            "Origin": "https://www.reddit.com",
            "Referer": "https://www.reddit.com/message/compose/",
        }

        # Fetch modhash via OAuth endpoint
        me_resp = session.get("https://oauth.reddit.com/api/me.json", headers=headers, timeout=15)
        if me_resp.status_code != 200:
            log(f"[API SEND] Auth failed ({me_resp.status_code}). Token may be expired. Run login setup.")
            return False

        me_data = me_resp.json()
        modhash = me_data.get("data", {}).get("modhash") or cookies_data.get("csrf_token", "")
        logged_in_user = me_data.get("data", {}).get("name", "unknown")
        log(f"[API SEND] Authenticated as u/{logged_in_user}")

        subject = lead.get("dm_subject", "Quick question")
        dm_text_content = lead.get("dm_text", "")

        payload = {
            "api_type": "json",
            "to": username,
            "subject": subject[:100],
            "text": dm_text_content[:10000],
            "uh": modhash,
        }

        send_headers = dict(headers)
        send_headers["Content-Type"] = "application/x-www-form-urlencoded"

        log(f"[API SEND] POSTing message to u/{username}...")
        resp = session.post(
            "https://oauth.reddit.com/api/compose",
            data=payload,
            headers=send_headers,
            timeout=30,
        )

        log(f"[API SEND] Response status: {resp.status_code}")

        if resp.status_code == 200:
            try:
                resp_json = resp.json()
                errors = resp_json.get("json", {}).get("errors", [])
                if errors:
                    error_codes = [e[0] if isinstance(e, list) else str(e) for e in errors]
                    log(f"[API SEND] Reddit errors: {errors}")
                    if any("USER_REQUIRED" in c for c in error_codes):
                        log("[API SEND] Not logged in - token expired. Run login setup.")
                    elif any("RATELIMIT" in c for c in error_codes):
                        log("[API SEND] Reddit rate limit hit. Will retry next cycle.")
                    elif any("RESTRICTED_TO_PM" in c for c in error_codes):
                        log(f"[API SEND] u/{username} has DMs disabled - trying chat request via browser...")
                        result = send_chat_request_browser(lead)
                        return result
                    return False
                else:
                    log(f"[SUCCESS] DM SENT to u/{username} via Reddit API")
                    record_dm_sent()
                    return True
            except Exception as parse_err:
                log(f"[API SEND] Could not parse response: {parse_err}")
                log(f"[API SEND] Assuming success (200 OK)")
                record_dm_sent()
                return True
        elif resp.status_code == 429:
            log(f"[API SEND] Rate limited (429). Will retry next cycle.")
            return False
        elif resp.status_code in (401, 403):
            log(f"[API SEND] Auth failed ({resp.status_code}). Token expired. Run login setup.")
            return False
        else:
            log(f"[API SEND] Unexpected status {resp.status_code}: {resp.text[:200]}")
            return False

    except Exception as e:
        log(f"[ERROR] Sending DM to u/{username}: {e}")
        import traceback
        log(f"Traceback: {traceback.format_exc()}")
        return False


def process_pending_dms():
    """Send pending DMs respecting rate limits - prevents duplicate sends to same user"""
    log("=" * 50)
    log("Processing pending DMs...")
    
    # Load permanent sent users record (persists across lead file resets)
    sent_users = load_sent_users()
    log(f"[DEDUPE CHECK] Permanent sent users record: {len(sent_users)} unique users already sent to")
    
    leads = load_leads()
    
    # Also check leads file for any sent users not yet in permanent record (backwards compatibility)
    for lead in leads:
        if lead.get("status") == "sent" and lead.get("username"):
            if lead.get("username") not in sent_users:
                sent_users.add(lead.get("username"))
                save_sent_user(lead.get("username"))
    
    # Filter pending - exclude users we've already sent to
    pending = []
    skipped_duplicates = 0
    for lead in leads:
        if lead.get("status") == "pending":
            username = lead.get("username")
            if username in sent_users:
                log(f"[DEDUPE] Skipping u/{username} - already sent DM to this user (in permanent record)")
                skipped_duplicates += 1
                # Also mark this lead as sent so it doesn't show as pending
                lead["status"] = "skipped_duplicate"
                lead["skipped_at"] = datetime.now().isoformat()
            else:
                pending.append(lead)
    
    if skipped_duplicates > 0:
        log(f"[DEDUPE] Skipped {skipped_duplicates} duplicate leads (already sent to these users)")
        save_leads(leads)  # Save the skipped status updates
    
    if not pending:
        log("No pending DMs to send")
        return
    
    log(f"Found {len(pending)} pending DMs to process")
    log(f"[RATE LIMIT OFF] Continuous sending mode - no hourly cap")
    
    sent_count = 0
    for lead in pending:
        # Rate limit disabled - continuous sending
        pass
        
        username = lead.get("username")
        
        # Double-check we haven't sent to this user (in case of race conditions)
        if username in sent_users:
            log(f"[DEDUPE] Skipping u/{username} - already in sent users (double-check)")
            continue
        
        log(f"\n[QUEUE] Processing DM {sent_count+1}/min({len(pending)},{MAX_DMS_PER_HOUR}) for u/{username}")
        
        if send_dm_with_puppeteer(lead):
            lead["status"] = "sent"
            lead["sent_at"] = datetime.now().isoformat()
            sent_count += 1
            # Save to permanent record immediately
            save_sent_user(username)
            sent_users.add(username)
            save_leads(leads)
            log(f"[PROGRESS] Successfully sent {sent_count}/{MAX_DMS_PER_HOUR} DMs this hour")
            log(f"[SENT_USERS] u/{username} permanently recorded - will never send to this user again")
        else:
            log(f"[FAILED] Could not send DM to u/{username}, will retry later")
        
        # Minimal delay between sends to avoid looking bot-like
        if sent_count < len(pending):
            delay = random.randint(5, 15)
            log(f"[COOLDOWN] Waiting {delay}s before next send...")
            time.sleep(delay)
    
    log(f"=" * 50)
    log(f"[SUMMARY] Sent {sent_count} DMs this cycle")
    log(f"[SUMMARY] {len(pending) - sent_count} leads remain pending")
    log(f"[SENT_USERS] Total unique users in permanent record: {len(sent_users)}")


def show_stats():
    """Show current statistics"""
    leads = load_leads()
    config = load_config()
    sent_users = load_sent_users()
    
    pending = len([l for l in leads if l.get("status") == "pending"])
    sent = len([l for l in leads if l.get("status") == "sent"])
    
    print("\n" + "=" * 50)
    print("AGENT STATISTICS")
    print("=" * 50)
    print(f"Total leads found: {len(leads)}")
    print(f"Pending DMs: {pending}")
    print(f"Sent DMs: {sent}")
    print(f"Unique users sent to (permanent record): {len(sent_users)}")
    print(f"Total DMs sent (all time): {config.get('total_dms_sent', 0)}")
    print(f"DMs this hour: {config.get('dm_count_last_hour', 0)} (unlimited mode)")
    if config.get("last_dm_time"):
        last = datetime.fromisoformat(config["last_dm_time"])
        ago = int((datetime.now() - last).total_seconds() / 60)
        print(f"Last DM: {ago} minutes ago")
    print("=" * 50)


def show_menu():
    """Show CLI menu"""
    print("\n" + "=" * 50)
    print("ARLO LEAD GENERATION AGENT")
    print("=" * 50)
    print("1. Scan for new leads now")
    print("2. Send pending DMs")
    print("3. Show statistics")
    print("4. View recent leads")
    print("5. Start autonomous mode (scan every 4h, unlimited sends)")
    print("6. Reset first run (force login)")
    print("7. Exit")
    print("8. Send DM to specific post URL (paste Reddit URL)")
    print("=" * 50)
    return input("Select option: ").strip()


def process_single_post_url():
    """Process a single Reddit post URL: fetch post, quality check, draft DM, send"""
    print("\n" + "=" * 50)
    print("SEND DM TO SPECIFIC POST")
    print("=" * 50)
    
    url = input("Paste Reddit post URL: ").strip()
    if not url:
        print("No URL entered. Cancelled.")
        return
    
    # Extract post ID from URL
    # URL format: https://www.reddit.com/r/SUBREDDIT/comments/POSTID/...
    match = re.search(r'/comments/([a-z0-9]+)/', url)
    if not match:
        print("Invalid Reddit URL. Could not extract post ID.")
        return
    
    post_id = match.group(1)
    print(f"Extracted post ID: {post_id}")
    
    # Fetch the post via Reddit JSON API
    try:
        print("Fetching post data...")
        headers = {"User-Agent": "Mozilla/5.0 (compatible; LeadBot/1.0)"}
        
        # Try to get from Reddit API
        reddit_url = f"https://www.reddit.com/comments/{post_id}.json"
        resp = requests.get(reddit_url, headers=headers, timeout=30)
        
        if resp.status_code != 200:
            print(f"Failed to fetch post: HTTP {resp.status_code}")
            return
        
        data = resp.json()
        if not data or len(data) < 1:
            print("Invalid post data received")
            return
        
        # Extract post from response
        post_data = data[0]["data"]["children"][0]["data"]
        post = {"data": post_data}
        
        author = post_data.get("author")
        title = post_data.get("title", "")
        subreddit = post_data.get("subreddit", "")
        
        print(f"Found post: {title[:60]}...")
        print(f"Author: u/{author}")
        print(f"Subreddit: r/{subreddit}")
        
        # Check if we've already sent to this user
        sent_users = load_sent_users()
        if author in sent_users:
            print(f"[DEDUPE] Already sent DM to u/{author} - cannot send again")
            return
        
        # Categorize
        category = categorize_post(post)
        print(f"Detected category: {category}")
        
        # Quality check
        print("Running quality check...")
        if not ai_quality_check(post, category):
            print("[QUALITY CHECK] AI determined this person doesn't need the product. Skipping.")
            return
        
        # Draft DM
        print("Drafting personalized DM...")
        dm_text = draft_dm_with_mimo(post, category)
        
        if not dm_text:
            print("Failed to draft DM")
            return
        
        print("\n" + "=" * 50)
        print("DRAFTED DM:")
        print("=" * 50)
        print(dm_text)
        print("=" * 50)
        print(f"Word count: {len(dm_text.split())} words")
        
        # Confirm before sending
        confirm = input("\nSend this DM? (yes/no): ").strip().lower()
        if confirm not in ["yes", "y"]:
            print("Cancelled.")
            return
        
        # Build lead and send
        subject = {
            "ecommerce": "Quick question about your store",
            "saas": "Thought this might help",
            "founder": "Saw your post and wanted to reach out"
        }.get(category, "Quick question")
        
        lead = {
            "id": f"manual_{int(time.time())}",
            "post_id": post_id,
            "username": author,
            "subreddit": subreddit,
            "post_title": title,
            "post_url": url,
            "category": category,
            "dm_text": dm_text,
            "dm_subject": subject,
            "dm_url": build_dm_url(author, subject, dm_text),
            "score": 10,
            "status": "pending",
            "scraped_at": datetime.now().isoformat()
        }
        
        # Send DM (bypass rate limit for manual mode)
        print("Sending DM (bypassing rate limit for manual mode)...")
        if send_dm_with_puppeteer(lead, bypass_rate_limit=True):
            lead["status"] = "sent"
            lead["sent_at"] = datetime.now().isoformat()
            save_leads(load_leads() + [lead])
            # Save to permanent record
            save_sent_user(author)
            print("[SUCCESS] DM sent successfully!")
            print(f"[SENT_USERS] u/{author} permanently recorded - will never send to this user again")
            # Note: We skip record_dm_sent() to avoid affecting autonomous rate limits
        else:
            print("[FAILED] Could not send DM")
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


def autonomous_mode():
    """Run autonomous mode - scans every 8 hours, sends DMs, then sleeps."""
    log("=" * 50)
    log("AUTONOMOUS MODE STARTED")
    log(f"Scan interval: {SCAN_INTERVAL_HOURS} hours")
    log(f"Rate limiting: DISABLED - unlimited DMs per hour")
    log("Press Ctrl+C to stop")
    log("=" * 50)

    try:
        while True:
            log("Starting scheduled scan...")
            new_leads = scan_for_leads()
            log(f"Scan complete. Found {len(new_leads)} new leads.")

            process_pending_dms()

            next_scan = datetime.now() + timedelta(hours=SCAN_INTERVAL_HOURS)
            log(f"Next scan at {next_scan.strftime('%Y-%m-%d %H:%M:%S')}")

            seconds_to_wait = SCAN_INTERVAL_HOURS * 3600
            while seconds_to_wait > 0:
                time.sleep(min(60, seconds_to_wait))
                seconds_to_wait -= 60

                if os.path.exists(".stop_agent"):
                    log("Stop signal detected. Exiting.")
                    os.remove(".stop_agent")
                    return

    except KeyboardInterrupt:
        log("Autonomous mode stopped by user")


def view_recent_leads():
    """Display recent leads"""
    leads = load_leads()
    recent = sorted(leads, key=lambda x: x.get("scraped_at", ""), reverse=True)[:10]
    
    print("\n" + "=" * 50)
    print("RECENT LEADS")
    print("=" * 50)
    for lead in recent:
        status = lead.get("status", "unknown")
        score = lead.get("score", 0)
        title = lead.get("post_title", "")[:50]
        print(f"[{status.upper()}] Score: {score} | {title}...")
        print(f"   User: u/{lead.get('username')} | r/{lead.get('subreddit')}")
        print()


# Global headless mode flag
HEADLESS = "--headless" in sys.argv or "-h" in sys.argv

def main():
    """Main entry point"""
    global HEADLESS
    # Check for --auto flag
    auto_mode = "--auto" in sys.argv or "-a" in sys.argv
    headless = "--headless" in sys.argv or "-h" in sys.argv
    if headless:
        HEADLESS = True
        log("Headless mode enabled")
    
    # Setup
    REDDIT_COOKIES_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    # Check first run
    if not check_first_run():
        print("Setup failed. Please try again.")
        return
    
    if auto_mode:
        log("Auto mode enabled - starting autonomous mode immediately")
        autonomous_mode()
        return
    
    # Main loop
    while True:
        choice = show_menu()
        
        if choice == "1":
            scan_for_leads()
        elif choice == "2":
            process_pending_dms()
        elif choice == "3":
            show_stats()
        elif choice == "4":
            view_recent_leads()
        elif choice == "5":
            autonomous_mode()
        elif choice == "6":
            if FIRST_RUN_FILE.exists():
                FIRST_RUN_FILE.unlink()
            print("First run flag reset. Restart agent to re-login.")
        elif choice == "7":
            print("Goodbye!")
            break
        elif choice == "8":
            process_single_post_url()
        else:
            print("Invalid option")


if __name__ == "__main__":
    main()
