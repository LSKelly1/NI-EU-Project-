"""
NI/EU Law Tracker - EUR-Lex Scraper
Fetches EU legislation and matches against Windsor Framework Annex 2 categories
"""

import os
import re
import json
import requests
from datetime import datetime, timedelta
from typing import Optional

# ============================================
# CONFIGURATION
# ============================================
SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://emvqzjeohuslrplmzddq.supabase.co')
SUPABASE_KEY = os.environ.get('SUPABASE_SERVICE_KEY', '')  # Use service key for write access

# EUR-Lex SPARQL endpoint
EURLEX_SPARQL_ENDPOINT = 'https://publications.europa.eu/webapi/rdf/sparql'

# ============================================
# ANNEX 2 CATEGORIES WITH KEYWORDS
# ============================================
ANNEX2_CATEGORIES = [
    {"number": 1, "name": "General customs aspects", "relevance": "low", "keywords": ["customs", "customs code", "mutual assistance", "recovery of claims"]},
    {"number": 2, "name": "Protection of the Union's financial interests", "relevance": "low", "keywords": ["anti-fraud", "OLAF", "financial interests"]},
    {"number": 3, "name": "Trade statistics", "relevance": "low", "keywords": ["trade statistics", "trading of goods", "external trade"]},
    {"number": 4, "name": "General trade related aspects", "relevance": "low", "keywords": ["tariff preferences", "exports", "imports", "textile", "conflict minerals"]},
    {"number": 5, "name": "Trade defence instruments", "relevance": "low", "keywords": ["anti-dumping", "anti-subsidy", "safeguard", "subsidised imports"]},
    {"number": 6, "name": "Regulations on bilateral safeguards", "relevance": "low", "keywords": ["bilateral safeguards", "stabilisation", "association agreement"]},
    {"number": 7, "name": "Others", "relevance": "medium", "keywords": ["compulsory licensing", "patents", "pharmaceutical products", "public health"]},
    {"number": 8, "name": "Goods - general provisions", "relevance": "high", "keywords": ["technical regulations", "standardisation", "market surveillance", "product safety", "CE marking", "general product safety"]},
    {"number": 9, "name": "Motor vehicles", "relevance": "high", "keywords": ["motor vehicles", "type-approval", "vehicle safety", "emissions", "Euro 5", "Euro 6", "tractors", "agricultural vehicles"]},
    {"number": 10, "name": "Lifting and mechanical handling appliances", "relevance": "medium", "keywords": ["lifts", "wire-ropes", "chains", "hooks", "lifting equipment"]},
    {"number": 11, "name": "Gas appliances", "relevance": "high", "keywords": ["gas appliances", "boilers", "hot-water boilers", "gaseous fuels"]},
    {"number": 12, "name": "Pressure vessels", "relevance": "medium", "keywords": ["pressure vessels", "aerosol", "transportable pressure equipment"]},
    {"number": 13, "name": "Measuring instruments", "relevance": "high", "keywords": ["measuring instruments", "metrological", "weighing", "prepackaged products"]},
    {"number": 14, "name": "Construction products, machinery, cableways, PPE", "relevance": "high", "keywords": ["construction products", "machinery", "cableways", "personal protective equipment", "PPE"]},
    {"number": 15, "name": "Electrical and radio equipment", "relevance": "high", "keywords": ["electrical equipment", "radio equipment", "electromagnetic compatibility", "voltage", "low voltage"]},
    {"number": 16, "name": "Textiles, footwear", "relevance": "high", "keywords": ["textiles", "footwear", "fibre composition", "labelling"]},
    {"number": 17, "name": "Cosmetics, toys", "relevance": "high", "keywords": ["cosmetics", "toys", "toy safety", "cosmetic products"]},
    {"number": 18, "name": "Recreational craft", "relevance": "medium", "keywords": ["recreational craft", "personal watercraft", "boats"]},
    {"number": 19, "name": "Explosives and pyrotechnic articles", "relevance": "medium", "keywords": ["explosives", "pyrotechnic", "fireworks"]},
    {"number": 20, "name": "Medicinal products", "relevance": "high", "keywords": ["medicinal products", "medicines", "pharmaceuticals", "veterinary medicinal", "clinical trials", "pharmacovigilance"]},
    {"number": 21, "name": "Medical devices", "relevance": "high", "keywords": ["medical devices", "in vitro diagnostic", "implantable"]},
    {"number": 22, "name": "Substances of human origin", "relevance": "high", "keywords": ["blood", "tissues", "cells", "organs", "transplantation"]},
    {"number": 23, "name": "Chemicals and related", "relevance": "high", "keywords": ["chemicals", "REACH", "fertilisers", "detergents", "batteries", "hazardous substances", "chemical substances"]},
    {"number": 24, "name": "Pesticides, biocides", "relevance": "high", "keywords": ["pesticides", "biocides", "plant protection products", "maximum residue levels", "MRL"]},
    {"number": 25, "name": "Waste", "relevance": "medium", "keywords": ["waste", "shipments of waste", "packaging waste", "ship recycling", "waste management"]},
    {"number": 26, "name": "Environment, energy efficiency", "relevance": "high", "keywords": ["environment", "energy efficiency", "invasive species", "ecolabel", "fluorinated gases", "energy labelling", "F-gases"]},
    {"number": 27, "name": "Marine equipment", "relevance": "low", "keywords": ["marine equipment", "ship equipment"]},
    {"number": 28, "name": "Rail transport", "relevance": "low", "keywords": ["rail", "railway", "interoperability"]},
    {"number": 29, "name": "Food - general", "relevance": "high", "keywords": ["food law", "food safety", "EFSA", "food information", "nutrition claims", "health claims"]},
    {"number": 30, "name": "Food - hygiene", "relevance": "high", "keywords": ["food hygiene", "hygiene of foodstuffs", "food of animal origin"]},
    {"number": 31, "name": "Food - ingredients, traces, residues", "relevance": "high", "keywords": ["food additives", "flavourings", "contaminants", "novel foods", "infant food", "food supplements"]},
    {"number": 32, "name": "Food contact material", "relevance": "high", "keywords": ["food contact", "food contact material", "materials intended to come into contact with food"]},
    {"number": 33, "name": "Food - other", "relevance": "high", "keywords": ["ionising radiation", "organic production", "organic products", "mineral waters"]},
    {"number": 34, "name": "Feed - products and hygiene", "relevance": "medium", "keywords": ["animal feed", "feed", "feed additives", "medicated feedingstuffs"]},
    {"number": 35, "name": "GMOs", "relevance": "high", "keywords": ["GMO", "genetically modified", "GM food", "GM feed", "traceability"]},
    {"number": 36, "name": "Live animals, germinal products", "relevance": "medium", "keywords": ["live animals", "animal health", "bovine", "swine", "poultry", "semen", "embryos"]},
    {"number": 37, "name": "Animal disease control", "relevance": "medium", "keywords": ["animal disease", "zoonosis", "TSE", "BSE", "avian influenza", "swine fever"]},
    {"number": 38, "name": "Animal identification", "relevance": "medium", "keywords": ["animal identification", "registration", "traceability", "beef labelling"]},
    {"number": 39, "name": "Animal breeding", "relevance": "low", "keywords": ["animal breeding", "zootechnical", "breeding animals"]},
    {"number": 40, "name": "Animal welfare", "relevance": "high", "keywords": ["animal welfare", "protection of animals", "transport of animals", "slaughter"]},
    {"number": 41, "name": "Plant health", "relevance": "medium", "keywords": ["plant health", "pests of plants", "harmful organisms", "phytosanitary"]},
    {"number": 42, "name": "Plant reproductive material", "relevance": "low", "keywords": ["seed", "cereal seed", "vegetable seed", "forest reproductive material"]},
    {"number": 43, "name": "Official controls, veterinary checks", "relevance": "medium", "keywords": ["official controls", "veterinary checks", "border inspection"]},
    {"number": 44, "name": "Sanitary and phytosanitary - Other", "relevance": "high", "keywords": ["hormones", "beta-agonists", "residue monitoring"]},
    {"number": 45, "name": "Intellectual property", "relevance": "high", "keywords": ["geographical indications", "PDO", "PGI", "spirit drinks", "wine"]},
    {"number": 46, "name": "Fisheries and aquaculture", "relevance": "medium", "keywords": ["fisheries", "aquaculture", "fish", "IUU fishing", "bluefin tuna"]},
    {"number": 47, "name": "Other", "relevance": "medium", "keywords": ["crude oil", "euro coins", "tobacco", "cultural goods", "dual-use items", "weapons", "firearms"]}
]


def fetch_recent_legislation(days_back: int = 30) -> list:
    """
    Fetch recent EU legislation from EUR-Lex using their REST API
    """
    print(f"Fetching legislation from the last {days_back} days...")
    
    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    
    # EUR-Lex search API
    # We'll search for Regulations and Directives published recently
    base_url = "https://eur-lex.europa.eu/search.html"
    
    legislation = []
    
    # Use the EUR-Lex web search API
    search_url = "https://eur-lex.europa.eu/search.html"
    
    # Alternative: Use the CELLAR SPARQL endpoint
    sparql_query = f"""
    PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    
    SELECT DISTINCT ?work ?celex ?title ?date ?type WHERE {{
        ?work cdm:work_has_resource-type ?type .
        ?work cdm:resource_legal_id_celex ?celex .
        ?work cdm:work_date_document ?date .
        ?work cdm:work_has_expression ?expr .
        ?expr cdm:expression_uses_language <http://publications.europa.eu/resource/authority/language/ENG> .
        ?expr cdm:expression_title ?title .
        
        FILTER (?type IN (
            <http://publications.europa.eu/resource/authority/resource-type/REG>,
            <http://publications.europa.eu/resource/authority/resource-type/DIR>,
            <http://publications.europa.eu/resource/authority/resource-type/DEC>
        ))
        
        FILTER (?date >= "{start_date.strftime('%Y-%m-%d')}"^^xsd:date)
    }}
    ORDER BY DESC(?date)
    LIMIT 200
    """
    
    try:
        response = requests.post(
            EURLEX_SPARQL_ENDPOINT,
            data={'query': sparql_query},
            headers={
                'Accept': 'application/sparql-results+json',
                'Content-Type': 'application/x-www-form-urlencoded'
            },
            timeout=60
        )
        
        if response.status_code == 200:
            results = response.json()
            
            for binding in results.get('results', {}).get('bindings', []):
                celex = binding.get('celex', {}).get('value', '')
                title = binding.get('title', {}).get('value', '')
                date = binding.get('date', {}).get('value', '')
                type_uri = binding.get('type', {}).get('value', '')
                
                # Determine legislation type
                if 'REG' in type_uri:
                    leg_type = 'Regulation'
                elif 'DIR' in type_uri:
                    leg_type = 'Directive'
                elif 'DEC' in type_uri:
                    leg_type = 'Decision'
                else:
                    leg_type = 'Other'
                
                legislation.append({
                    'celex_number': celex,
                    'title': title,
                    'date_published': date[:10] if date else None,
                    'legislation_type': leg_type,
                    'eurlex_url': f"https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:{celex}"
                })
            
            print(f"Found {len(legislation)} items from SPARQL endpoint")
        else:
            print(f"SPARQL query failed with status {response.status_code}")
            print(response.text[:500])
            
    except Exception as e:
        print(f"Error fetching from SPARQL: {e}")
    
    # If SPARQL didn't work well, try alternative approach with REST API
    if len(legislation) < 10:
        print("Trying alternative EUR-Lex REST API...")
        legislation.extend(fetch_from_eurlex_rest(start_date, end_date))
    
    return legislation


def fetch_from_eurlex_rest(start_date: datetime, end_date: datetime) -> list:
    """
    Alternative method using EUR-Lex REST search
    """
    legislation = []
    
    # EUR-Lex offers an RSS feed for recent legislation
    rss_url = "https://eur-lex.europa.eu/EN/display-feed.html?rssId=legislation"
    
    try:
        response = requests.get(rss_url, timeout=30)
        if response.status_code == 200:
            # Parse RSS (simple regex extraction)
            import re
            
            # Find all items
            items = re.findall(r'<item>(.*?)</item>', response.text, re.DOTALL)
            
            for item in items[:100]:  # Limit to 100 items
                title_match = re.search(r'<title>(.*?)</title>', item)
                link_match = re.search(r'<link>(.*?)</link>', item)
                date_match = re.search(r'<pubDate>(.*?)</pubDate>', item)
                
                if title_match and link_match:
                    title = title_match.group(1).replace('<![CDATA[', '').replace(']]>', '').strip()
                    link = link_match.group(1).strip()
                    
                    # Extract CELEX from link
                    celex_match = re.search(r'CELEX[:%](\d+[A-Z]\d+)', link)
                    celex = celex_match.group(1) if celex_match else None
                    
                    if not celex:
                        celex_match = re.search(r'uri=CELEX:(\d+[A-Z]\d+)', link)
                        celex = celex_match.group(1) if celex_match else ''
                    
                    # Determine type from CELEX or title
                    leg_type = 'Other'
                    if celex:
                        if 'R' in celex[4:5]:
                            leg_type = 'Regulation'
                        elif 'L' in celex[4:5]:
                            leg_type = 'Directive'
                        elif 'D' in celex[4:5]:
                            leg_type = 'Decision'
                    elif 'Regulation' in title:
                        leg_type = 'Regulation'
                    elif 'Directive' in title:
                        leg_type = 'Directive'
                    
                    # Parse date
                    pub_date = None
                    if date_match:
                        try:
                            from email.utils import parsedate_to_datetime
                            pub_date = parsedate_to_datetime(date_match.group(1)).strftime('%Y-%m-%d')
                        except:
                            pass
                    
                    if celex:
                        legislation.append({
                            'celex_number': celex,
                            'title': title,
                            'date_published': pub_date,
                            'legislation_type': leg_type,
                            'eurlex_url': f"https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:{celex}"
                        })
            
            print(f"Found {len(legislation)} items from RSS feed")
            
    except Exception as e:
        print(f"Error fetching from RSS: {e}")
    
    return legislation


def match_to_category(title: str) -> tuple[Optional[int], bool, list]:
    """
    Match legislation title to Annex 2 category based on keywords
    Returns: (category_number, is_direct_match, matched_keywords)
    """
    title_lower = title.lower()
    
    best_match = None
    best_score = 0
    matched_keywords = []
    
    for category in ANNEX2_CATEGORIES:
        score = 0
        keywords_found = []
        
        for keyword in category['keywords']:
            if keyword.lower() in title_lower:
                score += 1
                keywords_found.append(keyword)
        
        if score > best_score:
            best_score = score
            best_match = category['number']
            matched_keywords = keywords_found
    
    # Consider it a direct match if 2+ keywords match, otherwise keyword match
    is_direct_match = best_score >= 2
    
    return best_match, is_direct_match, matched_keywords


def calculate_score(item: dict) -> tuple[int, str]:
    """
    Calculate priority score based on our scoring rubric
    Returns: (score, priority_level)
    """
    score = 0
    
    # Category match
    if item.get('is_direct_annex2_match'):
        score += 10
    elif item.get('is_keyword_match'):
        score += 5
    
    # Consumer relevance
    category_num = item.get('category_number')
    if category_num:
        category = next((c for c in ANNEX2_CATEGORIES if c['number'] == category_num), None)
        if category:
            if category['relevance'] == 'high':
                score += 3
            elif category['relevance'] == 'medium':
                score += 1
    
    # Consultation (we'll add this later when we scrape consultations)
    consultation_days = item.get('consultation_days_remaining')
    if consultation_days is not None:
        if consultation_days < 14:
            score += 5
        elif consultation_days < 30:
            score += 3
        else:
            score += 2
    
    # DSC status (we'll add this later)
    dsc_status = item.get('dsc_status')
    if dsc_status in ['proposed_new', 'proposed_replacement']:
        score += 4
    elif dsc_status == 'published':
        score += 2
    
    # Legislation type
    leg_type = item.get('legislation_type')
    if leg_type == 'Regulation':
        score += 2
    elif leg_type in ['Directive', 'Decision']:
        score += 1
    
    # Determine priority level
    if score >= 18:
        priority = 'critical'
    elif score >= 12:
        priority = 'high'
    elif score >= 6:
        priority = 'medium'
    else:
        priority = 'low'
    
    return score, priority


def save_to_supabase(legislation: list) -> dict:
    """
    Save legislation to Supabase database
    """
    if not SUPABASE_KEY:
        print("ERROR: SUPABASE_SERVICE_KEY not set")
        return {'inserted': 0, 'updated': 0, 'errors': ['No API key']}
    
    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'resolution=merge-duplicates'  # Upsert
    }
    
    inserted = 0
    updated = 0
    errors = []
    
    for item in legislation:
        # Prepare data for insert
        data = {
            'celex_number': item['celex_number'],
            'title': item['title'],
            'legislation_type': item.get('legislation_type'),
            'category_number': item.get('category_number'),
            'is_direct_annex2_match': item.get('is_direct_annex2_match', False),
            'is_keyword_match': item.get('is_keyword_match', False),
            'matched_keywords': item.get('matched_keywords', []),
            'date_published': item.get('date_published'),
            'eurlex_url': item.get('eurlex_url'),
            'status': 'active',
            'date_scraped': datetime.now().isoformat()
        }
        
        try:
            # Try to insert/upsert
            response = requests.post(
                f"{SUPABASE_URL}/rest/v1/legislation",
                headers=headers,
                json=data,
                timeout=30
            )
            
            if response.status_code in [200, 201]:
                inserted += 1
            elif response.status_code == 409:  # Conflict - already exists
                updated += 1
            else:
                errors.append(f"{item['celex_number']}: {response.status_code} - {response.text[:100]}")
                
        except Exception as e:
            errors.append(f"{item['celex_number']}: {str(e)}")
    
    return {'inserted': inserted, 'updated': updated, 'errors': errors}


def save_analysis_results(legislation: list) -> dict:
    """
    Save calculated scores to analysis_results table
    """
    if not SUPABASE_KEY:
        return {'saved': 0, 'errors': ['No API key']}
    
    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json'
    }
    
    saved = 0
    errors = []
    
    # First, get legislation IDs from database
    for item in legislation:
        try:
            # Get the legislation ID
            response = requests.get(
                f"{SUPABASE_URL}/rest/v1/legislation?celex_number=eq.{item['celex_number']}&select=id",
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 200:
                results = response.json()
                if results:
                    leg_id = results[0]['id']
                    score, priority = calculate_score(item)
                    
                    # Calculate score breakdown
                    analysis_data = {
                        'legislation_id': leg_id,
                        'score_category_match': 10 if item.get('is_direct_annex2_match') else (5 if item.get('is_keyword_match') else 0),
                        'score_consumer_relevance': 3 if item.get('consumer_relevance') == 'high' else (1 if item.get('consumer_relevance') == 'medium' else 0),
                        'score_consultation': 0,  # Will be updated when we add consultation scraping
                        'score_dsc': 0,  # Will be updated when we add DSC scraping
                        'score_legislation_type': 2 if item.get('legislation_type') == 'Regulation' else 1,
                        'total_score': score,
                        'priority_level': priority,
                        'calculated_at': datetime.now().isoformat()
                    }
                    
                    # Upsert analysis results
                    response = requests.post(
                        f"{SUPABASE_URL}/rest/v1/analysis_results",
                        headers={**headers, 'Prefer': 'resolution=merge-duplicates'},
                        json=analysis_data,
                        timeout=30
                    )
                    
                    if response.status_code in [200, 201]:
                        saved += 1
                    else:
                        errors.append(f"{item['celex_number']}: Analysis save failed - {response.status_code}")
                        
        except Exception as e:
            errors.append(f"{item['celex_number']}: {str(e)}")
    
    return {'saved': saved, 'errors': errors}


def log_scraper_run(source: str, results: dict) -> None:
    """
    Log the scraper run to scraper_log table
    """
    if not SUPABASE_KEY:
        return
    
    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json'
    }
    
    log_data = {
        'source': source,
        'items_found': results.get('found', 0),
        'items_new': results.get('inserted', 0),
        'items_updated': results.get('updated', 0),
        'errors': results.get('errors', [])[:10],  # Limit errors stored
        'started_at': results.get('started_at'),
        'completed_at': datetime.now().isoformat(),
        'status': 'completed' if not results.get('errors') else 'completed'
    }
    
    try:
        requests.post(
            f"{SUPABASE_URL}/rest/v1/scraper_log",
            headers=headers,
            json=log_data,
            timeout=30
        )
    except Exception as e:
        print(f"Failed to log scraper run: {e}")


def main():
    """
    Main scraper function
    """
    print("=" * 50)
    print("NI/EU Law Tracker - EUR-Lex Scraper")
    print(f"Started at: {datetime.now().isoformat()}")
    print("=" * 50)
    
    started_at = datetime.now().isoformat()
    
    # Step 1: Fetch recent legislation
    legislation = fetch_recent_legislation(days_back=30)
    print(f"\nFetched {len(legislation)} legislation items")
    
    if not legislation:
        print("No legislation found, exiting")
        return
    
    # Step 2: Match to Annex 2 categories
    print("\nMatching to Annex 2 categories...")
    matched_count = 0
    for item in legislation:
        category_num, is_direct, keywords = match_to_category(item['title'])
        item['category_number'] = category_num
        item['is_direct_annex2_match'] = is_direct
        item['is_keyword_match'] = bool(keywords) and not is_direct
        item['matched_keywords'] = keywords
        
        if category_num:
            matched_count += 1
            category = next((c for c in ANNEX2_CATEGORIES if c['number'] == category_num), None)
            if category:
                item['consumer_relevance'] = category['relevance']
    
    print(f"Matched {matched_count} items to Annex 2 categories")
    
    # Step 3: Save to database
    print("\nSaving to Supabase...")
    save_results = save_to_supabase(legislation)
    print(f"Inserted: {save_results['inserted']}, Updated: {save_results['updated']}")
    if save_results['errors']:
        print(f"Errors: {len(save_results['errors'])}")
        for err in save_results['errors'][:5]:
            print(f"  - {err}")
    
    # Step 4: Calculate and save analysis scores
    print("\nCalculating priority scores...")
    analysis_results = save_analysis_results(legislation)
    print(f"Saved {analysis_results['saved']} analysis results")
    
    # Step 5: Log the run
    log_scraper_run('eurlex', {
        'found': len(legislation),
        'inserted': save_results['inserted'],
        'updated': save_results['updated'],
        'errors': save_results['errors'] + analysis_results.get('errors', []),
        'started_at': started_at
    })
    
    print("\n" + "=" * 50)
    print("Scraper completed successfully!")
    print("=" * 50)


if __name__ == '__main__':
    main()
