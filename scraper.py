"""
NI/EU Law Tracker - EUR-Lex Scraper
Fetches EU legislation and matches against Windsor Framework Annex 2 categories
"""

import os
import re
import json
import requests
from datetime import datetime, timedelta
from xml.etree import ElementTree

# ============================================
# CONFIGURATION
# ============================================
SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_KEY = os.environ.get('SUPABASE_SERVICE_KEY', '')

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


def fetch_eurlex_cellar_api():
    """
    Fetch from EUR-Lex CELLAR API using SPARQL
    """
    print("Fetching from CELLAR SPARQL API...")
    legislation = []
    
    sparql_endpoint = "https://publications.europa.eu/webapi/rdf/sparql"
    
    # Query for recent regulations, directives, and decisions
    query = """
    PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    
    SELECT DISTINCT ?celex ?title ?date WHERE {
        ?work cdm:resource_legal_id_celex ?celex .
        ?work cdm:work_date_document ?date .
        ?expr cdm:expression_belongs_to_work ?work .
        ?expr cdm:expression_uses_language <http://publications.europa.eu/resource/authority/language/ENG> .
        ?expr cdm:expression_title ?title .
        
        FILTER(
            STRSTARTS(STR(?celex), "32024") || 
            STRSTARTS(STR(?celex), "32025") || 
            STRSTARTS(STR(?celex), "32026")
        )
    }
    ORDER BY DESC(?date)
    LIMIT 150
    """
    
    try:
        response = requests.post(
            sparql_endpoint,
            data={'query': query},
            headers={
                'Accept': 'application/sparql-results+json',
                'Content-Type': 'application/x-www-form-urlencoded',
                'User-Agent': 'Mozilla/5.0 (compatible; NI-EU-Law-Tracker/1.0)'
            },
            timeout=60
        )
        
        print(f"  SPARQL response status: {response.status_code}")
        
        if response.status_code == 200:
            try:
                results = response.json()
                bindings = results.get('results', {}).get('bindings', [])
                print(f"  Found {len(bindings)} results from SPARQL")
                
                for binding in bindings:
                    celex = binding.get('celex', {}).get('value', '')
                    title = binding.get('title', {}).get('value', '')
                    date = binding.get('date', {}).get('value', '')[:10] if binding.get('date', {}).get('value') else None
                    
                    if celex and title and is_relevant_celex(celex):
                        leg_type = determine_legislation_type(celex, title)
                        legislation.append({
                            'celex_number': celex,
                            'title': clean_title(title),
                            'date_published': date,
                            'legislation_type': leg_type,
                            'eurlex_url': f"https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:{celex}"
                        })
            except json.JSONDecodeError as e:
                print(f"  Could not parse JSON response: {e}")
        else:
            print(f"  SPARQL query failed: {response.text[:300]}")
            
    except Exception as e:
        print(f"  SPARQL error: {e}")
    
    return legislation


def fetch_eurlex_rss():
    """
    Fetch recent legislation from EUR-Lex RSS feeds
    """
    print("Fetching from EUR-Lex RSS feeds...")
    legislation = []
    
    # EUR-Lex RSS feed for recent OJ L series (legislation)
    rss_url = "https://eur-lex.europa.eu/rss.do?rssId=legislation"
    
    try:
        response = requests.get(rss_url, timeout=30, headers={
            'User-Agent': 'Mozilla/5.0 (compatible; NI-EU-Law-Tracker/1.0)'
        })
        
        print(f"  RSS response status: {response.status_code}")
        
        if response.status_code == 200:
            # Parse RSS XML
            try:
                root = ElementTree.fromstring(response.content)
                items = root.findall('.//item')
                print(f"  Found {len(items)} items in RSS feed")
                
                for item in items:
                    title_elem = item.find('title')
                    link_elem = item.find('link')
                    pub_date_elem = item.find('pubDate')
                    
                    if title_elem is not None and title_elem.text:
                        title = title_elem.text
                        link = link_elem.text if link_elem is not None else ''
                        
                        # Extract CELEX from link
                        celex = extract_celex(link, title)
                        
                        if celex and is_relevant_celex(celex):
                            # Parse date
                            pub_date = None
                            if pub_date_elem is not None and pub_date_elem.text:
                                try:
                                    from email.utils import parsedate_to_datetime
                                    pub_date = parsedate_to_datetime(pub_date_elem.text).strftime('%Y-%m-%d')
                                except:
                                    pass
                            
                            leg_type = determine_legislation_type(celex, title)
                            legislation.append({
                                'celex_number': celex,
                                'title': clean_title(title),
                                'date_published': pub_date,
                                'legislation_type': leg_type,
                                'eurlex_url': f"https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:{celex}"
                            })
            except ElementTree.ParseError as e:
                print(f"  Could not parse RSS XML: {e}")
    except Exception as e:
        print(f"  RSS error: {e}")
    
    return legislation


def extract_celex(link, title):
    """Extract CELEX number from link or title"""
    patterns = [
        r'CELEX[:%3A](\d{5}[A-Z]\d{4})',
        r'uri=CELEX:(\d{5}[A-Z]\d{4})',
        r'/(\d{5}[A-Z]\d{4})',
        r'(\d{5}[RLD]\d{4})',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, link, re.I)
        if match:
            return match.group(1).upper()
    
    for pattern in patterns:
        match = re.search(pattern, title, re.I)
        if match:
            return match.group(1).upper()
    
    # Try to construct from regulation/directive number
    reg_match = re.search(r'(?:EU\)?\s*)?(\d{4})/(\d+)', title)
    if reg_match:
        year = reg_match.group(1)
        num = reg_match.group(2).zfill(4)
        if 'Regulation' in title:
            return f"3{year}R{num}"
        elif 'Directive' in title:
            return f"3{year}L{num}"
        elif 'Decision' in title:
            return f"3{year}D{num}"
    
    return None


def is_relevant_celex(celex):
    """Check if CELEX indicates a Regulation, Directive, or Decision"""
    if not celex or len(celex) < 6:
        return False
    type_char = celex[5].upper()
    return type_char in ['R', 'L', 'D']


def determine_legislation_type(celex, title):
    """Determine legislation type from CELEX or title"""
    if celex and len(celex) >= 6:
        type_char = celex[5].upper()
        if type_char == 'R':
            return 'Regulation'
        elif type_char == 'L':
            return 'Directive'
        elif type_char == 'D':
            return 'Decision'
    
    title_lower = title.lower()
    if 'regulation' in title_lower:
        return 'Regulation'
    elif 'directive' in title_lower:
        return 'Directive'
    elif 'decision' in title_lower:
        return 'Decision'
    
    return 'Other'


def clean_title(title):
    """Clean up title text"""
    title = ' '.join(title.split())
    if len(title) > 500:
        title = title[:497] + '...'
    return title


def match_to_category(title):
    """Match legislation title to Annex 2 category based on keywords"""
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
    
    is_direct_match = best_score >= 2
    
    return best_match, is_direct_match, matched_keywords


def calculate_score(item):
    """Calculate priority score"""
    score = 0
    
    if item.get('is_direct_annex2_match'):
        score += 10
    elif item.get('is_keyword_match'):
        score += 5
    
    category_num = item.get('category_number')
    if category_num:
        category = next((c for c in ANNEX2_CATEGORIES if c['number'] == category_num), None)
        if category:
            if category['relevance'] == 'high':
                score += 3
            elif category['relevance'] == 'medium':
                score += 1
    
    leg_type = item.get('legislation_type')
    if leg_type == 'Regulation':
        score += 2
    elif leg_type in ['Directive', 'Decision']:
        score += 1
    
    if score >= 18:
        priority = 'critical'
    elif score >= 12:
        priority = 'high'
    elif score >= 6:
        priority = 'medium'
    else:
        priority = 'low'
    
    return score, priority


def save_to_supabase(legislation):
    """Save legislation to Supabase database"""
    if not SUPABASE_KEY:
        print("ERROR: SUPABASE_SERVICE_KEY not set")
        return {'inserted': 0, 'errors': ['No API key']}
    
    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'resolution=merge-duplicates'
    }
    
    inserted = 0
    errors = []
    
    for item in legislation:
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
            response = requests.post(
                f"{SUPABASE_URL}/rest/v1/legislation",
                headers=headers,
                json=data,
                timeout=30
            )
            
            if response.status_code in [200, 201, 409]:
                inserted += 1
            else:
                errors.append(f"{item['celex_number']}: {response.status_code} - {response.text[:100]}")
                
        except Exception as e:
            errors.append(f"{item['celex_number']}: {str(e)}")
    
    return {'inserted': inserted, 'errors': errors}


def save_analysis_results(legislation):
    """Save calculated scores to analysis_results table"""
    if not SUPABASE_KEY:
        return {'saved': 0, 'errors': ['No API key']}
    
    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'resolution=merge-duplicates'
    }
    
    saved = 0
    errors = []
    
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
                    
                    analysis_data = {
                        'legislation_id': leg_id,
                        'score_category_match': 10 if item.get('is_direct_annex2_match') else (5 if item.get('is_keyword_match') else 0),
                        'score_consumer_relevance': 3 if item.get('consumer_relevance') == 'high' else (1 if item.get('consumer_relevance') == 'medium' else 0),
                        'score_consultation': 0,
                        'score_dsc': 0,
                        'score_legislation_type': 2 if item.get('legislation_type') == 'Regulation' else 1,
                        'total_score': score,
                        'priority_level': priority,
                        'calculated_at': datetime.now().isoformat()
                    }
                    
                    response = requests.post(
                        f"{SUPABASE_URL}/rest/v1/analysis_results",
                        headers=headers,
                        json=analysis_data,
                        timeout=30
                    )
                    
                    if response.status_code in [200, 201, 409]:
                        saved += 1
                    else:
                        errors.append(f"{item['celex_number']}: Analysis {response.status_code}")
                        
        except Exception as e:
            errors.append(f"{item['celex_number']}: {str(e)}")
    
    return {'saved': saved, 'errors': errors}


def main():
    """Main scraper function"""
    print("=" * 50)
    print("NI/EU Law Tracker - EUR-Lex Scraper")
    print(f"Started at: {datetime.now().isoformat()}")
    print(f"SUPABASE_URL: {SUPABASE_URL[:30]}..." if SUPABASE_URL else "SUPABASE_URL: NOT SET")
    print(f"SUPABASE_KEY: {'SET' if SUPABASE_KEY else 'NOT SET'}")
    print("=" * 50)
    
    legislation = []
    
    # Method 1: CELLAR SPARQL API
    legislation.extend(fetch_eurlex_cellar_api())
    
    # Method 2: RSS feed
    if len(legislation) < 20:
        legislation.extend(fetch_eurlex_rss())
    
    # Remove duplicates by CELEX
    seen = set()
    unique_legislation = []
    for item in legislation:
        if item['celex_number'] not in seen:
            seen.add(item['celex_number'])
            unique_legislation.append(item)
    
    legislation = unique_legislation
    print(f"\nTotal unique legislation items: {len(legislation)}")
    
    if not legislation:
        print("\nNo legislation found from any source!")
        print("This may be a temporary issue with EUR-Lex APIs.")
        print("The scraper will try again on the next scheduled run.")
        return
    
    # Show some examples
    print("\nSample legislation found:")
    for item in legislation[:5]:
        print(f"  - {item['celex_number']}: {item['title'][:60]}...")
    
    # Match to Annex 2 categories
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
    
    # Save to database
    print("\nSaving to Supabase...")
    save_results = save_to_supabase(legislation)
    print(f"Saved: {save_results['inserted']} legislation items")
    if save_results['errors']:
        print(f"Errors: {len(save_results['errors'])}")
        for err in save_results['errors'][:5]:
            print(f"  - {err}")
    
    # Calculate and save analysis scores
    print("\nCalculating priority scores...")
    analysis_results = save_analysis_results(legislation)
    print(f"Analysis results saved: {analysis_results['saved']}")
    if analysis_results['errors']:
        print(f"Analysis errors: {len(analysis_results['errors'])}")
        for err in analysis_results['errors'][:3]:
            print(f"  - {err}")
    
    print("\n" + "=" * 50)
    print("Scraper completed successfully!")
    print("=" * 50)


if __name__ == '__main__':
    main()
