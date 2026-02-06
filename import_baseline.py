"""
NI/EU Law Tracker - Historical Baseline Import
One-time script to import foundational EU legislation from Windsor Framework Annex 2
"""

import os
import re
import json
import requests
from datetime import datetime

# ============================================
# CONFIGURATION
# ============================================
SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_KEY = os.environ.get('SUPABASE_SERVICE_KEY', '')

# ============================================
# ANNEX 2 BASELINE LEGISLATION
# These are the foundational acts explicitly listed in Windsor Framework Annex 2
# Organised by category number
# ============================================

ANNEX2_BASELINE = [
    # Category 1: General customs aspects
    {"celex": "32013R0952", "category": 1, "title": "Regulation (EU) No 952/2013 - Union Customs Code"},
    {"celex": "32015R2446", "category": 1, "title": "Commission Delegated Regulation (EU) 2015/2446 - Union Customs Code (Delegated)"},
    {"celex": "32015R2447", "category": 1, "title": "Commission Implementing Regulation (EU) 2015/2447 - Union Customs Code (Implementing)"},
    
    # Category 2: Protection of financial interests
    {"celex": "32017R1939", "category": 2, "title": "Council Regulation (EU) 2017/1939 - European Public Prosecutor's Office"},
    
    # Category 3: Trade statistics
    {"celex": "32009R0471", "category": 3, "title": "Regulation (EC) No 471/2009 - Community statistics on external trade"},
    
    # Category 4: General trade related
    {"celex": "32015R0478", "category": 4, "title": "Regulation (EU) 2015/478 - Common rules for imports"},
    {"celex": "32015R0479", "category": 4, "title": "Regulation (EU) 2015/479 - Common rules for exports"},
    {"celex": "32017R0821", "category": 4, "title": "Regulation (EU) 2017/821 - Conflict minerals"},
    
    # Category 5: Trade defence
    {"celex": "32016R1036", "category": 5, "title": "Regulation (EU) 2016/1036 - Protection against dumped imports"},
    {"celex": "32016R1037", "category": 5, "title": "Regulation (EU) 2016/1037 - Protection against subsidised imports"},
    
    # Category 7: Others (pharma patents)
    {"celex": "32006R0816", "category": 7, "title": "Regulation (EC) No 816/2006 - Compulsory licensing of patents"},
    
    # Category 8: Goods - general provisions (IMPORTANT - product safety)
    {"celex": "32008R0765", "category": 8, "title": "Regulation (EC) No 765/2008 - Accreditation and market surveillance"},
    {"celex": "32019R1020", "category": 8, "title": "Regulation (EU) 2019/1020 - Market surveillance and compliance of products"},
    {"celex": "32001L0095", "category": 8, "title": "Directive 2001/95/EC - General product safety"},
    {"celex": "32023R0988", "category": 8, "title": "Regulation (EU) 2023/988 - General Product Safety Regulation"},
    {"celex": "31998L0034", "category": 8, "title": "Directive 98/34/EC - Technical standards and regulations"},
    {"celex": "32012R1025", "category": 8, "title": "Regulation (EU) No 1025/2012 - European standardisation"},
    
    # Category 9: Motor vehicles
    {"celex": "32018R0858", "category": 9, "title": "Regulation (EU) 2018/858 - Motor vehicle type-approval"},
    {"celex": "32007R0715", "category": 9, "title": "Regulation (EC) No 715/2007 - Emissions from light vehicles (Euro 5/6)"},
    {"celex": "32009R0595", "category": 9, "title": "Regulation (EC) No 595/2009 - Emissions from heavy duty vehicles (Euro VI)"},
    {"celex": "32014R0044", "category": 9, "title": "Regulation (EU) No 44/2014 - Vehicle construction requirements"},
    {"celex": "32013R0168", "category": 9, "title": "Regulation (EU) No 168/2013 - Two/three-wheel vehicles and quadricycles"},
    {"celex": "32013R0167", "category": 9, "title": "Regulation (EU) No 167/2013 - Agricultural and forestry vehicles"},
    
    # Category 10: Lifting equipment
    {"celex": "32014L0033", "category": 10, "title": "Directive 2014/33/EU - Lifts and safety components"},
    {"celex": "31991L0368", "category": 10, "title": "Directive 91/368/EEC - Machinery (lifting)"},
    
    # Category 11: Gas appliances
    {"celex": "32016R0426", "category": 11, "title": "Regulation (EU) 2016/426 - Appliances burning gaseous fuels"},
    {"celex": "32013R0811", "category": 11, "title": "Regulation (EU) No 811/2013 - Energy labelling of heaters"},
    {"celex": "32013R0812", "category": 11, "title": "Regulation (EU) No 812/2013 - Energy labelling of water heaters"},
    {"celex": "31992L0042", "category": 11, "title": "Directive 92/42/EEC - Hot-water boilers"},
    
    # Category 12: Pressure vessels
    {"celex": "32014L0029", "category": 12, "title": "Directive 2014/29/EU - Simple pressure vessels"},
    {"celex": "32014L0068", "category": 12, "title": "Directive 2014/68/EU - Pressure equipment"},
    {"celex": "32010L0035", "category": 12, "title": "Directive 2010/35/EU - Transportable pressure equipment"},
    {"celex": "31975L0324", "category": 12, "title": "Directive 75/324/EEC - Aerosol dispensers"},
    
    # Category 13: Measuring instruments
    {"celex": "32014L0031", "category": 13, "title": "Directive 2014/31/EU - Non-automatic weighing instruments"},
    {"celex": "32014L0032", "category": 13, "title": "Directive 2014/32/EU - Measuring instruments"},
    {"celex": "32009L0034", "category": 13, "title": "Directive 2009/34/EC - Measuring instruments (common provisions)"},
    {"celex": "32007L0045", "category": 13, "title": "Directive 2007/45/EC - Nominal quantities for prepacked products"},
    {"celex": "31976L0211", "category": 13, "title": "Directive 76/211/EEC - Making-up by weight or volume"},
    
    # Category 14: Construction, machinery, cableways, PPE
    {"celex": "32011R0305", "category": 14, "title": "Regulation (EU) No 305/2011 - Construction products"},
    {"celex": "32006L0042", "category": 14, "title": "Directive 2006/42/EC - Machinery"},
    {"celex": "32016R0424", "category": 14, "title": "Regulation (EU) 2016/424 - Cableway installations"},
    {"celex": "32016R0425", "category": 14, "title": "Regulation (EU) 2016/425 - Personal protective equipment"},
    
    # Category 15: Electrical and radio equipment
    {"celex": "32014L0035", "category": 15, "title": "Directive 2014/35/EU - Low voltage electrical equipment"},
    {"celex": "32014L0030", "category": 15, "title": "Directive 2014/30/EU - Electromagnetic compatibility"},
    {"celex": "32014L0053", "category": 15, "title": "Directive 2014/53/EU - Radio equipment"},
    {"celex": "32009L0125", "category": 15, "title": "Directive 2009/125/EC - Ecodesign of energy-related products"},
    
    # Category 16: Textiles, footwear
    {"celex": "32011R1007", "category": 16, "title": "Regulation (EU) No 1007/2011 - Textile fibre names and labelling"},
    {"celex": "31994L0011", "category": 16, "title": "Directive 94/11/EC - Labelling of footwear materials"},
    
    # Category 17: Cosmetics, toys (HIGH CONSUMER RELEVANCE)
    {"celex": "32009R1223", "category": 17, "title": "Regulation (EC) No 1223/2009 - Cosmetic products"},
    {"celex": "32009L0048", "category": 17, "title": "Directive 2009/48/EC - Safety of toys"},
    
    # Category 18: Recreational craft
    {"celex": "32013L0053", "category": 18, "title": "Directive 2013/53/EU - Recreational craft and personal watercraft"},
    
    # Category 19: Explosives and pyrotechnics
    {"celex": "32014L0028", "category": 19, "title": "Directive 2014/28/EU - Explosives for civil uses"},
    {"celex": "32013L0029", "category": 19, "title": "Directive 2013/29/EU - Pyrotechnic articles"},
    
    # Category 20: Medicinal products (HIGH CONSUMER RELEVANCE)
    {"celex": "32001L0083", "category": 20, "title": "Directive 2001/83/EC - Medicinal products for human use"},
    {"celex": "32001L0082", "category": 20, "title": "Directive 2001/82/EC - Veterinary medicinal products"},
    {"celex": "32019R0006", "category": 20, "title": "Regulation (EU) 2019/6 - Veterinary medicinal products"},
    {"celex": "32004R0726", "category": 20, "title": "Regulation (EC) No 726/2004 - European Medicines Agency procedures"},
    {"celex": "32014R0536", "category": 20, "title": "Regulation (EU) No 536/2014 - Clinical trials"},
    {"celex": "32006R1901", "category": 20, "title": "Regulation (EC) No 1901/2006 - Paediatric medicinal products"},
    {"celex": "32004L0024", "category": 20, "title": "Directive 2004/24/EC - Traditional herbal medicinal products"},
    
    # Category 21: Medical devices (HIGH CONSUMER RELEVANCE)
    {"celex": "32017R0745", "category": 21, "title": "Regulation (EU) 2017/745 - Medical devices"},
    {"celex": "32017R0746", "category": 21, "title": "Regulation (EU) 2017/746 - In vitro diagnostic medical devices"},
    
    # Category 22: Substances of human origin
    {"celex": "32002L0098", "category": 22, "title": "Directive 2002/98/EC - Standards for blood and blood components"},
    {"celex": "32004L0023", "category": 22, "title": "Directive 2004/23/EC - Standards for human tissues and cells"},
    {"celex": "32010L0053", "category": 22, "title": "Directive 2010/53/EU - Standards for human organs"},
    
    # Category 23: Chemicals (HIGH CONSUMER RELEVANCE)
    {"celex": "32006R1907", "category": 23, "title": "Regulation (EC) No 1907/2006 - REACH (chemicals)"},
    {"celex": "32008R1272", "category": 23, "title": "Regulation (EC) No 1272/2008 - CLP (classification, labelling, packaging)"},
    {"celex": "32019R1021", "category": 23, "title": "Regulation (EU) 2019/1021 - Persistent organic pollutants"},
    {"celex": "32012R0528", "category": 23, "title": "Regulation (EU) No 528/2012 - Biocidal products"},
    {"celex": "32003R2003", "category": 23, "title": "Regulation (EC) No 2003/2003 - Fertilisers"},
    {"celex": "32019R1009", "category": 23, "title": "Regulation (EU) 2019/1009 - EU fertilising products"},
    {"celex": "32004R0648", "category": 23, "title": "Regulation (EC) No 648/2004 - Detergents"},
    {"celex": "32006L0066", "category": 23, "title": "Directive 2006/66/EC - Batteries and accumulators"},
    {"celex": "32023R1542", "category": 23, "title": "Regulation (EU) 2023/1542 - Batteries and waste batteries"},
    {"celex": "32011L0065", "category": 23, "title": "Directive 2011/65/EU - RoHS (hazardous substances in electrical equipment)"},
    {"celex": "32017R0852", "category": 23, "title": "Regulation (EU) 2017/852 - Mercury"},
    {"celex": "32019R1148", "category": 23, "title": "Regulation (EU) 2019/1148 - Explosives precursors"},
    {"celex": "32009R1005", "category": 23, "title": "Regulation (EC) No 1005/2009 - Ozone depleting substances"},
    
    # Category 24: Pesticides, biocides
    {"celex": "32009R1107", "category": 24, "title": "Regulation (EC) No 1107/2009 - Plant protection products"},
    {"celex": "32005R0396", "category": 24, "title": "Regulation (EC) No 396/2005 - Maximum residue levels of pesticides"},
    {"celex": "32009L0128", "category": 24, "title": "Directive 2009/128/EC - Sustainable use of pesticides"},
    {"celex": "32012R0528", "category": 24, "title": "Regulation (EU) No 528/2012 - Biocidal products"},
    
    # Category 25: Waste
    {"celex": "32008L0098", "category": 25, "title": "Directive 2008/98/EC - Waste Framework Directive"},
    {"celex": "32006R1013", "category": 25, "title": "Regulation (EC) No 1013/2006 - Shipments of waste"},
    {"celex": "31994L0062", "category": 25, "title": "Directive 94/62/EC - Packaging and packaging waste"},
    {"celex": "32012L0019", "category": 25, "title": "Directive 2012/19/EU - WEEE (electronic waste)"},
    {"celex": "32000L0053", "category": 25, "title": "Directive 2000/53/EC - End-of-life vehicles"},
    {"celex": "32013R1257", "category": 25, "title": "Regulation (EU) No 1257/2013 - Ship recycling"},
    
    # Category 26: Environment, energy efficiency (HIGH CONSUMER RELEVANCE)
    {"celex": "32010L0030", "category": 26, "title": "Directive 2010/30/EU - Energy labelling"},
    {"celex": "32017R1369", "category": 26, "title": "Regulation (EU) 2017/1369 - Energy labelling framework"},
    {"celex": "32009L0125", "category": 26, "title": "Directive 2009/125/EC - Ecodesign requirements"},
    {"celex": "32010R0066", "category": 26, "title": "Regulation (EC) No 66/2010 - EU Ecolabel"},
    {"celex": "32014R1143", "category": 26, "title": "Regulation (EU) No 1143/2014 - Invasive alien species"},
    {"celex": "32014R0517", "category": 26, "title": "Regulation (EU) No 517/2014 - Fluorinated greenhouse gases"},
    {"celex": "32024R0573", "category": 26, "title": "Regulation (EU) 2024/573 - Fluorinated greenhouse gases (recast)"},
    
    # Category 27: Marine equipment
    {"celex": "32014L0090", "category": 27, "title": "Directive 2014/90/EU - Marine equipment"},
    
    # Category 28: Rail transport
    {"celex": "32016L0797", "category": 28, "title": "Directive (EU) 2016/797 - Railway interoperability"},
    {"celex": "32016L0798", "category": 28, "title": "Directive (EU) 2016/798 - Railway safety"},
    
    # Category 29: Food - general (HIGH CONSUMER RELEVANCE)
    {"celex": "32002R0178", "category": 29, "title": "Regulation (EC) No 178/2002 - General food law"},
    {"celex": "32011R1169", "category": 29, "title": "Regulation (EU) No 1169/2011 - Food information to consumers"},
    {"celex": "32006R1924", "category": 29, "title": "Regulation (EC) No 1924/2006 - Nutrition and health claims"},
    {"celex": "32009R1925", "category": 29, "title": "Regulation (EC) No 1925/2006 - Addition of vitamins and minerals to foods"},
    
    # Category 30: Food - hygiene
    {"celex": "32004R0852", "category": 30, "title": "Regulation (EC) No 852/2004 - Hygiene of foodstuffs"},
    {"celex": "32004R0853", "category": 30, "title": "Regulation (EC) No 853/2004 - Hygiene for food of animal origin"},
    {"celex": "32005R2073", "category": 30, "title": "Regulation (EC) No 2073/2005 - Microbiological criteria for foodstuffs"},
    
    # Category 31: Food - ingredients, additives
    {"celex": "32008R1333", "category": 31, "title": "Regulation (EC) No 1333/2008 - Food additives"},
    {"celex": "32008R1334", "category": 31, "title": "Regulation (EC) No 1334/2008 - Flavourings in food"},
    {"celex": "32009R0470", "category": 31, "title": "Regulation (EC) No 470/2009 - Residue limits in foodstuffs"},
    {"celex": "32006R1881", "category": 31, "title": "Regulation (EC) No 1881/2006 - Maximum levels for contaminants"},
    {"celex": "32015R2283", "category": 31, "title": "Regulation (EU) 2015/2283 - Novel foods"},
    {"celex": "32006L0141", "category": 31, "title": "Directive 2006/141/EC - Infant formulae"},
    {"celex": "32013R0609", "category": 31, "title": "Regulation (EU) No 609/2013 - Food for specific groups"},
    
    # Category 32: Food contact materials
    {"celex": "32004R1935", "category": 32, "title": "Regulation (EC) No 1935/2004 - Materials in contact with food"},
    {"celex": "32011R0010", "category": 32, "title": "Regulation (EU) No 10/2011 - Plastic materials in contact with food"},
    
    # Category 33: Food - other
    {"celex": "31999L0002", "category": 33, "title": "Directive 1999/2/EC - Ionising radiation of food"},
    {"celex": "32018R0848", "category": 33, "title": "Regulation (EU) 2018/848 - Organic production"},
    {"celex": "32009L0054", "category": 33, "title": "Directive 2009/54/EC - Natural mineral waters"},
    
    # Category 34: Feed
    {"celex": "32009R0767", "category": 34, "title": "Regulation (EC) No 767/2009 - Feed marketing"},
    {"celex": "32003R1831", "category": 34, "title": "Regulation (EC) No 1831/2003 - Feed additives"},
    {"celex": "32005R0183", "category": 34, "title": "Regulation (EC) No 183/2005 - Feed hygiene"},
    {"celex": "32019R0004", "category": 34, "title": "Regulation (EU) 2019/4 - Medicated feed"},
    
    # Category 35: GMOs
    {"celex": "32001L0018", "category": 35, "title": "Directive 2001/18/EC - Release of GMOs into environment"},
    {"celex": "32003R1829", "category": 35, "title": "Regulation (EC) No 1829/2003 - GM food and feed"},
    {"celex": "32003R1830", "category": 35, "title": "Regulation (EC) No 1830/2003 - Traceability of GMOs"},
    
    # Category 36: Live animals
    {"celex": "32016R0429", "category": 36, "title": "Regulation (EU) 2016/429 - Animal health law"},
    {"celex": "32020R0692", "category": 36, "title": "Delegated Regulation (EU) 2020/692 - Entry of animals into Union"},
    
    # Category 37: Animal disease control
    {"celex": "32016R0429", "category": 37, "title": "Regulation (EU) 2016/429 - Animal health law (disease provisions)"},
    {"celex": "32001R0999", "category": 37, "title": "Regulation (EC) No 999/2001 - TSE (BSE/scrapie) rules"},
    {"celex": "32005L0094", "category": 37, "title": "Directive 2005/94/EC - Avian influenza control"},
    {"celex": "32001L0089", "category": 37, "title": "Directive 2001/89/EC - Classical swine fever"},
    
    # Category 38: Animal identification
    {"celex": "32021R0520", "category": 38, "title": "Regulation (EU) 2021/520 - Traceability of animals"},
    {"celex": "32000R1760", "category": 38, "title": "Regulation (EC) No 1760/2000 - Beef labelling and identification"},
    
    # Category 39: Animal breeding
    {"celex": "32016R1012", "category": 39, "title": "Regulation (EU) 2016/1012 - Animal breeding"},
    
    # Category 40: Animal welfare (HIGH CONSUMER RELEVANCE)
    {"celex": "32005R0001", "category": 40, "title": "Regulation (EC) No 1/2005 - Animal transport"},
    {"celex": "32009R1099", "category": 40, "title": "Regulation (EC) No 1099/2009 - Animal slaughter"},
    {"celex": "31998L0058", "category": 40, "title": "Directive 98/58/EC - Farm animal protection"},
    {"celex": "31999L0074", "category": 40, "title": "Directive 1999/74/EC - Laying hens welfare"},
    {"celex": "32007L0043", "category": 40, "title": "Directive 2007/43/EC - Broiler chicken welfare"},
    {"celex": "32008L0119", "category": 40, "title": "Directive 2008/119/EC - Calf welfare"},
    {"celex": "32008L0120", "category": 40, "title": "Directive 2008/120/EC - Pig welfare"},
    
    # Category 41: Plant health
    {"celex": "32016R2031", "category": 41, "title": "Regulation (EU) 2016/2031 - Plant health"},
    {"celex": "32017R0625", "category": 41, "title": "Regulation (EU) 2017/625 - Official controls"},
    
    # Category 42: Plant reproductive material
    {"celex": "32002L0053", "category": 42, "title": "Directive 2002/53/EC - Agricultural plant species"},
    {"celex": "32002L0054", "category": 42, "title": "Directive 2002/54/EC - Beet seed"},
    {"celex": "32002L0055", "category": 42, "title": "Directive 2002/55/EC - Vegetable seed"},
    {"celex": "32002L0056", "category": 42, "title": "Directive 2002/56/EC - Seed potatoes"},
    {"celex": "32002L0057", "category": 42, "title": "Directive 2002/57/EC - Oil and fibre plants seed"},
    {"celex": "31999L0105", "category": 42, "title": "Directive 1999/105/EC - Forest reproductive material"},
    
    # Category 43: Official controls
    {"celex": "32017R0625", "category": 43, "title": "Regulation (EU) 2017/625 - Official controls on food/feed"},
    {"celex": "32019R2130", "category": 43, "title": "Implementing Regulation (EU) 2019/2130 - Border control posts"},
    
    # Category 44: SPS - Other
    {"celex": "31996L0022", "category": 44, "title": "Directive 96/22/EC - Hormones in animal production"},
    {"celex": "31996L0023", "category": 44, "title": "Directive 96/23/EC - Residue monitoring in animals"},
    
    # Category 45: Intellectual property (GIs)
    {"celex": "32012R1151", "category": 45, "title": "Regulation (EU) No 1151/2012 - Quality schemes (PDO/PGI)"},
    {"celex": "32019R0787", "category": 45, "title": "Regulation (EU) 2019/787 - Spirit drinks"},
    {"celex": "32013R1308", "category": 45, "title": "Regulation (EU) No 1308/2013 - CMO (wine GIs)"},
    {"celex": "32019R0033", "category": 45, "title": "Delegated Regulation (EU) 2019/33 - Wine labelling"},
    
    # Category 46: Fisheries
    {"celex": "32008R1005", "category": 46, "title": "Regulation (EC) No 1005/2008 - IUU fishing"},
    {"celex": "32009R1224", "category": 46, "title": "Regulation (EC) No 1224/2009 - Fisheries control"},
    {"celex": "32016R1627", "category": 46, "title": "Regulation (EU) 2016/1627 - Bluefin tuna recovery"},
    
    # Category 47: Other
    {"celex": "32009R0428", "category": 47, "title": "Regulation (EC) No 428/2009 - Dual-use items"},
    {"celex": "32021R0821", "category": 47, "title": "Regulation (EU) 2021/821 - Dual-use items (recast)"},
    {"celex": "32019L1153", "category": 47, "title": "Directive (EU) 2019/1153 - Use of financial info (anti-money laundering)"},
    {"celex": "32014L0040", "category": 47, "title": "Directive 2014/40/EU - Tobacco products"},
]


def fetch_legislation_details(celex):
    """
    Fetch additional details from EUR-Lex for a CELEX number
    """
    sparql_endpoint = "https://publications.europa.eu/webapi/rdf/sparql"
    
    query = f"""
    PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
    
    SELECT ?title ?date WHERE {{
        ?work cdm:resource_legal_id_celex "{celex}" .
        ?work cdm:work_date_document ?date .
        ?expr cdm:expression_belongs_to_work ?work .
        ?expr cdm:expression_uses_language <http://publications.europa.eu/resource/authority/language/ENG> .
        ?expr cdm:expression_title ?title .
    }}
    LIMIT 1
    """
    
    try:
        response = requests.post(
            sparql_endpoint,
            data={'query': query},
            headers={
                'Accept': 'application/sparql-results+json',
                'Content-Type': 'application/x-www-form-urlencoded',
                'User-Agent': 'NI-EU-Law-Tracker/1.0'
            },
            timeout=30
        )
        
        if response.status_code == 200:
            results = response.json()
            bindings = results.get('results', {}).get('bindings', [])
            if bindings:
                return {
                    'title': bindings[0].get('title', {}).get('value'),
                    'date': bindings[0].get('date', {}).get('value', '')[:10]
                }
    except Exception as e:
        print(f"    Could not fetch details for {celex}: {e}")
    
    return None


def determine_legislation_type(celex):
    """Determine type from CELEX number"""
    if len(celex) >= 6:
        type_char = celex[5].upper()
        if type_char == 'R':
            return 'Regulation'
        elif type_char == 'L':
            return 'Directive'
        elif type_char == 'D':
            return 'Decision'
    return 'Other'


def save_to_supabase(item):
    """Save a single legislation item to Supabase"""
    if not SUPABASE_KEY:
        return False
    
    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'resolution=merge-duplicates'
    }
    
    data = {
        'celex_number': item['celex'],
        'title': item['title'],
        'legislation_type': item['type'],
        'category_number': item['category'],
        'is_baseline': True,
        'is_direct_annex2_match': True,
        'is_keyword_match': False,
        'date_published': item.get('date'),
        'eurlex_url': f"https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:{item['celex']}",
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
        return response.status_code in [200, 201, 409]
    except Exception as e:
        print(f"    Error saving {item['celex']}: {e}")
        return False


def save_analysis(celex, category):
    """Save analysis results for baseline legislation"""
    if not SUPABASE_KEY:
        return False
    
    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'resolution=merge-duplicates'
    }
    
    # Get legislation ID
    try:
        response = requests.get(
            f"{SUPABASE_URL}/rest/v1/legislation?celex_number=eq.{celex}&select=id",
            headers=headers,
            timeout=30
        )
        
        if response.status_code == 200:
            results = response.json()
            if results:
                leg_id = results[0]['id']
                
                # Baseline legislation gets high scores
                analysis_data = {
                    'legislation_id': leg_id,
                    'score_category_match': 10,  # Direct Annex 2 match
                    'score_consumer_relevance': 3,  # Assume high for baseline
                    'score_consultation': 0,
                    'score_dsc': 0,
                    'score_legislation_type': 2,
                    'total_score': 15,  # Base score for baseline legislation
                    'priority_level': 'high',
                    'calculated_at': datetime.now().isoformat()
                }
                
                response = requests.post(
                    f"{SUPABASE_URL}/rest/v1/analysis_results",
                    headers=headers,
                    json=analysis_data,
                    timeout=30
                )
                return response.status_code in [200, 201, 409]
    except:
        pass
    
    return False


def main():
    print("=" * 60)
    print("NI/EU Law Tracker - Historical Baseline Import")
    print(f"Started at: {datetime.now().isoformat()}")
    print(f"SUPABASE_URL: {'SET' if SUPABASE_URL else 'NOT SET'}")
    print(f"SUPABASE_KEY: {'SET' if SUPABASE_KEY else 'NOT SET'}")
    print("=" * 60)
    
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: Missing Supabase credentials")
        return
    
    print(f"\nImporting {len(ANNEX2_BASELINE)} baseline legislation items...")
    
    saved = 0
    errors = 0
    
    for i, item in enumerate(ANNEX2_BASELINE):
        celex = item['celex']
        category = item['category']
        fallback_title = item['title']
        
        print(f"\n[{i+1}/{len(ANNEX2_BASELINE)}] {celex}")
        
        # Try to fetch real title from EUR-Lex
        details = fetch_legislation_details(celex)
        
        if details and details.get('title'):
            title = details['title']
            date = details.get('date')
            print(f"    Found: {title[:60]}...")
        else:
            title = fallback_title
            date = None
            print(f"    Using fallback: {fallback_title[:60]}...")
        
        # Prepare item
        leg_item = {
            'celex': celex,
            'title': title,
            'type': determine_legislation_type(celex),
            'category': category,
            'date': date
        }
        
        # Save to database
        if save_to_supabase(leg_item):
            if save_analysis(celex, category):
                saved += 1
                print(f"    ✓ Saved")
            else:
                saved += 1
                print(f"    ✓ Saved (analysis failed)")
        else:
            errors += 1
            print(f"    ✗ Failed to save")
    
    print("\n" + "=" * 60)
    print(f"Import complete!")
    print(f"Saved: {saved}")
    print(f"Errors: {errors}")
    print("=" * 60)


if __name__ == '__main__':
    main()
