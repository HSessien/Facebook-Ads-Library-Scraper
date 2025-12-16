"""
Version Hybride Optimisée - Facebook ID Scraper
Combine l'architecture robuste de V0 avec les techniques d'extraction efficaces de V1

USAGE: Uniquement pour récupérer les ID permanents des pages WHITELIST
"""

import sys
import json
import asyncio
import re
from datetime import datetime
from playwright.async_api import async_playwright
import argparse
from pathlib import Path
import io

# Fix pour l'encodage Windows
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# Fichiers de configuration
STATUS_FILE = "fb_id_status.json"
RESULT_FILE = "fb_id_result.json"
STARTED_FILE = "fb_id_started.txt"
WHITELIST_FILE = "whitelist.json"
DEBUG_HTML_FILE = "debug_page_content.html"

# Mapping des mois français vers numéros
MOIS_FR = {
    "janvier": "01", "février": "02", "mars": "03", "avril": "04",
    "mai": "05", "juin": "06", "juillet": "07", "août": "08",
    "septembre": "09", "octobre": "10", "novembre": "11", "décembre": "12"
}

def log(message, level="INFO"):
    """Affiche un message de log avec timestamp"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] [{level}] {message}", flush=True)

def update_status(status, current=0, total=0, current_page="", message="", results=None):
    """Met à jour le fichier de statut pour Streamlit"""
    status_data = {
        "status": status,
        "current": current,
        "total": total,
        "current_page": current_page,
        "message": message,
        "results": results or [],
        "last_update": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    with open(STATUS_FILE, 'w', encoding='utf-8') as f:
        json.dump(status_data, f, indent=2, ensure_ascii=False)
    log(f"Status updated: {status} - {message}")

def load_whitelist():
    """Charge la whitelist"""
    log(f"Loading whitelist from {WHITELIST_FILE}")
    if Path(WHITELIST_FILE).exists():
        try:
            with open(WHITELIST_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                log(f"Loaded {len(data)} items from whitelist")
                return data
        except Exception as e:
            log(f"Error loading whitelist: {e}", "ERROR")
            return []
    log(f"Whitelist file not found, returning empty list")
    return []

def save_whitelist(data):
    """Sauvegarde la whitelist"""
    log(f"Saving {len(data)} items to whitelist")
    with open(WHITELIST_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    log(f"Successfully saved whitelist")

def parse_date_french(date_str):
    """Convertit une date française en format DD/MM/YYYY"""
    log(f"Parsing French date: '{date_str}'")
    try:
        parts = date_str.strip().split()
        if len(parts) == 3:
            day = parts[0].zfill(2)
            month_name = parts[1].lower()
            year = parts[2]
            
            if month_name in MOIS_FR:
                month = MOIS_FR[month_name]
                parsed_date = f"{day}/{month}/{year}"
                log(f"Date parsed successfully: {parsed_date}")
                return parsed_date
        log(f"Date format not recognized, returning as-is", "WARNING")
    except Exception as e:
        log(f"Error parsing date: {e}", "ERROR")
    return date_str

def is_duplicate(id_permanent):
    """Vérifie si l'ID permanent existe déjà dans la whitelist"""
    log(f"Checking for duplicate ID: {id_permanent} in whitelist")
    existing_list = load_whitelist()
    is_dup = any(item.get('id_permanent') == id_permanent for item in existing_list)
    log(f"Duplicate check result: {is_dup}")
    return is_dup

async def get_page_info(page_profile_id, headless=False):
    """
    Récupère les informations d'une page Facebook via la page de transparence
    VERSION HYBRIDE: Architecture V0 + Extraction V1
    
    Returns:
        dict: {
            'success': bool,
            'page_profile_id': str,
            'id_permanent': str,
            'nom_page': str,
            'date_creation': str,
            'date_ajout': str,
            'error': str (si échec)
        }
    """
    log(f"=== Starting scraping for page ID: {page_profile_id} ===")
    result = {
        'success': False,
        'page_profile_id': page_profile_id,
        'id_permanent': 'N/A',
        'nom_page': 'N/A',
        'date_creation': 'N/A',
        'date_ajout': datetime.now().strftime('%d-%m-%Y %H:%M:%S')
    }
    
    try:
        log("Launching Playwright browser...")
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=headless,
                args=['--disable-blink-features=AutomationControlled']
            )
            log(f"Browser launched (headless={headless})")
            
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                locale='fr-FR'
            )
            log("Browser context created with French locale")
            
            page = await context.new_page()
            
            # Désactiver webdriver (technique V1)
            await page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => false
                });
            """)
            log("Webdriver detection disabled")
            
            # URL de la page de transparence
            url = f"https://web.facebook.com/profile.php?id={page_profile_id}&sk=about_profile_transparency"
            log(f"Navigating to URL: {url}")
            
            # AMÉLIORATION V1: Attendre networkidle avec fallback
            try:
                log("Attempting navigation with networkidle...")
                await page.goto(url, wait_until="networkidle", timeout=45000)
                log("Page loaded (networkidle)")
            except Exception as nav_error:
                log(f"NetworkIdle timeout, falling back to domcontentloaded: {nav_error}", "WARNING")
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    log("Page loaded (domcontentloaded fallback)")
                except Exception as e:
                    log(f"Navigation failed completely: {e}", "ERROR")
                    raise
            
            # Attente initiale
            log("Waiting 5 seconds for initial content...")
            await asyncio.sleep(5)
            
            # Vérifier si on est sur une page de connexion
            login_form = await page.query_selector('input[name="email"]')
            if login_form:
                await browser.close()
                result['error'] = "Facebook demande une connexion"
                log("Login page detected, aborting", "ERROR")
                return result
            
            # AMÉLIORATION V1: Scrolling stratégique pour trigger lazy loading
            log("Strategic scrolling to trigger lazy loading...")
            await page.evaluate("window.scrollTo(0, 500)")
            await asyncio.sleep(1)
            await page.evaluate("window.scrollTo(0, 0)")
            await asyncio.sleep(2)
            log("Scroll sequence completed")
            
            # AMÉLIORATION V1: Tentatives multiples avec extraction JavaScript
            max_attempts = 3
            extraction_result = None
            
            for attempt in range(max_attempts):
                log(f"Extraction attempt {attempt + 1}/{max_attempts}...")
                
                # TECHNIQUE V1: Extraction JavaScript directe sur DOM vivant
                extraction_result = await page.evaluate("""
                    () => {
                        let permanentId = null;
                        let pageCreationDate = null;
                        let pageName = null;
                        
                        // Extraire le nom de la page depuis le titre
                        const title = document.querySelector('title');
                        if (title) {
                            pageName = title.textContent.replace(/\\s*\\|\\s*Facebook.*$/i, '').trim();
                        }
                        
                        // Chercher dans toutes les spans
                        const allSpans = document.querySelectorAll('span');
                        
                        // Méthode 1: Recherche bidirectionnelle stricte (V1)
                        for (let i = 0; i < allSpans.length; i++) {
                            const span = allSpans[i];
                            const text = span.textContent.trim();
                            
                            // Chercher "ID de la Page"
                            if (!permanentId) {
                                // Regarder le span suivant
                                if (i + 1 < allSpans.length) {
                                    const nextSpan = allSpans[i + 1];
                                    const nextText = nextSpan.textContent.trim();
                                    
                                    if (nextText === 'ID de la Page' || nextText === 'Page ID') {
                                        if (/^\\d{10,}$/.test(text)) {
                                            permanentId = text;
                                        }
                                    }
                                }
                                
                                // Regarder le span précédent
                                if (i > 0 && !permanentId) {
                                    const prevSpan = allSpans[i - 1];
                                    const prevText = prevSpan.textContent.trim();
                                    
                                    if ((text === 'ID de la Page' || text === 'Page ID') && /^\\d{10,}$/.test(prevText)) {
                                        permanentId = prevText;
                                    }
                                }
                            }
                            
                            // Chercher "Date de création"
                            if (!pageCreationDate) {
                                const datePattern = /\\d{1,2}\\s+[a-zéû]+\\s+\\d{4}/i;
                                
                                if (i + 1 < allSpans.length) {
                                    const nextSpan = allSpans[i + 1];
                                    const nextText = nextSpan.textContent.trim();
                                    
                                    if (nextText === 'Date de création' || nextText === 'Date created') {
                                        if (datePattern.test(text)) {
                                            pageCreationDate = text;
                                        }
                                    }
                                }
                                
                                if (i > 0 && !pageCreationDate) {
                                    const prevSpan = allSpans[i - 1];
                                    const prevText = prevSpan.textContent.trim();
                                    
                                    if ((text === 'Date de création' || text === 'Date created') && datePattern.test(prevText)) {
                                        pageCreationDate = prevText;
                                    }
                                }
                            }
                        }
                        
                        // Méthode 2: Recherche par proximité dans divs (fallback V1)
                        if (!permanentId) {
                            const allDivs = document.querySelectorAll('div');
                            for (let div of allDivs) {
                                const divText = div.textContent;
                                
                                if (divText.includes('ID de la Page') || divText.includes('Page ID')) {
                                    const numbers = divText.match(/\\b\\d{10,}\\b/g);
                                    if (numbers && numbers.length > 0) {
                                        permanentId = numbers[0];
                                        break;
                                    }
                                }
                            }
                        }
                        
                        return {
                            permanentId: permanentId,
                            pageCreationDate: pageCreationDate,
                            pageName: pageName
                        };
                    }
                """)
                
                permanent_id = extraction_result.get('permanentId')
                
                if permanent_id:
                    log(f"[SUCCESS] Permanent ID found on attempt {attempt + 1}: {permanent_id}")
                    break
                
                # Si échec et pas dernière tentative, re-scroll
                if attempt < max_attempts - 1:
                    log("ID not found, scrolling and retrying...")
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(2)
                    await page.evaluate("window.scrollTo(0, 0)")
                    await asyncio.sleep(1)
            
            # Sauvegarder le HTML pour débogage (technique V0)
            log(f"Saving HTML content to {DEBUG_HTML_FILE} for debugging...")
            html_content = await page.content()
            with open(DEBUG_HTML_FILE, 'w', encoding='utf-8') as f:
                f.write(html_content)
            log(f"HTML saved ({len(html_content)} chars)")
            
            log("Closing browser...")
            await browser.close()
            log("Browser closed")
            
            # Traiter les résultats
            if extraction_result:
                permanent_id = extraction_result.get('permanentId')
                page_creation_date = extraction_result.get('pageCreationDate')
                page_name = extraction_result.get('pageName')
                
                if permanent_id:
                    result['success'] = True
                    result['id_permanent'] = permanent_id
                    result['nom_page'] = page_name or "Nom non trouvé"
                    result['date_creation'] = parse_date_french(page_creation_date) if page_creation_date else "N/A"
                    log("[SUCCESS] Scraping SUCCESSFUL")
                    log(f"  - Name: {result['nom_page']}")
                    log(f"  - ID: {result['id_permanent']}")
                    log(f"  - Created: {result['date_creation']}")
                else:
                    result['error'] = "ID permanent non trouvé après 3 tentatives"
                    log("[FAILED] No permanent ID found after all attempts", "ERROR")
            else:
                result['error'] = "Échec de l'extraction JavaScript"
                log("[FAILED] JavaScript extraction returned null", "ERROR")
            
    except Exception as e:
        result['error'] = str(e)
        log(f"[EXCEPTION] Error occurred: {e}", "ERROR")
        import traceback
        log(f"Traceback:\n{traceback.format_exc()}", "ERROR")
    
    log(f"=== Scraping completed for page ID: {page_profile_id} ===")
    return result

async def process_single(page_id, headless=False):
    """Traite une seule page et l'ajoute à la whitelist"""
    log(f"Processing single page: {page_id} for whitelist")
    update_status("running", 0, 1, "", f"Récupération des infos pour {page_id}...", [])
    
    result = await get_page_info(page_id, headless)
    
    if result['success']:
        # Vérifier les doublons
        if is_duplicate(result['id_permanent']):
            result['success'] = False
            result['error'] = "Cette page existe déjà dans la whitelist"
            log("Page already exists in whitelist", "WARNING")
            update_status("completed", 1, 1, result['nom_page'], "Page déjà existante", [result])
        else:
            # Ajouter à la whitelist
            log(f"Adding page to whitelist...")
            current_list = load_whitelist()
            current_list.append(result)
            save_whitelist(current_list)
            log("Page successfully added to whitelist")
            
            update_status("completed", 1, 1, result['nom_page'], "Page ajoutée avec succès", [result])
    else:
        error_msg = result.get('error', 'Erreur inconnue')
        log(f"Processing failed: {error_msg}", "ERROR")
        update_status("error", 1, 1, "", f"Erreur: {error_msg}", [result])
    
    # Sauvegarder le résultat final
    log(f"Saving final result to {RESULT_FILE}")
    with open(RESULT_FILE, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    
    return result

async def process_batch(page_ids, headless=False):
    """Traite plusieurs pages en séquence et les ajoute à la whitelist"""
    total = len(page_ids)
    results = []
    success_count = 0
    
    log(f"Processing batch of {total} pages for whitelist")
    update_status("running", 0, total, "", f"Traitement de {total} page(s)...", [])
    
    for i, page_id in enumerate(page_ids, 1):
        log(f"--- Processing page {i}/{total}: {page_id} ---")
        update_status("running", i, total, "", f"Traitement de la page {i}/{total}: {page_id}...", results)
        
        result = await get_page_info(page_id, headless)
        
        if result['success']:
            # Vérifier les doublons
            if is_duplicate(result['id_permanent']):
                result['success'] = False
                result['error'] = "Page déjà existante"
                log(f"Page {page_id} already exists in whitelist", "WARNING")
            else:
                # Ajouter à la whitelist
                current_list = load_whitelist()
                current_list.append(result)
                save_whitelist(current_list)
                success_count += 1
                log(f"Page {page_id} added successfully to whitelist ({success_count} total)")
        
        results.append(result)
        
        # Petite pause entre chaque requête
        if i < total:
            log("Waiting 3 seconds before next page...")
            await asyncio.sleep(3)
    
    log(f"Batch processing completed: {success_count}/{total} pages added to whitelist")
    update_status("completed", total, total, "", f"{success_count}/{total} page(s) ajoutée(s)", results)
    
    # Sauvegarder le résultat final
    final_result = {
        'success': True,
        'total': total,
        'success_count': success_count,
        'results': results
    }
    
    log(f"Saving final batch result to {RESULT_FILE}")
    with open(RESULT_FILE, 'w', encoding='utf-8') as f:
        json.dump(final_result, f, indent=2, ensure_ascii=False)
    
    return final_result

async def main():
    log("=== Facebook ID Scraper HYBRID VERSION - WHITELIST ONLY ===")
    parser = argparse.ArgumentParser(description='Récupère les ID permanents de pages Facebook pour la WHITELIST')
    parser.add_argument('page_id', nargs='?', help='ID de profil de la page (mode single)')
    parser.add_argument('--batch', help='Fichier JSON contenant une liste d\'IDs (mode batch)')
    parser.add_argument('--headless', action='store_true', default=True, help='Mode invisible (défaut: True)')
    parser.add_argument('--visible', action='store_true', help='Mode visible (raccourci pour --no-headless)')
    
    args = parser.parse_args()
    
    # Gérer le flag --visible
    if args.visible:
        args.headless = False
    
    log(f"Arguments: page_id={args.page_id}, batch={args.batch}, headless={args.headless}")
    
    # Créer le fichier de démarrage
    with open(STARTED_FILE, 'w') as f:
        f.write(f"Started at {datetime.now().isoformat()}")
    log(f"Created start marker file: {STARTED_FILE}")
    
    try:
        if args.batch:
            # Mode batch
            log(f"Loading batch file: {args.batch}")
            with open(args.batch, 'r', encoding='utf-8') as f:
                page_ids = json.load(f)
            
            if not isinstance(page_ids, list):
                raise ValueError("Le fichier JSON doit contenir une liste d'IDs")
            
            log(f"Loaded {len(page_ids)} page IDs from batch file")
            await process_batch(page_ids, args.headless)
        else:
            # Mode single
            if not args.page_id:
                raise ValueError("page_id est requis en mode single")
            
            await process_single(args.page_id, args.headless)
    
    except Exception as e:
        log(f"FATAL ERROR: {e}", "ERROR")
        import traceback
        log(f"Traceback:\n{traceback.format_exc()}", "ERROR")
        
        error_result = {
            'success': False,
            'error': str(e),
            'date_ajout': datetime.now().strftime('%d-%m-%Y %H:%M:%S')
        }
        
        with open(RESULT_FILE, 'w', encoding='utf-8') as f:
            json.dump(error_result, f, indent=2, ensure_ascii=False)
        
        update_status("error", 0, 0, "", f"Erreur: {str(e)}", [])
        
        raise
    
    log("=== Facebook ID Scraper HYBRID Finished ===")

if __name__ == "__main__":
    asyncio.run(main())