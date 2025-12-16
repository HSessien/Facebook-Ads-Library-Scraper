"""
Script pour récupérer les ID permanents manquants dans la whitelist
Utilise les cookies sauvegardés pour la connexion automatique
En cas d'échec, demande une connexion manuelle avec confirmation
"""

import sys
import json
import asyncio
from datetime import datetime
from playwright.async_api import async_playwright
from pathlib import Path
import io

# Fix pour l'encodage Windows
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# Fichiers de configuration
WHITELIST_FILE = "whitelist.json"
STATUS_FILE = "fb_update_status.json"
DEBUG_HTML_FILE = "debug_page_content.html"
COOKIES_FILE = "fb_cookies.json"

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

def update_status(status, current=0, total=0, current_page="", message="", processed=None, waiting_login=False):
    """Met à jour le fichier de statut pour Streamlit"""
    status_data = {
        "status": status,
        "current": current,
        "total": total,
        "current_page": current_page,
        "message": message,
        "processed": processed or [],
        "waiting_login": waiting_login,
        "last_update": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    with open(STATUS_FILE, 'w', encoding='utf-8') as f:
        json.dump(status_data, f, indent=2, ensure_ascii=False)
    log(f"Status: {status} - {message}")

def parse_date_french(date_str):
    """Convertit une date française en format DD/MM/YYYY"""
    if not date_str or date_str == "N/A":
        return "N/A"
    
    try:
        parts = date_str.strip().split()
        if len(parts) == 3:
            day = parts[0].zfill(2)
            month_name = parts[1].lower()
            year = parts[2]
            
            if month_name in MOIS_FR:
                month = MOIS_FR[month_name]
                return f"{day}/{month}/{year}"
    except Exception as e:
        log(f"Error parsing date: {e}", "ERROR")
    return date_str

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
    log(f"Whitelist file not found")
    return []

def save_whitelist(data):
    """Sauvegarde la whitelist"""
    log(f"Saving {len(data)} items to whitelist")
    with open(WHITELIST_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    log(f"Whitelist saved successfully")

def get_missing_ids():
    """Retourne la liste des pages sans id_permanent"""
    whitelist = load_whitelist()
    missing = []
    
    for item in whitelist:
        id_permanent = item.get('id_permanent')
        # Vérifier si id_permanent est manquant, vide, null ou N/A
        if not id_permanent or id_permanent == "N/A" or id_permanent == "":
            missing.append(item)
            log(f"Found page without permanent ID: {item.get('nom_page')} (id_page: {item.get('id_page')})")
    
    log(f"Total pages without permanent ID: {len(missing)}")
    return missing

def load_cookies():
    """Charge les cookies sauvegardés"""
    if Path(COOKIES_FILE).exists():
        try:
            with open(COOKIES_FILE, 'r', encoding='utf-8') as f:
                cookies = json.load(f)
                log(f"✅ Cookies loaded successfully ({len(cookies)} cookies)")
                return cookies
        except Exception as e:
            log(f"⚠️ Error loading cookies: {e}", "WARNING")
            return None
    else:
        log("ℹ️ No cookies file found")
        return None

def save_cookies(cookies):
    """Sauvegarde les cookies"""
    try:
        with open(COOKIES_FILE, 'w', encoding='utf-8') as f:
            json.dump(cookies, f, indent=2, ensure_ascii=False)
        log(f"✅ Cookies saved successfully ({len(cookies)} cookies)")
    except Exception as e:
        log(f"⚠️ Error saving cookies: {e}", "ERROR")

async def check_if_logged_in(page):
    """Vérifie si l'utilisateur est connecté à Facebook"""
    try:
        is_logged_in = await page.evaluate("""
            () => {
                # Méthode 1: Chercher l'input email (page de login)
                const emailInput = document.querySelector('input[name="email"]');
                if (emailInput) {
                    return false; # Toujours sur la page de login
                }
                
                # Méthode 2: Chercher des éléments typiques d'une page connectée
                const loggedInIndicators = [
                    'div[role="banner"]',
                    'a[aria-label*="rofil"]',
                    'svg[aria-label*="Facebook"]',
                ];
                
                for (const selector of loggedInIndicators) {
                    if (document.querySelector(selector)) {
                        return true;
                    }
                }
                
                # Méthode 3: Vérifier l'URL
                const url = window.location.href;
                if (url.includes('/login') || url.includes('/checkpoint')) {
                    return false;
                }
                
                return false;
            }
        """)
        
        return is_logged_in
    except Exception as e:
        log(f"Error checking login status: {e}", "WARNING")
        return False

async def wait_for_manual_confirmation(page):
    """
    Attend que l'utilisateur se connecte puis confirme via un popup
    Le popup apparaît après 10 secondes avec 3 options
    Attend obligatoirement le clic sur "Oui, connecté"
    """
    log("=" * 60)
    log("🔐 CONNEXION FACEBOOK REQUISE")
    log("=" * 60)
    log("📌 Connectez-vous à Facebook dans le navigateur")
    log("⏳ Un popup de confirmation apparaîtra dans 10 secondes...")
    
    update_status("waiting_login", 0, 0, "", 
                  "🔐 Connectez-vous à Facebook (popup dans 10s)", 
                  [], waiting_login=True)
    
    # Laisser 10 secondes pour se connecter
    await asyncio.sleep(10)
    
    log("✅ Affichage du popup de confirmation")
    
    while True:
        # Injecter le popup de confirmation
        await page.evaluate("""
            () => {
                # Supprimer ancien popup si existe
                const oldPopup = document.getElementById('fb-login-popup');
                const oldOverlay = document.getElementById('fb-login-overlay');
                if (oldPopup) oldPopup.remove();
                if (oldOverlay) oldOverlay.remove();
                
                # Créer overlay semi-transparent
                const overlay = document.createElement('div');
                overlay.id = 'fb-login-overlay';
                overlay.style.cssText = `
                    position: fixed;
                    top: 0;
                    left: 0;
                    width: 100%;
                    height: 100%;
                    background: rgba(0,0,0,0.7);
                    z-index: 999998;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                `;
                
                # Créer le popup compact
                const popup = document.createElement('div');
                popup.id = 'fb-login-popup';
                popup.style.cssText = `
                    background: white;
                    padding: 20px;
                    border-radius: 8px;
                    box-shadow: 0 4px 20px rgba(0,0,0,0.4);
                    text-align: center;
                    font-family: Arial, sans-serif;
                    max-width: 400px;
                `;
                
                popup.innerHTML = `
                    <h3 style="margin: 0 0 15px 0; color: #1877f2; font-size: 18px;">
                        🔐 Connexion Facebook
                    </h3>
                    <p style="margin: 0 0 20px 0; font-size: 14px; color: #555;">
                        Êtes-vous complètement connecté ?<br>
                        <small>(Toutes les étapes de validation terminées)</small>
                    </p>
                    <div style="display: flex; flex-direction: column; gap: 10px;">
                        <button id="btn-yes" style="
                            background: #28a745;
                            color: white;
                            border: none;
                            padding: 10px 20px;
                            border-radius: 5px;
                            font-size: 14px;
                            font-weight: bold;
                            cursor: pointer;
                        ">✅ Oui, connecté</button>
                        <button id="btn-wait" style="
                            background: #ffc107;
                            color: #333;
                            border: none;
                            padding: 10px 20px;
                            border-radius: 5px;
                            font-size: 14px;
                            font-weight: bold;
                            cursor: pointer;
                        ">⏳ Attendre 30 sec</button>
                        <button id="btn-cancel" style="
                            background: #dc3545;
                            color: white;
                            border: none;
                            padding: 10px 20px;
                            border-radius: 5px;
                            font-size: 14px;
                            font-weight: bold;
                            cursor: pointer;
                        ">❌ Annuler</button>
                    </div>
                `;
                
                overlay.appendChild(popup);
                document.body.appendChild(overlay);
                
                # Réinitialiser la réponse
                window.loginConfirmation = null;
                
                document.getElementById('btn-yes').onclick = () => {
                    window.loginConfirmation = 'yes';
                    overlay.remove();
                };
                
                document.getElementById('btn-wait').onclick = () => {
                    window.loginConfirmation = 'wait';
                    overlay.remove();
                };
                
                document.getElementById('btn-cancel').onclick = () => {
                    window.loginConfirmation = 'cancel';
                    overlay.remove();
                };
            }
        """)
        
        # Attendre la réponse (boucle infinie jusqu'à une action)
        log("⏳ En attente de la réponse utilisateur...")
        update_status("waiting_login", 0, 0, "", 
                      "🔘 Cliquez sur un bouton dans le popup", 
                      [], waiting_login=True)
        
        response = None
        while response is None:
            await asyncio.sleep(0.5)
            response = await page.evaluate("() => window.loginConfirmation")
        
        if response == 'yes':
            log("✅ User confirmed connection")
            return True
        elif response == 'cancel':
            log("❌ User cancelled")
            return False
        elif response == 'wait':
            log("⏳ User requested 30 more seconds - waiting...")
            update_status("waiting_login", 0, 0, "", 
                          "⏳ 30 secondes supplémentaires... Le popup réapparaîtra", 
                          [], waiting_login=True)
            await asyncio.sleep(30)
            log("⏰ 30 seconds elapsed - showing popup again")
            # La boucle while True va réafficher le popup
            continue

async def scrape_permanent_id(id_page, page, context):
    """
    Récupère l'ID permanent d'une page Facebook
    Utilise le contexte de navigateur partagé (déjà connecté)
    """
    log(f"=== Starting scraping for id_page: {id_page} ===")
    
    try:
        url = f"https://web.facebook.com/profile.php?id={id_page}&sk=about_profile_transparency"
        log(f"Navigating to: {url}")
        
        # Attendre networkidle avec fallback
        try:
            await page.goto(url, wait_until="networkidle", timeout=45000)
            log("Page loaded (networkidle)")
        except Exception as nav_error:
            log(f"NetworkIdle timeout, using domcontentloaded: {nav_error}", "WARNING")
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            log("Page loaded (domcontentloaded)")
        
        await asyncio.sleep(5)
        
        # Vérifier si on est redirigé vers le login
        login_form = await page.query_selector('input[name="email"]')
        if login_form:
            log("⚠️ Login page detected again - Session may have expired")
            return None, "Session expirée - reconnexion nécessaire"
        
        # Scrolling stratégique
        log("Strategic scrolling...")
        await page.evaluate("window.scrollTo(0, 500)")
        await asyncio.sleep(1)
        await page.evaluate("window.scrollTo(0, 0)")
        await asyncio.sleep(2)
        
        # Tentatives multiples
        extraction_result = None
        for attempt in range(3):
            log(f"Extraction attempt {attempt + 1}/3...")
            
            extraction_result = await page.evaluate("""
                () => {
                    let permanentId = null;
                    let pageCreationDate = null;
                    let pageName = null;
                    
                    const title = document.querySelector('title');
                    if (title) {
                        pageName = title.textContent.replace(/\\s*\\|\\s*Facebook.*$/i, '').trim();
                    }
                    
                    const allSpans = document.querySelectorAll('span');
                    
                    for (let i = 0; i < allSpans.length; i++) {
                        const span = allSpans[i];
                        const text = span.textContent.trim();
                        
                        if (!permanentId) {
                            if (i + 1 < allSpans.length) {
                                const nextSpan = allSpans[i + 1];
                                const nextText = nextSpan.textContent.trim();
                                
                                if (nextText === 'ID de la Page' || nextText === 'Page ID') {
                                    if (/^\\d{10,}$/.test(text)) {
                                        permanentId = text;
                                    }
                                }
                            }
                            
                            if (i > 0 && !permanentId) {
                                const prevSpan = allSpans[i - 1];
                                const prevText = prevSpan.textContent.trim();
                                
                                if ((text === 'ID de la Page' || text === 'Page ID') && /^\\d{10,}$/.test(prevText)) {
                                    permanentId = prevText;
                                }
                            }
                        }
                        
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
            
            if extraction_result.get('permanentId'):
                log(f"[SUCCESS] Found permanent ID: {extraction_result['permanentId']}")
                break
            
            if attempt < 2:
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(2)
        
        # Sauvegarder HTML pour debug
        html_content = await page.content()
        with open(DEBUG_HTML_FILE, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        if extraction_result and extraction_result.get('permanentId'):
            return {
                'permanentId': extraction_result['permanentId'],
                'pageName': extraction_result.get('pageName'),
                'pageCreationDate': extraction_result.get('pageCreationDate')
            }, None
        else:
            return None, "ID permanent non trouvé"
            
    except Exception as e:
        log(f"[EXCEPTION] {e}", "ERROR")
        import traceback
        log(traceback.format_exc(), "ERROR")
        return None, str(e)

async def update_missing_permanent_ids():
    """
    Fonction principale : récupère les ID permanents manquants
    Utilise les cookies sauvegardés ou demande une connexion manuelle
    """
    log("=== UPDATE MISSING PERMANENT IDS - STARTED ===")
    
    # Charger la whitelist
    whitelist = load_whitelist()
    if not whitelist:
        log("Whitelist is empty", "ERROR")
        update_status("error", 0, 0, "", "La whitelist est vide", [])
        return
    
    # Identifier les pages sans ID permanent
    missing_pages = get_missing_ids()
    
    if not missing_pages:
        log("No pages without permanent ID found")
        update_status("completed", 0, 0, "", "Aucune page sans ID permanent", [])
        return
    
    total = len(missing_pages)
    log(f"Found {total} pages to process")
    update_status("running", 0, total, "", f"Démarrage du navigateur...", [])
    
    try:
        async with async_playwright() as p:
            # Lancer le navigateur en mode VISIBLE
            browser = await p.chromium.launch(
                headless=False,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--start-maximized'
                ]
            )
            log("Browser launched in VISIBLE mode")
            
            context = await browser.new_context(
                viewport={'width': 1280, 'height': 720},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                locale='fr-FR'
            )
            
            # Charger les cookies sauvegardés
            saved_cookies = load_cookies()
            if saved_cookies:
                log("🔄 Attempting to use saved cookies...")
                update_status("running", 0, total, "", "Tentative de connexion avec cookies...", [])
                await context.add_cookies(saved_cookies)
            
            page = await context.new_page()
            
            # Désactiver webdriver detection
            await page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => false
                });
            """)
            
            # Tester la connexion
            log("Testing connection to Facebook...")
            update_status("running", 0, total, "", "Vérification de la connexion...", [])
            
            await page.goto("https://www.facebook.com/", wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(3)
            
            # Vérifier si connecté
            is_logged_in = await check_if_logged_in(page)
            
            if not is_logged_in:
                log("❌ Not logged in - Manual login required")
                
                # Aller sur la page de login
                await page.goto("https://www.facebook.com/login", wait_until="domcontentloaded")
                await asyncio.sleep(2)
                
                # Attendre la confirmation manuelle
                confirmed = await wait_for_manual_confirmation(page)
                
                if not confirmed:
                    log("User cancelled login", "WARNING")
                    update_status("error", 0, 0, "", "Connexion annulée par l'utilisateur", [])
                    await browser.close()
                    return
                
                # Vérifier à nouveau la connexion
                await asyncio.sleep(2)
                is_logged_in = await check_if_logged_in(page)
                
                if not is_logged_in:
                    log("❌ Still not logged in after confirmation", "ERROR")
                    update_status("error", 0, 0, "", "Connexion non détectée", [])
                    
                    # NE PAS fermer le navigateur - laisser l'utilisateur réessayer
                    log("⚠️ Keeping browser open for retry...")
                    update_status("waiting_login", 0, total, "", 
                                  "⚠️ Connexion échouée - Le navigateur reste ouvert", 
                                  [], waiting_login=True)
                    
                    # Redemander confirmation
                    log("🔄 Retrying login confirmation...")
                    confirmed = await wait_for_manual_confirmation(page)
                    
                    if not confirmed:
                        await browser.close()
                        return
                    
                    # Revérifier
                    await asyncio.sleep(2)
                    is_logged_in = await check_if_logged_in(page)
                    
                    if not is_logged_in:
                        log("❌ Login failed after retry", "ERROR")
                        update_status("error", 0, 0, "", "Impossible de se connecter", [])
                        await browser.close()
                        return
                
                # Sauvegarder les cookies
                log("💾 Saving cookies for future use...")
                cookies = await context.cookies()
                save_cookies(cookies)
            
            log("✅ Successfully logged in!")
            update_status("running", 0, total, "", "Connexion réussie - Démarrage du traitement...", [])
            await asyncio.sleep(2)
            
            # Maintenant on peut traiter toutes les pages
            processed = []
            success_count = 0
            
            for i, page_item in enumerate(missing_pages, 1):
                id_page = page_item.get('id_page')
                nom_page = page_item.get('nom_page', 'Inconnu')
                
                log(f"\n--- Processing {i}/{total}: {nom_page} (id_page: {id_page}) ---")
                update_status("running", i, total, nom_page, f"Traitement de {nom_page}...", processed)
                
                # Scraper l'ID permanent
                result, error = await scrape_permanent_id(id_page, page, context)
                
                if result:
                    # Mettre à jour l'entrée dans la whitelist
                    for item in whitelist:
                        if item.get('id_page') == id_page:
                            item['id_permanent'] = result['permanentId']
                            item['success'] = True
                            
                            if result.get('pageName'):
                                item['nom_page'] = result['pageName']
                            
                            if result.get('pageCreationDate'):
                                item['date_creation'] = parse_date_french(result['pageCreationDate'])
                            
                            log(f"[SUCCESS] Updated {nom_page} with permanent ID: {result['permanentId']}")
                            success_count += 1
                            
                            processed.append({
                                'nom_page': item['nom_page'],
                                'id_page': id_page,
                                'id_permanent': result['permanentId'],
                                'status': 'success'
                            })
                            break
                else:
                    log(f"[FAILED] Could not get permanent ID for {nom_page}: {error}", "WARNING")
                    processed.append({
                        'nom_page': nom_page,
                        'id_page': id_page,
                        'id_permanent': None,
                        'status': 'failed',
                        'error': error
                    })
                
                # Sauvegarder après chaque mise à jour
                save_whitelist(whitelist)
                
                # Pause entre chaque page
                if i < total:
                    log("Waiting 3 seconds before next page...")
                    await asyncio.sleep(3)
            
            log(f"\n=== UPDATE COMPLETED: {success_count}/{total} pages updated ===")
            update_status("completed", total, total, "", f"{success_count}/{total} page(s) mise(s) à jour", processed)
            
            # Sauvegarder les cookies à la fin
            log("💾 Saving cookies for future use...")
            cookies = await context.cookies()
            save_cookies(cookies)
            
            # Laisser le navigateur ouvert 5 secondes pour voir le résultat
            await asyncio.sleep(5)
            await browser.close()
            
    except Exception as e:
        log(f"FATAL ERROR: {e}", "ERROR")
        import traceback
        log(traceback.format_exc(), "ERROR")
        update_status("error", 0, 0, "", f"Erreur: {str(e)}", [])
        raise

async def main():
    """Point d'entrée"""
    try:
        await update_missing_permanent_ids()
    except Exception as e:
        log(f"FATAL ERROR: {e}", "ERROR")
        import traceback
        log(traceback.format_exc(), "ERROR")
        update_status("error", 0, 0, "", f"Erreur: {str(e)}", [])
        raise

if __name__ == "__main__":
    asyncio.run(main())