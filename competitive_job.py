"""
competitive_job.py
Script autonome de veille concurrentielle
Fonctionne indépendamment de Streamlit
"""

import json
import os
import sys
import time
import random
import logging
from datetime import datetime, timedelta
from pathlib import Path

# Utiliser playwright en mode synchrone
from playwright.sync_api import sync_playwright

# ============================================
# CONFIGURATION
# ============================================

WHITELIST_FILE = "whitelist.json"
DAILY_REPORT_FILE = "daily_competitive_reports.json"
STATUS_FILE = "competitive_status.json"
LOG_FILE = "competitive_job.log"

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Configuration de la veille
SCRAPING_CONFIG = {
    'headless': True,
    'pause_min': 2,
    'pause_max': 10,
    'max_ads': 100,
    'max_time': 5,  # minutes
    'pause_between_competitors_min': 30,  # 30 secondes
    'pause_between_competitors_max': 180,  # 3 minutes
}

# ============================================
# FONCTIONS UTILITAIRES
# ============================================

def load_json(filename, default=None):
    """Charge un fichier JSON"""
    if os.path.exists(filename):
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Erreur lecture {filename}: {e}")
            return default if default is not None else []
    return default if default is not None else []


def save_json(filename, data):
    """Sauvegarde dans un fichier JSON"""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return True
    except Exception as e:
        logger.error(f"Erreur sauvegarde {filename}: {e}")
        return False


def update_status(status_data):
    """Met à jour le fichier de statut pour Streamlit"""
    status_data['last_update'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    save_json(STATUS_FILE, status_data)


def clear_status():
    """Efface le fichier de statut"""
    if os.path.exists(STATUS_FILE):
        os.remove(STATUS_FILE)


# ============================================
# CLASSE DE SCRAPING
# ============================================

class CompetitiveIntelligenceScraper:
    def __init__(self, config):
        self.config = config
        self.ads_data = []
        self.request_count = 0
        self.start_time = None
    
    def scrape_competitor(self, page_id, page_name, date_filter):
        """Scrappe un concurrent spécifique"""
        
        # ✅ FIX: Forcer l'utilisation du bon protocole AVANT de lancer Playwright
        import asyncio
        if sys.platform == 'win32':
            # Forcer l'utilisation de ProactorEventLoop qui supporte mieux les subprocesses sur Windows
            asyncio.set_event_loop(asyncio.ProactorEventLoop())
        
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=self.config['headless'],
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox'
                ]
            )
            
            context = browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                locale='fr-FR'
            )
            
            page = context.new_page()
            
            page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => false
                });
            """)
            
            # Construction de l'URL
            #       https://web.facebook.com/ads/library/?active_status=active&ad_type=all&country=ALL&is_targeted_country=false&media_type=all&search_type=page&view_all_page_id=734097606445876
            url = f"https://web.facebook.com/ads/library/?active_status=active&ad_type=all&country=ALL&is_targeted_country=false&media_type=all&search_type=page"
            url += f"&view_all_page_id={page_id}"
            #url += f"&page_ids[0]={page_id}"
            #url += f"&q={page_name}"
            
            logger.info(f"Navigation vers: {url}")
            
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=90000)
                time.sleep(8)
                page.evaluate("window.scrollTo(0, 1000)")
                time.sleep(5)
            except Exception as e:
                logger.error(f"Erreur chargement page pour {page_name}: {e}")
                browser.close()
                raise
            
            self.start_time = time.time()
            last_count = 0
            consecutive_same_count = 0
            scroll_attempts = 0
            max_ads = self.config['max_ads']
            max_time = self.config['max_time'] * 60
            
            while len(self.ads_data) < max_ads:
                elapsed = time.time() - self.start_time
                if elapsed > max_time:
                    logger.info(f"Temps maximum atteint ({max_time}s)")
                    break
                
                # Extraire les pubs
                self._extract_ads_from_page(page)
                self.request_count += 1
                scroll_attempts += 1
                
                current_count = len(self.ads_data)
                
                logger.info(f"{page_name}: {current_count} pubs | {int(elapsed)}s | {scroll_attempts} scrolls")
                
                # Vérifier si on trouve de nouvelles pubs
                if current_count == last_count:
                    consecutive_same_count += 1
                    if consecutive_same_count >= 5:
                        logger.info(f"Aucune nouvelle pub après 5 tentatives pour {page_name}")
                        break
                else:
                    consecutive_same_count = 0
                
                last_count = current_count
                
                # Scroll
                scroll_amount = random.randint(800, 1500) if consecutive_same_count == 0 else random.randint(1500, 2500)
                page.evaluate(f"window.scrollBy(0, {scroll_amount})")
                
                # Pause
                pause_duration = random.uniform(
                    self.config['pause_min'],
                    self.config['pause_max']
                )
                time.sleep(pause_duration)
                
                # Vérifier si bas de page atteint
                is_at_bottom = page.evaluate("""
                    () => {
                        return (window.innerHeight + window.scrollY) >= document.body.offsetHeight - 100;
                    }
                """)
                
                if is_at_bottom and consecutive_same_count >= 3:
                    logger.info(f"Bas de page atteint pour {page_name}")
                    break
            
            browser.close()
            
            logger.info(f"Scraping terminé pour {page_name}: {len(self.ads_data)} publicités")
            return self.ads_data
    
    def _extract_ads_from_page(self, page):
        """Extrait les publicités de la page - VERSION SYNCHRONE"""
        new_ads = page.evaluate('''() => {
            const ads = [];
            const processedIds = new Set();
            
            const allDivs = document.querySelectorAll('div');
            
            allDivs.forEach(div => {
                const text = div.innerText || '';
                
                if (text.includes('ID dans la bibliothèque') || text.includes('Library ID')) {
                    const idMatch = text.match(/ID dans la bibliothèque[\\s:]*([0-9]+)/i) ||
                                  text.match(/Library ID[\\s:]*([0-9]+)/i);
                    
                    if (!idMatch) return;
                    
                    const adId = idMatch[1];
                    if (processedIds.has(adId)) return;
                    processedIds.add(adId);
                    
                    const adLibraryUrl = `https://www.facebook.com/ads/library/?id=${adId}`;
                    
                    let advertiser = 'N/A';
                    let pageId = 'N/A';
                    
                    const pageLinks = div.querySelectorAll('a[href*="facebook.com/"]');
                    for (let link of pageLinks) {
                        const linkText = link.innerText.trim();
                        const href = link.href;
                        
                        if (linkText && !linkText.includes('Sponsorisé') && 
                            !linkText.includes('Sponsored') &&
                            linkText.length < 100 && linkText.length > 2) {
                            advertiser = linkText;
                            
                            const pageIdMatch = href.match(/facebook\\.com\\/(\\d+)/);
                            if (pageIdMatch) {
                                pageId = pageIdMatch[1];
                            }
                            break;
                        }
                    }
                    
                    let adStatus = 'Active';
                    const statusText = text.toLowerCase();
                    if (statusText.includes('inactive') || statusText.includes('plus diffusée')) {
                        adStatus = 'Inactive';
                    }
                    
                    let startDate = 'N/A';
                    const dateMatch = text.match(/Début de la diffusion le ([^·]+)/i) ||
                                    text.match(/Started running on ([^·]+)/i) ||
                                    text.match(/(\\d{1,2}\\s+[a-zéû]+\\s+\\d{4})/i);
                    if (dateMatch) {
                        startDate = dateMatch[1].trim();
                    }
                    
                    let platforms = 'N/A';
                    if (text.includes('Plateformes') || text.includes('Platforms')) {
                        platforms = 'Multiple';
                    }
                    
                    let adText = 'N/A';
                    const sponsoredIndex = text.indexOf('Sponsorisé');
                    if (sponsoredIndex !== -1) {
                        const afterSponsored = text.substring(sponsoredIndex + 10);
                        const lines = afterSponsored.split('\\n').filter(l => l.trim().length > 20);
                        if (lines.length > 0) {
                            adText = lines[0].substring(0, 500);
                        }
                    }
                    
                    const allImages = div.querySelectorAll('img[src*="scontent"]');
                    let mediaUrl = 'N/A';
                    let mediaType = 'N/A';
                    
                    let largestImage = null;
                    let maxSize = 0;
                    
                    allImages.forEach(img => {
                        const width = img.naturalWidth || img.width || 0;
                        const height = img.naturalHeight || img.height || 0;
                        const size = width * height;
                        
                        if (size > maxSize && width > 100 && height > 100) {
                            maxSize = size;
                            largestImage = img;
                        }
                    });
                    
                    const videos = div.querySelectorAll('video[src]');
                    
                    if (videos.length > 0) {
                        mediaUrl = videos[0].src;
                        mediaType = 'video';
                    } else if (largestImage) {
                        mediaUrl = largestImage.src;
                        mediaType = 'image';
                    }
                    
                    let ctaUrl = 'N/A';
                    let ctaText = 'N/A';
                    
                    const ctaLinks = div.querySelectorAll('a[href*="l.facebook.com"], a[role="button"]');
                    
                    for (let link of ctaLinks) {
                        const linkText = link.innerText.trim();
                        
                        if (linkText && linkText.length < 50 && linkText.length > 2) {
                            const lowerText = linkText.toLowerCase();
                            
                            const ctaKeywords = [
                                'en savoir plus', 'commander', 'acheter', 'réserver',
                                'télécharger', 'essayer', 'découvrir', 'profiter',
                                'voir plus', 's\\'inscrire', 'obtenir', 'contacter',
                                'shop now', 'learn more', 'buy now', 'sign up',
                                'get', 'download', 'book', 'order'
                            ];
                            
                            const isCta = ctaKeywords.some(keyword => lowerText.includes(keyword));
                            
                            if (isCta) {
                                ctaText = linkText;
                                if (link.href && link.href.includes('l.facebook.com')) {
                                    ctaUrl = link.href;
                                }
                                break;
                            }
                        }
                    }
                    
                    ads.push({
                        ad_id: adId,
                        page_id: pageId,
                        advertiser: advertiser,
                        country: 'ALL',
                        ad_status: adStatus,
                        search_term: 'Concurrent',
                        text: adText.trim(),
                        start_date: startDate,
                        platforms: platforms,
                        media_type: mediaType,
                        media_url: mediaUrl,
                        cta_text: ctaText,
                        cta_url: ctaUrl,
                        ad_library_url: adLibraryUrl,
                        scraped_at: new Date().toISOString()
                    });
                }
            });
            
            return ads;
        }''')
        
        # Filtrer les doublons
        existing_ids = {ad['ad_id'] for ad in self.ads_data}
        
        for ad in new_ads:
            if ad['ad_id'] not in existing_ids:
                self.ads_data.append(ad)


# ============================================
# FONCTION PRINCIPALE - VERSION SYNCHRONE
# ============================================

def run_competitive_intelligence():
    """Exécute la veille concurrentielle - VERSION SYNCHRONE"""
    logger.info("=" * 60)
    logger.info("DEMARRAGE DE LA VEILLE CONCURRENTIELLE")
    logger.info("=" * 60)
    
    # Charger la whitelist
    whitelist = load_json(WHITELIST_FILE, [])
    
    if not whitelist:
        logger.warning("Aucun concurrent dans la whitelist")
        update_status({
            'status': 'error',
            'message': 'Aucun concurrent dans la whitelist',
            'progress_percent': 0
        })
        return
    
    # Créer le rapport
    report_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    report = {
        'id': report_id,
        'date': report_date,
        'competitors_scanned': 0,
        'results_count': 0,
        'results': [],
        'status': 'in_progress',
        'errors': []
    }
    
    # Sauvegarder le rapport initial
    reports = load_json(DAILY_REPORT_FILE, [])
    reports.insert(0, report)
    save_json(DAILY_REPORT_FILE, reports)
    
    # Calculer les dates (aujourd'hui et 3 jours avant)
    today = datetime.now().date()
    three_days_ago = today - timedelta(days=300)
    
    date_filter = {
        'date1': three_days_ago.strftime('%Y-%m-%d'),
        'date2': today.strftime('%Y-%m-%d')
    }
    
    total_competitors = len(whitelist)
    logger.info(f"Nombre de concurrents à scanner: {total_competitors}")
    
    # Statut initial
    update_status({
        'status': 'running',
        'current_competitor': '',
        'competitor_index': 0,
        'total_competitors': total_competitors,
        'progress_percent': 0,
        'message': 'Démarrage de la veille...',
        'results_count': 0
    })
    
    # Scanner chaque concurrent
    for index, competitor in enumerate(whitelist, start=1):
        competitor_name = competitor.get('nom_page', 'N/A')
        competitor_id = competitor.get('id_page', 'N/A')
        
        logger.info(f"\n{'='*60}")
        logger.info(f"CONCURRENT {index}/{total_competitors}: {competitor_name}")
        logger.info(f"{'='*60}")
        
        progress_percent = ((index - 1) / total_competitors) * 100
        
        update_status({
            'status': 'running',
            'current_competitor': competitor_name,
            'competitor_index': index,
            'total_competitors': total_competitors,
            'progress_percent': int(progress_percent),
            'message': f'Analyse {index}/{total_competitors}: {competitor_name}',
            'results_count': report['results_count']
        })
        
        try:
            # Créer le scraper
            scraper = CompetitiveIntelligenceScraper(SCRAPING_CONFIG)
            
            # Lancer le scraping - VERSION SYNCHRONE
            competitor_results = scraper.scrape_competitor(
                competitor_id,
                competitor_name,
                date_filter
            )
            
            # Ajouter les métadonnées
            for result in competitor_results:
                result['competitor_name'] = competitor_name
                result['competitor_id'] = competitor_id
                result['scan_date'] = report_date
            
            # Mettre à jour le rapport
            report['results'].extend(competitor_results)
            report['results_count'] = len(report['results'])
            report['competitors_scanned'] = index
            
            # Sauvegarder immédiatement
            reports = load_json(DAILY_REPORT_FILE, [])
            for i, r in enumerate(reports):
                if r['id'] == report_id:
                    reports[i] = report
                    break
            save_json(DAILY_REPORT_FILE, reports)
            
            logger.info(f"[OK] {len(competitor_results)} publicités trouvées pour {competitor_name}")
            
            # Pause entre concurrents
            if index < total_competitors:
                pause_duration = random.uniform(
                    SCRAPING_CONFIG['pause_between_competitors_min'],
                    SCRAPING_CONFIG['pause_between_competitors_max']
                )
                
                logger.info(f"Pause de {int(pause_duration/60)} minutes avant le prochain concurrent")
                
                update_status({
                    'status': 'running',
                    'current_competitor': competitor_name,
                    'competitor_index': index,
                    'total_competitors': total_competitors,
                    'progress_percent': int(progress_percent),
                    'message': f"Pause {int(pause_duration/60)} min avant prochain concurrent",
                    'results_count': report['results_count']
                })
                
                time.sleep(pause_duration)
        
        except Exception as e:
            error_msg = f"Erreur pour {competitor_name}: {str(e)}"
            logger.error(error_msg)
            
            report['errors'].append({
                'competitor': competitor_name,
                'error': str(e),
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
            
            # Sauvegarder même en cas d'erreur
            reports = load_json(DAILY_REPORT_FILE, [])
            for i, r in enumerate(reports):
                if r['id'] == report_id:
                    reports[i] = report
                    break
            save_json(DAILY_REPORT_FILE, reports)
    
    # Finaliser le rapport
    report['status'] = 'completed'
    reports = load_json(DAILY_REPORT_FILE, [])
    for i, r in enumerate(reports):
        if r['id'] == report_id:
            reports[i] = report
            break
    save_json(DAILY_REPORT_FILE, reports)
    
    # Statut final
    update_status({
        'status': 'completed',
        'current_competitor': '',
        'competitor_index': total_competitors,
        'total_competitors': total_competitors,
        'progress_percent': 100,
        'message': f"Veille terminée ! {report['results_count']} publicités trouvées",
        'results_count': report['results_count']
    })
    
    logger.info("=" * 60)
    logger.info(f"VEILLE TERMINEE: {report['results_count']} publicités trouvées")
    logger.info("=" * 60)


# ============================================
# POINT D'ENTRÉE
# ============================================

if __name__ == "__main__":
    try:
        # VERSION SYNCHRONE - Pas besoin de loop asyncio !
        run_competitive_intelligence()
        
    except Exception as e:
        logger.error(f"ERREUR FATALE: {e}", exc_info=True)
        update_status({
            'status': 'error',
            'message': f'Erreur: {str(e)}',
            'progress_percent': 0
        })
        sys.exit(1)