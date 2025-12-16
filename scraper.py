import streamlit as st
import asyncio
import queue
from playwright.async_api import async_playwright
import pandas as pd
import json
import os
import sys
import time
import random
import threading
import schedule
from datetime import datetime, timedelta
from pathlib import Path
import logging
import nest_asyncio
import subprocess
from urllib.parse import quote
from urllib.parse import unquote
nest_asyncio.apply()

# ============================================
# CONFIGURATION POUR MASQUER LE TERMINAL
# ============================================
#if sys.platform == 'win32':
#    import ctypes
#    ctypes.windll.user32.ShowWindow(ctypes.windll.kernel32.GetConsoleWindow(), 0)

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('competitive_intelligence.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================
# CONFIGURATION DU LOGGING AMÉLIORÉ
# ============================================

# Fonction helper pour logger sans emojis sur Windows
def safe_log(level, message):
    """Log un message en retirant les emojis si nécessaire"""
    try:
        if level == 'info':
            logger.info(message)
        elif level == 'warning':
            logger.warning(message)
        elif level == 'error':
            logger.error(message)
    except UnicodeEncodeError:
        # Retirer les emojis et réessayer
        clean_message = message.encode('ascii', 'ignore').decode('ascii')
        if level == 'info':
            logger.info(clean_message)
        elif level == 'warning':
            logger.warning(clean_message)
        elif level == 'error':
            logger.error(clean_message)

# File pour la communication entre threads
progress_queue = queue.Queue()

# Configuration de la page
st.set_page_config(
    page_title="Facebook Ads Scraper",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS personnalisé
st.markdown("""
    <style>
    .main {
        padding: 2rem;
    }
    .stButton>button {
        width: 100%;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        font-weight: bold;
        padding: 0.75rem;
        border-radius: 8px;
        border: none;
    }
    .clickable-link {
        color: #1e90ff;
        text-decoration: none;
    }
    .clickable-link:hover {
        text-decoration: underline;
    }
    </style>
""", unsafe_allow_html=True)

# ============================================
# GESTION DES FICHIERS DE CONFIGURATION
# ============================================

CONFIG_FILE = "config.json"
BLACKLIST_FILE = "blacklist.json"
WHITELIST_FILE = "whitelist.json"
HISTORY_FILE = "scraping_history.json"
DAILY_REPORT_FILE = "daily_competitive_reports.json"
SCRAPING_STATE_FILE = "scraping_state.json"
FB_ID_STATUS_FILE = "fb_id_status.json"
FB_ID_RESULT_FILE = "fb_id_result.json"

# Mapping des codes pays vers noms
COUNTRY_NAMES = {
    "ALL": "Tous les pays",
    "AU": "Australie",
    "BE": "Belgique",
    "BJ": "Bénin",
    "BF": "Burkina Faso",
    "BI": "Burundi",
    "CM": "Cameroun",
    "CA": "Canada",
    "CF": "Centrafrique",
    "KM": "Comores",
    "CG": "Congo",
    "CD": "Congo (RDC)",
    "CI": "Côte d'Ivoire",
    "DJ": "Djibouti",
    "FR": "France",
    "GA": "Gabon",
    "GN": "Guinée",
    "GW": "Guinée-Bissau",
    "GQ": "Guinée équatoriale",
    "IT": "Italie",
    "JP": "Japon",
    "LU": "Luxembourg",
    "MG": "Madagascar",
    "ML": "Mali",
    "MA": "Maroc",
    "MR": "Mauritanie",
    "MU": "Maurice",
    "NE": "Niger",
    "NG": "Nigéria",
    "NL": "Pays-Bas",
    "BR": "Brésil",
    "ES": "Espagne",
    "CH": "Suisse",
    "RW": "Rwanda",
    "SN": "Sénégal",
    "SC": "Seychelles",
    "TG": "Togo",
    "TN": "Tunisie",
    "GB": "Royaume-Uni",
    "IN": "Inde",
    "MX": "Mexique",
    "US": "États-Unis",
}

def load_config():
    """Charge la configuration depuis config.json"""
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    else:
        return {
            "headless": False,
            "pause_min": 2,
            "pause_max": 5,
            "max_ads": 500,
            "max_time": 30,
            "auto_scrape_enabled": False,
            "auto_scrape_time": "08:00"
        }

def save_config(config):
    """Sauvegarde la configuration dans config.json"""
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2)

def load_blacklist():
    """Charge la blacklist depuis le JSON"""
    if os.path.exists(BLACKLIST_FILE):
        try:
            with open(BLACKLIST_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return []
    return []

def save_blacklist(blacklist):
    """Sauvegarde la blacklist dans le JSON"""
    with open(BLACKLIST_FILE, 'w', encoding='utf-8') as f:
        json.dump(blacklist, f, indent=2, ensure_ascii=False)

def load_whitelist():
    """Charge la whitelist depuis le JSON"""
    if os.path.exists(WHITELIST_FILE):
        try:
            with open(WHITELIST_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return []
    return []

def save_whitelist(whitelist):
    """Sauvegarde la whitelist dans le JSON"""
    with open(WHITELIST_FILE, 'w', encoding='utf-8') as f:
        json.dump(whitelist, f, indent=2, ensure_ascii=False)

def load_history():
    """Charge l'historique des scraping"""
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return []
    return []

def save_history(history):
    """Sauvegarde l'historique des scraping"""
    with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
        json.dump(history, f, indent=2, ensure_ascii=False)

def load_daily_reports():
    """Charge les rapports journaliers de veille concurrentielle"""
    if os.path.exists(DAILY_REPORT_FILE):
        try:
            with open(DAILY_REPORT_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return []
    return []

def save_daily_reports(reports):
    """Sauvegarde les rapports journaliers"""
    with open(DAILY_REPORT_FILE, 'w', encoding='utf-8') as f:
        json.dump(reports, f, indent=2, ensure_ascii=False)

def load_scraping_state():
    """Charge l'état du scraping en cours"""
    if os.path.exists(SCRAPING_STATE_FILE):
        try:
            with open(SCRAPING_STATE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return None
    return None

def save_scraping_state(state):
    """Sauvegarde l'état du scraping en cours"""
    with open(SCRAPING_STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=2, ensure_ascii=False)

def clear_scraping_state():
    """Efface l'état du scraping"""
    if os.path.exists(SCRAPING_STATE_FILE):
        os.remove(SCRAPING_STATE_FILE)

def add_to_history(query_info, results_count, results_data, url=None, status="success", error_message=None, entry_id=None):
    """Ajoute ou met à jour une entrée dans l'historique"""
    history = load_history()
    
    # Si un ID est fourni, chercher et mettre à jour l'entrée existante
    if entry_id:
        for i, entry in enumerate(history):
            if entry.get('id') == entry_id:
                history[i]['results_count'] = results_count
                history[i]['results'] = results_data
                history[i]['status'] = status
                if error_message:
                    history[i]['error_message'] = error_message
                save_history(history)
                return entry_id
    
    # Sinon créer une nouvelle entrée
    new_id = entry_id or datetime.now().strftime('%Y%m%d_%H%M%S')
    entry = {
        "id": new_id,
        "date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "query": query_info,
        "results_count": results_count,
        "results": results_data,
        "status": status,
        "error_message": error_message
    }
    
    if url:
        entry["url"] = url
    
    history.insert(0, entry)
    save_history(history)
    return new_id

def update_history_incrementally(entry_id, new_results):
    """Met à jour progressivement une entrée d'historique avec de nouveaux résultats"""
    history = load_history()
    for i, entry in enumerate(history):
        if entry.get('id') == entry_id:
            history[i]['results'].extend(new_results)
            history[i]['results_count'] = len(history[i]['results'])
            save_history(history)
            return True
    return False

def get_permanent_id_from_script(page_profile_id, list_type, headless=True):
    """
    Appelle le script fb_id_retriever.py pour récupérer l'ID permanent
    
    Args:
        page_profile_id: ID de profil de la page
        list_type: 'blacklist' ou 'whitelist'
        headless: Mode invisible du navigateur
    
    Returns:
        dict: Résultat du script (success, id_permanent, nom_page, etc.)
    """
    try:
        # Nettoyer les anciens fichiers
        for old_file in [FB_ID_RESULT_FILE, FB_ID_STATUS_FILE]:
            if os.path.exists(old_file):
                os.remove(old_file)
        
        # Construire la commande
        cmd = [
            sys.executable, 
            'fb_id_retriever.py', 
            str(page_profile_id), 
            list_type
        ]
        
        if headless:
            cmd.append('--headless')
        
        # Lancer le script
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding='utf-8'
        )
        
        # Attendre la fin
        stdout, stderr = process.communicate(timeout=90)
        
        # Attendre que le fichier soit écrit
        time.sleep(2)
        
        # Lire le résultat
        if os.path.exists(FB_ID_RESULT_FILE):
            with open(FB_ID_RESULT_FILE, 'r', encoding='utf-8') as f:
                result = json.load(f)
            return result
        else:
            return {
                'success': False,
                'error': f"Pas de résultat.\n\nSTDOUT:\n{stdout}\n\nSTDERR:\n{stderr}"
            }
            
    except subprocess.TimeoutExpired:
        process.kill()
        return {
            'success': False,
            'error': "Timeout (>90s)"
        }
    except Exception as e:
        import traceback
        return {
            'success': False,
            'error': f"Exception: {str(e)}\n\n{traceback.format_exc()}"
        }
        
def get_fb_id_status():
    """Récupère le statut actuel du processus de récupération d'ID"""
    if os.path.exists(FB_ID_STATUS_FILE):
        try:
            with open(FB_ID_STATUS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return None
    return None

def add_pages_to_list_batch(page_ids, list_type, source_id, config=None):
    """
    Ajoute un batch de pages à la blacklist ou whitelist
    
    Args:
        page_ids: 
            - Pour blacklist: [{'page_id': '123', 'nom_page': 'Nom'}, ...]
            - Pour whitelist: ['123456', '789012', ...]
        list_type: 'blacklist' ou 'whitelist'
        source_id: Identifiant unique pour le fichier temporaire
        config: Configuration (requis uniquement pour whitelist)
    
    Returns:
        dict: {
            'success': bool,
            'added_count': int,
            'total_count': int,
            'message': str,
            'skipped_count': int (optionnel)
        }
    """
    from datetime import datetime
    
    # ============================================
    # BLACKLIST - AJOUT DIRECT SANS SCRAPING
    # ============================================
    if list_type == 'blacklist':
        try:
            # Charger la blacklist actuelle
            current_blacklist = load_blacklist()
            
            # Extraire les page_ids existants pour vérification doublon
            existing_page_ids = {item.get('page_id') for item in current_blacklist}
            
            # Compteurs
            added_count = 0
            skipped_count = 0
            
            # Traiter chaque page
            for page_data in page_ids:
                page_id = page_data.get('page_id')
                nom_page = page_data.get('nom_page', 'N/A')
                
                # Vérifier si déjà existant
                if page_id in existing_page_ids:
                    skipped_count += 1
                    continue
                
                # Créer l'entrée
                new_entry = {
                    'id_page': page_id,
                    'nom_page': nom_page,
                    'date_ajout': datetime.now().strftime('%d-%m-%Y %H:%M:%S')
                }
                
                # Ajouter à la liste
                current_blacklist.append(new_entry)
                existing_page_ids.add(page_id)
                added_count += 1
            
            # Sauvegarder la blacklist mise à jour
            if added_count > 0:
                save_blacklist(current_blacklist)
            
            # Préparer le message de retour
            total_count = len(page_ids)
            
            if added_count == 0:
                message = f"⚠️ Aucune page ajoutée (toutes déjà présentes)"
            elif skipped_count == 0:
                message = f"✅ {added_count}/{total_count} page(s) ajoutée(s) à la blacklist !"
            else:
                message = f"✅ {added_count}/{total_count} page(s) ajoutée(s) à la blacklist ({skipped_count} déjà présente(s))"
            
            return {
                'success': added_count > 0,
                'added_count': added_count,
                'total_count': total_count,
                'skipped_count': skipped_count,
                'message': message
            }
        
        except Exception as e:
            return {
                'success': False,
                'added_count': 0,
                'total_count': len(page_ids) if page_ids else 0,
                'message': f"❌ Erreur lors de l'ajout à la blacklist: {str(e)}"
            }
    
    # ============================================
    # WHITELIST - AVEC SCRAPING (NON CODÉE)
    # ============================================
    elif list_type == 'whitelist':
        # TODO: Implémenter le scraping pour récupérer id_permanent
        return {
            'success': False,
            'added_count': 0,
            'total_count': len(page_ids) if page_ids else 0,
            'message': "⚠️ Ajout à la whitelist non codé pour le moment"
        }
    
    # ============================================
    # TYPE DE LISTE INVALIDE
    # ============================================
    else:
        return {
            'success': False,
            'added_count': 0,
            'total_count': 0,
            'message': f"❌ Type de liste invalide: {list_type}. Utilisez 'blacklist' ou 'whitelist'."
        }

# ============================================
# INITIALISATION DE LA SESSION
# ============================================

if 'config' not in st.session_state:
    st.session_state.config = load_config()

if 'blacklist' not in st.session_state:
    st.session_state.blacklist = load_blacklist()

if 'whitelist' not in st.session_state:
    st.session_state.whitelist = load_whitelist()

if 'current_page' not in st.session_state:
    st.session_state.current_page = "scraper"

if 'last_results' not in st.session_state:
    st.session_state.last_results = None

if 'scraping_in_progress' not in st.session_state:
    st.session_state.scraping_in_progress = False

if 'current_scraping_id' not in st.session_state:
    st.session_state.current_scraping_id = None

# ============================================
# NAVIGATION
# ============================================

def set_page(page_name):
    st.session_state.current_page = page_name
    st.rerun()

# ============================================
# AFFICHAGE DE LA PROGRESSION DANS LA SIDEBAR
# ============================================

def display_competitive_progress():
    """
    Affiche la progression de la veille concurrentielle dans la sidebar
    """
    try:
        progress_data = progress_queue.get_nowait()
        
        if 'competitive_progress' not in st.session_state:
            st.session_state.competitive_progress = {
                'progress': 0,
                'message': '',
                'competitor_index': 0,
                'total_competitors': 0
            }
        
        st.session_state.competitive_progress = progress_data
        
    except queue.Empty:
        pass
    
    # Afficher la progression si elle existe
    if 'competitive_progress' in st.session_state:
        progress_data = st.session_state.competitive_progress
        
        if progress_data['progress'] > 0 or progress_data['message']:
            st.sidebar.markdown("---")
            st.sidebar.subheader("🤖 Veille en cours")
            
            st.sidebar.progress(int(progress_data['progress']) / 100)
            st.sidebar.caption(progress_data['message'])
            
            if progress_data['total_competitors'] > 0:
                st.sidebar.caption(
                    f"Concurrent {progress_data['competitor_index']}/{progress_data['total_competitors']}"
                )
            
            # Bouton pour voir le rapport en cours
            if st.sidebar.button("📊 Voir le rapport en cours"):
                st.session_state.current_page = "competitive"
                st.rerun()

# ============================================
# SIDEBAR - NAVIGATION ET PARAMÈTRES
# ============================================

with st.sidebar:
    st.header("🧭 Navigation")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        if st.button("🏠", width="content", help="Scraper"):
            set_page("scraper")
    with col2:
        if st.button("📜", width="content", help="Historique"):
            set_page("history")
    with col3:
        if st.button("🚫", width="content", help="Blacklist"):
            set_page("blacklist")
    with col4:
        if st.button("⭐", width="content", help="Whitelist"):
            set_page("whitelist")
    
    # Bouton pour veille concurrentielle
    if st.button("📊 Veille Concurrentielle", width="stretch"):
        set_page("competitive")

    # Afficher la progression de la veille concurrentielle
    display_competitive_progress()
    
    st.markdown("---")
    
    # Options de scraping
    with st.expander("⚙️ Options de scraping", expanded=False):
        
        headless = st.toggle(
            "Mode invisible",
            value=st.session_state.config.get('headless', False),
            help="Masquer le navigateur pendant le scraping"
        )
        
        st.caption("Pause entre requêtes (secondes)")
        col1, col2 = st.columns(2)
        with col1:
            pause_min = st.number_input(
                "Min",
                min_value=1,
                max_value=30,
                value=st.session_state.config.get('pause_min', 2),
                step=1
            )
        with col2:
            pause_max = st.number_input(
                "Max",
                min_value=1,
                max_value=30,
                value=st.session_state.config.get('pause_max', 5),
                step=1
            )
        
        max_ads = st.number_input(
            "Nombre max de publicités",
            min_value=10,
            max_value=10000,
            value=st.session_state.config.get('max_ads', 500),
            step=50
        )
        
        max_time = st.number_input(
            "Temps max (minutes)",
            min_value=1,
            max_value=180,
            value=st.session_state.config.get('max_time', 30),
            step=5
        )
        
        if pause_min > pause_max:
            st.error("⚠️ Min doit être ≤ Max")

    # Statut du scheduler
    with st.expander("🤖 Veille automatique", expanded=False):

        # Configuration veille auto
        auto_enabled = st.toggle(
            "Activer la veille quotidienne",
            value=st.session_state.config.get('auto_scrape_enabled', False),
            help="Scraper automatiquement les concurrents chaque jour"
        )
        
        auto_time = st.time_input(
            "Heure de démarrage",
            value=datetime.strptime(st.session_state.config.get('auto_scrape_time', '08:00'), '%H:%M').time()
        )
        
        # Sauvegarder si changements
        new_config = {
            'headless': headless,
            'pause_min': pause_min,
            'pause_max': pause_max,
            'max_ads': max_ads,
            'max_time': max_time,
            'auto_scrape_enabled': auto_enabled,
            'auto_scrape_time': auto_time.strftime('%H:%M')
        }
        
        if new_config != st.session_state.config:
            st.session_state.config = new_config
            save_config(st.session_state.config)
            st.success("💾 Paramètres sauvegardés", icon="✅")
                    
    # Stats rapides
    with st.expander("📊 Statistiques", expanded=True):
        history = load_history()
        st.metric("Requêtes totales", len(history))
        st.metric("Blacklist", len(st.session_state.blacklist))
        st.metric("Whitelist (Concurrents)", len(st.session_state.whitelist))
        
        # Indicateur de scraping en cours
        if st.session_state.scraping_in_progress:
            st.warning("⚙️ Scraping en cours...")

# ============================================
# CLASSE DE SCRAPING AMÉLIORÉE
# ============================================

class FacebookAdsLibraryScraper:
    def __init__(self, country, status, media_type, blacklist, config, entry_id=None):
        self.country = country[0] if isinstance(country, tuple) else country
        self.status = status
        self.media_type = media_type
        self.blacklist = blacklist
        self.config = config
        self.ads_data = []
        self.progress_callback = None
        self.request_count = 0
        self.start_time = None
        self.entry_id = entry_id
        self.last_save_count = 0
        
    def set_progress_callback(self, callback):
        self.progress_callback = callback
    
    def _save_checkpoint(self):
        """Sauvegarde automatique tous les 50 résultats"""
        if len(self.ads_data) - self.last_save_count >= 50:
            if self.entry_id:
                new_ads = self.ads_data[self.last_save_count:]
                update_history_incrementally(self.entry_id, new_ads)
                self.last_save_count = len(self.ads_data)
                if self.progress_callback:
                    # ✅ CORRECTION : Si progression > 100%, on la ramène à 100%
                    progress = (len(self.ads_data) / self.config.get('max_ads', 500)) * 100
                    if progress > 100:
                        progress = 100
                        
                    self.progress_callback(
                        progress,
                        f"💾 Sauvegarde automatique : {len(self.ads_data)} pubs"
                    )
        
    async def scrape(self, keyword="", date_filter=None, max_ads=500, max_scroll_time=1800, page_id=None):
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=self.config['headless'],
                args=['--disable-blink-features=AutomationControlled']
            )
            
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                locale='fr-FR'
            )
            
            page = await context.new_page()
            
            await page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => false
                });
            """)
            
            base_url = "https://www.facebook.com/ads/library"
            url = f"{base_url}?active_status={self.status}&ad_type=all&country={self.country}"
            
            if self.media_type != "all":
                url += f"&media_type={self.media_type}"
            else:
                url += "&media_type=all"
            
            if keyword:
                url += f"&q={keyword}"
            
            # Si un page_id est fourni, l'ajouter
            if page_id:
                url += f"&search_page_ids={page_id}"
            
            # Ajouter le filtre de date
            if date_filter:
                date_type = date_filter.get('type')
                date1 = date_filter.get('date1')
                date2 = date_filter.get('date2')
                
                if date_type == "before" and date1:
                    url += f"&start_date[max]={date1}"
                elif date_type == "on" and date1:
                    url += f"&start_date[min]={date1}&start_date[max]={date1}"
                elif date_type == "after" and date1:
                    url += f"&start_date[min]={date1}"
                elif date_type == "between" and date1 and date2:
                    url += f"&start_date[min]={date1}&start_date[max]={date2}"
            
            if self.progress_callback:
                self.progress_callback(0, f"🌍 Navigation vers Facebook Ads Library...")
            
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                await asyncio.sleep(8)
                await page.evaluate("window.scrollTo(0, 1000)")
                await asyncio.sleep(5)
            except Exception as e:
                if self.progress_callback:
                    self.progress_callback(0, f"❌ Erreur de chargement: {str(e)}")
                await browser.close()
                raise Exception(f"Impossible de charger la page Facebook")
            
            self.start_time = time.time()
            last_count = 0
            consecutive_same_count = 0
            scroll_attempts = 0
            
            while len(self.ads_data) < max_ads:
                elapsed = time.time() - self.start_time
                if elapsed > max_scroll_time:
                    if self.progress_callback:
                        self.progress_callback(100, f"⏱️ Temps maximum atteint ({max_scroll_time}s)")
                    break
                
                # Pause longue après 20 minutes
                if elapsed > 1200:
                    if self.request_count % random.randint(20, 40) == 0 and self.request_count > 0:
                        pause_duration = random.uniform(120, 300)
                        if self.progress_callback:
                            self.progress_callback(
                                (len(self.ads_data) / max_ads) * 100,
                                f"⏸️ Pause longue de {int(pause_duration/60)} minutes..."
                            )
                        await asyncio.sleep(pause_duration)
                
                await self._extract_ads_from_page(page)
                self.request_count += 1
                scroll_attempts += 1
                
                # Sauvegarde automatique tous les 50 résultats
                self._save_checkpoint()
                
                current_count = len(self.ads_data)
                progress = min((current_count / max_ads) * 100, 100)
                
                if self.progress_callback:
                    self.progress_callback(
                        progress, 
                        f"📊 {current_count} pubs | ⏱️ {int(elapsed)}s | 🔄 {self.request_count} req | 📜 {scroll_attempts} scrolls"
                    )
                
                if current_count == last_count:
                    consecutive_same_count += 1
                    
                    if consecutive_same_count >= 5:
                        if self.progress_callback:
                            self.progress_callback(
                                progress,
                                f"⚠️ Aucune nouvelle pub après 5 tentatives. Arrêt."
                            )
                        break
                        
                    if consecutive_same_count >= 2:
                        scroll_amount = random.randint(2000, 3000)
                        await page.evaluate(f"window.scrollBy(0, {scroll_amount})")
                        await asyncio.sleep(4)
                        await page.evaluate(f"window.scrollBy(0, -500)")
                        await asyncio.sleep(2)
                else:
                    consecutive_same_count = 0
                
                last_count = current_count
                
                if consecutive_same_count == 0:
                    scroll_amount = random.randint(800, 1200)
                else:
                    scroll_amount = random.randint(1500, 2500)
                
                await page.evaluate(f"window.scrollBy(0, {scroll_amount})")
                
                if consecutive_same_count >= 2:
                    pause_duration = random.uniform(
                        self.config['pause_max'], 
                        self.config['pause_max'] + 3
                    )
                else:
                    pause_duration = random.uniform(
                        self.config['pause_min'], 
                        self.config['pause_max']
                    )
                
                await asyncio.sleep(pause_duration)
                
                is_at_bottom = await page.evaluate("""
                    () => {
                        return (window.innerHeight + window.scrollY) >= document.body.offsetHeight - 100;
                    }
                """)
                
                if is_at_bottom and consecutive_same_count >= 3:
                    if self.progress_callback:
                        self.progress_callback(
                            progress,
                            f"📍 Bas de page atteint. Fin du scraping."
                        )
                    break
            
            await browser.close()
            
            if self.progress_callback:
                self.progress_callback(
                    100,
                    f"✅ Scraping terminé : {len(self.ads_data)} publicités extraites"
                )
            
            return self.ads_data
    
    async def _extract_ads_from_page(self, page):
        """EXTRACTION AMÉLIORÉE avec correction des champs"""
        new_ads = await page.evaluate('''() => {
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
                    
                    // Extraire l'annonceur et l'ID de la page
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
                    
                    // ✅ CORRECTION: Extraire le statut réel de la pub
                    let adStatus = 'N/A';
                    const statusText = text.toLowerCase();
                    if (statusText.includes('inactive') || statusText.includes('plus diffusée')) {
                        adStatus = 'Inactive';
                    } else if (statusText.includes('active') || statusText.includes('en cours')) {
                        adStatus = 'Active';
                    } else {
                        // Par défaut, si on trouve une pub sur la page "active", elle est active
                        adStatus = 'Active';
                    }
                    
                    // Extraire date
                    let startDate = 'N/A';
                    const dateMatch = text.match(/Début de la diffusion le ([^·]+)/i) ||
                                    text.match(/Started running on ([^·]+)/i) ||
                                    text.match(/(\\d{1,2}\\s+[a-zéû]+\\s+\\d{4})/i);
                    if (dateMatch) {
                        startDate = dateMatch[1].trim();
                    }
                    
                    // Extraire plateformes
                    let platforms = 'N/A';
                    if (text.includes('Plateformes') || text.includes('Platforms')) {
                        platforms = 'Multiple';
                    }
                    
                    // Extraire le texte de la pub
                    let adText = 'N/A';
                    const sponsoredIndex = text.indexOf('Sponsorisé');
                    if (sponsoredIndex !== -1) {
                        const afterSponsored = text.substring(sponsoredIndex + 10);
                        const lines = afterSponsored.split('\\n').filter(l => l.trim().length > 20);
                        if (lines.length > 0) {
                            adText = lines[0].substring(0, 500);
                        }
                    }
                    
                    // ✅ CORRECTION: Récupérer la vraie image de la créative, pas le logo
                    const allImages = div.querySelectorAll('img[src*="scontent"]');
                    let mediaUrl = 'N/A';
                    let mediaType = 'N/A';
                    
                    // Filtrer pour trouver la plus grande image (créative, pas logo)
                    let largestImage = null;
                    let maxSize = 0;
                    
                    allImages.forEach(img => {
                        const width = img.naturalWidth || img.width || 0;
                        const height = img.naturalHeight || img.height || 0;
                        const size = width * height;
                        
                        // Ignorer les petites images (logos, icônes)
                        if (size > maxSize && width > 100 && height > 100) {
                            maxSize = size;
                            largestImage = img;
                        }
                    });
                    
                    // Vérifier aussi les vidéos
                    const videos = div.querySelectorAll('video[src]');
                    
                    if (videos.length > 0) {
                        mediaUrl = videos[0].src;
                        mediaType = 'video';
                    } else if (largestImage) {
                        mediaUrl = largestImage.src;
                        mediaType = 'image';
                    }
                    
                    // ✅ CORRECTION: Extraire le VRAI texte du CTA
                    let ctaUrl = 'N/A';
                    let ctaText = 'N/A';
                    
                    // Chercher les liens CTA
                    const ctaLinks = div.querySelectorAll('a[href*="l.facebook.com"], a[role="button"]');
                    
                    for (let link of ctaLinks) {
                        const linkText = link.innerText.trim();
                        
                        // Filtrer les vrais boutons CTA (courts et significatifs)
                        if (linkText && linkText.length < 50 && linkText.length > 2) {
                            const lowerText = linkText.toLowerCase();
                            
                            // Mots-clés typiques des CTA
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
                        country: window.location.href.match(/country=([A-Z]+)/)?.[1] || 'ALL',
                        ad_status: adStatus,
                        search_term: new URLSearchParams(window.location.search).get('q') || 'N/A',
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
        
        # Filtrer les pages en blacklist et les doublons
        existing_ids = {ad['ad_id'] for ad in self.ads_data}
        
        for ad in new_ads:
            if ad['ad_id'] in existing_ids:
                continue
            
            # Vérifier la blacklist
            should_ignore = False
            advertiser = ad['advertiser']
            page_id = ad['page_id']
            
            if advertiser != 'N/A' or page_id != 'N/A':
                for ignored in self.blacklist:
                    nom_page = ignored.get('nom_page', '')
                    id_page = str(ignored.get('id_page', ''))
                    
                    if (nom_page and nom_page.lower() in advertiser.lower()) or \
                       (id_page and id_page == page_id):
                        should_ignore = True
                        break
            
            if not should_ignore:
                self.ads_data.append(ad)

# ============================================
# FONCTION DE SCRAPING EN ARRIÈRE-PLAN
# ============================================

def run_scraping_task(country, status, media_type, search_term, date_filter, blacklist, config, entry_id, page_id=None):
    """Exécute le scraping dans un thread séparé"""
    try:
        st.session_state.scraping_in_progress = True
        st.session_state.current_scraping_id = entry_id
        
        scraper = FacebookAdsLibraryScraper(
            country=country,
            status=status,
            media_type=media_type,
            blacklist=blacklist,
            config=config,
            entry_id=entry_id
        )
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        results = loop.run_until_complete(
            scraper.scrape(
                keyword=search_term,
                date_filter=date_filter,
                max_ads=config['max_ads'],
                max_scroll_time=config['max_time'] * 60,
                page_id=page_id
            )
        )
        loop.close()
        
        # Mise à jour finale
        add_to_history(
            query_info={},
            results_count=len(results),
            results_data=results,
            status="success",
            entry_id=entry_id
        )
        
    except Exception as e:
        add_to_history(
            query_info={},
            results_count=0,
            results_data=[],
            status="error",
            error_message=str(e),
            entry_id=entry_id
        )
    finally:
        st.session_state.scraping_in_progress = False
        st.session_state.current_scraping_id = None
        clear_scraping_state()

# ============================================
# INTÉGRATION VEILLE CONCURRENTIELLE
# À ajouter dans scraper.py
# ============================================

def launch_competitive_intelligence():
    """Lance le script de veille en arrière-plan"""
    try:
        if sys.platform == 'win32':
            # Windows: masquer la fenêtre console
            subprocess.Popen(
                ["python", "competitive_job.py"],
                creationflags=subprocess.CREATE_NO_WINDOW
            )
        else:
            # Linux/Mac
            subprocess.Popen(["python", "competitive_job.py"])
        
        return True
    except Exception as e:
        logger.error(f"Erreur lancement veille: {e}")
        return False

def get_competitive_status():
    """Récupère le statut actuel de la veille"""
    if os.path.exists("competitive_status.json"):
        try:
            with open("competitive_status.json", 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return None
    return None

def display_competitive_progress():
    """Affiche la progression dans la sidebar"""
    status = get_competitive_status()
    
    if status and status.get('status') in ['running', 'completed']:
        st.sidebar.markdown("---")
        st.sidebar.subheader("🤖 Veille Concurrentielle")
        
        progress = status.get('progress_percent', 0)
        st.sidebar.progress(progress / 100)
        
        st.sidebar.caption(status.get('message', ''))
        
        if status.get('total_competitors', 0) > 0:
            st.sidebar.caption(
                f"Concurrent {status.get('competitor_index', 0)}/{status.get('total_competitors', 0)}"
            )
        
        st.sidebar.caption(f"📊 {status.get('results_count', 0)} pubs trouvées")
        
        last_update = status.get('last_update', '')
        if last_update:
            st.sidebar.caption(f"🕐 {last_update}")
        
        # Auto-refresh si en cours
        if status.get('status') == 'running':
            time.sleep(5)
            st.rerun()
        elif status.get('status') == 'completed':
            if st.sidebar.button("📊 Voir les résultats"):
                st.session_state.current_page = "competitive"
                st.rerun()

# ============================================
# PAGE HISTORIQUE
# ============================================

# ============================================
# PAGE HISTORIQUE - AVEC VUE FUSIONNÉE
# ============================================

if st.session_state.current_page == "history":
    st.title("📜 Historique des scraping")
    
    history = load_history()
    
    if not history:
        st.info("Aucun historique disponible. Lancez un scraping pour commencer.")
    else:
        # ============================================
        # TOGGLE ENTRE LES DEUX VUES
        # ============================================
        view_mode = st.radio(
            "Mode d'affichage",
            options=["Vue par scraping", "Vue globale fusionnée"],
            horizontal=True,
            help="Vue par scraping : voir chaque scraping séparément | Vue globale : fusionner toutes les publicités de tous les scrapings"
        )
        
        # ============================================
        # VUE PAR SCRAPING
        # ============================================
        if view_mode == "Vue par scraping":
            
            with st.expander("🔍 Filtres de recherche", expanded=False):
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    search_term = st.text_input(
                        "Recherche globale", 
                        placeholder="Annonceur, texte, page ID, pays...",
                        help="Recherche dans : annonceur, texte de recherche, page ID, pays"
                    )
                
                with col2:
                    status_filter = st.selectbox("Statut", ["Tous", "Succès", "Erreur"])
                
                with col3:
                    media_filter = st.selectbox("Type de média", ["Tous", "Image", "Vidéo", "Mixte"])
                
                col4, col5 = st.columns(2)
                
                with col4:
                    list_filter = st.selectbox(
                        "Filtrer par liste", 
                        ["Tous", "Contient pages blacklist", "Sans pages blacklist", "Contient pages whitelist", "Sans pages whitelist"]
                    )
                
                with col5:
                    if st.button("🔄 Réinitialiser les filtres"):
                        st.rerun()
            
            # Filtrer l'historique (MAINTENANT EN DEHORS DE L'EXPANDER)
            filtered_history = history
            
            # Filtre de recherche globale
            if search_term:
                filtered_history = []
                search_lower = search_term.lower()
                
                for h in history:
                    match = False
                    
                    query_str = json.dumps(h['query']).lower()
                    if search_lower in query_str:
                        match = True
                    
                    for result in h.get('results', []):
                        if (search_lower in result.get('advertiser', '').lower() or
                            search_lower in result.get('search_term', '').lower() or
                            search_lower in result.get('page_id', '').lower() or
                            search_lower in result.get('country', '').lower() or
                            search_lower in result.get('text', '').lower()):
                            match = True
                            break
                    
                    if match:
                        filtered_history.append(h)
            
            # Filtre par statut
            if status_filter == "Succès":
                filtered_history = [h for h in filtered_history if h.get('status') == 'success']
            elif status_filter == "Erreur":
                filtered_history = [h for h in filtered_history if h.get('status') == 'error']
            
            # Filtre par type de média
            if media_filter != "Tous":
                temp_filtered = []
                for h in filtered_history:
                    results = h.get('results', [])
                    if not results:
                        continue
                    
                    media_types = set(r.get('media_type', 'N/A') for r in results)
                    
                    if media_filter == "Image" and 'image' in media_types:
                        temp_filtered.append(h)
                    elif media_filter == "Vidéo" and 'video' in media_types:
                        temp_filtered.append(h)
                    elif media_filter == "Mixte" and len(media_types) > 1:
                        temp_filtered.append(h)
                
                filtered_history = temp_filtered
            
            # Filtre par listes
            if list_filter != "Tous":
                blacklist = st.session_state.blacklist
                whitelist = st.session_state.whitelist
                temp_filtered = []
                
                for h in filtered_history:
                    results = h.get('results', [])
                    if not results:
                        continue
                    
                    has_blacklist = False
                    has_whitelist = False
                    
                    for result in results:
                        advertiser = result.get('advertiser', '')
                        page_id = result.get('page_id', '')
                        
                        # Check blacklist
                        for item in blacklist:
                            nom_page = item.get('nom_page', '')
                            id_page = str(item.get('id_page', ''))
                            
                            if ((nom_page and nom_page.lower() in advertiser.lower()) or
                                (id_page and id_page == page_id)):
                                has_blacklist = True
                                break
                        
                        # Check whitelist
                        for item in whitelist:
                            nom_page = item.get('nom_page', '')
                            id_page = str(item.get('id_page', ''))
                            
                            if ((nom_page and nom_page.lower() in advertiser.lower()) or
                                (id_page and id_page == page_id)):
                                has_whitelist = True
                                break
                        
                        if has_blacklist and has_whitelist:
                            break
                    
                    if list_filter == "Contient pages blacklist" and has_blacklist:
                        temp_filtered.append(h)
                    elif list_filter == "Sans pages blacklist" and not has_blacklist:
                        temp_filtered.append(h)
                    elif list_filter == "Contient pages whitelist" and has_whitelist:
                        temp_filtered.append(h)
                    elif list_filter == "Sans pages whitelist" and not has_whitelist:
                        temp_filtered.append(h)
                
                filtered_history = temp_filtered
            
            st.markdown(f"### 📊 {len(filtered_history)} entrée(s) trouvée(s)")
            
            # Afficher l'historique (CODE EXISTANT - INCHANGÉ)
            for entry in filtered_history:
                # Afficher le nom du pays
                if 'countries' in entry['query']:
                    # Multi-pays
                    countries_display = ', '.join(entry['query']['countries'])
                else:
                    # Ancien format (compatibilité)
                    country_code = entry['query'].get('country_code', 'N/A')
                    countries_display = COUNTRY_NAMES.get(country_code, country_code)
                
                with st.expander(f"🗓️ {entry['date']} - {entry['results_count']} résultats - {countries_display} - {entry.get('status', 'unknown')}"):
                    
                    col1, col2 = st.columns([2, 1])
                    
                    with col1:
                        st.subheader("ℹ️ Informations de la requête")
                        st.json(entry['query'])
                    
                    with col2:
                        st.metric("Résultats obtenus", entry['results_count'])
                        st.metric("Statut", entry.get('status', 'unknown'))
                        
                        if entry.get('url'):
                            st.markdown("**🔗 URL de la page :**")
                            st.markdown(f"[{entry['url']}]({entry['url']})")
                        
                        if entry.get('error_message'):
                            st.error(f"Erreur: {entry['error_message']}")
                        
                        if st.button("🔄 Relancer ce scraping", key=f"rerun_{entry['id']}", width="stretch"):
                            st.session_state.rerun_params = entry['query']
                            st.session_state.current_page = "scraper"
                            st.rerun()
                    
                    if entry['results']:
                        st.markdown("---")
                        st.subheader("📊 Résultats")
                        
                        # Champ de recherche spécifique à ce scraping
                        result_search = st.text_input(
                            "🔍 Rechercher dans ces résultats",
                            key=f"search_{entry['id']}",
                            placeholder="Nom de page, texte, CTA..."
                        )
                        
                        # Filtrer les résultats si recherche
                        display_results = entry['results']
                        if result_search:
                            search_lower = result_search.lower()
                            display_results = [
                                r for r in entry['results']
                                if (search_lower in r.get('advertiser', '').lower() or
                                    search_lower in r.get('text', '').lower() or
                                    search_lower in r.get('cta_text', '').lower() or
                                    search_lower in r.get('page_id', '').lower())
                            ]
                        
                        if not display_results:
                            st.warning("Aucun résultat ne correspond à votre recherche")
                        else:
                            st.info(f"📊 {len(display_results)} résultat(s) affiché(s)")
                            
                            df = pd.DataFrame(display_results)
                            
                            # Réorganisation des colonnes
                            colonnes_a_afficher = [
                                'media_url', 'cta_url', 'ad_library_url', 'page_id',
                                'advertiser', 'country', 'ad_status', 'media_type',
                                'text', 'start_date',
                            ]
                            colonnes_existantes = [col for col in colonnes_a_afficher if col in df.columns]
                            df = df[colonnes_existantes]
                            
                            df.insert(0, '⭐ Whitelist', False)
                            df.insert(0, '🚫 Blacklist', False)
                            
                            edited_df = st.data_editor(
                                df,
                                width="stretch",
                                hide_index=False,
                                key=f"history_editor_{entry['id']}_{result_search}",
                                column_config={
                                    "🚫 Blacklist": st.column_config.CheckboxColumn("🚫", help="Ajouter à la blacklist", default=False, width="small"),
                                    "⭐ Whitelist": st.column_config.CheckboxColumn("⭐", help="Ajouter à la whitelist", default=False, width="small"),
                                    "media_url": st.column_config.LinkColumn("Média", display_text="📥 Voir", width="small"),
                                    "cta_url": st.column_config.LinkColumn("Lien CTA", display_text="🔗 Ouvrir", width="small"),
                                    "ad_library_url": st.column_config.LinkColumn("Voir pub", display_text="🔗 Ouvrir", width="small"),
                                    "page_id": st.column_config.TextColumn("Page ID", width="small"),
                                    "advertiser": st.column_config.TextColumn("Annonceur", width="medium"),
                                    "country": st.column_config.TextColumn("Pays", width="small"),
                                    "ad_status": st.column_config.TextColumn("Statut", width="small"),
                                    "media_type": st.column_config.TextColumn("Type média", width="small"),
                                    "text": st.column_config.TextColumn("Texte", width="large"),
                                    "start_date": st.column_config.TextColumn("Date début", width="medium"),
                                },
                                disabled=["media_url", "cta_url", "ad_library_url", "page_id", "advertiser", "country", "ad_status", "media_type", "text", "start_date"]
                            )
                            
                            # Validation et boutons d'ajout (code existant inchangé)
                            both_checked = edited_df[(edited_df['🚫 Blacklist'] == True) & (edited_df['⭐ Whitelist'] == True)]
                            if not both_checked.empty:
                                st.error("❌ Une page ne peut pas être à la fois en blacklist ET whitelist.")
                            else:
                                # Boutons pour ajouter aux listes
                                col_btn1, col_btn2 = st.columns(2)
                                
                                with col_btn1:
                                    blacklist_pages = edited_df[edited_df['🚫 Blacklist'] == True]
                                    if not blacklist_pages.empty:
                                        if st.button(f"✅ Ajouter {len(blacklist_pages)} page(s) à la blacklist", type="primary", key=f"add_blacklist_{entry['id']}"):
                                            # Préparer les données
                                            pages_data = [
                                                {'page_id': row['page_id'], 'nom_page': row['advertiser']}
                                                for _, row in blacklist_pages.iterrows()
                                            ]
                                            
                                            # Appeler la fonction
                                            result = add_pages_to_list_batch(
                                                page_ids=pages_data,
                                                list_type='blacklist',
                                                source_id=entry['id']
                                            )
                                            
                                            # Afficher le résultat
                                            if result['success']:
                                                st.success(result['message'])
                                                st.session_state.blacklist = load_blacklist()
                                                st.session_state.whitelist = load_whitelist()
                                                st.rerun()
                                            else:
                                                if result.get('skipped_count', 0) > 0:
                                                    st.warning(result['message'])
                                                    st.session_state.blacklist = load_blacklist()
                                                    st.session_state.whitelist = load_whitelist()
                                                else:
                                                    st.error(result['message'])
                                
                                with col_btn2:
                                    whitelist_pages = edited_df[edited_df['⭐ Whitelist'] == True]
                                    if not whitelist_pages.empty:
                                        if st.button(f"✅ Ajouter {len(whitelist_pages)} page(s) à la whitelist", type="primary", key=f"add_whitelist_{entry['id']}"):
                                            page_ids = whitelist_pages['page_id'].tolist()
                                            
                                            result = add_pages_to_list_batch(
                                                page_ids=page_ids,
                                                list_type='whitelist',
                                                source_id=entry['id'],
                                                config=st.session_state.config
                                            )
                                            
                                            if result['success']:
                                                st.success(result['message'])
                                                st.session_state.whitelist = load_whitelist()
                                                st.session_state.blacklist = load_blacklist()
                                                st.rerun()
                                            else:
                                                st.warning(result['message'])
                            
                            # Téléchargements
                            st.markdown("---")
                            col_dl1, col_dl2 = st.columns(2)
                            with col_dl1:
                                df_export = pd.DataFrame(display_results)
                                csv = df_export.to_csv(index=False, encoding='utf-8-sig')
                                st.download_button("📥 Télécharger CSV", data=csv, file_name=f"facebook_ads_{entry['id']}.csv", mime="text/csv", key=f"csv_{entry['id']}")
                            with col_dl2:
                                json_str = json.dumps(display_results, ensure_ascii=False, indent=2)
                                st.download_button("📥 Télécharger JSON", data=json_str, file_name=f"facebook_ads_{entry['id']}.json", mime="application/json", key=f"json_{entry['id']}")
        
        # ============================================
        # VUE GLOBALE FUSIONNÉE (NOUVEAU)
        # ============================================
        else:
            # Initialiser la page si nécessaire
            if 'current_page_history_fusion' not in st.session_state:
                st.session_state.current_page_history_fusion = 1
            
            # ============================================
            # FUSION DE TOUTES LES PUBLICITÉS
            # ============================================
            
            def fusionner_publicites(all_ads):
                """Fusionne les doublons en complétant les données manquantes"""
                pubs_par_id = {}
                
                for ad in all_ads:
                    ad_id = ad.get('ad_id')
                    
                    if not ad_id or ad_id == 'N/A':
                        continue
                    
                    if ad_id not in pubs_par_id:
                        pubs_par_id[ad_id] = ad.copy()
                    else:
                        existing = pubs_par_id[ad_id]
                        
                        # Compléter les champs 'N/A' avec les nouvelles valeurs
                        for key, value in ad.items():
                            if existing.get(key) in ['N/A', '', None] and value not in ['N/A', '', None]:
                                existing[key] = value
                        
                        # Garder la date de scraping la plus récente
                        if ad.get('scraped_at', '') > existing.get('scraped_at', ''):
                            existing['scraped_at'] = ad['scraped_at']
                
                return list(pubs_par_id.values())
            
            # Collecter toutes les pubs
            all_ads_raw = []
            scrapings_count = 0
            
            for entry in history:
                if entry.get('results'):
                    all_ads_raw.extend(entry['results'])
                    scrapings_count += 1
            
            # Fusionner et supprimer doublons
            all_ads_unique = fusionner_publicites(all_ads_raw)
            total_ads = len(all_ads_unique)
            
            # ============================================
            # FILTRES (DANS L'EXPANDER)
            # ============================================
            
            with st.expander("🔍 Filtres de recherche", expanded=False):
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    search_term_fusion = st.text_input(
                        "🔍 Recherche textuelle",
                        placeholder="Annonceur, texte, CTA, page ID...",
                        key="search_fusion"
                    )
                
                with col2:
                    media_filter_fusion = st.selectbox(
                        "🎬 Type de média",
                        ["Tous", "Image", "Vidéo"],
                        key="media_fusion"
                    )
                
                with col3:
                    list_filter_fusion = st.selectbox(
                        "📋 Filtrer par liste",
                        ["Tous", "Contient blacklist", "Sans blacklist", "Contient whitelist", "Sans whitelist"],
                        key="list_fusion"
                    )
                
                col4, col5, col6 = st.columns(3)
                
                with col4:
                    # Extraire les pays uniques
                    pays_disponibles = sorted(list(set(ad.get('country', 'N/A') for ad in all_ads_unique if ad.get('country') and ad.get('country') != 'N/A')))
                    pays_filter_fusion = st.multiselect(
                        "🌍 Pays",
                        options=pays_disponibles,
                        key="pays_fusion"
                    )
                
                # Filtre par dates de scraping
                with col5:
                    date_debut_fusion = st.date_input("📅 Scrapé du", value=None, key="date_debut_fusion")
                with col6:
                    date_fin_fusion = st.date_input("📅 Au", value=None, key="date_fin_fusion")

                col7, col8, col9 = st.columns(3)
                with col7:
                    pass
                with col8:
                    pass
                with col9:
                    if st.button("🔄 Réinitialiser", key="reset_fusion", use_container_width=True):
                        st.session_state.current_page_history_fusion = 1
                        st.rerun()
            
            # ============================================
            # APPLIQUER LES FILTRES (EN DEHORS DE L'EXPANDER)
            # ============================================
            
            filtered_ads = all_ads_unique.copy()
            
            # Filtre recherche textuelle
            if search_term_fusion:
                search_lower = search_term_fusion.lower()
                filtered_ads = [
                    ad for ad in filtered_ads
                    if (search_lower in ad.get('advertiser', '').lower() or
                        search_lower in ad.get('text', '').lower() or
                        search_lower in ad.get('cta_text', '').lower() or
                        search_lower in str(ad.get('page_id', '')))
                ]
            
            # Filtre type de média
            if media_filter_fusion == "Image":
                filtered_ads = [ad for ad in filtered_ads if ad.get('media_type') == 'image']
            elif media_filter_fusion == "Vidéo":
                filtered_ads = [ad for ad in filtered_ads if ad.get('media_type') == 'video']
            
            # Filtre blacklist/whitelist
            if list_filter_fusion != "Tous":
                blacklist = st.session_state.blacklist
                whitelist = st.session_state.whitelist
                temp_filtered = []
                
                for ad in filtered_ads:
                    advertiser = ad.get('advertiser', '')
                    page_id = ad.get('page_id', '')
                    
                    has_blacklist = False
                    has_whitelist = False
                    
                    for item in blacklist:
                        nom_page = item.get('nom_page', '')
                        id_page = str(item.get('id_page', ''))
                        if ((nom_page and nom_page.lower() in advertiser.lower()) or (id_page and id_page == page_id)):
                            has_blacklist = True
                            break
                    
                    for item in whitelist:
                        nom_page = item.get('nom_page', '')
                        id_page = str(item.get('id_page', ''))
                        if ((nom_page and nom_page.lower() in advertiser.lower()) or (id_page and id_page == page_id)):
                            has_whitelist = True
                            break
                    
                    if list_filter_fusion == "Contient blacklist" and has_blacklist:
                        temp_filtered.append(ad)
                    elif list_filter_fusion == "Sans blacklist" and not has_blacklist:
                        temp_filtered.append(ad)
                    elif list_filter_fusion == "Contient whitelist" and has_whitelist:
                        temp_filtered.append(ad)
                    elif list_filter_fusion == "Sans whitelist" and not has_whitelist:
                        temp_filtered.append(ad)
                
                filtered_ads = temp_filtered
            
            # Filtre par pays
            if pays_filter_fusion:
                filtered_ads = [ad for ad in filtered_ads if ad.get('country') in pays_filter_fusion]
            
            # Filtre par dates de scraping
            if date_debut_fusion and date_fin_fusion:
                temp_filtered = []
                for ad in filtered_ads:
                    scraped_at = ad.get('scraped_at', '')
                    if scraped_at:
                        try:
                            scraped_date = datetime.fromisoformat(scraped_at.replace('Z', '+00:00')).date()
                            if date_debut_fusion <= scraped_date <= date_fin_fusion:
                                temp_filtered.append(ad)
                        except:
                            continue
                filtered_ads = temp_filtered
            
            # ============================================
            # STATISTIQUES ET RÉSULTATS (TOUJOURS VISIBLES)
            # ============================================
            
            col_stat1, col_stat2, col_stat3 = st.columns(3)
            with col_stat1:
                st.metric("📊 Publicités trouvées", f"{len(filtered_ads)} / {total_ads}")
            with col_stat2:
                st.metric("📁 Scrapings sources", scrapings_count)
            with col_stat3:
                st.metric("🗑️ Doublons supprimés", len(all_ads_raw) - total_ads)
            
            # ============================================
            # PAGINATION
            # ============================================
            
            items_per_page = 100
            total_items = len(filtered_ads)
            total_pages = max(1, (total_items + items_per_page - 1) // items_per_page)
            
            current_page = st.session_state.current_page_history_fusion
            
            # S'assurer que current_page est dans les limites
            if current_page > total_pages:
                current_page = total_pages
                st.session_state.current_page_history_fusion = total_pages
            
            start_idx = (current_page - 1) * items_per_page
            end_idx = min(start_idx + items_per_page, total_items)
            
            display_ads = filtered_ads[start_idx:end_idx]
            
            if total_items == 0:
                st.warning("⚠️ Aucune publicité ne correspond à vos critères de recherche")
            else:
                st.info(f"📄 Page {current_page}/{total_pages} (résultats {start_idx + 1}-{end_idx} sur {total_items})")
                
                # ============================================
                # TABLEAU FUSIONNÉ
                # ============================================
                
                df_fusion = pd.DataFrame(display_ads)
                
                # Réorganisation des colonnes (+ scraped_at)
                colonnes_a_afficher = [
                    'scraped_at',
                    'media_url', 'cta_url', 'ad_library_url', 'page_id',
                    'advertiser', 'country', 'ad_status', 'media_type',
                    'text', 'start_date',
                ]
                colonnes_existantes = [col for col in colonnes_a_afficher if col in df_fusion.columns]
                df_fusion = df_fusion[colonnes_existantes]
                
                df_fusion.insert(0, '⭐ Whitelist', False)
                df_fusion.insert(0, '🚫 Blacklist', False)
                
                edited_df_fusion = st.data_editor(
                    df_fusion,
                    width="stretch",
                    hide_index=False,
                    key=f"fusion_editor_page_{current_page}",
                    column_config={
                        "🚫 Blacklist": st.column_config.CheckboxColumn("🚫", help="Ajouter à la blacklist", default=False, width="small"),
                        "⭐ Whitelist": st.column_config.CheckboxColumn("⭐", help="Ajouter à la whitelist", default=False, width="small"),
                        "scraped_at": st.column_config.TextColumn("Date scraping", help="Date et heure du scraping", width="medium"),
                        "media_url": st.column_config.LinkColumn("Média", display_text="📥 Voir", width="small"),
                        "cta_url": st.column_config.LinkColumn("Lien CTA", display_text="🔗 Ouvrir", width="small"),
                        "ad_library_url": st.column_config.LinkColumn("Voir pub", display_text="🔗 Ouvrir", width="small"),
                        "page_id": st.column_config.TextColumn("Page ID", width="small"),
                        "advertiser": st.column_config.TextColumn("Annonceur", width="medium"),
                        "country": st.column_config.TextColumn("Pays", width="small"),
                        "ad_status": st.column_config.TextColumn("Statut", width="small"),
                        "media_type": st.column_config.TextColumn("Type média", width="small"),
                        "text": st.column_config.TextColumn("Texte", width="large"),
                        "start_date": st.column_config.TextColumn("Date début", width="medium"),
                    },
                    disabled=["scraped_at", "media_url", "cta_url", "ad_library_url", "page_id", "advertiser", "country", "ad_status", "media_type", "text", "start_date"]
                )
                
                # Validation
                both_checked = edited_df_fusion[(edited_df_fusion['🚫 Blacklist'] == True) & (edited_df_fusion['⭐ Whitelist'] == True)]
                if not both_checked.empty:
                    st.error("❌ Une page ne peut pas être à la fois en blacklist ET whitelist.")
                else:
                                # Boutons pour ajouter aux listes
                                col_btn1, col_btn2 = st.columns(2)
                                
                                with col_btn1:
                                    blacklist_pages = edited_df[edited_df['🚫 Blacklist'] == True]
                                    if not blacklist_pages.empty:
                                        if st.button(f"✅ Ajouter {len(blacklist_pages)} page(s) à la blacklist", type="primary", key=f"add_blacklist_{entry['id']}"):
                                            # Préparer les données
                                            pages_data = [
                                                {'page_id': row['page_id'], 'nom_page': row['advertiser']}
                                                for _, row in blacklist_pages.iterrows()
                                            ]
                                            
                                            # Appeler la fonction
                                            result = add_pages_to_list_batch(
                                                page_ids=pages_data,
                                                list_type='blacklist',
                                                source_id=entry['id']
                                            )
                                            
                                            # Afficher le résultat
                                            if result['success']:
                                                st.success(result['message'])
                                                st.session_state.blacklist = load_blacklist()
                                                st.session_state.whitelist = load_whitelist()
                                                st.rerun()
                                            else:
                                                if result.get('skipped_count', 0) > 0:
                                                    st.warning(result['message'])
                                                    st.session_state.blacklist = load_blacklist()
                                                    st.session_state.whitelist = load_whitelist()
                                                else:
                                                    st.error(result['message'])
                                
                                with col_btn2:
                                    whitelist_pages = edited_df[edited_df['⭐ Whitelist'] == True]
                                    if not whitelist_pages.empty:
                                        if st.button(f"✅ Ajouter {len(whitelist_pages)} page(s) à la whitelist", type="primary", key=f"add_whitelist_{entry['id']}"):
                                            page_ids = whitelist_pages['page_id'].tolist()
                                            
                                            result = add_pages_to_list_batch(
                                                page_ids=page_ids,
                                                list_type='whitelist',
                                                source_id=entry['id'],
                                                config=st.session_state.config
                                            )
                                            
                                            if result['success']:
                                                st.success(result['message'])
                                                st.session_state.whitelist = load_whitelist()
                                                st.session_state.blacklist = load_blacklist()
                                                st.rerun()
                                            else:
                                                st.warning(result['message'])
                
                # Boutons de navigation pagination
                st.markdown("---")
                col_nav1, col_nav2, col_nav3 = st.columns([1, 2, 1])
                with col_nav1:
                    if st.button("◀️ Précédent", disabled=(current_page == 1), key="prev_fusion"):
                        st.session_state.current_page_history_fusion -= 1
                        st.rerun()
                with col_nav2:
                    st.markdown(f"<div style='text-align: center; padding-top: 5px;'>Page {current_page} / {total_pages}</div>", unsafe_allow_html=True)
                with col_nav3:
                    if st.button("Suivant ▶️", disabled=(current_page == total_pages), key="next_fusion"):
                        st.session_state.current_page_history_fusion += 1
                        st.rerun()
                
                # Téléchargements (TOUTES les pubs filtrées, pas juste la page)
                col_dl1, col_dl2 = st.columns(2)
                with col_dl1:
                    df_export_fusion = pd.DataFrame(filtered_ads)
                    csv_fusion = df_export_fusion.to_csv(index=False, encoding='utf-8-sig')
                    st.download_button(
                        f"📥 Télécharger CSV ({len(filtered_ads)} pubs)",
                        data=csv_fusion,
                        file_name=f"facebook_ads_fusion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        key="csv_fusion"
                    )
                with col_dl2:
                    json_str_fusion = json.dumps(filtered_ads, ensure_ascii=False, indent=2)
                    st.download_button(
                        f"📥 Télécharger JSON ({len(filtered_ads)} pubs)",
                        data=json_str_fusion,
                        file_name=f"facebook_ads_fusion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                        mime="application/json",
                        key="json_fusion"
                    )

# ============================================
# PAGE BLACKLIST
# ============================================

elif st.session_state.current_page == "blacklist":
    st.title("🚫 Gestion de la Blacklist")
    
    st.markdown("---")
    st.subheader("➕ Ajouter une page manuellement")
    
    col1, col2 = st.columns(2)
    with col1:
        new_page_name = st.text_input("Nom de la page")
    with col2:
        new_page_id = st.text_input("ID de la page", value="61583111846265")

    if st.button("➕ Ajouter à la blacklist"):
        if new_page_id:
            # Conteneur pour le statut en temps réel
            status_container = st.empty()
            progress_container = st.empty()

            with st.spinner("🔍 Recherche de l'ID permanent..."):
                # Lancer le script
                result = get_permanent_id_from_script(
                    new_page_id,
                    'blacklist',
                    headless=st.session_state.config.get('headless', True)
                    )

                # Afficher la progression pendant le traitement
                max_wait = 90  # secondes
                elapsed = 0
                while elapsed < max_wait:
                    status = get_fb_id_status()

                    if status:
                        progress_container.progress(
                            int((status.get('current', 0) / max(status.get('total', 1), 1)) * 100)
                            )
                        status_container.info(f"📊 {status.get('message', 'Traitement en cours...')}")

                        if status.get('status') in ['completed', 'error']:
                            break

                    if os.path.exists(FB_ID_RESULT_FILE):
                        break

                    time.sleep(1)
                    elapsed += 1
            
            if result['success']:
                # Vérifier si déjà présent (avec id_permanent)
                already_exists = any(
                    p.get('id_permanent') == result['id_permanent'] 
                    for p in st.session_state.blacklist
                )
                
                if not already_exists:
                    # Utiliser directement le résultat de la fonction
                    st.session_state.blacklist.append(result)
                    save_blacklist(st.session_state.blacklist)
                    
                    st.success("✅ Page ajoutée avec succès !")
                    with st.expander("📋 Détails de la page ajoutée", expanded=True):
                        st.info(f"**Nom :** {result['nom_page']}")
                        st.info(f"**ID de profil :** {result['id_page']}")
                        st.info(f"**ID permanent :** {result['id_permanent']}")
                        st.info(f"**Créée le :** {result['date_creation']}")
                    st.rerun()
                else:
                    st.warning("⚠️ Cette page est déjà dans la blacklist")
            else:
                st.error(f"❌ {result['error']}")
                st.code(result['error'], language='text')
                
                # Option de secours : ajouter quand même sans ID permanent
                if st.button("➕ Ajouter quand même (sans ID permanent)", key="fallback_blacklist"):
                    fallback_page = {
                        'date_ajout': result['date_ajout'],
                        'nom_page': new_page_name or "Nom inconnu",
                        'id_page': new_page_id,
                        'date_creation': 'N/A',
                        'id_permanent': 'N/A'
                    }
                    st.session_state.blacklist.append(fallback_page)
                    save_blacklist(st.session_state.blacklist)
                    st.success("✅ Page ajoutée (sans ID permanent)")
                    st.rerun()
        else:
            st.error("⚠️ Veuillez fournir un ID de page")
    
    st.markdown("---")
    blacklist_pages = st.session_state.blacklist
    
    if not blacklist_pages:
        st.info("Aucune page en blacklist pour le moment.")
    else:
        st.markdown(f"### {len(blacklist_pages)} page(s) en blacklist")
        
        search_term = st.text_input("🔍 Rechercher une page", placeholder="Nom de la page ou ID")
        
        filtered_pages = blacklist_pages
        if search_term:
            filtered_pages = [
                p for p in blacklist_pages
                if search_term.lower() in p.get('nom_page', '').lower() or 
                   search_term in str(p.get('id_page', ''))
            ]
        
        df_blacklist = pd.DataFrame(filtered_pages)
        
        if not df_blacklist.empty:
            df_blacklist.insert(0, 'Retirer', False)
            
            edited_df = st.data_editor(
                df_blacklist,
                width="stretch",
                hide_index=False,
                column_config={
                    "Retirer": st.column_config.CheckboxColumn(
                        "Retirer de la blacklist",
                        help="Cochez pour retirer cette page",
                        default=False,
                    ),
                    "date_ajout": "Date d'ajout",
                    "nom_page": "Nom de la page",
                    "id_page": "ID de profil",
                    "date_creation": "Date de création",
                    "id_permanent": "ID permanent"
                },
                disabled=["date_ajout", "nom_page", "id_page"]
            )
            
            if st.button("🗑️ Retirer les pages cochées", type="primary"):
                pages_to_remove = edited_df[edited_df['Retirer'] == True]
                if not pages_to_remove.empty:
                    ids_to_remove = set(pages_to_remove['id_page'].tolist())
                    st.session_state.blacklist = [
                        p for p in st.session_state.blacklist
                        if p.get('id_page') not in ids_to_remove
                    ]
                    save_blacklist(st.session_state.blacklist)
                    st.success(f"✅ {len(ids_to_remove)} page(s) retirée(s) de la blacklist !")
                    st.rerun()
                else:
                    st.warning("Aucune page sélectionnée")
        else:
            st.warning("Aucune page trouvée avec ces critères de recherche")

# ============================================
# PAGE WHITELIST
# ============================================

elif st.session_state.current_page == "whitelist":
    st.title("⭐ Gestion de la Whitelist (Concurrents)")
    
    st.info("💡 Les pages en whitelist seront automatiquement scannées lors de la veille concurrentielle quotidienne")
    
    st.markdown("---")
    st.subheader("➕ Ajouter un concurrent")
    
    col1, col2 = st.columns(2)
    with col1:
        new_page_name = st.text_input("Nom du concurrent")
    with col2:
        new_page_id = st.text_input("ID de la page", value="61582778275134")
        
    if st.button("➕ Ajouter à la whitelist"):
        if new_page_id:
            # Conteneur pour le statut en temps réel
            status_container = st.empty()
            progress_container = st.empty()

            with st.spinner("🔍 Recherche de l'ID permanent..."):
                # Lancer le script
                result = get_permanent_id_from_script(
                    new_page_id,
                    'whitelist',
                    headless=st.session_state.config.get('headless', True)
                    )

                # Afficher la progression pendant le traitement
                max_wait = 90  # secondes
                elapsed = 0
                while elapsed < max_wait:
                    status = get_fb_id_status()

                    if status:
                        progress_container.progress(
                            int((status.get('current', 0) / max(status.get('total', 1), 1)) * 100)
                            )
                        status_container.info(f"📊 {status.get('message', 'Traitement en cours...')}")

                        if status.get('status') in ['completed', 'error']:
                            break

                    if os.path.exists(FB_ID_RESULT_FILE):
                        break

                    time.sleep(1)
                    elapsed += 1
            
            if result['success']:
                # Vérifier si déjà présent (avec id_permanent)
                already_exists = any(
                    p.get('id_permanent') == result['id_permanent'] 
                    for p in st.session_state.whitelist
                )
                
                if not already_exists:
                    # Utiliser directement le résultat de la fonction
                    st.session_state.whitelist.append(result)
                    save_whitelist(st.session_state.whitelist)
                    
                    st.success("✅ Concurrent ajouté avec succès !")
                    with st.expander("📋 Détails du concurrent ajouté", expanded=True):
                        st.info(f"**Nom :** {result['nom_page']}")
                        st.info(f"**ID de profil :** {result['id_page']}")
                        st.info(f"**ID permanent :** {result['id_permanent']}")
                        st.info(f"**Créée le :** {result['date_creation']}")
                    st.rerun()
                else:
                    st.warning("⚠️ Ce concurrent est déjà dans la whitelist")
            else:
                st.error(f"❌ {result['error']}")
                st.code(result['error'], language='text')
                
                # Option de secours : ajouter quand même sans ID permanent
                if st.button("➕ Ajouter quand même (sans ID permanent)", key="fallback_whitelist"):
                    fallback_page = {
                        'date_ajout': result['date_ajout'],
                        'nom_page': new_page_name or "Nom inconnu",
                        'id_page': new_page_id,
                        'date_creation': 'N/A',
                        'id_permanent': 'N/A'
                    }
                    st.session_state.whitelist.append(fallback_page)
                    save_whitelist(st.session_state.whitelist)
                    st.success("✅ Concurrent ajouté (sans ID permanent)")
                    st.rerun()
        else:
            st.error("⚠️ Veuillez fournir un ID de page")
    
    st.markdown("---")
    whitelist_pages = st.session_state.whitelist
    
    if not whitelist_pages:
        st.info("Aucun concurrent en whitelist pour le moment.")
    else:
        st.markdown(f"### {len(whitelist_pages)} concurrent(s) en whitelist")
        
        search_term = st.text_input("🔍 Rechercher un concurrent", placeholder="Nom ou ID")
        
        filtered_pages = whitelist_pages
        if search_term:
            filtered_pages = [
                p for p in whitelist_pages
                if search_term.lower() in p.get('nom_page', '').lower() or 
                   search_term in str(p.get('id_page', ''))
            ]
        
        df_whitelist = pd.DataFrame(filtered_pages)
        
        if not df_whitelist.empty:
            df_whitelist.insert(0, 'Retirer', False)
            
            edited_df = st.data_editor(
                df_whitelist,
                width="stretch",
                hide_index=False,
                column_config={
                    "Retirer": st.column_config.CheckboxColumn(
                        "Retirer de la whitelist",
                        help="Cochez pour retirer ce concurrent",
                        default=False,
                    ),
                    "date_ajout": "Date d'ajout",
                    "nom_page": "Nom du concurrent",
                    "id_page": "ID de profil",
                    "date_creation": "Date de création",
                    "id_permanent": "ID permanent"
                },
                disabled=["date_ajout", "nom_page", "id_page"]
            )
            
            if st.button("🗑️ Retirer les concurrents cochés", type="primary"):
                pages_to_remove = edited_df[edited_df['Retirer'] == True]
                if not pages_to_remove.empty:
                    ids_to_remove = set(pages_to_remove['id_page'].tolist())
                    st.session_state.whitelist = [
                        p for p in st.session_state.whitelist
                        if p.get('id_page') not in ids_to_remove
                    ]
                    save_whitelist(st.session_state.whitelist)
                    st.success(f"✅ {len(ids_to_remove)} concurrent(s) retiré(s) de la whitelist !")
                    st.rerun()
                else:
                    st.warning("Aucun concurrent sélectionné")
        else:
            st.warning("Aucun concurrent trouvé avec ces critères de recherche")

    """
    Code à intégrer dans ton interface Streamlit
    Bouton pour lancer la récupération des ID permanents manquants
    """

    import streamlit as st
    import subprocess
    import json
    import time
    from pathlib import Path

    # Fichiers de statut
    STATUS_FILE = "fb_update_status.json"

    def load_update_status():
        """Charge le statut de la mise à jour"""
        if Path(STATUS_FILE).exists():
            try:
                with open(STATUS_FILE, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                return None
        return None

    def run_update_missing_ids():
        """Lance le script de mise à jour en subprocess"""
        try:
            # Lancer le script Python
            process = subprocess.Popen(
                ['python', 'update_missing_permanent_ids.py'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding='utf-8'
            )
            return process
        except Exception as e:
            st.error(f"Erreur lors du lancement du script: {e}")
            return None

    # === INTERFACE STREAMLIT ===

    st.title("🔄 Mise à jour des ID Permanents")

    # Section d'information
    st.info("""
    **Ce bouton va :**
    1. Identifier toutes les pages sans ID permanent (vide, N/A, ou inexistant)
    2. Lancer le scraping en mode **visible** pour chaque page
    3. Mettre à jour automatiquement la whitelist avec les nouveaux ID trouvés
    """)

    # Afficher le nombre de pages à traiter
    try:
        with open('whitelist.json', 'r', encoding='utf-8') as f:
            whitelist = json.load(f)
            
        missing_count = sum(1 for item in whitelist 
                           if not item.get('id_permanent') 
                           or item.get('id_permanent') == 'N/A' 
                           or item.get('id_permanent') == '')
        
        if missing_count > 0:
            st.warning(f"📊 **{missing_count} page(s)** sans ID permanent détectée(s)")
        else:
            st.success("✅ Toutes les pages ont un ID permanent !")
            
    except Exception as e:
        st.error(f"Erreur lors de la lecture de la whitelist: {e}")
        missing_count = 0

    # Bouton principal
    col1, col2 = st.columns([1, 3])

    with col1:
        if st.button("🚀 Lancer la mise à jour", type="primary", disabled=missing_count == 0):
            # Supprimer l'ancien fichier de statut
            if Path(STATUS_FILE).exists():
                Path(STATUS_FILE).unlink()
            
            # Lancer le script
            st.session_state['update_process'] = run_update_missing_ids()
            st.session_state['update_running'] = True
            st.rerun()

    with col2:
        if missing_count == 0:
            st.caption("Aucune page à traiter")

    # Affichage de la progression en temps réel
    if st.session_state.get('update_running', False):
        st.divider()
        
        # Placeholder pour la progression
        progress_placeholder = st.empty()
        status_placeholder = st.empty()
        login_placeholder = st.empty()
        details_placeholder = st.empty()
        
        # Boucle de mise à jour
        while st.session_state.get('update_running', False):
            status_data = load_update_status()
            
            if status_data:
                status = status_data.get('status')
                current = status_data.get('current', 0)
                total = status_data.get('total', 0)
                current_page = status_data.get('current_page', '')
                message = status_data.get('message', '')
                processed = status_data.get('processed', [])
                waiting_login = status_data.get('waiting_login', False)
                
                # Si en attente de login
                if waiting_login:
                    login_placeholder.warning("""
                    ### 🔐 Connexion Facebook requise
                    
                    **Un navigateur s'est ouvert. Veuillez :**
                    1. Vous connecter à Facebook dans le navigateur
                    2. Attendre que la connexion soit détectée automatiquement
                    
                    ⏳ Le script vérifie automatiquement toutes les 5 secondes...
                    """)
                    progress_placeholder.empty()
                    status_placeholder.info("⏳ En attente de connexion...")
                
                # Barre de progression
                elif total > 0:
                    login_placeholder.empty()
                    progress = current / total
                    progress_placeholder.progress(progress, text=f"Page {current}/{total}")
                    
                    # Message de statut
                    if status == "running":
                        status_placeholder.info(f"🔄 {message}")
                        if current_page:
                            st.write(f"**Page en cours:** {current_page}")
                    
                    elif status == "completed":
                        progress_placeholder.progress(1.0, text="Terminé !")
                        status_placeholder.success(f"✅ {message}")
                        st.session_state['update_running'] = False
                        
                        # Afficher les résultats
                        if processed:
                            with details_placeholder.expander("📋 Détails des pages traitées", expanded=True):
                                for item in processed:
                                    if item['status'] == 'success':
                                        st.success(f"✅ **{item['nom_page']}** - ID: `{item['id_permanent']}`")
                                    else:
                                        st.error(f"❌ **{item['nom_page']}** - Erreur: {item.get('error', 'Inconnue')}")
                        
                        time.sleep(2)
                        st.rerun()
                    
                    elif status == "error":
                        progress_placeholder.empty()
                        login_placeholder.empty()
                        status_placeholder.error(f"❌ {message}")
                        st.session_state['update_running'] = False
                        time.sleep(2)
                        st.rerun()
            
            # Attendre avant la prochaine vérification
            time.sleep(1)
            st.rerun()

    # Section d'aide
    with st.expander("ℹ️ Aide"):
        st.markdown("""
        ### Comment ça fonctionne ?
        
        1. **Identification** : Le script parcourt la whitelist et identifie toutes les pages où `id_permanent` est :
           - Inexistant
           - Vide (`""`)
           - Égal à `"N/A"`
        
        2. **Scraping** : Pour chaque page identifiée, le script :
           - Ouvre un navigateur **visible** (pour que tu puisses voir le processus)
           - Accède à la page de transparence Facebook
           - Récupère l'ID permanent
        
        3. **Mise à jour** : Les entrées existantes sont mises à jour (pas de doublons créés)
        
        ### Mode visible
        Le scraping s'effectue en mode **visible** pour que tu puisses :
        - Voir ce qui se passe en temps réel
        - Intervenir si nécessaire (par exemple, si Facebook demande une connexion)
        - Vérifier que tout fonctionne correctement
        
        ### Temps d'exécution
        - ~15-20 secondes par page
        - Pour 10 pages : environ 3-4 minutes
        """)

    # Note importante
    st.divider()
    st.caption("⚠️ **Note :** Le navigateur s'ouvrira en mode visible. Ne le fermez pas pendant le processus.")

# ============================================
# PAGE VEILLE CONCURRENTIELLE
# ============================================

elif st.session_state.current_page == "competitive":
    st.title("📊 Veille Concurrentielle")
    
    st.info("💡 Cette section affiche les rapports journaliers de veille sur vos concurrents")
    
    # Bouton pour lancer manuellement
    col1, col2 = st.columns([2, 2])
    with col1:
        st.markdown("### Lancer une veille manuelle")
    with col2:
        if st.button("🚀 Démarrer", type="primary", use_container_width=True):
            if not st.session_state.whitelist:
                st.error("❌ Aucun concurrent dans la whitelist")
            else:
                if launch_competitive_intelligence():
                    st.success("✅ Veille lancée en arrière-plan !")
                    st.info("📊 La progression s'affiche dans la sidebar")
                    time.sleep(2)
                    st.rerun()
                else:
                    st.error("❌ Erreur lors du lancement")
            
        # Bouton refresh
        if st.button("🔄 Rafraîchir", use_container_width=True):
            st.rerun()

            st.markdown("---")

            # Bouton pour rafraîchir la page et voir les nouveaux résultats
            if st.button("🔄 Rafraîchir les rapports", use_container_width=True):
                st.rerun()
        
    # Charger les rapports
    reports = load_daily_reports()
    
    if not reports:
        st.info("Aucun rapport de veille disponible pour le moment.")
    else:
        st.markdown(f"### 📊 {len(reports)} rapport(s) disponible(s)")
        
        for report in reports:
            report_date = report.get('date', 'N/A')
            competitors_count = report.get('competitors_scanned', 0)
            results_count = report.get('results_count', 0)
            
            with st.expander(f"📅 {report_date} - {results_count} publicités - {competitors_count} concurrent(s) scannés"):
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Publicités trouvées", results_count)
                with col2:
                    st.metric("Concurrents scannés", competitors_count)
                with col3:
                    st.metric("Date", report_date.split()[0])
                
                if report.get('results'):
                    st.markdown("---")
                    st.subheader("📊 Résultats")
                    
                    # Champ de recherche
                    result_search = st.text_input(
                        "🔍 Rechercher dans ces résultats",
                        key=f"comp_search_{report['id']}",
                        placeholder="Concurrent, texte, CTA..."
                    )
                    
                    # Filtrer les résultats
                    display_results = report['results']
                    if result_search:
                        search_lower = result_search.lower()
                        display_results = [
                            r for r in report['results']
                            if (search_lower in r.get('competitor_name', '').lower() or
                                search_lower in r.get('advertiser', '').lower() or
                                search_lower in r.get('text', '').lower() or
                                search_lower in r.get('cta_text', '').lower())
                        ]
                    
                    if not display_results:
                        st.warning("Aucun résultat ne correspond à votre recherche")
                    else:
                        st.info(f"📊 {len(display_results)} résultat(s) affiché(s)")
                        
                        df = pd.DataFrame(display_results)
                        
                        # Correction codes pays
                        if 'country' in df.columns:
                            df['country'] = df['country'].apply(lambda x: COUNTRY_NAMES.get(x, x))
                        
                        st.dataframe(
                            df,
                            width="stretch",
                            hide_index=True,
                            column_config={
                                "ad_library_url": st.column_config.LinkColumn("Voir la pub", display_text="🔗 Ouvrir"),
                                "cta_url": st.column_config.LinkColumn("Lien CTA", display_text="🔗 Ouvrir"),
                                "media_url": st.column_config.LinkColumn("Média", display_text="📥 Voir"),
                            }
                        )
                        
                        # Téléchargement
                        col_dl1, col_dl2 = st.columns(2)
                        with col_dl1:
                            csv = df.to_csv(index=False, encoding='utf-8-sig')
                            st.download_button(
                                "📥 Télécharger CSV",
                                data=csv,
                                file_name=f"veille_competitive_{report['id']}.csv",
                                mime="text/csv",
                                key=f"comp_csv_{report['id']}"
                            )
                        with col_dl2:
                            json_str = json.dumps(display_results, ensure_ascii=False, indent=2)
                            st.download_button(
                                "📥 Télécharger JSON",
                                data=json_str,
                                file_name=f"veille_competitive_{report['id']}.json",
                                mime="application/json",
                                key=f"comp_json_{report['id']}"
                            )

# ============================================
# PAGE PRINCIPALE - SCRAPER
# ============================================

elif st.session_state.current_page == "scraper":
    st.title("📊 Facebook Ads Library Scraper")
    st.markdown("---")
    
    # Vérifier si on doit relancer avec des paramètres sauvegardés
    rerun_params = st.session_state.get('rerun_params', None)
    
    if rerun_params:
        st.info("🔄 Paramètres rechargés depuis l'historique. Vous pouvez les modifier avant de relancer.")
        if st.button("❌ Effacer les paramètres rechargés"):
            del st.session_state.rerun_params
            st.rerun()
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🎯 Critères de recherche")
        
        ALL_COUNTRIES = [
            ("", "-- Sélectionnez un ou plusieurs pays --"),
            ("ALL", "Tous les pays"),
            ("AU", "Australie"),
            ("BE", "Belgique"),
            ("BJ", "Bénin"),
            ("BF", "Burkina Faso"),
            ("BI", "Burundi"),
            ("CM", "Cameroun"),
            ("CA", "Canada"),
            ("CF", "Centrafrique"),
            ("KM", "Comores"),
            ("CG", "Congo"),
            ("CD", "Congo (RDC)"),
            ("CI", "Côte d'Ivoire"),
            ("DJ", "Djibouti"),
            ("FR", "France"),
            ("GA", "Gabon"),
            ("GN", "Guinée"),
            ("GW", "Guinée-Bissau"),
            ("GQ", "Guinée équatoriale"),
            ("IT", "Italie"),
            ("JP", "Japon"),
            ("LU", "Luxembourg"),
            ("MG", "Madagascar"),
            ("ML", "Mali"),
            ("MA", "Maroc"),
            ("MR", "Mauritanie"),
            ("MU", "Maurice"),
            ("NE", "Niger"),
            ("NG", "Nigéria"),
            ("NL", "Pays-Bas"),
            ("BR", "Brésil"),
            ("ES", "Espagne"),
            ("CH", "Suisse"),
            ("RW", "Rwanda"),
            ("SN", "Sénégal"),
            ("SC", "Seychelles"),
            ("TG", "Togo"),
            ("TN", "Tunisie"),
            ("GB", "Royaume-Uni"),
            ("IN", "Inde"),
            ("MX", "Mexique"),
            ("US", "États-Unis"),
        ]
        
        # Pré-sélection des pays si rerun depuis l'historique multi-pays
        default_countries = []
        if rerun_params and 'countries_list' in rerun_params:
            default_codes = rerun_params.get('countries_list', [])
            default_countries = [c for c in ALL_COUNTRIES if c[0] in default_codes]
        
        selected_countries = st.multiselect(
            "🌍 Pays (sélection multiple)",
            options=ALL_COUNTRIES,
            format_func=lambda x: x[1],
            default=default_countries,
            help="Sélectionnez un ou plusieurs pays. Le scraping sera exécuté successivement pour chaque pays."
        )
        
        default_status = rerun_params.get('status', 'active') if rerun_params else 'active'
        status_options = ["active", "inactive", "all"]
        status = st.radio(
            "📌 État de la publicité",
            options=status_options,
            format_func=lambda x: {
                "active": "Active",
                "inactive": "Inactive", 
                "all": "Toutes"
            }[x],
            horizontal=True,
            index=status_options.index(default_status) if default_status in status_options else 0
        )
        
        default_media = rerun_params.get('media_type', 'all') if rerun_params else 'all'
        media_options = ["all", "image", "video"]
        media_type = st.radio(
            "🎬 Type de média",
            options=media_options,
            format_func=lambda x: {
                "all": "Tous",
                "image": "Images uniquement",
                "video": "Vidéos uniquement"
            }[x],
            horizontal=True,
            index=media_options.index(default_media) if default_media in media_options else 0
        )
    
    with col2:
        st.subheader("📅 Filtres avancés")
        
        default_date_type = rerun_params.get('date_filter_type', 'none') if rerun_params else 'none'
        date_type_options = ["none", "before", "on", "after", "between"]
        date_type = st.selectbox(
            "📅 Date de lancement",
            options=date_type_options,
            format_func=lambda x: {
                "none": "Aucun filtre de date",
                "before": "Avant le",
                "on": "Le",
                "after": "Après le",
                "between": "Entre"
            }[x],
            index=date_type_options.index(default_date_type) if default_date_type in date_type_options else 0
        )
        
        date1 = None
        date2 = None
        
        if date_type != "none":
            if date_type == "between":
                col_date1, col_date2 = st.columns(2)
                with col_date1:
                    default_date1 = None
                    if rerun_params and rerun_params.get('date_filter'):
                        date1_str = rerun_params['date_filter'].get('date1')
                        if date1_str:
                            default_date1 = datetime.strptime(date1_str, '%Y-%m-%d').date()
                    date1 = st.date_input("Date de début", value=default_date1)
                with col_date2:
                    default_date2 = None
                    if rerun_params and rerun_params.get('date_filter'):
                        date2_str = rerun_params['date_filter'].get('date2')
                        if date2_str:
                            default_date2 = datetime.strptime(date2_str, '%Y-%m-%d').date()
                    date2 = st.date_input("Date de fin", value=default_date2)
            else:
                default_date1 = None
                if rerun_params and rerun_params.get('date_filter'):
                    date1_str = rerun_params['date_filter'].get('date1')
                    if date1_str:
                        default_date1 = datetime.strptime(date1_str, '%Y-%m-%d').date()
                date1 = st.date_input("Date", value=default_date1)
        
        default_search = rerun_params.get('search_term', '') if rerun_params else ''
        default_search = '' if default_search == "Toutes les publicités" else default_search
        search_term = quote(
            st.text_input(
            "🔍 Terme de recherche",
            placeholder="Ex: nike, iphone, restaurant...",
            help="Veuillez renseigner un terme de recherche",
            value=default_search
            )
        )
    
    st.markdown("---")
    
    # Préparer le filtre de date
    date_filter_obj = None
    if date_type != "none":
        date_filter_obj = {
            'type': date_type,
            'date1': date1.strftime('%Y-%m-%d') if date1 else None,
            'date2': date2.strftime('%Y-%m-%d') if date2 else None
        }

    if st.button("🚀 Lancer le scraping", type="primary"):
        # Validations
        if not selected_countries:
            st.error("⚠️ Veuillez sélectionner au moins un pays")
        elif date_type == "between" and (not date1 or not date2):
            st.error("⚠️ Veuillez spécifier les deux dates pour l'intervalle")
        elif date_type == "between" and date1 > date2:
            st.error("⚠️ La date de début doit être antérieure à la date de fin")
        elif st.session_state.config['pause_min'] > st.session_state.config['pause_max']:
            st.error("⚠️ La pause min doit être ≤ à la pause max")
        else:
            # Préparation des données
            countries_count = len(selected_countries)
            countries_names = [country[1] for country in selected_countries]
            countries_codes = [country[0] for country in selected_countries]
            
            # Query info global
            query_info = {
                'countries': countries_names,
                'countries_list': countries_codes,
                'countries_count': countries_count,
                'status': status,
                'media_type': media_type,
                'search_term': unquote(search_term) if unquote(search_term) else "Toutes les publicités",
                'date_filter_type': date_type,
                'date_filter': date_filter_obj,
                'max_ads': st.session_state.config['max_ads'],
                'max_time': f"{st.session_state.config['max_time']} minutes",
                'blacklist_count': len(st.session_state.blacklist),
                'mode': "Invisible" if st.session_state.config['headless'] else "Visible",
                'pause': f"{st.session_state.config['pause_min']}-{st.session_state.config['pause_max']}s",
                'scraped_urls' : []  # Liste pour stocker les URLs par pays
            }

            scraped_urls = []
            
            st.info("🔧 Configuration du scraping multi-pays...")
            st.json(query_info)
            
            # Conteneurs pour la progression
            progress_bar = st.progress(0)
            status_text = st.empty()
            countries_status_container = st.empty()
            
            # Variables pour le tracking
            all_results = []
            temp_success = []
            temp_failed = []
            
            # Créer une entrée d'historique unique
            entry_id = add_to_history(
                query_info=query_info,
                results_count=0,
                results_data=[],
                status="in_progress"
            )
            
            # Boucle sur chaque pays
            for index, country in enumerate(selected_countries, 1):
                country_code = country[0]
                country_name = country[1]
                
                # Affichage empilé de la progression
                countries_progress = []
                for i in range(1, index + 1):
                    if i < index:
                        # Pays précédents (terminés)
                        prev_country = selected_countries[i-1]
                        if prev_country[1] in temp_success:
                            countries_progress.append(f"🌍 Pays {i}/{countries_count} : {prev_country[1]} ✅")
                        else:
                            countries_progress.append(f"🌍 Pays {i}/{countries_count} : {prev_country[1]} ❌")
                    else:
                        # Pays actuel
                        countries_progress.append(f"🌍 Pays {i}/{countries_count} : {country_name} ⏳ (en cours)")
                
                countries_status_container.markdown("\n\n".join(countries_progress))
                
                # Construction de l'URL pour ce pays
                base_url = "https://www.facebook.com/ads/library"
                scraping_url = f"{base_url}?active_status={status}&ad_type=all&country={country_code}"
                
                if media_type != "all":
                    scraping_url += f"&media_type={media_type}"
                else:
                    scraping_url += "&media_type=all"
                
                if search_term:
                    scraping_url += f"&q={search_term}"
                
                if date_filter_obj:
                    date_type_val = date_filter_obj.get('type')
                    date1_val = date_filter_obj.get('date1')
                    date2_val = date_filter_obj.get('date2')
                    
                    if date_type_val == "before" and date1_val:
                        scraping_url += f"&start_date[max]={date1_val}"
                    elif date_type_val == "on" and date1_val:
                        scraping_url += f"&start_date[min]={date1_val}&start_date[max]={date1_val}"
                    elif date_type_val == "after" and date1_val:
                        scraping_url += f"&start_date[min]={date1_val}"
                    elif date_type_val == "between" and date1_val and date2_val:
                        scraping_url += f"&start_date[min]={date1_val}&start_date[max]={date2_val}"
                
                query_info["scraped_urls"].append({
                    "country": country_name,
                    "url": scraping_url
                })
                print(query_info["scraped_urls"])


                status_text.text(f"🌍 Scraping du pays {index}/{countries_count} : {country_name}")
                
                # Callback de progression pour ce pays
                def update_progress(progress, message):
                    progress_bar.progress(int(progress))
                    status_text.text(f"🌍 Pays {index}/{countries_count} : {country_name} - {message}")
                
                # Scraping de ce pays
                try:
                    if sys.platform == 'win32':
                        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
                    
                    scraper = FacebookAdsLibraryScraper(
                        country=country,
                        status=status,
                        media_type=media_type,
                        blacklist=st.session_state.blacklist,
                        config=st.session_state.config,
                        entry_id=entry_id
                    )
                    scraper.set_progress_callback(update_progress)
                    
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    country_results = loop.run_until_complete(
                        scraper.scrape(
                            keyword=search_term,
                            date_filter=date_filter_obj,
                            max_ads=st.session_state.config['max_ads'],
                            max_scroll_time=st.session_state.config['max_time'] * 60
                        )
                    )
                    loop.close()
                    
                    # Ajouter les résultats de ce pays
                    all_results.extend(country_results)
                    temp_success.append(country_name)
                    
                    # Mise à jour incrémentale de l'historique
                    update_history_incrementally(entry_id, country_results)
                    
                    logger.info(f"✅ {country_name} : {len(country_results)} publicités extraites")
                    
                except Exception as e:
                    error_msg = str(e)
                    temp_failed.append(f"{country_name} ({error_msg})")
                    logger.error(f"❌ Erreur sur {country_name} : {error_msg}")
                    st.warning(f"⚠️ Erreur lors du scraping de {country_name} : {error_msg}")
                    continue
                
                # Pause entre pays (sauf après le dernier)
                if index < countries_count:
                    pause_duration = random.uniform(
                        st.session_state.config['pause_min'],
                        st.session_state.config['pause_max']
                    )
                    status_text.text(f"⏸️ Pause de {int(pause_duration)}s avant le pays suivant...")
                    time.sleep(pause_duration)
            
            # Affichage final empilé
            final_countries_progress = []
            for i, country in enumerate(selected_countries, 1):
                if country[1] in temp_success:
                    final_countries_progress.append(f"🌍 Pays {i}/{countries_count} : {country[1]} ✅")
                else:
                    final_countries_progress.append(f"🌍 Pays {i}/{countries_count} : {country[1]} ❌")
            
            countries_status_container.markdown("\n\n".join(final_countries_progress))
            
            # Déterminer le statut final
            if len(temp_failed) == 0:
                final_status = "success"
            elif len(temp_success) == 0:
                final_status = "error"
            else:
                final_status = "partial"

            # Mettre à jour query_info avec les URLs
            query_info['scraped_urls'] = scraped_urls
            
            # Mise à jour finale de l'historique
            add_to_history(
                query_info=query_info,
                results_count=len(all_results),
                results_data=all_results,
                status=final_status,
                error_message="; ".join(temp_failed) if temp_failed else None,
                entry_id=entry_id
            )
            
            # Effacer les paramètres de rerun
            if 'rerun_params' in st.session_state:
                del st.session_state.rerun_params
            
            # Message final détaillé
            progress_bar.progress(100)
            
            if final_status == "success":
                st.success(f"✅ Scraping terminé ! {len(all_results)} publicités extraites")
                st.info(f"**Succès ({len(temp_success)}/{countries_count})** : {', '.join(temp_success)}")
                st.balloons()
            elif final_status == "partial":
                st.warning(f"⚠️ Scraping terminé avec erreurs partielles")
                st.info(f"**Résultats** : {len(all_results)} publicités extraites")
                st.success(f"**Succès ({len(temp_success)}/{countries_count})** : {', '.join(temp_success)}")
                st.error(f"**Erreurs ({len(temp_failed)}/{countries_count})** : {', '.join(temp_failed)}")
            else:
                st.error(f"❌ Échec complet du scraping")
                st.error(f"**Erreurs ({len(temp_failed)}/{countries_count})** : {', '.join(temp_failed)}")

st.markdown("---")
st.caption("💡 Astuce : Utilisez la navigation en haut de la sidebar pour accéder aux différentes sections")