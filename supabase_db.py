import os
from supabase import create_client, Client
import streamlit as st

def get_supabase() -> Client:
    """Connexion Supabase (local + cloud)"""
    try:
        # Local (.env)
        if "SUPABASE_URL" in os.environ:
            url = os.environ["SUPABASE_URL"]
            key = os.environ["SUPABASE_KEY"]
        # Streamlit Cloud (secrets.toml)
        else:
            url = st.secrets["supabase"]["url"]
            key = st.secrets["supabase"]["key"]
        
        return create_client(url, key)
    except Exception as e:
        st.error(f"Erreur connexion Supabase: {e}")
        raise

# ============================================
# BLACKLIST
# ============================================

def load_blacklist():
    """Charge la blacklist depuis Supabase"""
    try:
        supabase = get_supabase()
        response = supabase.table('blacklist').select('*').execute()
        return response.data
    except Exception as e:
        st.error(f"Erreur load_blacklist: {e}")
        return []

def save_blacklist(blacklist):
    """Sauvegarde la blacklist (REMPLACE tout)"""
    try:
        supabase = get_supabase()
        
        # Supprimer tout
        supabase.table('blacklist').delete().neq('id', 0).execute()
        
        # Réinsérer
        if blacklist:
            supabase.table('blacklist').insert(blacklist).execute()
        
        return True
    except Exception as e:
        st.error(f"Erreur save_blacklist: {e}")
        return False

# ============================================
# WHITELIST
# ============================================

def load_whitelist():
    """Charge la whitelist depuis Supabase"""
    try:
        supabase = get_supabase()
        response = supabase.table('whitelist').select('*').execute()
        return response.data
    except Exception as e:
        st.error(f"Erreur load_whitelist: {e}")
        return []

def save_whitelist(whitelist):
    """Sauvegarde la whitelist (REMPLACE tout)"""
    try:
        supabase = get_supabase()
        
        # Supprimer tout
        supabase.table('whitelist').delete().neq('id', 0).execute()
        
        # Réinsérer
        if whitelist:
            supabase.table('whitelist').insert(whitelist).execute()
        
        return True
    except Exception as e:
        st.error(f"Erreur save_whitelist: {e}")
        return False

# ============================================
# SCRAPING HISTORY
# ============================================

def load_history():
    """Charge l'historique des scraping"""
    try:
        supabase = get_supabase()
        response = supabase.table('scraping_history').select('*').order('created_at', desc=True).execute()
        return response.data
    except Exception as e:
        st.error(f"Erreur load_history: {e}")
        return []

def save_history(history):
    """NE PAS UTILISER - Utiliser add_to_history à la place"""
    pass

def add_to_history(query_info, results_count, results_data, url=None, status="success", error_message=None, entry_id=None):
    """Ajoute ou met à jour une entrée dans l'historique"""
    try:
        supabase = get_supabase()
        
        # Mise à jour
        if entry_id:
            supabase.table('scraping_history').update({
                'results_count': results_count,
                'results': results_data,
                'status': status,
                'error_message': error_message
            }).eq('id', entry_id).execute()
            return entry_id
        
        # Création
        from datetime import datetime
        new_id = entry_id or datetime.now().strftime('%Y%m%d_%H%M%S')
        
        entry = {
            'id': new_id,
            'date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'query': query_info,
            'results_count': results_count,
            'results': results_data,
            'status': status,
            'error_message': error_message
        }
        
        supabase.table('scraping_history').insert(entry).execute()
        return new_id
        
    except Exception as e:
        st.error(f"Erreur add_to_history: {e}")
        return None

def update_history_incrementally(entry_id, new_results):
    """Met à jour progressivement une entrée"""
    try:
        supabase = get_supabase()
        
        # Récupérer l'entrée actuelle
        response = supabase.table('scraping_history').select('results').eq('id', entry_id).execute()
        
        if response.data:
            current_results = response.data[0].get('results', [])
            current_results.extend(new_results)
            
            # Mettre à jour
            supabase.table('scraping_history').update({
                'results': current_results,
                'results_count': len(current_results)
            }).eq('id', entry_id).execute()
            
            return True
        return False
    except Exception as e:
        st.error(f"Erreur update_history_incrementally: {e}")
        return False

# ============================================
# CONFIG (optionnel - peut rester en local)
# ============================================

def load_config():
    """Charge la config (local uniquement)"""
    import json
    CONFIG_FILE = "config.json"
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
    """Sauvegarde config (local uniquement)"""
    import json
    CONFIG_FILE = "config.json"
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2)