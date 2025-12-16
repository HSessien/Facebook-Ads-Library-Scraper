"""
display_utils.py
Fonctions utilitaires pour l'affichage des tableaux de publicités
"""

import streamlit as st
import pandas as pd
import json
import os
import sys
import time
import subprocess
from datetime import datetime


# ============================================
# CONFIGURATION DES COLONNES POUR TABLEAUX
# ============================================

def get_column_config():
    """
    Retourne la configuration standard des colonnes pour st.data_editor
    """
    return {
        "🚫 Blacklist": st.column_config.CheckboxColumn(
            "🚫",
            help="Ajouter à la blacklist",
            default=False,
            width="small"
        ),
        "⭐ Whitelist": st.column_config.CheckboxColumn(
            "⭐",
            help="Ajouter à la whitelist",
            default=False,
            width="small"
        ),
        "scraped_at": st.column_config.TextColumn(
            "Date scraping",
            help="Date et heure du scraping",
            width="medium"
        ),
        "media_url": st.column_config.LinkColumn(
            "Média",
            help="Image ou vidéo de la publicité",
            display_text="📥 Voir",
            width="small"
        ),
        "cta_url": st.column_config.LinkColumn(
            "Lien CTA",
            help="Destination du bouton d'action",
            display_text="🔗 Ouvrir",
            width="small"
        ),
        "ad_library_url": st.column_config.LinkColumn(
            "Voir pub",
            help="Ouvrir dans Facebook Ads Library",
            display_text="🔗 Ouvrir",
            width="small"
        ),
        "page_id": st.column_config.TextColumn(
            "Page ID",
            help="Identifiant de la page Facebook",
            width="small"
        ),
        "advertiser": st.column_config.TextColumn(
            "Annonceur",
            help="Nom de la page Facebook",
            width="medium"
        ),
        "country": st.column_config.TextColumn(
            "Pays",
            help="Pays de diffusion",
            width="small"
        ),
        "ad_status": st.column_config.TextColumn(
            "Statut",
            help="Active ou Inactive",
            width="small"
        ),
        "media_type": st.column_config.TextColumn(
            "Type média",
            help="Image, vidéo ou N/A",
            width="small"
        ),
        "text": st.column_config.TextColumn(
            "Texte",
            help="Contenu textuel de la publicité",
            width="large"
        ),
        "start_date": st.column_config.TextColumn(
            "Date début",
            help="Date de lancement de la pub",
            width="medium"
        ),
    }


def get_disabled_columns(include_scraped_at=False):
    """
    Retourne la liste des colonnes en lecture seule
    
    Args:
        include_scraped_at: Inclure scraped_at dans les colonnes désactivées
    """
    disabled = [
        "media_url", "cta_url", "ad_library_url", "page_id",
        "advertiser", "country", "ad_status", "media_type",
        "text", "start_date"
    ]
    
    if include_scraped_at:
        disabled.append("scraped_at")
    
    return disabled


def prepare_dataframe_for_display(results, include_scraped_at=False):
    """
    Prépare un DataFrame avec les colonnes dans le bon ordre
    
    Args:
        results: Liste de dictionnaires (publicités)
        include_scraped_at: Inclure la colonne scraped_at
    
    Returns:
        DataFrame pandas prêt pour l'affichage
    """
    if not results:
        return pd.DataFrame()
    
    df = pd.DataFrame(results)
    
    # Ordre des colonnes
    if include_scraped_at:
        colonnes_a_afficher = [
            'scraped_at',
            'media_url', 'cta_url', 'ad_library_url', 'page_id',
            'advertiser', 'country', 'ad_status', 'media_type',
            'text', 'start_date',
        ]
    else:
        colonnes_a_afficher = [
            'media_url', 'cta_url', 'ad_library_url', 'page_id',
            'advertiser', 'country', 'ad_status', 'media_type',
            'text', 'start_date',
        ]
    
    # Filtrer uniquement les colonnes existantes
    colonnes_existantes = [col for col in colonnes_a_afficher if col in df.columns]
    df = df[colonnes_existantes]
    
    # Ajouter les colonnes de sélection
    df.insert(0, '⭐ Whitelist', False)
    df.insert(0, '🚫 Blacklist', False)
    
    return df


# ============================================
# AFFICHAGE DU TABLEAU AVEC GESTION COMPLÈTE
# ============================================

def display_ads_table(results, entry_id, search_key="", include_scraped_at=False, session_state=None, config=None):
    """
    Affiche le tableau de publicités avec toutes les fonctionnalités
    (sélection blacklist/whitelist, téléchargements, validation)
    
    Args:
        results: Liste des publicités à afficher
        entry_id: ID unique pour les clés des widgets
        search_key: Suffixe pour différencier les tables (ex: recherche active)
        include_scraped_at: Afficher la colonne scraped_at
        session_state: st.session_state pour accéder à la config
        config: Configuration (headless, etc.)
    """
    
    if not results:
        st.warning("Aucun résultat à afficher")
        return
    
    st.info(f"📊 {len(results)} résultat(s) affiché(s)")
    
    # Préparer le DataFrame
    df = prepare_dataframe_for_display(results, include_scraped_at)
    
    # Afficher le tableau éditable
    edited_df = st.data_editor(
        df,
        width="stretch",
        hide_index=False,
        key=f"ads_editor_{entry_id}_{search_key}",
        column_config=get_column_config(),
        disabled=get_disabled_columns(include_scraped_at)
    )
    
    # ============================================
    # VALIDATION ET BOUTONS D'AJOUT AUX LISTES
    # ============================================
    
    both_checked = edited_df[(edited_df['🚫 Blacklist'] == True) & (edited_df['⭐ Whitelist'] == True)]
    
    if not both_checked.empty:
        st.error("❌ Une page ne peut pas être à la fois en blacklist ET whitelist. Veuillez décocher l'une des deux options.")
    else:
        col_btn1, col_btn2 = st.columns(2)
        
        # ============================================
        # BOUTON BLACKLIST
        # ============================================
        with col_btn1:
            blacklist_pages = edited_df[edited_df['🚫 Blacklist'] == True]
            if not blacklist_pages.empty:
                if st.button(
                    f"✅ Ajouter {len(blacklist_pages)} page(s) à la blacklist",
                    type="primary",
                    key=f"add_blacklist_{entry_id}_{search_key}"
                ):
                    _add_to_list(blacklist_pages, 'blacklist', entry_id, config)
        
        # ============================================
        # BOUTON WHITELIST
        # ============================================
        with col_btn2:
            whitelist_pages = edited_df[edited_df['⭐ Whitelist'] == True]
            if not whitelist_pages.empty:
                if st.button(
                    f"✅ Ajouter {len(whitelist_pages)} page(s) à la whitelist",
                    type="primary",
                    key=f"add_whitelist_{entry_id}_{search_key}"
                ):
                    _add_to_list(whitelist_pages, 'whitelist', entry_id, config)
    
    # ============================================
    # BOUTONS DE TÉLÉCHARGEMENT
    # ============================================
    st.markdown("---")
    col_dl1, col_dl2 = st.columns(2)
    
    with col_dl1:
        df_export = pd.DataFrame(results)
        csv = df_export.to_csv(index=False, encoding='utf-8-sig')
        st.download_button(
            "📥 Télécharger CSV",
            data=csv,
            file_name=f"facebook_ads_{entry_id}.csv",
            mime="text/csv",
            key=f"csv_{entry_id}_{search_key}"
        )
    
    with col_dl2:
        json_str = json.dumps(results, ensure_ascii=False, indent=2)
        st.download_button(
            "📥 Télécharger JSON",
            data=json_str,
            file_name=f"facebook_ads_{entry_id}.json",
            mime="application/json",
            key=f"json_{entry_id}_{search_key}"
        )


# ============================================
# FONCTION PRIVÉE : AJOUT AUX LISTES
# ============================================

def _add_to_list(pages_df, list_type, entry_id, config):
    """
    Ajoute des pages à la blacklist ou whitelist via le script externe
    
    Args:
        pages_df: DataFrame des pages sélectionnées
        list_type: 'blacklist' ou 'whitelist'
        entry_id: ID pour le fichier temporaire
        config: Configuration (headless, etc.)
    """
    
    # Préparer la liste des IDs
    page_ids = pages_df['page_id'].tolist()
    
    # Sauvegarder dans un fichier temporaire
    temp_file = f"temp_batch_{entry_id}_{list_type}.json"
    with open(temp_file, 'w', encoding='utf-8') as f:
        json.dump(page_ids, f)
    
    # Conteneurs pour le statut
    status_container = st.empty()
    progress_container = st.empty()
    
    with st.spinner(f"🔍 Traitement de {len(page_ids)} page(s)..."):
        # Lancer le script en mode batch
        cmd = [
            sys.executable,
            'fb_id_retriever.py',
            list_type,
            '--batch',
            temp_file
        ]
        
        if config and config.get('headless', True):
            cmd.append('--headless')
        
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        # Afficher la progression
        max_wait = 300  # 5 minutes max
        elapsed = 0
        
        while elapsed < max_wait:
            status = _get_fb_id_status()
            
            if status:
                total = status.get('total', len(page_ids))
                current = status.get('current', 0)
                
                if total > 0:
                    progress_container.progress(int((current / total) * 100))
                
                status_container.info(
                    f"📊 {status.get('message', 'Traitement en cours...')}\n"
                    f"Page actuelle: {status.get('current_page', 'N/A')}"
                )
                
                if status.get('status') in ['completed', 'error']:
                    break
            
            if os.path.exists('fb_id_result.json'):
                break
            
            time.sleep(1)
            elapsed += 1
        
        # Récupérer le résultat
        if os.path.exists('fb_id_result.json'):
            with open('fb_id_result.json', 'r', encoding='utf-8') as f:
                result = json.load(f)
            
            success_count = result.get('success_count', 0)
            
            if success_count > 0:
                st.success(f"✅ {success_count}/{len(page_ids)} page(s) ajoutée(s) à la {list_type} !")
                st.rerun()
            else:
                st.warning("⚠️ Aucune page n'a pu être ajoutée")
        else:
            st.error("❌ Erreur lors du traitement")
        
        # Nettoyer le fichier temporaire
        if os.path.exists(temp_file):
            os.remove(temp_file)


def _get_fb_id_status():
    """Récupère le statut du script fb_id_retriever"""
    if os.path.exists('fb_id_status.json'):
        try:
            with open('fb_id_status.json', 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return None
    return None


# ============================================
# FONCTION DE FUSION DES PUBLICITÉS
# ============================================

def fusionner_publicites(all_ads):
    """
    Fusionne les publicités en doublon (même ad_id) en complétant les données manquantes
    
    Args:
        all_ads: Liste de toutes les publicités (avec potentiels doublons)
    
    Returns:
        Liste de publicités uniques avec données fusionnées
    """
    pubs_par_id = {}
    
    for ad in all_ads:
        ad_id = ad.get('ad_id')
        
        if not ad_id or ad_id == 'N/A':
            continue
        
        if ad_id not in pubs_par_id:
            # Première occurrence
            pubs_par_id[ad_id] = ad.copy()
        else:
            # Doublon : fusionner
            existing = pubs_par_id[ad_id]
            
            # Compléter les champs 'N/A' avec les nouvelles valeurs
            for key, value in ad.items():
                if existing.get(key) in ['N/A', '', None] and value not in ['N/A', '', None]:
                    existing[key] = value
            
            # Garder la date de scraping la plus récente
            if ad.get('scraped_at', '') > existing.get('scraped_at', ''):
                existing['scraped_at'] = ad['scraped_at']
    
    return list(pubs_par_id.values())