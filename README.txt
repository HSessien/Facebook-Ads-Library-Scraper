# ============================================
# README.txt
# ============================================
================================
FACEBOOK ADS LIBRARY SCRAPER
================================

📦 INSTALLATION
-----------------------

WINDOWS :
1. Double-cliquez sur "install_windows.bat"
2. Attendez 5-10 minutes
3. Double-cliquez sur "lancer.bat"

MAC/LINUX :
1. Terminal : chmod +x install_mac_linux.sh lancer.sh
2. Exécuter : ./install_mac_linux.sh
3. Lancer : ./lancer.sh


📁 STRUCTURE DES FICHIERS
-----------------------
scraper.py              → Application principale
config.json             → Paramètres (créé automatiquement)
ignored_pages.csv       → Pages à ignorer (créé automatiquement)
install_windows.bat     → Installation Windows
install_mac_linux.sh    → Installation Mac/Linux
lancer.bat              → Lanceur Windows
lancer.sh               → Lanceur Mac/Linux
requirements.txt        → Dépendances Python


🚀 UTILISATION
-----------------------

1. PAGE PRINCIPALE :
   - Sélectionnez le pays
   - Choisissez l'état (actif/inactif/tous)
   - (Optionnel) Filtrez par date
   - (Optionnel) Entrez un terme de recherche
   - Cliquez sur "Lancer le scraping"

2. PARAMÈTRES (barre latérale) :
   - Gérez les pages à ignorer via le CSV
   - Choisissez mode visible/invisible
   - Ajustez les pauses entre requêtes


🚫 PAGES À IGNORER
-----------------------

Le fichier "ignored_pages.csv" contient 3 colonnes :
- date_ajout : Date d'ajout de la page
- nom_page : Nom de la page Facebook
- id_page : ID de la page (optionnel)

Exemple :
date_ajout,nom_page,id_page
2024-11-16,Nike Official,123456789
2024-11-16,Adidas France,987654321

Pour gérer la liste :
1. Cliquez sur "Ouvrir le fichier CSV" dans les paramètres
2. Modifiez avec Excel ou Notepad
3. Sauvegardez
4. Cliquez sur "Recharger la liste"


⚙️ PARAMÈTRES
-----------------------

MODE INVISIBLE :
- Activé : le navigateur est masqué (plus rapide)
- Désactivé : vous voyez le navigateur (pour déboguer)

PAUSES ENTRE REQUÊTES :
- Min/Max : l'application choisit aléatoirement dans cet intervalle
- Recommandé : 2-5 secondes pour éviter la détection
- Plus lent : 5-10 secondes (très sûr mais long)

Tous les paramètres sont sauvegardés automatiquement.


📤 PARTAGE SUR UN AUTRE PC
-----------------------

1. Copiez TOUT le dossier
2. Sur le nouveau PC :
   - Installez Python (python.org)
   - Lancez install_windows.bat (ou .sh)
   - Lancez lancer.bat (ou .sh)

Les fichiers config.json et ignored_pages.csv sont copiés aussi,
donc vos paramètres et liste de pages ignorées sont conservés.


⚠️ LIMITATIONS
-----------------------

- Facebook peut bloquer si trop de requêtes
- Recommandé : 30-50 pubs/heure maximum
- Utilisez des pauses de 2-5 secondes minimum
- Le mode invisible peut être moins stable


🔧 DÉPANNAGE
-----------------------

L'application ne démarre pas :
→ Vérifiez que Python est installé
→ Réexécutez install_*.bat/sh

Erreur "module not found" :
→ pip install streamlit playwright pandas
→ playwright install chromium

Le CSV ne s'ouvre pas :
→ Ouvrez-le manuellement avec Excel/Notepad
→ Chemin : même dossier que scraper.py

Le scraping ne trouve rien :
→ Vérifiez vos critères de recherche
→ Essayez sans filtre de date
→ Vérifiez votre connexion internet


💡 CONSEILS
-----------------------

POUR DÉBUTER :
1. Testez avec 100-200 pubs maximum
2. Mode visible pour voir ce qui se passe
3. Pauses 3-5 secondes

POUR PRODUCTION :
1. Mode invisible (plus rapide)
2. Pauses 2-4 secondes
3. 500-1000 pubs max par session
4. Espacez vos sessions de 30-60 minutes

POUR ÉVITER LES BLOCAGES :
1. Ne pas dépasser 50 pubs/heure
2. Utiliser des pauses longues (5-10s)
3. Espacer les sessions
4. Varier les termes de recherche


📊 FORMAT DES RÉSULTATS
-----------------------

Les résultats contiennent :
- ad_id : Identifiant unique de la pub
- advertiser : Nom de la page/annonceur
- text : Texte de la publicité (500 premiers caractères)
- date : Date de lancement
- platforms : Plateformes de diffusion
- scraped_at : Date/heure d'extraction

Formats disponibles : CSV et JSON


🆘 SUPPORT
-----------------------

En cas de problème :
1. Vérifiez ce README
2. Consultez les messages d'erreur dans le terminal
3. Vérifiez que tous les fichiers sont présents
4. Réinstallez avec install_*.bat/sh


Version 1.0 - Novembre 2024