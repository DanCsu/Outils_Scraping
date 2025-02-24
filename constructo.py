import httpx, threading, os, csv, time, re
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin
from datetime import datetime
from colorama import Fore, Back, init; init()

# Variables globales et configuration
HEADERS = {"User-Agent": "Mozilla/5.0"}
LOCK = threading.Lock()
links, lienvisite = 0, 0
project_id_counter = 1
threads = 3


# Classe pour afficher des messages colorés et enregistrer des données
class MagicPrinterUIX:
    def __init__(self):
        self.StatusClass = { # Définition des styles pour chaque type de message
            "info": {"icon": "*", "color": Fore.LIGHTBLUE_EX, "reset": Fore.RESET},
            "success": {"icon": "+", "color": Fore.LIGHTGREEN_EX, "reset": Fore.RESET},
            "fail": {"icon": "!", "color": Fore.LIGHTRED_EX, "reset": Fore.RESET},
            "warn": {"icon": "x", "color": Back.LIGHTRED_EX, "reset": Back.RESET},
        }

    def display(self, status, text, Ecrase=False):
        pre = self.StatusClass[status] # Récupère le style du statut
        time_now = datetime.now().strftime('%H:%M:%S') # Obtient l'heure actuelle
        end_char = "\r" if Ecrase else "\n" # Gère le retour à la ligne ou l'écrasement de la ligne précédente

        with LOCK: # Assure que l'affichage est thread-safe
            print(f"[{Fore.LIGHTYELLOW_EX}{time_now}{Fore.RESET}] ({pre['color']} {pre['icon']} {pre['reset']}) {text}", end=end_char)  

    def Save(self, data, path):
        with LOCK: # Empêche les écritures concurrentes sur le fichier
            with open(path, "a+") as f:
                f.write(data + "\n")
                

# Dictionnaire des départements par code postal
cdpostal_departement = {"01": "Ain", "02": "Aisne", "03": "Allier", "04": "Alpes-de-Haute-Provence", "05": "Hautes-Alpes", "06": "Alpes-Maritimes", "07": "Ardèche", "08": "Ardennes", "09": "Ariège", "10": "Aube", "11": "Aude",
    "12": "Aveyron", "13": "Bouches-du-Rhône", "14": "Calvados", "15": "Cantal", "16": "Charente", "17": "Charente-Maritime", "18": "Cher", "19": "Corrèze", "2A": "Corse-du-Sud", "2B": "Haute-Corse", "21": "Côte-d'Or", "22": "Côtes-d'Armor",
    "23": "Creuse", "24": "Dordogne", "25": "Doubs", "26": "Drôme", "27": "Eure", "28": "Eure-et-Loir", "29": "Finistère", "30": "Gard", "31": "Haute-Garonne", "32": "Gers", "33": "Gironde", "34": "Hérault", "35": "Ille-et-Vilaine", "36": "Indre",
    "37": "Indre-et-Loire", "38": "Isère", "39": "Jura", "40": "Landes", "41": "Loir-et-Cher", "42": "Loire", "43": "Haute-Loire", "44": "Loire-Atlantique", "45": "Loiret", "46": "Lot", "47": "Lot-et-Garonne", "48": "Lozère", "49": "Maine-et-Loire",
    "50": "Manche", "51": "Marne", "52": "Haute-Marne", "53": "Mayenne", "54": "Meurthe-et-Moselle", "55": "Meuse", "56": "Morbihan", "57": "Moselle", "58": "Nièvre", "59": "Nord", "60": "Oise", "61": "Orne", "62": "Pas-de-Calais", "63": "Puy-de-Dôme",
    "64": "Pyrénées-Atlantiques", "65": "Hautes-Pyrénées", "66": "Pyrénées-Orientales", "67": "Bas-Rhin", "68": "Haut-Rhin", "69": "Rhône", "70": "Haute-Saône", "71": "Saône-et-Loire", "72": "Sarthe", "73": "Savoie", "74": "Haute-Savoie",
    "75": "Paris", "76": "Seine-Maritime", "77": "Seine-et-Marne", "78": "Yvelines", "79": "Deux-Sèvres", "80": "Somme", "81": "Tarn", "82": "Tarn-et-Garonne", "83": "Var", "84": "Vaucluse", "85": "Vendée", "86": "Vienne", "87": "Haute-Vienne", "88": "Vosges",
    "89": "Yonne", "90": "Territoire de Belfort", "91": "Essonne", "92": "Hauts-de-Seine", "93": "Seine-Saint-Denis", "94": "Val-de-Marne", "95": "Val-d'Oise", "971": "Guadeloupe", "972": "Martinique", "973": "Guyane", "974": "La Réunion", "976": "Mayotte", "988": "Nouvelle-Calédonie"}


# Met à jour le titre de la fenêtre avec les statistiques
def UpdateTitle():
    global links, lienvisite
    
    while True:
        os.system(f"title Lien: {links} ~ Scrap: {lienvisite}")
        time.sleep(3)


# Récupère les liens d'un projet depuis une URL donnée
def get_project_links(url):
    global links

    printer = MagicPrinterUIX()
    try:
        response = httpx.get(url, headers=HEADERS, timeout=10) # Envoie une requête GET avec un timeout
        if response.status_code != 200:
            printer.display("fail", f"Erreur {response.status_code} lors de l'accès à la page {url}.")
            return []
            
        # Extraction de la section contenant les liens
        soup = BeautifulSoup(response.text, 'html.parser')
        section = soup.find('div', class_='cst-section')
        if not section:
            printer.display("warn", "Section non trouvé.")
            return []
        
        # Extraction et nettoyage des liens
        found_links = list(set(urljoin(url, a['href']) for a in section.find_all('a', href=True)))

        with LOCK:
            links += len(found_links)

        printer.display("info", f"{len(found_links)} liens trouvés.")
        return found_links

    except httpx.RequestError as e:
        printer.display("warn", f"Erreur de requête : {e}")
        return []


# Extrait les données à partir d'un lien récupérer avec le get_project_links
def scrape_project_data(url):
    global project_id_counter
    printer = MagicPrinterUIX()
    for _ in range(3):
        try:
            response = httpx.get(url, headers=HEADERS, timeout=10)
            if response.status_code == 200:
                break
            time.sleep(2)
        except httpx.RequestError:
            printer.display("warn", f"Erreur d'accès à {url}, nouvelle tentative..")
            time.sleep(2)
    else:
        printer.display("fail", f"Échec après plusieurs tentatives : {url}")
        return None

    soup = BeautifulSoup(response.text, 'html.parser')
    project_data = {}

    # Extraction du nom et du département depuis le h1
    h1_tag = soup.find('h1')
    categories = h1_tag.get_text(strip=True) if h1_tag else "Departement non trouvé"

    # Recherche du code postal dans le titre
    postal_code_match = re.search(r"\((\d{2,3})\)", categories)
    if postal_code_match:
        postal_code = postal_code_match.group(1)
        nom_departement = cdpostal_departement.get(postal_code)
        categories = f"{postal_code} {nom_departement}"
    else:
        categories = "Departement non trouvé"

    # Extraction des informations pour le sauvegarder dans le csv
    project_data["Categories (x,y,z…)"] = f"Skatepark {categories}"

    # Regroupe les informations récolter sur le site pour en faire la description
    description = ""
    p_tag = soup.find('p')
    if p_tag:
        programme_tag = p_tag.find('strong', string="Programme :")
        if programme_tag:
            programme_value = programme_tag.find_next('br').previous_sibling.strip() if programme_tag.find_next('br') else "Programme non trouvé"
            description += ", " + programme_value if description else programme_value

        equipe_tag = p_tag.find('strong', string="Equipe :")
        if equipe_tag:
            equipe_value = equipe_tag.find_next('br').previous_sibling.strip() if equipe_tag.find_next('br') else "Equipe non trouvée"
            description += ", " + equipe_value if description else equipe_value

        surface_tag = p_tag.find('strong', string="Surface :")
        if surface_tag:
            surface_value = surface_tag.find_next('br').previous_sibling.strip() if surface_tag.find_next('br') else "Surface non trouvée"
            description += ", " + surface_value if description else surface_value

        cout_tag = p_tag.find('strong', string="Coût des travaux :")
        if cout_tag:
            cout_value = cout_tag.find_next('br').previous_sibling.strip() if cout_tag.find_next('br') else "Coût des travaux non trouvé"
            description += ", " + cout_value if description else cout_value

    if description and description.endswith(", "):
        description = description[:-2]

    h1_tag = soup.find('h1')
    villecp = h1_tag.get_text(strip=True) if h1_tag else "Departement non trouvé"
    nomville = villecp.split()[0]

    code_postal_match = re.search(r"\((\d{2,3})\)", villecp)
    if code_postal_match:
        code_postal = code_postal_match.group(1)
        departement_nom = cdpostal_departement.get(code_postal)
        villecp = f"{code_postal} {departement_nom}"
    else:
        villecp = "Departement non trouvé"

    project_data["Description"] = (
    f'Skatepark {nomville} {villecp}, {description} plus d\'infos: <p><a href="{url}" target="_blank">constructo.fr</a></p>'
    if description else "Description non trouvée"
    )

    categorie = [""]
    p_tag = soup.find('p')
    if p_tag:
        maitrise_tag = p_tag.find('strong', string="Maitrise d’ouvrage :")
        if maitrise_tag:
            maitrise_value = maitrise_tag.find_next('br').previous_sibling.strip() if maitrise_tag.find_next('br') else "Maitrise d’ouvrage non trouvée"

            prefixes_to_remove = ["Ville de", "Ville d’"]
            for prefix in prefixes_to_remove:
                if prefix in maitrise_value:
                    maitrise_value = maitrise_value.replace(prefix, "").strip()
            
            categorie.append(maitrise_value)
        else:
            categorie.append("Maitrise d’ouvrage non trouvée")

    # Ecrit les donnéees récolter dans le project_data
    project_data["Name *"] = "Skatepark ".join(categorie) if categorie else "Categories non trouvée"
    project_data["Meta title"] = f"Skatepark {nomville} {villecp}"
    project_data["Meta Description"] = f"Skatepark {nomville} {villecp}, {description[:160]}" if description else "Description non trouvée"

    # Prend l'url de la page, et la modifie
    parsed_url = urlparse(url)
    url_rewritten = f"skatepark-{parsed_url.path.replace('/', '')}" if parsed_url.path else "URL rewritten non trouvé"
    project_data["URL rewritten"] = url_rewritten

    # Sauvegarde l'url de la première image trouvé
    main_image = None
    possible_selectors = [
        'div.main img',
        'div.content img',
        'section img',
        'article img',
        'div img'
    ]
    for selector in possible_selectors:
        img_tag = soup.select_one(selector)
        if img_tag and 'src' in img_tag.attrs:
            main_image = urljoin(url, img_tag['src'])
            break
    project_data["Image"] = main_image if main_image else "Image non trouvée"

    project_data["Active (0/1)"] = 1
    project_data["Visibility"] = "both"
    project_data["Available for order (0 = No 1 = Yes)"] = 0
    project_data["Show price (0 = No  1 = Yes)"] = 0

    project_data["ID"] = project_id_counter
    project_id_counter += 1

    printer.display("success", f"Scraping réussi pour {url}")
    return project_data


# Sauvegarde les données récupérer dans un fichier csv
def save_to_csv(data, filename="resultat_constructo.csv"):
    if not data:
        return
    
    file_exists = False
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            file_exists = bool(file.readline())  # Vérifie si le fichier contient déjà des données
    except FileNotFoundError:
        file_exists = False  # Indique que le fichier n'existe pas encore

    # Définition des colonnes du fichier CSV
    keys = ["ID", "Active (0/1)", "Name *", "Categories (x,y,z…)", "Visibility", "Description", "Available for order (0 = No 1 = Yes)", "Show price (0 = No  1 = Yes)", "Meta title", "Meta Description", "URL rewritten", "Image"]
    
    try:
        with LOCK: # Assure la synchronisation entre les threads
            with open(filename, mode='a', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=keys)

                if not file_exists:
                    writer.writeheader()

                writer.writerow(data)
                file.flush()
                
    except Exception as e:
        print(f"Erreur lors de l'enregistrement des données dans le fichier : {e}")


# Fonction principale pour récupérer et traiter les données
def main():
    global lienvisite

    base_url = "https://www.constructo.fr/projets-par-date/"
    project_links = get_project_links(base_url) # Récupère les liens des projets

    visited_links = set() # Ensemble pour éviter les doublons

    with ThreadPoolExecutor(max_workers=threads) as executor: # Système de threading pour visiter plusieurs pages simultanément
        futures = []
        for link in project_links:
            if link not in visited_links:
                futures.append(executor.submit(scrape_project_data, link)) # Lance le scrape

        for future in as_completed(futures):
            try:
                project_data = future.result()
                if project_data:
                    save_to_csv(project_data, "resultat_constructo.csv")
                    visited_links.add(project_data["URL rewritten"])
                    with LOCK:
                        lienvisite += 1
                else:
                    print("Aucune donnée trouvée pour ce lien.")
            except Exception as e:
                print(f"Erreur lors de l'exécution du thread: {e}")


# Exécute le script
if __name__ == "__main__":
    with ThreadPoolExecutor() as executor:
        executor.submit(UpdateTitle)
        main()