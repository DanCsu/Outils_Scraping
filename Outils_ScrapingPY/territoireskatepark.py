import requests, threading,os, time, csv, httpx
from bs4 import BeautifulSoup
from lxml import html
from urllib.parse import urlparse, urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from colorama import Fore, Back, init; init()

# Variables globales et configuration
HEADERS = {"User-Agent": "Mozilla/5.0"}
LOCK = threading.Lock()
links, lienvisite = 0, 0
project_id_counter = 1
threads = 4


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
cdpostal_departement = {"01": "Ain", "02": "Aisne", "03": "Allier", "04": "Alpes-de-Haute-Provence", "05": "Hautes-Alpes", "06": "Alpes-Maritimes", "07": "Ardèche", "08": "Ardennes",
    "09": "Ariège", "10": "Aube", "11": "Aude", "12": "Aveyron", "13": "Bouches-du-Rhône", "14": "Calvados", "15": "Cantal", "16": "Charente", "17": "Charente-Maritime", "18": "Cher",
    "19": "Corrèze", "2A": "Corse-du-Sud", "2B": "Haute-Corse", "21": "Côte-d'Or", "22": "Côtes-d'Armor", "23": "Creuse", "24": "Dordogne", "25": "Doubs", "26": "Drôme", "27": "Eure",
    "28": "Eure-et-Loir", "29": "Finistère", "30": "Gard", "31": "Haute-Garonne", "32": "Gers", "33": "Gironde", "34": "Hérault", "35": "Ille-et-Vilaine", "36": "Indre",  "39": "Jura",
    "37": "Indre-et-Loire", "38": "Isère", "40": "Landes", "41": "Loir-et-Cher", "42": "Loire", "43": "Haute-Loire", "44": "Loire-Atlantique", "45": "Loiret", "46": "Lot", "48": "Lozère",
    "47": "Lot-et-Garonne", "49": "Maine-et-Loire", "50": "Manche", "51": "Marne", "52": "Haute-Marne", "53": "Mayenne", "54": "Meurthe-et-Moselle", "55": "Meuse", "56": "Morbihan",
    "57": "Moselle", "58": "Nièvre", "59": "Nord", "60": "Oise", "61": "Orne", "62": "Pas-de-Calais", "63": "Puy-de-Dôme", "64": "Pyrénées-Atlantiques", "65": "Hautes-Pyrénées", "78": "Achères",
    "66": "Pyrénées-Orientales", "67": "Bas-Rhin", "68": "Haut-Rhin", "69": "Rhône", "70": "Haute-Saône", "71": "Saône-et-Loire", "72": "Sarthe", "73": "Savoie", "74": "Haute-Savoie",
    "75": "Paris", "76": "Seine-Maritime", "77": "Seine-et-Marne", "78": "Yvelines", "79": "Deux-Sèvres", "80": "Somme", "81": "Tarn", "82": "Tarn-et-Garonne", "83": "Var", "84": "Vaucluse",
    "85": "Vendée", "86": "Vienne", "87": "Haute-Vienne", "88": "Vosges", "89": "Yonne", "90": "Territoire de Belfort", "91": "Essonne", "92": "Hauts-de-Seine", "93": "Seine-Saint-Denis",
    "94": "Val-de-Marne", "95": "Val-d'Oise", "97": "Outre-mer", "971": "Guadeloupe", "972": "Martinique", "973": "Guyane", "974": "La Réunion", "976": "Mayotte", "988": "Nouvelle-Calédonie"}

# Dictionnaire des code postal par départements
departement_cdpostal = {"Ain": "01", "Aisne": "02", "Allier": "03", "Alpes-de-Haute-Provence": "04", "Hautes-Alpes": "05", "Alpes-Maritimes": "06", "Ardèche": "07", "Ardennes": "08",
    "Ariège": "09", "Aube": "10", "Aude": "11", "Aveyron": "12", "Bouches-du-Rhône": "13", "Calvados": "14", "Cantal": "15", "Charente": "16", "Charente-Maritime": "17", "Cher": "18",
    "Corrèze": "19", "Corse-du-Sud": "2A", "Haute-Corse": "2B", "Côte-d'Or": "21", "Côtes-d'Armor": "22", "Creuse": "23", "Dordogne": "24", "Doubs": "25", "Drôme": "26", "Eure": "27",
    "Eure-et-Loir": "28", "Finistère": "29", "Gard": "30", "Haute-Garonne": "31", "Gers": "32", "Gironde": "33", "Hérault": "34", "Ille-et-Vilaine": "35", "Indre": "36", "Vendée": "85",
    "Indre-et-Loire": "37", "Isère": "38", "Jura": "39", "Landes": "40", "Loir-et-Cher": "41", "Loire": "42", "Haute-Loire": "43", "Loire-Atlantique": "44", "Loiret": "45","Lot": "46",
    "Lot-et-Garonne": "47", "Lozère": "48", "Maine-et-Loire": "49", "Manche": "50", "Marne": "51", "Haute-Marne": "52", "Mayenne": "53", "Meurthe-et-Moselle": "54", "Meuse": "55",
    "Morbihan": "56", "Moselle": "57", "Nièvre": "58", "Nord": "59", "Oise": "60", "Orne": "61", "Pas-de-Calais": "62", "Puy-de-Dôme": "63", "Pyrénées-Atlantiques": "64", "Tarn-et-Garonne": "82",
    "Hautes-Pyrénées": "65", "Pyrénées-Orientales": "66", "Bas-Rhin": "67", "Haut-Rhin": "68", "Rhône": "69", "Haute-Saône": "70", "Saône-et-Loire": "71", "Sarthe": "72", "Savoie": "73",
    "Haute-Savoie": "74", "Paris": "75", "Seine-Maritime": "76", "Seine-et-Marne": "77", "Yvelines": "78", "Deux-Sèvres": "79", "Somme": "80", "Tarn": "81", "Var": "83", "Vaucluse": "84",
    "Vienne": "86", "Haute-Vienne": "87", "Vosges": "88", "Yonne": "89", "Territoire de Belfort": "90", "Essonne": "91", "Hauts-de-Seine": "92", "Seine-Saint-Denis": "93",
    "Val-de-Marne": "94", "Val-d'Oise": "95", "Outre-mer": "97", "Guadeloupe": "971", "Martinique": "972", "Guyane": "973", "La Réunion": "974", "Mayotte": "976", "Nouvelle-Calédonie": "988"}

# Dictionnaire des villes par code postal
villes_cp = {"Balma": "31", "Berre d'Etang": "13", "Bordeaux": "33", "Calais": "62", "Cannes": "06", "Clermont-Ferrand": "63", "Apt": "84", "Les Portes en Ré": "17", "Aix-en-Provence": "13",
    "Cabriès": "13", "Carqueiranne": "83", "Carry-le-Rouet": "13", "Chaponost": "69", "Corbas": "69", "Cranves-Sales": "74", "Gréasque": "13", "La-Roche-Sur-Foron": "74", "Les-2-Alpes": "38",
    "Lumbin": "38", "Saint-André": "97", "Saint-Joseph": "97", "Saint-Just-Saint-Rambert": "42", "Saint-Martin-de-Crau": "13", "Sartrouville": "78", "Solaize": "69", "Isle sur la Sorgue": "84",
    "la Ciotat": "13", "Corcieux": "88", "Cuers": "83", "FISE": "34", "Goudargues": "30", "Hyères-les-Palmiers": "83", "Marseille": "13", "Montceau-les-Mines": "71", "Noisy-le-Grand": "93",
    "Paulhan": "34", "Plan d'Orgon": "13", "Port-Saint-Louis-du-Rhône": "13", "Privas": "07", "Puy-en-Velay": "43", "Robion": "84", "Lyon": "69", "Saint-Chamas": "13", "Saint-Pierre-d'Oléron": "17",
    "Teil": "07", "Venelles": "13", "la Tour d'Aigues": "84", "Illkirch-Graffenstaden": "67", "Ville d'Illkirch-Graffenstaden": "67", "Carpentras": "84", "Berre l'Etang": "13", "Berre Etang": "13"}


# Met à jour le titre de la fenêtre avec les statistiques
def UpdateTitle():
    global links, lienvisite

    while True:
        os.system(f"title Lien: {links} ~ Scrap: {lienvisite}")
        time.sleep(3)


# Récupère les liens d'un projet depuis une URL donnée  
def get_project_links(base_url):
    printer = MagicPrinterUIX()
    try:
        response = httpx.get(base_url, headers=HEADERS, timeout=10) # Envoie une requête GET avec un timeout
        if response.status_code != 200:
            printer.display("fail", f"Erreur {response.status_code} lors de l'accès à {base_url}")
            return []

        soup = BeautifulSoup(response.text, 'html.parser')  # Analyse le HTML de la page
        
        # Recherche de la section contenant les projets
        portfolio_grid = soup.find('div', {'class': 'wpex-row vcex-portfolio-grid wpex-clr entries match-height-grid vcex-isotope-grid wpex-overflow-hidden'})
        all_links = []  # Liste pour stocker les liens

        if portfolio_grid:  
            linkss = portfolio_grid.find_all('a', href=True)  # Récupère toutes les balises <a> avec un href
            for link in linkss:
                href = link['href']
                if "https://" in href:  # Vérifie si le lien est absolu
                    all_links.append(href) 

        with LOCK:
            global links
            links += len(all_links)

        printer.display("info", f"{len(all_links)} liens trouvés sur {base_url}")
        return all_links

    except Exception as e:
        printer.display("warn", f"Erreur lors de la récupération des liens : {e}")
        return []


# Extrait les données à partir d'un lien récupérer avec le get_project_links
def scrape_project_data(link):
    global project_id_counter
    printer = MagicPrinterUIX()
    try:
        response = httpx.get(link, headers=HEADERS, timeout=10, follow_redirects=True)  # Envoie une requête avec gestion des redirections
        response.encoding = 'utf-8' # Assure l'encodage en UTF-8
        if response.status_code != 200:
            printer.display("fail", f"Erreur {response.status_code} pour {link}")
            return None # Retourne None en cas d'erreur

        html = response.text # Récupère le contenu HTML de la page
        blocprincipal = html.split('class="wpb_text_column wpb_content_element"')[2].split('class="vc_empty_space"')[0] # Extrait une section spécifique du HTML

        soup = BeautifulSoup(response.text, 'html.parser') # Analyse le HTML avec BeautifulSoup
        project_data = {} # Initialise le dictionnaire pour stocker les données du projet

        # Recupère les données spécifique dans les balises p des strong
        try:
            ouvrage = blocprincipal.split('ouvrage</strong> ')[1].split("</p")[0]
            ouvrage = ouvrage.replace(": ", "")
            ouvrage = ouvrage.replace("Ville de ", "")
            ouvrage = ouvrage.replace("Ville d’", "")
            ouvrage = ouvrage.replace("Ville d'", "")
            ouvrage = ouvrage.replace("Ville du ", "")
            ouvrage = ouvrage.replace("d&rsquo;", "d'")
            ouvrage = ouvrage.replace("l&rsquo;", "l'")
            ouvrage = ouvrage.replace("l'", "")
            ouvrage = ouvrage.replace("Commune de ", "")
            ouvrage = ouvrage.replace("Commune Les", "Les")
            ouvrage = ouvrage.replace("Communauté des Communes Pays d'", "")
            ouvrage = ouvrage.replace(" Luberon, SPL-Territoire", "")
            ouvrage = ouvrage.replace("Commune de d'", "")
            ouvrage = ouvrage.replace("SPL ", "")
            ouvrage = ouvrage.replace(" Confluence", "")
            ouvrage = ouvrage.replace("la Ciotat ", "la Ciotat")
            ouvrage = ouvrage.replace(" (13)", "")
            ouvrage = ouvrage.replace(" (74)", "")
            ouvrage = ouvrage.replace(" (69)", "")
            ouvrage = ouvrage.replace(" (17)", "")
            ouvrage = ouvrage.replace(" (38)", "")
            ouvrage = ouvrage.replace(" (97)", "")
            ouvrage = ouvrage.replace(" (84)", "")
            ouvrage = ouvrage.replace(" (07)", "")
            ouvrage = ouvrage.replace(" (42)", "")
            ouvrage = ouvrage.replace(" (83)", "")
            ouvrage = ouvrage.replace(" (71)", "")
            ouvrage = ouvrage.replace(" (63)", "")
            ouvrage = ouvrage.replace(" (62)", "")
            ouvrage = ouvrage.replace(" (67)", "")
            ouvrage = ouvrage.replace(" (34)", "")
            ouvrage = ouvrage.replace(" (93)", "")
            ouvrage = ouvrage.replace(" (95)", "")
        except:
            ouvrage = ""

        try:
            oeuvre = blocprincipal.split('vre</strong> ')[1].split("</p")[0]
            oeuvre = oeuvre.replace(": ", "")
            oeuvre = oeuvre.replace("d&rsquo;", "d'")
            oeuvre = oeuvre.replace("l&rsquo;", "d'")
        except:
            oeuvre = ""

        try:
            livraison = blocprincipal.split('ison</strong> ')[1].split("</p")[0]
            livraison = livraison.replace(": ", "")
            livraison = livraison.replace("d&rsquo;", "d'")
            livraison = livraison.replace("l&rsquo;", "d'")
        except:
            livraison = ""

        try:
            surface = blocprincipal.split('epark</strong> ')[1].split("</p")[0]
            surface = surface.replace(": ", "")
            surface = surface.replace("d&rsquo;", "d'")
            surface = surface.replace("l&rsquo;", "d'")
        except:
            surface = ""

        project_data["ID"] = project_id_counter
        project_id_counter += 1
        project_data["Name *"] = f"Skatepark {ouvrage}"
        project_data["Active (0/1)"] = 1

        postal_code = villes_cp.get(ouvrage)
        if postal_code:
            nom_departement = cdpostal_departement.get(postal_code, "Département non trouvé")
            categories = f"{postal_code} {nom_departement}"
        else:
            categories = "Code postal ou département non trouvé"
        project_data["Categories (x,y,z…)"] = f"Skatepark {categories}"

        project_data["Visibility"] = "both"

        description_parts = []
        if ouvrage and categories:
            description_parts.append(f"Skatepark {ouvrage} {categories}")
        elif ouvrage:
            description_parts.append(f"Skatepark {ouvrage}")
        else:
            description_parts.append("Skatepark")
            if categories:
                description_parts[-1] += f" {categories}"
        if oeuvre:
            description_parts.append(oeuvre)
        if livraison:
            description_parts.append(livraison)
        if surface:
            description_parts.append(surface)

        description = ", ".join(description_parts)
        project_data["Description"] = f'{description} <br><p>Source : <a href="https://www.territoireskatepark.fr" target="_blank">territoireskatepark.fr</a></p>'

        project_data["Available for order (0 = No 1 = Yes)"] = 0
        project_data["Show price (0 = No  1 = Yes)"] = 0
        project_data["Meta title"] = f"Skatepark {ouvrage} {categories}"
        project_data["Meta Description"] = f"{description[:160]}"  if description else "Description non trouvée"

        parsed_url = urlparse(link)
        if parsed_url.path:
            cleaned_path = parsed_url.path.replace('/', '-').replace('--', '-')
            while '--' in cleaned_path:
                cleaned_path = cleaned_path.replace('--', '-')
            url_rewritten = f"skatepark-{cleaned_path.strip('-')}"
        else:
            url_rewritten = "URL rewritten non trouvé"

        project_data["URL rewritten"] = url_rewritten
            
        # Enregistre l'url de la première image trouvé
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
                main_image = urljoin(link, img_tag['src'])
                break
        project_data["Image"] = main_image if main_image else "Image non trouvée"

        printer.display("success", f"Données extraites pour {link}")
        return project_data

    except Exception as e:
        printer.display("fail", f"Erreur lors du scraping de {link} : {e}")
        return None


# Sauvegarde les données récupérer dans un fichier csv
def save_to_csv(data, filename="resultat_territoires.csv"):
    if not data:
        return

    file_exists = False
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            file_exists = bool(file.readline()) # Vérifie si le fichier contient déjà des données
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

    base_url = "https://www.territoireskatepark.fr/archives/"
    project_links = get_project_links(base_url)

    visited_links = set()

    with ThreadPoolExecutor(max_workers=threads) as executor:  # Exécute le scraping en parallèle
        futures = []
        for link in project_links:
            if link not in visited_links:
                futures.append(executor.submit(scrape_project_data, link))

        for future in as_completed(futures): # Gère l'exécution des threads fini
            try:
                project_data = future.result() # Récupère les données du scraping
                if project_data:
                    save_to_csv(project_data, "resultat_territoires.csv") # Enregistre les données dans le CSV
                    visited_links.add(project_data["URL rewritten"]) # Marque le lien comme visité
                    with LOCK:
                        lienvisite += 1 # Incrémente le compteur des projets visités
                else:
                    print("Aucune donnée trouvée pour ce lien.")
            except Exception as e:
                print(f"Erreur lors de l'exécution du thread: {e}")


# Exécute le script
if __name__ == "__main__":
    with ThreadPoolExecutor() as executor:
        executor.submit(UpdateTitle)
        main()
