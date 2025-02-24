import requests, threading,os, time, csv, httpx, re
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
villes_cp = {"Tressin": "59", "Regrippière": "44", "Reugny": "37", "Crèche": "79", "Bonnat": "23", "Paris": "75", "Pradez-le-Lez": "34", "Marly-la-Ville": "95", "Arlon": "67", "Mérindol": "84",
    "Loulans-Verchamp": "70", "Arras": "62", "Chenevelles": "86", "Duneau": "72", "Castillon-la-Bataille": "33", "Tignes": "73", "Herbignac": "44", "Montpellier": "34", "Bourgueil": "37",
    "Chaulgnes": "58", "Chabournay": "86", "Maisonneuve": "86", "Saint-Just-en-Chaussée": "60", "Devecey": "25", "Coudray-Macouard": "49", "Lacq": "64", "Sombernon": "21",
    "Lesquin": "59", "Droue-sur-Drouette": "28", "Fresnoy-lès-Roye": "80", "Rivery": "80", "Besançon": "25", "Montignac-Lascaux": "24", "Arlon": "67", "Authon": "41", "Luçon": "85", "Fontaines": "71",
    "Richelieu": "37", "Geaune": "40", "Kremlin-Bicêtre": "94", "Mionnay": "01", "Arras": "62", "Biscarrosse": "40", "Fons-sur-Lussan": "30", "Lyon": "69", "Loches": "37", "Charpey": "26", "Lit-et-Mixe": "40",
    "Neufchâtel-Hardelot": "62", "Mortagne-sur-Sèvre": "85", "Rully": "71", "Moncoutant": "79", "Saméon": "59", "Cuchery": "51", "Melesse": "35", "Pignan": "34", "Châteauneuf-du-Pape": "84", "Fontaines-les-Dijon": "21",
    "Rueil-Malmaison": "92", "Calonne-Ricouart": "62", "Latour-de-France": "66", "Tuffé": "72", "Vivy": "49", "Angers": "49", "Trégastel": "22", "Fosse": "62", "Panazol": "87", "St Georges d’Oleron": "17", "Guichen": "35",
    "Brinon-sur-Sauldre": "18", "Saintes-Maries-de-la-Mer": "13", "Nazelles-Négron": "37", "Sarreguemines": "57", "Pierrefite-Nestalas": "65", "Dijon": "21", "Contis": "40", "Artannes-sur-Indre": "37", "Montbazon": "37",
    "Joué-les-Tours": "37", "ile-Bouchard": "37", "Rosny-sous-bois": "93", "Chateauneuf-du-Pape": "84", "Tregastel": "22"}

# Blacklist les url non traitable
liensinutile = ["rampe-skate-sur-camion/", "module-extension-bowl-marseille/", "rampe-skate-champtoceaux-49/", "rampe-skate-a-villeneuve-la-dondagre-89", "module-skate-table-street-combi-au-skatepark-des-pupilles-de-lair",
    "kit-rampe-skate-wheels-waves", "mini-rampe-skate-artigues-pre-bordeaux", "skatepark-de-vodk-unicornwal-angers-49", "curb-rampe-skate-zz10-saint-denis-93", "kit-mini-rampe-video-de-montage",
    "mini-rampe-skate-les-arcs-1800-bourg-saint-maurice", "rampe-skate-globe-eu-seignosse-40-landes", "rampe-skatepark-kremlin-bicetre-94", "rampe-skate-deco-football", "modules-de-skatepark-lac-de-caniel",
    "skatepark-du-lac-de-caniel-76", "skatepark-de-hellemmes-59", "mini-rampe-skatepark-de-val-thorens-savoie-auvergne-rhone-alpes"]


# Met à jour le titre de la fenêtre avec les statistiques
def UpdateTitle():
    global links, lienvisite

    while True:
        os.system(f"title Lien: {links} ~ Scrap: {lienvisite}")
        time.sleep(3)


# Récupère les liens d'un projet depuis une URL donnée      
def get_project_links(page_url):
    printer = MagicPrinterUIX()
    try:
        response = httpx.get(page_url, headers=HEADERS, timeout=10) # Envoie une requête GET avec un timeout
        if response.status_code != 200:
            printer.display("fail", f"Erreur {response.status_code} lors de l'accès à {page_url}")
            return []

        soup = BeautifulSoup(response.text, 'html.parser') # Analyse le HTML de la page

        # Recherche des liens dans les balises <a> avec la classe spécifique
        portfolio_links = soup.find_all('a', {'class': 'fusion-link-wrapper'}, href=True)
        all_links = [link['href'] for link in portfolio_links if link['href'].startswith("https://")]

        with LOCK:
            global links
            links += len(all_links)

        printer.display("info", f"{len(all_links)} liens trouvés sur {page_url}")
        return all_links

    except Exception as e:
        printer.display("warn", f"Erreur lors de la récupération des liens : {e}")
        return []


# Extrait les données à partir d'un lien récupérer avec le get_project_links
def scrape_project_data(link):
    global project_id_counter
    printer = MagicPrinterUIX()

    if any(x in link for x in liensinutile):
        printer.display("warn", f"Le lien {link} est blacklisté car il correspond à un lien inutile.")
        return None

    try:
        response = httpx.get(link, headers=HEADERS, timeout=10, follow_redirects=True)
        response.encoding = 'utf-8'
        if response.status_code != 200:
            printer.display("fail", f"Erreur {response.status_code} pour {link}")
            return None
        
        html = response.text
        blocprincipal = html.split('class="project-description post-content"')[1].split('class="portfolio-sep"')[0]

        soup = BeautifulSoup(response.text, 'html.parser')
        soupliste = BeautifulSoup(blocprincipal, 'html.parser')
        project_data = {}

        # Récupère le titre de la page et supprime les données non traitable
        h1_tag = soup.find('h1')
        if h1_tag:
            titrepage = h1_tag.text.strip()
            titrepage = titrepage.replace("Skatepark de ", "")
            titrepage = titrepage.replace("La ", "")
            titrepage = titrepage.replace(" – Modules Street & Rampe", "")
            titrepage = titrepage.replace("L’", "")
            titrepage = titrepage.replace("Skatepark ", "")
            titrepage = titrepage.replace("Rampe Skate de ", "")
            titrepage = titrepage.replace("Rampe Skatepark ", "")
            titrepage = titrepage.replace("Skatepark de ", "")
            titrepage = titrepage.replace(" – Mini Rampe H100L366", "")
            titrepage = titrepage.replace("Skatepark des ", "")
            titrepage = titrepage.replace("Pro Kit Ramp Skate – ", "")
            titrepage = titrepage.replace("Funbox | Nouvelle Table Street au ", "")
            titrepage = titrepage.replace("Nouveau Skatepark à ", "")
            titrepage = titrepage.replace(" – Street & Rampe", "")
            titrepage = titrepage.replace("Skatepark à ", "")
            titrepage = titrepage.replace("Nouveau Skatepark avec Funbox à ", "")
            titrepage = titrepage.replace(" – Rampe Spine", "")
            titrepage = titrepage.replace("Big Ramp – Halfpipe à ", "")
            titrepage = titrepage.replace("BMX Skatepark international de ", "")
            titrepage = titrepage.replace("BMX international de ", "")
            titrepage = titrepage.replace(" Ronan Pointeau", "")
            titrepage = titrepage.replace("Virage Wallride – Pumptrack de ", "")
            titrepage = titrepage.replace("Cycloïde Piazza de Raphaël Zarka – Centre Pompidou – ", "")
            titrepage = titrepage.replace("VAl-Thorens", "Val-Thorens")
            titrepage = titrepage.replace("Nouveau avec Funbox à ", "")
            titrepage = titrepage.replace("à ", "")
            titrepage = titrepage.replace("de ", "")
            titrepage = titrepage.replace("Nouveau ", "")
            titrepage = titrepage.replace("des ", "")
            titrepage = titrepage.replace("Rampe ", "")
            titrepage = titrepage.replace("de Latour-de-France", "Latour-de-France")
            titrepage = titrepage.replace("de Chateauneuf-du-Pape", "Chateauneuf-du-Pape")
            titrepage = re.sub(r'\s*\([A-Za-z0-9\s-]*\)$', '', titrepage)
        else:
            titrepage = "Titre non trouvé"

        ul_tags = soupliste.find('ul')
        if ul_tags:
            all_li = ""
            for i in ", ".join((i.split('</li>')[0]) for i in str(ul_tags).split('<li>')[1:]).split("<"):
                if ">" in i:
                    all_li += i.split(">")[1]
                else:
                    all_li += i

            if all_li.startswith("Skatepark"):
                all_li = all_li[len("Skatepark"):].strip()

            all_li = all_li.replace(".,", ",").replace(" ,", ",").replace(" &amp;","")
            if all_li.endswith("."):
                all_li = all_li[:-1]
        else:
            p_tags = soupliste.find_all('p')
            if p_tags:
                all_li = ""
                for i in ", ".join((i.split('</p>')[0]) for i in str(p_tags).split('<p>')[1:]).split("<"):
                    if ">" in i:
                        all_li += i.split(">")[1]
                    else:
                        all_li += i

                if all_li.startswith("Skatepark"):
                    all_li = all_li[len("Skatepark"):].strip()

                all_li = all_li.replace(".,", ",").replace(" ,", ",").replace(" &amp;", "")
                all_li = all_li[:160]

        project_data["ID"] = project_id_counter
        project_id_counter += 1
        project_data["Active (0/1)"] = 1
        project_data["Name *"] = f"Skatepark en bois {titrepage}"

        postal_code = villes_cp.get(titrepage)
        if postal_code:
            nom_departement = cdpostal_departement.get(postal_code, "Département non trouvé")
            categories = f"{postal_code} {nom_departement}"
        else:
            categories = "Code postal ou département non trouvé"

        project_data["Categories (x,y,z…)"] = f"Skatepark {categories}"
        project_data["Visibility"] = "both"
        project_data["Description"] = f'Skatepark en bois {titrepage} {categories}, {all_li[:1].lower()}{all_li[1:]} <br><p>Source : <a href="https://www.wood-structure.fr" target="_blank">wood-structure.fr</a></p>'
        project_data["Available for order (0 = No 1 = Yes)"] = 0
        project_data["Show price (0 = No  1 = Yes)"] = 0
        project_data["Meta title"] = f"Skatepark en bois {titrepage[:1].lower()}{titrepage[1:]} {categories[:1].lower()}{categories[1:]}"
        project_data["Meta Description"] = f"Skatepark en bois {titrepage} {categories}, {all_li[:1].lower()}{all_li[1:160]}"  if all_li else "Description non trouvée"

        parsed_url = urlparse(link)
        if parsed_url.path:
            path_parts = parsed_url.path.split('/', 2)
            cleaned_path = path_parts[2] if len(path_parts) > 2 else ""

            cleaned_path = cleaned_path.replace('/', '-').replace('--', '-')
            while '--' in cleaned_path:
                cleaned_path = cleaned_path.replace('--', '-')
            cleaned_path = cleaned_path.lstrip('-').removeprefix("skatepark-")
            url_rewritten = f"skatepark-bois-{cleaned_path.strip('-')}"
        else:
            url_rewritten = "URL rewritten non trouvé"
        project_data["URL rewritten"] = url_rewritten

        # Prend l'url de la première image trouvé et l'a sauvegarde
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
def save_to_csv(data, filename="resultat_woodstructure.csv"):
    if not data:
        return

    file_exists = False
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            file_exists = bool(file.readline())
    except FileNotFoundError:
        file_exists = False
   
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

    base_url = "https://wood-structure.com/portfolio_skills/skatepark-exterieur/"
    project_links = []  # Liste pour stocker tous les liens des projets

    project_links.extend(get_project_links(base_url))  # Récupère les liens de la première page

    for page_num in range(2, 6):  # Boucle pour récupérer les liens des pages suivantes
        page_url = f"{base_url}page/{page_num}/"  # Génère l'URL de la page
        project_links.extend(get_project_links(page_url))  # Ajoute les liens trouvés

    visited_links = set()  # Ensemble pour éviter les doublons


    with ThreadPoolExecutor(max_workers=threads) as executor:  # Gestion du multi-threading
        futures = []
        for link in project_links:
            if link not in visited_links:  # Vérifie si le lien a déjà été visité
                futures.append(executor.submit(scrape_project_data, link))  # Lance le scraping en parallèle

        for future in as_completed(futures):
            try:
                project_data = future.result()
                if project_data:
                    save_to_csv(project_data, "resultat_woodstructure.csv")  # Enregistre les résultats dans le CSV
                    visited_links.add(project_data["URL rewritten"])  # Ajoute à la liste des liens visités
                    with LOCK:
                        lienvisite += 1  # Met à jour le compteur des liens visités
                else:
                    print("Aucune donnée trouvée pour ce lien.")
            except Exception as e:
                print(f"Erreur lors de l'exécution du thread: {e}")


# Exécute le script
if __name__ == "__main__":
    with ThreadPoolExecutor() as executor:
        executor.submit(UpdateTitle)
        main()