import httpx, threading, os, csv, time, re
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    "94": "Val-de-Marne", "95": "Val-d'Oise", "971": "Guadeloupe", "972": "Martinique", "973": "Guyane", "974": "La Réunion", "976": "Mayotte", "988": "Nouvelle-Calédonie"}

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
    "Val-de-Marne": "94", "Val-d'Oise": "95", "Guadeloupe": "971", "Martinique": "972", "Guyane": "973", "La Réunion": "974", "Mayotte": "976", "Nouvelle-Calédonie": "988"}

# Dictionnaire des villes par code postal
villes_cp = {"Ablis": "78", "Achères": "78", "Annoeullin": "59", "Ardres": "62", "Ault": "80", "Ayacucho": "Pérou", "Bayeux": "14", "Beaucouzé": "49", "Beny-Bocage": "14", "Bessières": "31",
    "Bondues": "59", "Bretteville-L’Orgueilleuse": "14", "Bruxelles": "Belgique", "Bruyères le Châtel": "91", "Cabourg": "14", "Campo Grande": "Brésil", "Caudry": "59", "Cernay": "68",
    "Chenove": "21", "Cherbourg en Cotentin": "50", "Corbehem": "62", "Cosmic Bowl": "Brésil", "Cournon d’Auvergne": "63", "Dainville": "59", "Deauville": "14", "Deurne": "Belgique",
    "Douai": "59", "Etaples": "62", "Fécamp": "76", "Gzuck": "Pérou", "Hazebrouck": "59", "Ifs": "14", "La Roche-sur-Foron": "74", "La Tremblade": "17", "Ladoix Serrigny": "21", "Lambersart": "59",
    "Le Foeil": "22", "Le Havre": "76", "Havre": "76", "Le Theil sur Huisne": "61", "Lima « The Obstacle »": "Pérou", "Livarot": "14", "Louviers": "27", "Maurepas": "78", "Mers-les-Bains": "80", "Meyzieu": "69",
    "Montalieu-Vercieu": "38", "Montceau-les-Mines": "71", "Nanterre": "92", "Noeux-les-Mines": "62", "Otro": "87", "Outreau": "62", "Paris Avron": "75", "Pessac": "33", "Petite Ile": "974",
    "Poissy": "78", "Portet-sur-Garonne": "31", "Praça Duo": "Brésil", "Pug bowl": "Brésil", "Roncq": "59", "Saint-Chéron": "91", "Saint-André-Lez-Lille": "59", "Saint-Pierre-en-Auge": "14",
    "Saint-Saturnin": "72", "San Borja": "Pérou", "Saulx-les-Chartreux": "91", "Serris": "77", "Sourdeval": "50", "Surgères": "17", "Tournan en Brie": "77", "Troarn": "14", "Ventanilla": "Pérou",
    "Vire": "14", "Wargnies-le-Grand": "59", "Wasquehal": "59", "Tournan-en-Brie": "77", "Bény-Bocage": "14", "Bruyères-le-Chatel": "91", "Bretteville": "50", "Cherbourg-en-Cotentin": "50",
    "La Deauville": "14", "La Roche sur Foron": "74", "Ladoix-Serrigny": "21", "Livarot": "14", "Vassivière": "23", "Paris": "75", "Petite Île": "97", "St-Pierre-en-Auge": "14", "Huisne": "61"}


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
        
        # Extraction des liens en découpant le HTML brut
        page_links = response.text.split('elementor-row')[2].split('<span style="color: #000000;"><a style="color: #000000;" href="')
        all_links = [link.split('"')[0] for link in page_links if "https://" in link]  # Filtre les liens valides


        with LOCK:
            global links
            links += len(all_links)  # Met à jour le compteur global des liens trouvés

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
        response = httpx.get(link, headers=HEADERS, timeout=10, follow_redirects=True)
        if response.status_code != 200:
            printer.display("fail", f"Erreur {response.status_code} pour {link}")
            return None

        # Parse uniquement les informations des balises p, dans les blocs "classe "text-align: justify;"
        html = response.text
        bloc1 = html.split('text-align: justify;')[1].split('</p>')[0]
        bloc2 = html.split('text-align: justify;')[2].split('</p>')[0]

        soup = BeautifulSoup(response.text, 'html.parser')
        project_data = {}

        # Récupère les données des pages entre les balises strong
        try:
            ouvrage_maitre = bloc1.split(':</strong> ')[1].split("<br")[0].replace('<strong>', '')
            ouvrage_maitre = BeautifulSoup(ouvrage_maitre, "html.parser").text

            ouvrage_maitre = ouvrage_maitre.replace("Municipalité provinciale de ", "")
            ouvrage_maitre = ouvrage_maitre.replace("EPADESA/ ", "")
            ouvrage_maitre = ouvrage_maitre.replace("Conseil régional du Limousin/ Centre National de l’art et du paysage de l’ile de ", "")
            ouvrage_maitre = ouvrage_maitre.replace("Skatepark ", "")
            ouvrage_maitre = ouvrage_maitre.replace("Ville de ", "")
            ouvrage_maitre = ouvrage_maitre.replace("Ville d’", "")
            ouvrage_maitre = ouvrage_maitre.replace(" (Anvers)", "")
            ouvrage_maitre = ouvrage_maitre.replace("Atelier du Marais", "Sourdeval")
            ouvrage_maitre = ouvrage_maitre.replace("Communauté de communes du val d’", "")
            ouvrage_maitre = ouvrage_maitre.replace("Communauté de communes du pays de ’", "")
            ouvrage_maitre = ouvrage_maitre.replace("Ville du ", "")
            ouvrage_maitre = ouvrage_maitre.replace("Más Arquitectura / Doctor / Municipalité de ", "")
            ouvrage_maitre = ouvrage_maitre.replace("polyvalent", "Achères")
            ouvrage_maitre = ouvrage_maitre.replace("Communauté de communes du pays de ", "")
            ouvrage_maitre = ouvrage_maitre.replace("/ ArtoisCom", "")
            ouvrage_maitre = ouvrage_maitre.replace("EPA Marne EPA France", "Serris")
        except:
            ouvrage_maitre = "ouvrage pas trouvé"

        try:
            mission = bloc1.split('Mission :</strong> ')[1].split("<br")[0].replace('<strong>', '')
            mission = mission.replace("d&rsquo;", "d'")
        except:
            mission = ""

        try:
            groupement = bloc1.split('Groupement :</strong> ')[1].split("<br")[0].replace('<strong>', '')
            groupement = groupement.replace("d&rsquo;", "d'")
        except:
            groupement = ""

        try:
            equipement_type = bloc2.split('équipement :</strong> ')[1].split("<br")[0].replace('<strong>', '')
            equipement_type = equipement_type.replace("d&rsquo;", "d'")
        except:
            equipement_type = ""

        try:
            surface = bloc2.split('Surface :</strong> ')[1].split("<br")[0].replace('<strong>', '')
            surface = surface.replace("d&rsquo;", "d'")
        except:
            surface = ""

        try:
            budget = bloc2.split('Budget :</strong> ')[1].split("<br")[0].replace('<strong>', '')
            budget = budget.replace("d&rsquo;", "d'")
        except:
            budget = ""

        # Incrémente de 1 l'ID
        project_data["ID"] = project_id_counter
        project_id_counter += 1

        project_data["Active (0/1)"] = 1
        project_data["Name *"] = f"Skatepark {ouvrage_maitre}"

        postal_code = villes_cp.get(ouvrage_maitre)
        if postal_code:
            nom_departement = cdpostal_departement.get(postal_code, "Département non trouvé")
            categories = f"{postal_code} {nom_departement}"
        else:
            categories = "Code postal ou département non trouvé"
        project_data["Categories (x,y,z…)"] = f"Skatepark {categories}"

        project_data["Visibility"] = "both"

        description_parts = []
        if ouvrage_maitre and categories:
            description_parts.append(f"Skatepark {ouvrage_maitre} {categories}")
        elif ouvrage_maitre:
            description_parts.append(f"Skatepark {ouvrage_maitre}")
        else:
            description_parts.append("Skatepark")
            if categories:
                description_parts[-1] += f" {categories}"
        if mission:
            description_parts.append(mission)
        if groupement:
            description_parts.append(groupement)
        if equipement_type:
            description_parts.append(equipement_type)
        if surface:
            description_parts.append(surface)
        if budget:
            description_parts.append(budget)

        description = ", ".join(description_parts)
        project_data["Description"] = f'{description} <br><p>Source : <a href="https://antidoteskateparks.fr" target="_blank">antidoteskateparks.fr</a></p>'

        project_data["Available for order (0 = No 1 = Yes)"] = 0
        project_data["Show price (0 = No  1 = Yes)"] = 0
        project_data["Meta title"] = f"Skatepark {ouvrage_maitre} {categories}"
        project_data["Meta Description"] = f"{description[:160]}"  if description else "Description non trouvée"
        
        parsed_url = urlparse(link)
        url_rewritten = f"skatepark-{parsed_url.path.replace('/', '')}" if parsed_url.path else "URL rewritten non trouvé"
        project_data["URL rewritten"] = url_rewritten
        
        #  Sauvegarde l'url de la première image trouvé
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
def save_to_csv(data, filename="resultat_antidotes.csv"):
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
        print(f"Erreur lors de l'enregistrement des données dans le fichier : {e}") # Renvoie les erreurs d'enregistrement


# Fonction principale pour récupérer et traiter les données
def main():
    global lienvisite

    base_url = "https://antidoteskateparks.fr/projets/"
    project_links = get_project_links(base_url)  # Récupère les liens des projets

    visited_links = set()  # Ensemble pour éviter les doublons

    with ThreadPoolExecutor(max_workers=threads) as executor: # Système de threading pour visiter plusieurs pages simultanément
        futures = []
        for link in project_links:
            if link not in visited_links:
                futures.append(executor.submit(scrape_project_data, link)) # Lance le scrape

        for future in as_completed(futures):
            try:
                project_data = future.result()
                if project_data:
                    save_to_csv(project_data, "resultat_antidotes.csv")
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