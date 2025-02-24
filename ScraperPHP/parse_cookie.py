from selenium import webdriver
import json
import time

options = webdriver.ChromeOptions()
options.add_argument("--start-maximized")
options.add_experimental_option("excludeSwitches", ["enable-logging"])
driver = webdriver.Chrome(options=options)

url = "https://b2b.v7distribution.com/"
driver.get(url)

print("Veuillez vous connecter manuellement, puis appuyez sur Entrée une fois connecté.")
input("Appuyez sur Entrée pour continuer...")

time.sleep(3)
cookies = driver.get_cookies()

if cookies:
    with open("cookies.json", "w") as file:
        json.dump(cookies, file, indent=4)
    print("Cookies sauvegardés dans cookies.json")
else:
    print("Aucun cookie récupéré. Assurez-vous d'être bien connecté.")

driver.quit()