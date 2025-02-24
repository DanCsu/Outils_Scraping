<?php
include_once('simple_html_dom.php');

// Fonction pour lire les cookies depuis un fichier JSON et les formater pour cURL
function getCookiesFromJson($file)
{
    if (!file_exists($file)) {
        die("Fichier cookies.json introuvable");
    }
    
    $cookiesArray = json_decode(file_get_contents($file), true);
    if (!$cookiesArray) {
        die("Erreur lors de la lecture du fichier cookies.json");
    }
    
    // Formate les cookies pour pouvoir les utiliser
    $cookies = [];
    foreach ($cookiesArray as $cookie) {
        $cookies[] = $cookie['name'] . '=' . $cookie['value'];
    }
    return implode('; ', $cookies);
}

// Fonction pour récupérer le HTML d'une URL en utilisant cURL avec des cookies de session
function getHTML($url, $cookiesFile)
{
    $cookies = getCookiesFromJson($cookiesFile);
    
    if (filter_var($url, FILTER_VALIDATE_URL)) {
        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, $url);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
        curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, false);
        curl_setopt($ch, CURLOPT_FOLLOWLOCATION, true);
        curl_setopt($ch, CURLOPT_USERAGENT, "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36");
        curl_setopt($ch, CURLOPT_COOKIE, $cookies); // Injection des cookies

        $html = curl_exec($ch);

        if (curl_errno($ch)) {
            echo "Erreur cURL : " . curl_error($ch);
        }
        
        curl_close($ch);
        return $html;
    } elseif (file_exists($url)) {
        return file_get_contents($url);
    }
    return false;
}

$fichier_sources = 'url_parse.txt';
$cookiesFile = 'cookies.json';
if (!file_exists($fichier_sources)) {
    die("Fichier url_parse.txt introuvable");
}

$sources = file($fichier_sources, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
$produits = [];

foreach ($sources as $source) {
    $html = getHTML($source, $cookiesFile);
    if (!$html) {
        echo "Erreur de chargement : $source\n";
        continue;
    }

    $dom = str_get_html($html);

    // Extraction des données à parser
    foreach ($dom->find('div.media') as $produit) {
        $item['ACTIVE'] = "1";
        $item['NAME'] = rtrim(ucwords(strtolower(after('DECK ', isset($produit->find('div.span.alib', 0)->plaintext) ? $produit->find('div.span.alib', 0)->plaintext : ''))));
        $item['CAT'] = "Planche Skateboard, Skateboards";
        $item['PRIX'] = between('PPC : ',',', isset($produit->find('div.attcprice', 0)->plaintext) ? $produit->find('div.attcprice', 0)->plaintext : '');
        $item['TVA'] = "12";
        $item['ONSALE'] = "0";
        $item['MARQUE'] = "Unity";
        $item['REF'] = isset($produit->find('div.span.aref', 0)->plaintext) ? $produit->find('div.span.aref', 0)->plaintext : '';
        $item['EAN13'] = after(': ', isset($produit->find('div.agencod', 0)->plaintext) ? $produit->find('div.agencod', 0)->plaintext : '');
        $item['QUANTITY'] = "2";
        $item['SHORTDESCRIPTION'] = "Unity planche skateboard";
        $item['DESCRIPTION'] = "Unity planche skateboard " . $item['NAME'] . " : avec grip Mob 9' classique offert, grippée par nos soins*. Cette planche de skateboard Unity célèbre l'inclusivité et la diversité tout en offrant une qualité exceptionnelle. Fabriquée en érable canadien, elle assure robustesse et réactivité, idéale pour le street ou le park. Avec un graphisme engagé et coloré, elle véhicule des messages positifs et progressistes, parfaite pour les skateurs souhaitant allier performance et valeurs fortes.";
        $item['METATITLE'] = "Unity planche skateboard " . $item['NAME'];
        $item['METADESCRIPTION'] = "Unity planche de skateboard " . $item['NAME'] . " : avec grip Mob 9' classique offert, au prix de " . $item['PRIX'] . " € ! Livraison offerte à partir de 50€* !";
        $item['AVAILABLEORDER'] = "1";
        $item['URL'] = strtolower("unity-planche-skateboard-" . str_replace(['.', ' '], '-', $item['NAME']));
        $item['VISUEL'] = isset($produit->find('a', 0)->href) ? $produit->find('a', 0)->href : '';
        $item['ONLYONLINE'] = "1";
        $item['QTE'] = after(': ', isset($produit->find('div.span.astock', 0)->plaintext) ? $produit->find('div.span.astock', 0)->plaintext : '');
        
        $produits[] = $item;
    }
}

// Sauvegarde des données en CSV
$dossier = 'resultat';
if (!file_exists($dossier)) {
    mkdir($dossier, 0777, true);
}

$chemin = $dossier . '/export-boards-unity.csv';
$delimiteur = ';';
$fichier_csv = fopen($chemin, 'w+');

foreach ($produits as $produit) {
    fputcsv($fichier_csv, $produit, $delimiteur);
}

fclose($fichier_csv);
echo "Scraping terminé : $chemin\n";
?>