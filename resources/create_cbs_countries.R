# Kansenkaart data - migration background (western/non-western) resource code
#
# LANDENREF resource code
#   - based on CBS: K:\8_Utilities\Code_Listings\SSBreferentiebestanden\LANDAKTUEELREF10.sav
#   - determine whether a country is a western or non-western country
# 
# (c) ODISSEI Social Data Science team 2021


#### PACKAGES ####
library(tidyverse)
library(haven)

# create countries
tab <- tibble(
LANDEN = c("Nederland", "Turkije", "Marokko", "Suriname", "Nederlandse Antillen (oud)",
  "Frankrijk", "Zwitserland", "Oostenrijk", "België", "Hongarije", "Cyprus", 
  "Zweden", "IJsland", "Griekenland", "Ierland", "Finland", "Luxemburg",
  "Noorwegen", "Oekraine", "Spanje", "Grootbrittannië", "Duitsland", "Monaco",
  "Albanië",  "Bosnië-Herzegovina", "Andorra", "Malta", "Bulgarije", "Polen",  
  "Italië", "Roemenië", "Tsjechoslowakije", "Kroatië", "Portugal", "Letland", 
  "Estland", "Litouwen", "Duitse Democratische Republiek", "Moldavië", 
  "Bondsrepubliek Duitsland", "Slovenië", "Azerbajdsjan", "Belarus (Wit-Rusland)", 
  "Macedonië", "Kosovo", "Georgië", "Tsjechië", "Slowakije",  "Kanaaleilanden",
  "Federale Republiek Joegoslavië", "Montserrat", "Man", "Azoren", "Groenland",
  "Servië en Montenegro",  "Servië", "Montenegro", "Denemarken",  "Joegoslavië",
  "Gibraltar", "Canada", "Verenigde Staten van Amerika", "Hawaii-eilanden", "Newfoundland",
  "Nieuwzeeland", "Australië","Australisch Nieuwguinea", "Cookeilanden",
  "Papua-Nieuwguinea", "Nederlands Nieuwguinea", "Tasmanië", "Nauru",
  "Nieuwcaledonië", "Marshalleilanden", "Gilberteilanden",  "Solomoneilanden", 
  "Nieuwehebriden", "Fiji", "Frans Polynesië", "Britse Salomons-eilanden",
  "Westsamoa", "Indonesië", "Japan", "Nederlands Indië", "Laos", "Filipijnen", 
  "Burma", "Zuidviëtnam", "Singapore", "Brunei", "Taiwan",
  "Bhutan",  "Macau",  "Malakka", "Portugees Indië",  "China",  "Noordviëtnam",
  "Zuidkorea", "Nepal", "Hongkong", "Sri Lanka", "Armenië", "Maleisië",  "Maldiven",
  "Thailand", "India", "Mongolië", "Tibet", "Rusland", "Brits Indië", "Brits Borneo",
  "Frans Indo China", "Brits Westborneo", "Siam",  "Brits Noordborneo", "Sabah",
  "Bangladesh", "Viëtnam", "Sovjetunie", "Ceylon", "Sarawak", "Korea","Noordkorea",
  "Johore", "Myanmar", "Frans Indië", "Kazachstan", "Kyrgyzstan", "Oezbekistan",
  "Portugees Timor", "Tadzjikistan", "Toerkmenistan", "Iran", "Saoediarabië", "Irak",
  "Bahrein", "Afganistan", "Israël", "Jordanië",
  "Verenigde Arabische Emiraten", "Syrië", "Zuidjemen", "Pakistan", "Libanon",  
  "Koeweit", "Oman", "Palestina", "Aden", "Katar", "Verenigde Arabische Republiek",
  "Aboe Dhabi", "Jemen", "Zuidarabische Federatie","Muscat en Oman", 
  "Oem el Koewein", "Doebai", "Noordjemen", "Tunesië", "Malawi", "Botswana",  
  "Zuidafrika", "Liberia", "Etiopië", "Zaïre",
  "Ghana", "Togo", "Angola", "Mali", "Zambia", "Ivoorkust", "Kameroen", "Mauritius",
  "Frans Somaliland", "Kaapverdische Eilanden", "Mozambique", "Guinee Bissau",
  "Seychellen en Amiranten","Tonga", "Zuidwest Afrika", "Burundi", "Burkina Faso",
  "Nigeria", "Libië", "Rwanda", "Somalia", "Mauritanië", "Kambodja",  "Niger",
  "Bovenvolta", "Gabon", "Algerije", "Sierra Leone", "São Tomé en Principe",
  "Sint Helena", "Réunion",  "Uganda", "Kenya",  "Gambia", "Egypte", "Lesotho",
  "Senegal",  "Dahomey", "Tanzania", "Soedan", "Guinee",  "Tanganyika",  
  "Portugees Afrika", "Brits Afrika", "Belgisch Congo",  "Noordrhodesië", 
  "Zuidrhodesië", "Goudkust", "Frans Congo", "Brits Oostafrika", "Benin",
  "Madeira-eilanden", "Spaanse Sahara", "Brits Territorium in de Indische Oceaan",
  "Kaapverdië", "Seychellen", "Zimbabwe",  "Nyasaland", "Eritrea", 
  "Brits Kameroen", "Kongo", "Kongo Kinshasa", "Madagaskar", "Kongo Brazzaville", 
  "Portugees Guinee", "Namibië", "Brits Somaliland", "Italiaans Somaliland", 
  "Brits Guyana", "Swaziland", "Equatoriaalguinee", "Zanzibar", "Bechuanaland", 
  "Frans Kameroen","Portugees Oost Afrika", "Portugees West Afrika", "Tsjaad",
  "Frans West Afrika", "Frans Equatoriaal Afrika", "Oeroendi", "Roeanda-Oeroendi", 
  "Goa", "Centrafrika", "Djibouti", "Westelijke Sahara", "Rhodesië", "Comoren",
  "Democratische Republiek Congo", "Cabinda", "Zuid-Soedan", "Basoetoland",
  "Frans gebied van Afars en Issa's", "Spaans Guinee",  "Spaans Noordafrika", 
  "Mayotte", "Abessinië", "Cuba", "Sint Vincent en de Grenadinen", "Aruba", 
  "Guatemala", "Jamaica",
  "Bahama-eilanden", "Haïti", "Trinidad en Tobago",  "Brits Honduras",
  "Canarische eilanden", "Costa Rica", "Guadeloupe",  "Martinique", "Mexico",
  "Britse Antillen", "Barbados", "Honduras", "Nicaragua", "Dominicaanse Republiek",
  "El Salvador", "Panama",  "Brits Westindië","Grenada", "Sint Lucia", "Dominica",
  "Anguilla", "Saint Kitts-Nevis", "Antigua", "Sint Vincent", "Panamakanaalzone",
  "Antigua en Barbuda", "Bermuda", "Falklandeilanden", "Belize", "Curaçao",
  "Turks- en Caicoseilanden", "Saint Kitts-Nevis-Anguilla", "Bonaire",
  "Sint Eustatius", "Sint Maarten", "Britse Maagdeneilanden", "Caymaneilanden", 
  "Amerikaanse Maagdeneilanden", "Puerto Rico", "Chili", "Colombia", "Paraguay",  
  "Brazilië", "Venezuela", "Bolivia",  "Guyana",
  "Frans Guyana", "Argentinië", "Uruguay", "Ecuador","Peru"))
  
  
  # create western/non-western variable
western <- c(
  
  # Europe
  "Frankrijk", "Zwitserland", "Oostenrijk", "België", "Hongarije", "Cyprus", 
  "Zweden", "IJsland", "Griekenland", "Ierland", "Finland", "Luxemburg",
  "Noorwegen", "Oekraine", "Spanje", "Grootbrittannië", "Duitsland", "Monaco",
  "Albanië",  "Bosnië-Herzegovina", "Andorra", "Malta", "Bulgarije", "Polen",  
  "Italië", "Roemenië", "Tsjechoslowakije", "Kroatië", "Portugal", "Letland", 
  "Estland", "Litouwen", "Duitse Democratische Republiek", "Moldavië", 
  "Bondsrepubliek Duitsland", "Slovenië", "Azerbajdsjan", "Belarus (Wit-Rusland)", 
  "Macedonië", "Kosovo", "Georgië", "Tsjechië", "Slowakije",  "Kanaaleilanden",
  "Federale Republiek Joegoslavië", "Montserrat", "Man", "Azoren", "Groenland",
  "Servië en Montenegro",  "Servië", "Montenegro", "Denemarken",  "Joegoslavië",
  "Gibraltar", "Nederland",
  
  # North America
  "Canada", "Verenigde Staten van Amerika", "Hawaii-eilanden", "Newfoundland",
  
  # Oceania
  "Nieuwzeeland", "Australië","Australisch Nieuwguinea", "Cookeilanden",
  "Papua-Nieuwguinea", "Nederlands Nieuwguinea", "Tasmanië", "Nauru",
  "Nieuwcaledonië", "Marshalleilanden", "Gilberteilanden",  "Solomoneilanden", 
  "Nieuwehebriden", "Fiji", "Frans Polynesië", "Britse Salomons-eilanden",
  "Westsamoa",
  
  # Asia
  "Indonesië", "Japan", "Nederlands Indië")


# create landtype
tab <- tab %>%
  mutate(LANDTYPE = ifelse(LANDEN %in% western, "Westers", "NietWesters"))


# save tab
write_sav(tab, "resources/LANDAKTUEELREF10.sav")

