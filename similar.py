import enchant

string1 = "NIPPON LIFE INDIA ASSET MANAGEMENT LIMITED"
string2 = "BANDHANBNK"
string3 = "NAM-INDIA"


string4 = "Apollo Hospitals Enterprise"
string5 = "APOLLOHOSP"
string6 = "INDIAMART"

print("INcorrect ONe:",enchant.utils.levenshtein(string1.upper(), string2))
print("Correct ONe:",enchant.utils.levenshtein(string1.upper(), string3))