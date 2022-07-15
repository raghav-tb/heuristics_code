import enchant

string1 = "Biocon Ltd"
string2 = "Biocon Limited"
string3 = "Bigbloc Construction Limited"


# string4 = "Apollo Hospitals Enterprise"
# string5 = "APOLLOHOSP"
# string6 = "INDIAMART"

print("INcorrect ONe:",enchant.utils.levenshtein(string1.upper(), string3.upper()))
print("Correct ONe:",enchant.utils.levenshtein(string1.upper(), string2.upper()))