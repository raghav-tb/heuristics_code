import enchant

string1 = "Abbott India"
string2 = "ABBOTINDIA"
string3 = "BANKINDIA"


# string4 = "Apollo Hospitals Enterprise"
# string5 = "APOLLOHOSP"
# string6 = "INDIAMART"

print("INcorrect ONe:",enchant.utils.levenshtein(string1.upper(), string3.upper()))
print("Correct ONe:",enchant.utils.levenshtein(string1.upper(), string2.upper()))