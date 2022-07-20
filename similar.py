# import enchant

# string1 = "Tata Consultancy Services Lt"
# string2 = "ATUL"
# string3 = "TCS"


# # string4 = "Apollo Hospitals Enterprise"
# # string5 = "APOLLOHOSP"
# # string6 = "INDIAMART"

# print("INcorrect ONe:",enchant.utils.levenshtein(string1.upper(), string2.upper()))
# print("Correct ONe:",enchant.utils.levenshtein(string1.upper(), string3.upper()))



s = 'canada japan australia - raghav'
l = s.split(' ')[-3][0]
print(l)