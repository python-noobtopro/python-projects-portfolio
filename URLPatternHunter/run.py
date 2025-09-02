from main.PatternClass import PatternHunter as ph
import pandas as pd

df = pd.read_csv("C:\\Users\\Rupesh.Ranjan\\Downloads\\Tier1_Requests.csv")
urls = df['careerURL'].to_list()
print(urls)
phunt = ph(urls).run()

df = pd.read_json(phunt)
print(df)
df.to_excel("Tier1_Requests_Pattern_run.xlsx", index=False)




