### INTRO ###

import os
import pandas as pd

dir_path = "/home/haze/Documentos/Programa/AtomProyects/RUCAS/tablas/TEST/test_consultas_SQL/Tablas/"

bases = {}

for file in os.listdir(dir_path):
    print(f"Procesando {file.split('.')[0]}")
    db = pd.read_csv(dir_path+file, sep=',', thousands=".", header = 0, error_bad_lines=False, encoding="ISO-8859-1")
    if db.columns.shape[0] < 2:
        db = pd.read_csv(dir_path+file, sep=';', thousands=",", header = 0, error_bad_lines=False, encoding="ISO-8859-1")
    bases[file.split('.')[0]] = db
          
bases["w1_bdm_e_beta"].head() 

for base in bases:
    bases[base]["folio_unico"] = bases[base]["folio_villa"].astype('str') + '-' + bases[base]["folio_vivienda"].astype('str')

### PARTE I ###

cols = ["folio_unico", "H3", "H5", "H9", "H7", "CS19", "CS20", "O19a", "O20"]

df1 = bases["w1_bdm_e_beta"].loc[:, ["folio_villa", "folio_vivienda"] + cols]
df2 = bases["w2_bdm_e_beta"].loc[:, cols]
df3 = bases["w3_bdm_e_beta"].loc[:, cols]
df = (df1.join(df2.set_index("folio_unico"), how="left", on="folio_unico", lsuffix="_00", rsuffix="_01")
         .join(df3.set_index("folio_unico"), how="left", on="folio_unico", lsuffix="", rsuffix="_02")
     )

df.head()
#df.to_csv('output_test1.csv', sep=',', encoding='utf-8', index = False)


#### PARTE II ###

cols2 = ["SB3", "SU2_1", "CS19", "SA15a", "folio_unico"]
cols3 = ["P1_1", "P1_2", "P11", "folio_unico"] 

dfa = bases["w1_bdm_e_beta"].loc[:, ["folio_vivienda"] + cols2]
dfb = bases["w1_bdm_p"].loc[:, cols3]
dfc = bases["w2_bdm_p"].loc[:, cols3]
dfd = bases["w3_bdm_p"].loc[:, cols3]


dfb1 = (dfa.join(dfb.set_index("folio_unico"), how="left", on="folio_unico", lsuffix="_00a", rsuffix="_01a")
        .join(dfc.set_index("folio_unico"), how="left", on="folio_unico", lsuffix="", rsuffix="_02a")
        .join(dfd.set_index("folio_unico"), how="left", on="folio_unico", lsuffix="", rsuffix="_03a")
       )


dfb1.head()
#dfb1.to_csv('ejemplo2.csv', sep=',', encoding='utf-8', index = False)
          
#### PARTE III ###

#### Base con información de Brisas del Mar y Marta Brunet de la pauta de observación, todas las preguntas. Las olas y villas se agregan como filas. 

df7 = bases["w1_bdm_p"].loc[:, ["folio_villa", "folio_vivienda"] + cols]
df8 = bases["w1_mb_p"].loc[:, cols]
#dfc = (df7.join(df8.set_index('folio_unico'), how="left", on="folio_unico", lsuffix="_00", rsuffix="_01")
#      )

result = pd.concat([df7, df8], sort = True) 

result.head()

result.to_csv('ejemplo3.csv', sep=',', encoding='utf-8', index = False)

# ###### P1_2 solo debería estar en bm1 
# Probar el concat con variables específicas y forzar poner variables diferentes (ver error) 
# Ordenar las variables (folios al principio)

def count_col(df, col):
    return df[col].count()


count_H3 = count_col(dfb1, 'SD1')


df["P1_1"].fillna(0).astype('int').head() 

df["P1_1"] = df["P1_1"].astype('int') 




