import pandas as pd

print("="*60)
print("ANÁLISIS DE PÉRDIDA DE DATOS WoS")
print("="*60)

# Leer CSV final
df = pd.read_csv('RESULTS/datawos_scopus.csv')

print(f"\nTotal de registros en CSV final: {len(df)}")
print(f"\nDistribución por Source:")
if 'Source' in df.columns:
    print(df['Source'].value_counts())
else:
    print("⚠️ COLUMNA 'Source' NO EXISTE")
    print(f"Columnas disponibles: {df.columns.tolist()}")

print(f"\nDistribución por In_Both:")
if 'In_Both' in df.columns:
    print(df['In_Both'].value_counts())
else:
    print("⚠️ COLUMNA 'In_Both' NO EXISTE")

# Verificar si hay registros de WoS
if 'Source' in df.columns:
    wos_count = (df['Source'] == 'WoS').sum()
    scopus_count = (df['Source'] == 'Scopus').sum()
    
    print(f"\n📊 RESUMEN:")
    print(f"   Scopus: {scopus_count}")
    print(f"   WoS: {wos_count}")
    
    if wos_count == 0:
        print("\n❌ PROBLEMA DETECTADO: No hay registros de WoS en el resultado final")
        print("   Esto significa que TODOS los registros de WoS fueron eliminados")
    else:
        print(f"\n✅ OK: {wos_count} registros de WoS presentes")
