import pandas as pd

__all__ = ['load_cities']


def load_cities(filepath: str) -> pd.DataFrame:
    col_used = ['city_code', 'zip_code', 'latitude', 'longitude']
    df_cities = pd.read_csv(filepath, sep=',', usecols=col_used)
    df_cities.rename(columns={'city_code': 'ville', 'zip_code': 'code_postal'}, inplace=True)
    df_cities.ville = df_cities.ville.str.upper()
    # Gestion des arrondissements idiots
    arrond = df_cities.loc[df_cities.ville.str.endswith('01'), :].copy()
    arrond.ville = arrond.ville.str.replace('01', '').str.strip()
    df_cities = pd.concat([df_cities, arrond], ignore_index=True)
    # Drop dup cities
    df_cities = df_cities.iloc[df_cities.ville.drop_duplicates().index].copy()
    return df_cities
