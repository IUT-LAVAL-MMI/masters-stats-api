import pandas as pd

__all__ = ['load_disc_mapping']


def load_disc_mapping(filepath: str) -> pd.DataFrame:
    df_mapping = pd.read_csv(filepath, sep=';')
    return (df_mapping.loc[:, ['cand_sect_disc', 'ins_disc']]
            .rename(columns={'cand_sect_disc': 'id', 'ins_disc': 'insDiscId'})
            .set_index('id'))