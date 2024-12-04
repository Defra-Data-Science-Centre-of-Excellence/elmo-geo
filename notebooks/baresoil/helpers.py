# aux


def get_cols(df, ptn):
    for col in df.columns:
        if col.startswith(ptn):
            yield col


def get_cols_wfm(df, ptn):
    """getting cols and ..."""
    if isinstance(ptn, tuple):
        ptn = ("id_businesses", "id_parcel", *ptn)
    else:
        ptn = ("id_businesses", "id_parcel", ptn)

    return get_cols(df, ptn)
