import os, asyncio, aiohttp, async_timeout, pandas as pd, requests
from supabase import create_client

class _O:
    v1, v2, v3, v4, v5, v6 = (os.getenv(f"V{i}") for i in range(1, 7))
    k1, k2, k3, k4 = (os.getenv(k) for k in ["API_KEY_ID", "API_KEY_RESULT", "API_KEY_MSG", "API_VAL_OK"])
    c1, c2, c6 = (os.getenv(k) for k in ["C1", "C2", "C6"])
    ic, iv, rk = (os.getenv(k) for k in ["INTERNAL_CAT_COL", "INTERNAL_VAL_COL", "RPC_PARAM_KEY"])
    ca, cb, cg, cd = (os.getenv(k) for k in ["CAT_A_NAME", "CAT_B_NAME", "CAT_G_NAME", "CAT_D_NAME"])
    h = {'user-agent': 'Dart/3.4 (dart:io)', 'Connection': 'close'}
    t, c = 10, 20
    m1 = {3: ca, 7: ca, 10: cb, 2: cb, 8: cg, 9: cg, 11: cd}
    m2 = {0: ca, 1: cb, 2: cg, 3: cd}

def _f1():
    x = []
    if not all([_O.v1, _O.k1, _O.k2]): return pd.DataFrame()
    for p1 in range(4):
        for p2 in range(1, 10):
            try:
                r = requests.get(url=_O.v1.format(p2, p1), headers=_O.h, timeout=30)
                r.raise_for_status()
                d = r.json().get(_O.k2, [])
                if d: df = pd.DataFrame(d); df['i_t_c'] = p1; x.append(df)
            except requests.RequestException: continue
    if not x: return pd.DataFrame()
    return pd.concat(x).reset_index(drop=True).drop_duplicates(subset=[_O.k1, 'i_t_c'])

async def _f2(s, a, k, t):
    if not all([_O.v2, _O.k2, _O.k3, _O.k4]): return None
    u = _O.v2.format(k, t)
    async with s:
        try:
            async with async_timeout.timeout(_O.t):
                async with a.get(u, headers=_O.h) as r:
                    r.raise_for_status(); d = await r.json()
                    if d.get(_O.k3) == _O.k4: return d.get(_O.k2, [])
                    else: return [{_O.c1: k, 'i_t_c': t, _O.c6: 0}]
        except Exception: return [{_O.c1: k, 'i_t_c': t, _O.c6: 0}]

async def _f3(d):
    s = asyncio.Semaphore(_O.c)
    async with aiohttp.ClientSession() as a:
        t = [_f2(s, a, getattr(r, _O.k1), r.i_t_c) for r in d.itertuples()]
        res = await asyncio.gather(*t)
        return [i for s in res if s for i in s]

def _f4(m):
    if not m: return pd.DataFrame()
    df = pd.DataFrame(m)
    c_p, c_t = df[_O.c2].map(_O.m1), df['i_t_c'].map(_O.m2)
    df[_O.ic] = c_p.fillna(c_t)
    df.dropna(subset=[_O.c1, _O.ic, _O.c6], inplace=True)
    agg = df.groupby([_O.c1, _O.ic])[_O.c6].sum().reset_index()
    return agg.rename(columns={_O.c1: 'id_eess', _O.c6: _O.iv})

def _f5(c, i):
    if not i: return pd.DataFrame()
    try: return pd.DataFrame(c.rpc(_O.v6, {_O.rk: i}).execute().data)
    except Exception: return pd.DataFrame()

def _f6(c, l):
    if l.empty: return c[c[_O.iv] > 0]
    c[_O.iv] = pd.to_numeric(c[_O.iv], errors='coerce').fillna(0)
    l[_O.iv] = pd.to_numeric(l[_O.iv], errors='coerce').fillna(0)
    l.rename(columns={_O.iv: 'v_l'}, inplace=True)
    m = pd.merge(c, l, on=['id_eess', _O.ic], how='left')
    i_n = m['v_l'].isna()
    m['v_l'].fillna(0, inplace=True)
    e_c = (~i_n & (m[_O.iv].round(2) != m['v_l'].round(2)))
    i_n_p = (i_n & (m[_O.iv] > 0))
    ch = m[e_c | i_n_p]
    return ch[['id_eess', _O.ic, _O.iv]]

if __name__ == '__main__':
    print("--- start ---")
    try:
        t = _f1()
        if not t.empty:
            m = asyncio.run(_f3(t))
            if m:
                a_s = _f4(m)
                if not a_s.empty:
                    a_i = a_s['id_eess'].unique().tolist()
                    db = create_client(_O.v3, _O.v4)
                    l_s = _f5(db, a_i)
                    r_u = _f6(a_s, l_s)
                    if not r_u.empty:
                        r_u['last_updated'] = pd.to_datetime('now', utc=True).isoformat()
                        db.table(_O.v5).upsert(
                            r_u.rename(columns={'id_eess': _O.c1, _O.ic: 'category_name', _O.iv: 'saldo'}).to_dict(orient='records'),
                            on_conflict=f"{_O.c1}, category_name"
                        ).execute()
        print("--- end: success ---")
    except Exception as e:
        print(f"--- end: error - {e} ---")