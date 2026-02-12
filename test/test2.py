from crypto_data_downloader.utils import glue_n_fix_data, load_pkl

a = "./data/futures_data_2025-07-01_2025-08-01.pkl"
b = "./data/futures_data_2025-08-01_2025-11-20.pkl"
a, b = [load_pkl(x, gz=True) for x in [a, b]]
res = glue_n_fix_data([a, b], dt=5 * 60e3, t_idx=0)
for k, v in res.items():
    print(k, v.shape)
