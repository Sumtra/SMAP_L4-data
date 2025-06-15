# merge_years.py —— 合并多个 *_daily.h5 成一个时间连续的大文件
import h5py, numpy as np
from pathlib import Path
from datetime import datetime

# ========================== 用户配置区 ==========================
years = [str(y) for y in range(2015, 2025)]  # 2015～2024 共10年
in_dir = Path(r"G:\SMAP\SMAP_daily")         # 每年 *_daily.h5 所在目录
out_h5 = Path(r"G:\SMAP\SMAP_daily\SMAP_2015_2024_merged.h5")

start_day = datetime(2015, 3, 31)
end_day   = datetime(2024, 12, 31)
# ===============================================================

data_all, time_all = [], []
lat_flat = lon_flat = None

for y in years:
    fp = in_dir / f"{y}_daily.h5"
    with h5py.File(fp, "r") as f:
        data = np.array(f["data"], dtype=np.float32)
        times = np.array(f["time"])  # 每个元素是 int，例如 20150401

        # 过滤时间范围
        valid_mask = (times >= int(start_day.strftime("%Y%m%d"))) & (times <= int(end_day.strftime("%Y%m%d")))
        data_all.append(data[valid_mask])
        time_all.append(times[valid_mask])

        if lat_flat is None:
            lat_flat = np.array(f["lat_flat"])
            lon_flat = np.array(f["lon_flat"])
        else:
            assert np.allclose(lat_flat, f["lat_flat"]), "lat mismatch!"
            assert np.allclose(lon_flat, f["lon_flat"]), "lon mismatch!"

# 拼接所有年份数据
print("Stitching along time dimension …")
data_cat = np.concatenate(data_all, axis=0)
time_cat = np.concatenate(time_all).astype(np.int32)

# 写入输出文件
with h5py.File(out_h5, "w") as hf:
    hf.create_dataset("data",     data=data_cat, compression="gzip", shuffle=True)
    hf.create_dataset("time",     data=time_cat)
    hf.create_dataset("lat_flat", data=lat_flat)
    hf.create_dataset("lon_flat", data=lon_flat)
    hf.attrs["flattened"] = True

print(f"Merged file saved to: {out_h5}  (days = {data_cat.shape[0]})")
