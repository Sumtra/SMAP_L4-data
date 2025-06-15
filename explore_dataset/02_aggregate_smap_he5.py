#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
将零散 3-小时 SMAP .he5 / .h5 聚合成【每年 1 个】日尺度文件（先裁剪，再日平均）
同时写出两份数据：
  • sm_rootzone      : [days, lat, lon]  —— 方便可视化
  • data             : [days, N_pts]     —— 训练用扁平矩阵
并保存 lat_flat / lon_flat 以便重建经纬度。
作者: Yang  |  2025-06-11（flatten 版）
"""
import h5py, numpy as np, warnings, re
from pathlib import Path
from collections import defaultdict

# ==============  CONFIG (参数修改区) ==============
ROOT_DIR   = Path(r"G:\SMAP")           # ROOT/<year>/*.he5|h5
YEARS      = [2019]                     # 可填写多年份
OUT_DIR    = Path(r"G:\SMAP\SMAP_daily")
LAT_MIN, LAT_MAX = 18, 54
LON_MIN, LON_MAX = 73, 136
STRICT     = True                       # 缺时刻是否报错
# =================================================

UTC_CODES = ["013000","043000","073000","103000",
             "133000","163000","193000","223000"]
TIME_RE   = re.compile(r"(\d{8})T(\d{6})")

# ---------- 工具 ----------
def parse_dt(name):
    m = TIME_RE.search(name)
    if not m:
        raise ValueError(f"无法解析时间: {name}")
    return m.group(1), m.group(2)

def get_lat_lon(sample):
    """返回 1-D lat, lon；自动判断层级"""
    with h5py.File(sample, "r") as f:
        if "HDFEOS" in f:                                    # 老 he5/h5
            lat2d = f["HDFEOS/GRIDS/FileMainGroup/Data Fields/cell_lat"][:]
            lon2d = f["HDFEOS/GRIDS/FileMainGroup/Data Fields/cell_lon"][:]
        else:                                                # 新 h5
            lat2d = f["cell_lat"][:]
            lon2d = f["cell_lon"][:]
    return lat2d[:, 0], lon2d[0, :]

def read_sm(fp, lat_idx, lon_idx):
    """读取并裁剪 sm_rootzone；兼容两种层级"""
    with h5py.File(fp, "r") as f:
        if "HDFEOS" in f:
            sm = f["HDFEOS/GRIDS/Geophysical_Data/Data Fields/sm_rootzone"][:]
        else:
            sm = f["Geophysical_Data/sm_rootzone"][:]
    sm = sm.astype(np.float32)
    sm[sm < -9000] = np.nan
    return sm[lat_idx[:, None], lon_idx]

def aggregate_one_year(year_folder: Path, out_path: Path,
                       lat_min, lat_max, lon_min, lon_max, strict):

    files = sorted(year_folder.glob("*.he5")) + sorted(year_folder.glob("*.h5"))
    if not files:
        print(f" {year_folder} 无 he5/h5，跳过")
        return

    # ① 经纬度裁剪索引
    lat_vec, lon_vec = get_lat_lon(files[0])
    lat_idx = np.where((lat_vec >= lat_min) & (lat_vec <= lat_max))[0]
    lon_idx = np.where((lon_vec >= lon_min) & (lon_vec <= lon_max))[0]
    if lat_idx.size == 0 or lon_idx.size == 0:
        raise RuntimeError("裁剪范围为空")

    # ② 汇集每一天的 8 个时刻文件
    daily = defaultdict(dict)
    for fp in files:
        day, utc = parse_dt(fp.name)
        if utc in UTC_CODES:
            daily[day][utc] = fp

    data_daily, date_list = [], []
    for day in sorted(daily):
        miss = [u for u in UTC_CODES if u not in daily[day]]
        if miss:
            msg = f"[{year_folder.name}] {day} 缺 {','.join(miss)}"
            if strict:
                raise RuntimeError(msg)
            warnings.warn(msg)
            continue
        sm_list = [read_sm(daily[day][u], lat_idx, lon_idx) for u in UTC_CODES]
        data_daily.append(np.nanmean(sm_list, axis=0, dtype=np.float32))  # [lat, lon]
        date_list.append(int(day))

    if not data_daily:
        print(f" {year_folder.name} 无完整天数，未写出")
        return

    # ③ 写 HDF5（3-D + 扁平 2-D）
    out_path.parent.mkdir(parents=True, exist_ok=True)
    data_3d   = np.stack(data_daily, axis=0)               # [T, H, W]
    T, H, W   = data_3d.shape
    data_flat = data_3d.reshape(T, H*W)                    # [T, N_pts]

    lon_mesh, lat_mesh = np.meshgrid(lon_vec[lon_idx], lat_vec[lat_idx])
    lat_flat = lat_mesh.ravel().astype(np.float32)         # [N_pts]
    lon_flat = lon_mesh.ravel().astype(np.float32)

    with h5py.File(out_path, "w") as hf:
        # 元坐标
        hf.create_dataset("latitude",  data=lat_vec[lat_idx],  compression="gzip")
        hf.create_dataset("longitude", data=lon_vec[lon_idx], compression="gzip")
        hf.create_dataset("time",      data=np.array(date_list, dtype=np.int32))

        # 3-D 原始矩阵（可视化用）
        hf.create_dataset("sm_rootzone", data=data_3d,
                          compression="gzip", shuffle=True)

        # 2-D 扁平矩阵（训练用）
        hf.create_dataset("data",       data=data_flat,
                          compression="gzip", shuffle=True)
        hf.create_dataset("lat_flat",   data=lat_flat)
        hf.create_dataset("lon_flat",   data=lon_flat)
        hf.attrs["flattened"] = True

    print(f"{year_folder.name} → {out_path}  (days={len(date_list)}, flat N={H*W})")

# ---------- 主入口 ----------
def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for y in YEARS:
        src   = ROOT_DIR / str(y)
        out_h = OUT_DIR / f"{y}_daily.h5"
        aggregate_one_year(src, out_h,
                           LAT_MIN, LAT_MAX, LON_MIN, LON_MAX,
                           STRICT)

if __name__ == "__main__":
    main()
