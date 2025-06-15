#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
检查指定年份的 SMAP .he5/.h5 数据完整性并移动或复制到目标文件夹
—— 双结构兼容版 (HDFEOS & 扁平层级)
作者: Yang  |  2025-06-11  （6-12做补充兼容功能，确保可以兼容h5格式文件及其内部数据的扁平化组织形式）
"""
import os, re, random, shutil, h5py
from pathlib import Path
from collections import defaultdict

# ============  CONFIG 可修改区 ============
SOURCE_DIR = Path(r"G:\SMAP")          # 原始数据根目录
YEAR       = "2019"                    # 目标年份
DEST_DIR   = Path(r"G:\SMAP\2019")     # 合格文件保存目录
SAMPLE_N   = 100                      # 随机抽查文件数量
MOVE_FILES = True                     # True=移动, False=复制

# HDFEOS 结构必须包含的键
REQUIRED_KEYS_HIER = [
    "HDFEOS/GRIDS/FileMainGroup/Data Fields/cell_column",
    "HDFEOS/GRIDS/FileMainGroup/Data Fields/cell_lat",
    "HDFEOS/GRIDS/FileMainGroup/Data Fields/cell_lon",
    "HDFEOS/GRIDS/FileMainGroup/Data Fields/cell_row",
    "HDFEOS/GRIDS/FileMainGroup/XDim",
    "HDFEOS/GRIDS/FileMainGroup/YDim",
    "HDFEOS/GRIDS/Geophysical_Data/Data Fields/sm_rootzone",
]

# 扁平结构必须包含的键（最小集合即可后续聚合）
REQUIRED_KEYS_FLAT = [
    "cell_lat",
    "cell_lon",
    "Geophysical_Data/sm_rootzone",
]
# SMAP 每日 8 个 UTC 时码
TIME_CODES = ["013000","043000","073000","103000",
              "133000","163000","193000","223000"]
# ============================================

# ---------- (1) 收集文件 ----------
def adjust_2015_date(day_str):
    """调整2015年的日期，从3月31日开始计数（包含3月31日）"""
    if not day_str.startswith("2015"):
        return day_str
    
    # 提取月和日
    month = int(day_str[4:6])
    day = int(day_str[6:8])
    
    # 早于3月31日的文件会被跳过
    if (month < 3) or (month == 3 and day < 31):
        raise ValueError(f"2015年日期早于3月31日: {day_str}")
    
    # 计算从3月31日开始的天数偏移
    days_in_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    
    # 计算当前日期是一年中的第几天
    day_of_year = sum(days_in_month[:month-1]) + day
    # 3月31日是第90天
    new_day_of_year = day_of_year - 90 + 1
    
    # 计算新的月份和日期（保持原始月份和日期，仅调整年份显示）
    new_month = month
    new_day = day
    
    # 构建新的日期字符串（年份仍显示2015，但月份和日期保持原始值）
    return f"2015{new_month:02d}{new_day:02d}"

PATTERN = re.compile(
    r"SMAP_L4_SM_gph_(\d{8})T(\d{6})_.*\.h(?:e)?5$", re.I
)

def gather_files(src: Path):
    by_day = defaultdict(list)
    for f in src.rglob("SMAP_L4_SM_gph_*.h*5"):
        m = PATTERN.search(f.name)
        if m:
            day, utc = m.group(1), m.group(2)
            if day.startswith(YEAR):
                # 对2015年进行特殊处理
                if YEAR == "2015":
                    try:
                        day = adjust_2015_date(day)
                    except ValueError as e:
                        print(f"跳过无效日期文件: {f.name} ({str(e)})")
                        continue
                by_day[day].append((utc, f))
    return by_day

# ---------- (2) 每日 8 时刻完整性 ----------
def check_daily_counts(by_day):
    ok = True
    for day, lst in sorted(by_day.items()):
        got = {t for t, _ in lst}
        missing = [tc for tc in TIME_CODES if tc not in got]
        if missing:
            ok = False
            print(f"[缺时刻] {day[:4]}-{day[4:6]}-{day[6:]} 缺 {', '.join(missing)}")
    if ok:
        print(f"{YEAR} 每日均有 8 个文件")
    return ok

# ---------- (3) 随机抽样 ----------
def random_sample(files, n):
    flat = [f for _, f in files]
    if not flat: return []
    return random.sample(flat, min(n, len(flat)))

# ---------- (4) 键完整性检查 ----------
def check_file(fname: Path):
    try:
        with h5py.File(fname, "r") as f:
            if "HDFEOS" in f:     # 老式层级
                miss = [k for k in REQUIRED_KEYS_HIER if k not in f]
            else:                 # 扁平层级
                miss = [k for k in REQUIRED_KEYS_FLAT if k not in f]
            return miss
    except Exception as e:
        return [f"[无法打开]: {e}"]

def sample_integrity_check(sample_files):
    bad = False
    for fp in sample_files:
        missing = check_file(fp)
        if missing:
            bad = True
            print(f"[内容缺失] {fp.name}")
            for m in missing:
                print("   ↳", m)
    if not bad:
        print(f"随机抽查 {len(sample_files)} 个文件全部合格")
    return not bad

# ---------- (5) 复制/移动 ----------
def copy_or_move(by_day, dest: Path, move=False):
    dest.mkdir(parents=True, exist_ok=True)
    files = [fp for _, fp in sorted(
        [item for v in by_day.values() for item in v], key=lambda x: x[1]
    )]
    for src in files:
        tgt = dest / src.name
        (shutil.move if move else shutil.copy2)(src, tgt)
    print(f"{'移动' if move else '复制'}完成，共 {len(files)} 个文件 → {dest}")

# ---------- main ----------
def main():
    print(f"=== 开始检查 {YEAR} 文件 ===")
    by_day = gather_files(SOURCE_DIR)
    if not by_day:
        print("未找到符合条件的文件")
        return

    counts_ok = check_daily_counts(by_day)
    sample_ok = sample_integrity_check(
        random_sample([(t, f) for v in by_day.values() for t, f in v], SAMPLE_N)
    )
    if counts_ok and sample_ok:
        copy_or_move(by_day, DEST_DIR, MOVE_FILES)
    else:
        print("数据异常，未执行复制/移动")

if __name__ == "__main__":
    main()