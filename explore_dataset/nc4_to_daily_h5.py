import os
import re
import h5py
import argparse
import numpy as np
from datetime import datetime
from collections import defaultdict


def parse_args():
    p = argparse.ArgumentParser(description='Aggregate SMAP nc4 3-hourly files to daily H5.')
    p.add_argument('--nc4-dir', default='datasets/nc4/data-2025', help='Directory with .nc4 files (default: ./datasets/nc4/data-2025)')
    p.add_argument('--output', required=True, help='Output H5 path')
    p.add_argument('--var', default='sm_rootzone', help='Variable name in Geophysical_Data (default: sm_rootzone)')
    p.add_argument('--limit-days', type=int, default=None, help='Limit number of days for a quick test')
    p.add_argument('--ref-h5', required=True, help='Reference H5 to match grid/format (e.g., 2025_daily.h5)')
    p.add_argument('--round', type=int, default=6, help='Decimal rounding for lat/lon matching')
    return p.parse_args()


def get_lat_lon_from_nc4(path):
    with h5py.File(path, 'r') as f:
        lat2 = f['cell_lat'][:]
        lon2 = f['cell_lon'][:]
    lat1 = lat2[:, 0]
    lon1 = lon2[0, :]
    return lat1.astype(np.float32), lon1.astype(np.float32)


def get_missing_value(dset):
    for k in ['missing_value', '_FillValue', 'fmissing_value']:
        if k in dset.attrs:
            try:
                return float(dset.attrs[k])
            except Exception:
                pass
    return None


def load_ref_grid(path):
    with h5py.File(path, 'r') as f:
        if 'latitude' not in f or 'longitude' not in f:
            raise SystemExit('Reference H5 missing latitude/longitude datasets.')
        lat = f['latitude'][:]
        lon = f['longitude'][:]
        lat1 = lat[:, 0] if lat.ndim == 2 else lat.flatten()
        lon1 = lon[0, :] if lon.ndim == 2 else lon.flatten()
    return lat1.astype(np.float32), lon1.astype(np.float32)


def build_index_map(source_vals, target_vals, ndigits):
    src = np.round(source_vals, ndigits)
    tgt = np.round(target_vals, ndigits)
    idx_map = {v: i for i, v in enumerate(src)}
    indices = []
    for v in tgt:
        if v not in idx_map:
            raise SystemExit('Grid mismatch: could not match value %.6f' % v)
        indices.append(idx_map[v])
    return np.array(indices, dtype=np.int64)


def crop_to_grid(data, src_lat, src_lon, tgt_lat, tgt_lon, ndigits):
    lat_idx = build_index_map(src_lat, tgt_lat, ndigits)
    lon_idx = build_index_map(src_lon, tgt_lon, ndigits)
    return data[:, lat_idx, :][:, :, lon_idx]


def main():
    args = parse_args()
    nc4_dir = os.path.abspath(args.nc4_dir)

    pattern = re.compile(r'SMAP_L4_SM_gph_(\d{8}T\d{6})_Vv\d+_\d+_.*\.nc4$', re.IGNORECASE)

    files = [f for f in sorted(os.listdir(nc4_dir)) if f.lower().endswith('.nc4')]
    if not files:
        raise SystemExit(f'No .nc4 files found in {nc4_dir}')

    daily_files = defaultdict(list)
    for fname in files:
        m = pattern.match(fname)
        if not m:
            continue
        ts = datetime.strptime(m.group(1), '%Y%m%dT%H%M%S')
        daily_files[ts.date()].append(os.path.join(nc4_dir, fname))

    all_days = sorted(daily_files.keys())
    if args.limit_days:
        all_days = all_days[:args.limit_days]

    # lat/lon from first file
    src_lat, src_lon = get_lat_lon_from_nc4(os.path.join(nc4_dir, files[0]))
    tgt_lat, tgt_lon = load_ref_grid(args.ref_h5)

    daily_data = []
    valid_dates = []

    for day in all_days:
        group = sorted(daily_files[day])
        day_arrays = []
        for path in group:
            with h5py.File(path, 'r') as f:
                dset = f['Geophysical_Data'][args.var]
                arr = dset[:].astype(np.float32)
                missing = get_missing_value(dset)
                if missing is not None:
                    arr = np.where(arr == missing, np.nan, arr)
                day_arrays.append(arr)

        if day_arrays:
            stacked = np.stack(day_arrays, axis=0)
            daily_avg = np.nanmean(stacked, axis=0)
            daily_data.append(daily_avg)
            valid_dates.append(day)

    if not daily_data:
        raise SystemExit('No valid daily data produced.')

    daily_data = np.stack(daily_data, axis=0)
    # crop to reference grid
    if src_lat.shape != tgt_lat.shape or not np.allclose(src_lat, tgt_lat) or \
       src_lon.shape != tgt_lon.shape or not np.allclose(src_lon, tgt_lon):
        daily_data = crop_to_grid(daily_data, src_lat, src_lon, tgt_lat, tgt_lon, args.round)
        src_lat, src_lon = tgt_lat, tgt_lon
    time_arr = np.array([int(d.strftime('%Y%m%d')) for d in valid_dates], dtype=np.int32)

    out_path = os.path.abspath(args.output)
    data_flat = daily_data.reshape(daily_data.shape[0], -1)
    lat_grid, lon_grid = np.meshgrid(src_lat, src_lon, indexing='ij')
    lat_flat = lat_grid.reshape(-1).astype(np.float32)
    lon_flat = lon_grid.reshape(-1).astype(np.float32)

    with h5py.File(out_path, 'w') as f:
        f.create_dataset('data', data=data_flat.astype(np.float32))
        f.create_dataset('sm_rootzone', data=daily_data.astype(np.float32))
        f.create_dataset('latitude', data=src_lat.astype(np.float32))
        f.create_dataset('longitude', data=src_lon.astype(np.float32))
        f.create_dataset('lat_flat', data=lat_flat)
        f.create_dataset('lon_flat', data=lon_flat)
        f.create_dataset('time', data=time_arr)
        f.attrs['flattened'] = True

    print('Wrote:', out_path)
    print('data shape:', data_flat.shape)
    print('Date range:', valid_dates[0], '~', valid_dates[-1])


if __name__ == '__main__':
    main()
