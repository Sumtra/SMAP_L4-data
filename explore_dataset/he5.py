import h5py

file_path = r"G:\SMAP\SMAP_daily\2020_daily.h5"

with h5py.File(file_path, 'r') as f:
    def print_dataset(name, obj):
        if isinstance(obj, h5py.Dataset):
            print(name)
    f.visititems(print_dataset)
