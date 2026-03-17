import os
import sys
def main():
    source_data_dir='/mnt/usb'
    pathlist = os.listdir(source_data_dir)
    data_dir='/dbdata'
    
    for filepath in pathlist:
        if not os.path.exists(os.path.join(source_data_dir, filepath)):
            print(f'{filepath} not found, please check the name')
            sys.exit(1)
        if not (filepath.endswith('.csv.bz2') or filepath.endswith('.csv')):
            print(f'skipping file {filepath}')
            continue
        outfilename = filepath.removesuffix('.bz2').removeprefix(f'{source_data_dir}')
        print(f'pbzip2 -dfk -p30 {os.path.join(source_data_dir, filepath)} > {os.path.join(data_dir, outfilename)}')

if __name__ == "__main__":
    main()