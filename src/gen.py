import h5py
import pandas as pd

rayx_bin = 'rayx' # TODO: create simlink
rml_file = 'METRIX_U41_G1_H1_318eV_PS_MLearn_v115.rml'
out_dir = 'out'
beam_metrics_file = 'beam_metrics.csv'


def main():
    # Log important steps in the script to make sure crash does not need full restart of generation
    # Start rayx on directory on background
    #   .rml files are created before running this script
    #   Check for subfolders ('0','1','2',...)
    #   Restart the process for each subfolder
    #   Write h5 files to out directory TODO: check if rayx supports this, if not add functionality
    #   Alternatively move h5 files after creation
    # Start post processing
    #   Scan for new h5 files
    #   After done writing, read h5 file
    #   Calculate beam metrics to df
    #   Compress h5 file to new location and delete old file
    #   Should not bottleneck since rayx does not use much CPU during tracing
    #   Save df for each subfolder

    pass


if __name__ == "__main__":
    main()

