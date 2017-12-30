import configparser
import glob
from lib import SimpleAnalyser as sa


config = configparser.ConfigParser()
config._interpolation = configparser.ExtendedInterpolation()
config.read('tempura.cfg')

files = glob.glob("./data_files/*.csv")

for file in files:
    with sa.SimpleAnalyser(file, config['FILE']) as myfile:
        myfile.perform_file_analysis(True)
        if myfile.column_types is not None:
            myfile.generate_template()
            myfile.generate_import_table()
