import configparser
import glob
import logging
from lib import SimpleAnalyser as sa


try:

    logger = logging.getLogger('tempura')
    hdlr = logging.FileHandler('tempura.log')
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.DEBUG)

    config = configparser.ConfigParser()
    config._interpolation = configparser.ExtendedInterpolation()
    config.read('tempura.cfg')

    debug_flag = (config['DEFAULT']['debug'] == 'True')

    files = glob.glob("./data_files/*.csv")

    for file in files:
        logger.info('Start analysis on: {}'.format(file))
        with sa.SimpleAnalyser(file, config['FILE'], logger, debug_flag) as myfile:
            myfile.perform_file_analysis()
            if myfile.column_types is not None:
                myfile.generate_template()
                myfile.generate_import_table()

except Exception as exp:
    logger.error(exp)
    print(exp)