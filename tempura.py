import configparser
import glob
import logging
from lib import SimpleAnalyser as sa

config = configparser.ConfigParser()
config._interpolation = configparser.ExtendedInterpolation()
config.read('tempura.cfg')

logger = logging.getLogger('tempura')
logger.setLevel(logging.INFO)
hdlr = logging.FileHandler('tempura.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)

debug_flag = (config['DEFAULT']['debug'] == 'True')
if debug_flag:
    logger.setLevel(logging.DEBUG)

files_to_analyse = '{}/{}.{}'.format(
    config['DEFAULT']['input_folder_path'],
    config['DEFAULT']['input_file_name_filter'],
    config['DEFAULT']['input_extension'])
files = glob.glob(files_to_analyse)

for file in files:
    try:
        logger.info('Starting analysis on: {}'.format(file))
        with sa.SimpleAnalyser(file, config['FILE'], logger, config['DEFAULT']['language']) as myfile:
            myfile.perform_file_analysis()
            logger.info('Analysis on: {} completed.'.format(file))
            if myfile.column_types is not None:
                myfile.generate_template(config['LOAD_TEMPLATE'])
                myfile.generate_import_table(config['TABLE_DDL'])

    except Exception as exp:
        logger.exception(exp)
        print(exp)