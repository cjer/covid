import logging
import pandas as pd
import os
import requests
import re
import datetime
import hashlib
import gzip
import pickle
import sys

latest_dir = 'datagov_latest'
archive_dir = 'datagov_archive'

# create logger with 'spam_application'
logger = logging.getLogger('get_datagov')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('datagov.log', encoding='utf8')
fh.setLevel(logging.DEBUG)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)

def get_fn(s, d):
    if s.title=='נתוני קורונה מאפייני נבדקים - טבלת עזר':
        return 'corona_tested_individuals_aid_table_ver_'+str(int(s.file_name.split('.')[0].strip('_-ver'))).zfill(3)+ '_'+d+'.csv'
    elif s.file_name.split('.')[0] in ('readme-','-readme', '-') or s.duplicated_fn:
        return s.title+'_'+d+'.'+s.file_name.split('.')[-1]
    else:
        return '.'.join(s.file_name.split('.')[:-1])+'_'+d+'.'+s.file_name.split('.')[-1]

    
def download_files(nlf, latest_nlf_dict=None):
    hashes = []
    nlf_dict = {} if latest_nlf_dict is None else latest_nlf_dict

    for i, s in nlf.iterrows():
        logger.info('downloading ' + s.file_name + ' from https://data.gov.il'+s.link )
        dr = requests.get('https://data.gov.il'+s.link,
                            headers={'user-agent':'datagov-external-client'})
        latest_path = os.path.join(latest_dir, s.output_file_name)
        archive_path = os.path.join(archive_dir, s.output_file_name)
        md5 = hashlib.md5(dr.content).hexdigest()
        hashes.append(md5)
        if (latest_nlf_dict is None or s.resource not in latest_nlf_dict):
            logger.info('new file')
            with gzip.open(latest_path, 'wb') as f, gzip.open(archive_path, 'wb') as af:
                logger.info('writing to latest path ' + latest_path)
                f.write(dr.content)
                logger.info('writing to archive path ' + archive_path)
                af.write(dr.content)
                
            nlf_dict[s.resource] = {
                'title': s.title,
                'file_name': s.file_name,
                'duplicated_fn': s.duplicated_fn,
                'output_file_name': s.output_file_name,
                'md5': md5,
                'latest_download_time': s.download_datetime
            }
            
        elif md5!=latest_nlf_dict[s.resource]['md5'] or s.file_name!=latest_nlf_dict[s.resource]['file_name']:
            logger.info('file changed')
            with gzip.open(latest_path, 'wb') as f, gzip.open(archive_path, 'wb') as af:
                logger.info('writing to latest path ' + latest_path)
                f.write(dr.content)
                logger.info('writing to archive path ' + archive_path)
                af.write(dr.content)
                remove_path = os.path.join(latest_dir, latest_nlf_dict[s.resource]['output_file_name'])
                logger.info('removing prior latest file from ' + remove_path)
                os.remove(remove_path)

            nlf_dict[s.resource] = {
                'title': s.title,
                'file_name': s.file_name,
                'duplicated_fn': s.duplicated_fn,
                'output_file_name': s.output_file_name,
                'md5': md5,
                'latest_download_time': s.download_datetime
            }
        else:
            logger.info('no changes')
            
    nlf['md5'] = hashes
    return nlf, nlf_dict


logger.info('Requesting covid-19 dataset page from data.gov.il')
x = requests.get('https://data.gov.il/dataset/covid-19',
                headers={'user-agent':'datagov-external-client'})
logger.info('extracting file metadata and links')
res = re.findall('(?:<a class="heading" href=".*" title="(.*)")|(?:<a href="(/dataset/covid-19/resource/([\w-]+)/download/(.*))")', x.text)
logger.info('found ' + str(len(res)/2) + ' files')

d = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
nlf = list(zip([x[0] for x in res[::2]], [x[1] for x in res[1::2]], [x[2] for x in res[1::2]],  [x[3] for x in res[1::2]]))
nlf = pd.DataFrame(nlf, columns=['title', 'link', 'resource', 'file_name'])
nlf['duplicated_fn'] = nlf.file_name.duplicated(keep=False)

nlf['output_file_name'] = nlf.apply(get_fn, args=(d,), axis=1)+'.gz'
nlf['download_datetime'] = d

if '-f' in sys.argv:
    logger.info('force mode - download anyway to latest and archive, BEWARE!!!')
    latest_nlf_dict=None
else:
    logger.info('reading latest pickle')
    latest_nlf_dict = pickle.load(open('datagov_latest.pkl', 'rb'))

logger.info('starting file download...')
nlf, nlf_dict = download_files(nlf, latest_nlf_dict)
logger.info('finished file download...')

logger.info('writing new latest pickle')
pickle.dump(nlf_dict, open('datagov_latest.pkl', 'wb' ) )
logger.info('get_datagov finished')




