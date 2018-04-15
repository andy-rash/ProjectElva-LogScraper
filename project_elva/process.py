import hashlib
import logging
import logging.handlers
import os
import re
import sndhdr
from datetime import datetime
from glob import glob
from models import (Base, 
        Instance, Session,
        Student
)
from sqlalchemy import create_engine
from sqlalchemy import exc
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker
from tqdm import *

##################
# Database  
##################

db_url = {
        'drivername': 'postgresql',
        'database': os.environ['POSTGRES_DB'],
        'username': os.environ['POSTGRES_USER'],
        'password': os.environ['POSTGRES_PASSWORD'],
        'host': 'localhost',
        'port': 5432
}
engine = create_engine(URL(**db_url))

DBSession = sessionmaker(bind=engine)
session = DBSession()

##################
# Logging
##################

LOG_FILENAME = '/home/elva/data-processing/log/log-processor.log'

proc_logger = logging.getLogger('proc-logger')
proc_logger.setLevel(logging.DEBUG)
handler = logging.handlers.RotatingFileHandler(
            LOG_FILENAME,
            maxBytes=5*1024*1024,
            backupCount=5 )
proc_logger.addHandler(handler)

##################
# Miscellaneous 
##################

BUFFER_SIZE = 65536
SOURCE_LOG_DIR = '/mnt/volume-nyc1-01-part1/OWL-elva-home/logs/PilotStudy/Vivianne/'

##################
# Regexes 
##################

AUDIO_REGEXP = re.compile('.*\.au')
COMP_REGEXP = re.compile('Computer\sName:\s[\d]{2}-[\d]{5}')
INSTANCE_REGEXP = re.compile('[a-zA-Z]+_[\d]_[\d]{6}_[\d]{2}-[\d]{2}-[\d]{4}')
TIMESTAMP_REGEXP = re.compile('[\d]{2}\/[\d]{2}\/[\d]{4}\s[\d]{2}:[\d]{2}:[\d]{2}')
XML_LOAD_REGEXP = re.compile('XML\/ELVA_[\w]+-[\w]+_[\w]+\.xml')
XML_COMPLETE_REGEXP = re.compile('\*\*\*\sSLIDE\sCOMPLETED\s\*\*\*')

class Processor(object):

    def __init__(self, root_log_dir):
        self.root = root_log_dir

        self.instances = []
        for roots, dirs, files in os.walk(self.root):
            if INSTANCE_REGEXP.search(roots):
                if not re.search('000000', roots):
                    if not os.path.exists(roots+'.txt'):
                        proc_logger.debug(
                                str(datetime.utcnow())[:-7]+ \
                                ' [WARNING] '+ \
                                roots+'.txt does not exist.'
                        )
                        continue
                    if not os.path.exists(roots+'.log'):
                        proc_logger.debug(
                                str(datetime.utcnow())[:-7]+ \
                                ' [WARNING] '+ \
                                roots+'.log does not exist.'
                        )
                        continue
                   
                    proc_logger.debug(
                            str(datetime.utcnow())[:-7]+ \
                            ' [INFO] '+ \
                            roots.split('/')[-1]+' was added to queue.'
                    )
                    self.instances.append(roots)

    def get_audio(self, log_file):
        """
        Return a list of audio files recorded in a specific log file.

        """
        
        audio = []
        with open(log_file, 'r') as fp:
            for line in fp:
                res = AUDIO_REGEXP.search(line)
                if res:
                    audio.append(res.group(0).split()[-1])
        return audio

    def get_computer(self, text_file):
        """
        Return a string representing the computer on which an instance
        was performed.

        """

        computer = None
        with open(text_file, 'r') as fp:
            for line in fp:
                res = COMP_REGEXP.search(line)
                if res:
                    computer = res.group(0)[-8:]
                    break
        return computer

    def get_logs(self, instance_path):
        """
        Return a list of log files associated with a particular instance.

        """
        
        instance_files = glob(instance_path+'*')
        log_files = []
        for path in instance_files:
            if not os.path.isdir(path):
                if re.search('\.log', path) and not \
                   re.search('\.lck', path):
                    log_files.append(path)
        return log_files

    def get_null_audio(self, instance_path, audio_list):
        """
        Return a list of the null audio files associated with an instance. 

        """

        null_audio = []
        for item in audio_list:
            if not os.path.isfile(instance_path+'/'+item):
                proc_logger.debug(
                        str(datetime.utcnow())[:-7]+ \
                        ' [WARNING] '+ \
                        'detected missing/deleted audio file - '+ \
                        instance_path.split('/')[-1]
                ) 
                continue
            res = sndhdr.what(instance_path+'/'+item)
            if res is None:
                null_audio.append(item)
        return null_audio

    def get_slides(self, log_file):
        """
        Return a list of the slides displayed during an instance.

        """

        slides = []
        with open(log_file, 'r') as fp:
            for line in fp:
                res = XML_LOAD_REGEXP.search(line)
                if res:
                    slides.append(res.group(0)[4:])
        return list(set(slides))

    def get_times(self, log_file):
        """
        Return the first and last timestamps from a log file.

        """

        timestamps = []
        with open(log_file, 'r') as fp:
            for line in fp:
                res = TIMESTAMP_REGEXP.match(line)
                if res:
                    timestamps.append(res.group(0))
        return [timestamps[0], timestamps[-1]]

    def hash_dir(self, dir_path):
        """
        Return a hash value representing the hash of each file in a directory.

        """

        file_hashes = []
        for path, dirs, files in os.walk(dir_path):
            for item in sorted(files):
                file_hashes.append(self.hash_file(os.path.join(path, item)))
            for item in sorted(dirs):
                file_hashes.append(self.hash_dir(os.path.join(path, item)))
            break
        return str(hashlib.sha1(''.join(file_hashes)).hexdigest())

    def hash_file(self, file_path):
        """
        Return a hash value for a given file.

        """

        hash_sha1 = hashlib.sha1()
        with open(file_path, 'rb') as fp:
            for chunk in iter(lambda: fp.read(BUFFER_SIZE), b""):
                hash_sha1.update(chunk)
        return hash_sha1.hexdigest()

    def hash_instance(self, instance_files):
        """
        Return a hash value representing a given instance.

        """

        hash_list = []
        if os.path.isdir(instance_files[0]):
            hash_list.append(self.hash_dir(instance_files[0]))
        if os.path.isfile(instance_files[1]):
            hash_list.append(self.hash_file(instance_files[1]))
        if os.path.isfile(instance_files[2]):
            hash_list.append(self.hash_file(instance_files[2]))

        return str(hashlib.sha1(''.join(hash_list)).hexdigest())

    def process(self, instance_path):
        """
        Insert each instance into the database.

        """

        log_files = self.get_logs(instance_path)

        # get data that are independent of the log file
        session_name = '_'.join(instance_path.split('/')[-1].split('_')[:2])
        student_id = instance_path.split('/')[-1].split('_')[2]

        for log in enumerate(log_files):
            instance_files = [instance_path, log[1], instance_path+'.txt']
            instance_hash = self.hash_instance(instance_files)

            result = session.query(Instance).filter_by(guid=instance_hash).first() 
            if result is None: 
                # get data that are dependent on the log file
                audio = self.get_audio(log[1])
                computer = self.get_computer(instance_files[2])
                null_audio = self.get_null_audio(instance_path, audio)
                slides = self.get_slides(log[1])
                start_time, end_time = self.get_times(log[1])

                sess = session.query(Session).filter_by(name=session_name).first()
                if sess is not None:
                    stud = session.query(Student).filter_by(id=student_id).first()
                    if stud is None:
                        proc_logger.debug(
                                str(datetime.utcnow())[:-7]+ \
                                ' [WARNING] '+ \
                                'student ID ' + str(student_id) + ' does not exist in table.'
                        )
                        continue
                    
                    inst = Instance(
                            guid=instance_hash,
                            computer=computer,
                            student_id=student_id,
                            session_id=sess.id,
                            start_time=datetime.strptime(start_time, '%d/%m/%Y %H:%M:%S'),
                            end_time=datetime.strptime(end_time, '%d/%m/%Y %H:%M:%S'),
                            null_audio_count=len(null_audio),
                            total_audio_count=len(audio),
                            slides_finished=len(slides),
                            audio_files=audio
                    )
                    session.add(inst)

                    proc_logger.debug(
                            str(datetime.utcnow())[:-7]+ \
                            ' [INFO] successfully added instance '+ \
                            instance_path.split('/')[-1]
                    )

                else:
                    proc_logger.debug(
                            str(datetime.utcnow())[:-7]+ \
                            ' [WARNING] '+ \
                            'session name was unable to be identified - '+ \
                            instance_path.split('/')[-1]
                    )
                    continue
            else:
                proc_logger.debug(
                        str(datetime.utcnow())[:-7]+ \
                        ' [WARNING] '+ \
                        'attempted to add instance with same GUID - '+ \
                        instance_path.split('/')[-1]
                )
                continue
        session.commit()

if __name__ == "__main__":

    proc = Processor(SOURCE_LOG_DIR)
    for item in tqdm(proc.instances):
        proc.process(item)
