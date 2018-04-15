import os, re
from project_elva.models import Session
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker

SESS_REGEXP = re.compile('[\w]+_[1-5]{1}')

# replace this with the proper root directory containing the
# session directories
data_dir = ''

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

# get slide count from session .csv file
def count_slides(csv_file):
    slides = []
    with open(csv_file, 'r') as fp:
        for line in fp:
            if line.count(',') < 4: continue
            line = line.strip().split(',')
            if line[4] != '' and line[4] != 'Slide':
                slides.append(line[4])
    return len(slides)

# compile slide counts
slide_count = {}
for item in sorted(os.listdir(data_dir)):	
    if SESS_REGEXP.match(item):
        phx_dir = data_dir + item + '/phoenix/'
        csv_file = phx_dir + item + '.csv'

        if os.path.isdir(phx_dir) and os.path.isfile(csv_file):
            slide_count[item] = count_slides(csv_file)
        elif os.path.isdir(phx_dir) and not os.path.isfile(csv_file):
            slide_count[item] = 0

# update slide counts in DB
sessions = session.query(Session).all()
for item in sessions:
    if item.name not in slide_count.keys(): continue
    if int(item.slides) != int(slide_count[item.name]):
        item.slides = int(slide_count[item.name])
        session.add(item)
session.commit()
