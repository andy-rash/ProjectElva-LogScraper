import os
import paramiko
import uuid
from datetime import datetime
from models import (Base,
        Computer, Instance,
        School, Session,
        Student, Teacher,
        Unit
)
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker

# TODO: make this a little bit more functional with
#       command line arguments
#       this needs to have more fine-grained abilities so
#       as to not wholesale delete and re-create tables
#       i.e. postgres-dbtest --drop-all or
#            postgres-dbtest --recreate units

#db_url = {  
#            'drivername': 'postgresql',
#            'database': os.environ['POSTGRES_DB'],
#            'username': os.environ['POSTGRES_USER'],
#            'password': os.environ['POSTGRES_PASSWORD'],
#            'host': 'localhost',
#            'port': 5432 }
#engine = create_engine(URL(**db_url))
#Base.metadata.drop_all(bind=engine)
#Base.metadata.create_all(bind=engine, checkfirst=True)
#
#DBSession = sessionmaker(bind=engine)
#session = DBSession()

data_dir = '../data/'

print os.path.dirname(os.path.realpath(__file__))

# add computers to DB from file
with open(data_dir+'computers.csv', 'r') as fp:
    for line in fp:
        line = line.strip().split(',')[:1][0]
        if line[0] != '#':
            computer = Computer(guid=line)
            session.add(computer)
    session.flush()

# add schools to DB from file
with open(data_dir+'schools.csv', 'r') as fp:
    for line in fp:
        line = line.strip().split(',')[:2]
        if line[0] != '#':
            school = School(id=line[0], name=line[1])
            session.add(school)
    session.flush()

# add units to DB from file
with open(data_dir+'units.csv', 'r') as fp:
    for line in fp:
        line = line.strip().split(',')[:1][0]
        if line[0] != '#':
            unit = Unit(name=line)
            session.add(unit)
    session.flush()

# add teachers to DB from file
with open(data_dir+'teachers.csv', 'r') as fp:
    for line in fp:
        line = line.strip().split(',')[:3]
        if line[0] != '#':
            school = session.query(School).filter_by(name=line[2]).first()
            teacher = Teacher(id=line[0],
                    name=line[1],
                    school_id=school.id
            )
            session.add(teacher)
    session.flush()

# add students to DB from file
with open(data_dir+'students.csv', 'r') as fp:
    for line in fp:
        line = line.strip().split(',')[:4]
        if line[0] != '#':
            computer = session.query(Computer).filter_by(guid=line[2]).first()
            student = Student(id=line[0],
                    name=line[1],
                    assigned_comp=computer.id,
                    teacher_id=line[3],
            )
            session.add(student)
    session.flush()

# add sessions to DB from file
with open(data_dir+'sessions.csv', 'r') as fp:
    for line in fp:
        line = line.strip().split(',')[:3]
        if line[0] != '#':
            unit = session.query(Unit).filter_by(name=line[0]).first()
            sess = Session(unit_id=unit.id,
                    name=line[1],
                    slides=line[2]
            )
            session.add(sess)
    session.flush()

with open(data_dir+'sample-instances.csv', 'r') as fp:
    for line in fp:
        line = line.strip().split(',')[:9]
        if line[0] != '#':
            sess = session.query(Session).filter_by(name=line[2]).first()
            audio = line[8].split('|')[1:-1]
            instance = Instance(guid=line[0],
                    student_id=line[1],
                    session_id=sess.id,
                    start_time=datetime.strptime(line[3], '%d/%m/%Y %H:%M:%S'),
                    end_time=datetime.strptime(line[4], '%d/%m/%Y %H:%M:%S'),
                    null_audio_count=line[5],
                    total_audio_count=line[6],
                    slides_finished=line[7],
                    audio_files=audio
            )
            session.add(instance)
    session.flush()

#instances = session.query(Instance).filter_by(student_id=103065).all()
#for item in instances:
#    print item.audio_files

# how to gather the school associated with each teacher
#teachers = session.query(Teacher).\
#                   join(Teacher.school).\
#                   values(Teacher.id, Teacher.name, School.name)
#for id, teacher, school in teachers:
#    print id, teacher, school

# how to gather teacher and computer associated with each student
#students = session.query(Student, Computer, Teacher, School).\
#                   join(Student.teacher).\
#                   join(Student.computer).\
#                   join(Teacher.school).\
#                   values(Student.id, Student.name, 
#                          Teacher.name, Computer.guid,
#                          School.name)
#
#sheet = {}
#for id, name, teacher, guid, school in students:
##    print id, teacher, guid, school
#
#    if school in sheet.keys():
#        if teacher in sheet[school].keys():
#            if guid in sheet[school][teacher].keys():
#                if id in sheet[school][teacher][guid].keys():
#                    pass 
#                else:
#                    sheet[school][teacher][guid][id] = {}
#                    sheet[school]['student_count'] += 1
#                    sheet[school][teacher]['student_count'] += 1
#                    sheet[school][teacher][guid]['student_count'] += 1
#            else:
#                sheet[school][teacher][guid] = {id: {}}
#                sheet[school]['student_count'] += 1
#                sheet[school][teacher]['student_count'] += 1
#                sheet[school][teacher][guid]['student_count'] = 1
#        else:
#            sheet[school][teacher] = {guid: {id: {}}}
#            sheet[school]['student_count'] += 1
#            sheet[school][teacher]['student_count'] = 1
#            sheet[school][teacher][guid]['student_count'] = 1
#    else:
#        sheet[school] = {teacher: {guid: {id: {}}}}
#        sheet[school]['student_count'] = 1
#        sheet[school][teacher]['student_count'] = 1
#        sheet[school][teacher][guid]['student_count'] = 1
#
#import json
#print json.dumps(sheet, sort_keys=True, indent=4, separators=[', ', ': '])

# how to get all sessions for a specific unit
#sessions = session.query(Session).\
#                   join(Session.unit).\
#                   filter(Unit.name == 'EllenOchoa').\
#                   values(Unit.name, Session.name, Session.slides)
#for unit, sess_name, slides in sessions:
#    print unit, sess_name, slides

#session.commit()
#session.close()
