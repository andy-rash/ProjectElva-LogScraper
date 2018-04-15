from datetime import datetime
from sqlalchemy import Column
from sqlalchemy import DateTime, Integer, String
from sqlalchemy import ForeignKey
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class Computer(Base):
    '''
    Class representing the table containing the computers.

    '''

    __tablename__ = 'computers'
    id = Column(Integer, primary_key=True, autoincrement=True)
    guid = Column(String(8), nullable=False)

    students = relationship('Student', back_populates='computer')

    def __init__(self, **kwargs):
        super(Computer, self).__init__(**kwargs)

    def __repr__(self):
        return '<Computer %r>' % self.guid

class Instance(Base):
    '''
    Class representing the table containing instances of a session.

    '''

    __tablename__ = 'instances'
    id = Column(Integer, primary_key=True, autoincrement=True)
    guid = Column(String(1000), nullable=False)
    computer = Column(String(8))
    student_id = Column(Integer, ForeignKey('students.id'))
    session_id = Column(Integer, ForeignKey('sessions.id'))
    start_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime, nullable=False)
    null_audio_count = Column(Integer, default=0)
    total_audio_count = Column(Integer, default=0)
    slides_finished = Column(Integer, default=0)
    audio_files = Column(postgresql.ARRAY(String(1000)), default=[])

    session = relationship('Session', back_populates='instance') 
    student = relationship('Student', back_populates='instances')

    def __init__(self, **kwargs):
        super(Instance, self).__init__(**kwargs)

    def __repr__(self):
        return '<Instance %r>' % self.guid

class School(Base):
    '''
    Class representing the table containing the schools.

    '''

    __tablename__ = 'schools'
    id = Column(Integer, primary_key=True)
    name = Column(String(1000), nullable=False)

    teachers = relationship('Teacher', back_populates='school')

    def __init__(self, **kwargs):
        super(School, self).__init__(**kwargs)

    def __repr__(self):
        return '<School %r>' % self.name

class Session(Base):
    '''
    Class representing the table containing the sessions.

    '''

    __tablename__ = 'sessions'
    id = Column(Integer, primary_key=True, autoincrement=True)
    unit_id = Column(Integer, ForeignKey('units.id'))
    name = Column(String(1000), nullable=False)
    slides = Column(Integer, nullable=False)

    instance = relationship('Instance', back_populates='session')
    unit = relationship('Unit', back_populates='session')

    def __init__(self, **kwargs):
        super(Session, self).__init__(**kwargs)

    def __repr__(self):
        return '<Session %r>' % self.name

class Student(Base):
    '''
    Class representing the table containing the students.

    '''

    __tablename__ = 'students'
    id = Column(Integer, primary_key=True)
    name = Column(String(1000), nullable=False)
    assigned_comp = Column(Integer, ForeignKey('computers.id'))
    teacher_id = Column(Integer, ForeignKey('teachers.id'))

    computer = relationship('Computer', back_populates='students')
    instances = relationship('Instance', back_populates='student')
    teacher = relationship('Teacher', back_populates='students')

    def __init__(self, **kwargs):
        super(Student, self).__init__(**kwargs)

    def __repr__(self):
        return '<Student %r>' % self.name

class Teacher(Base):
    '''
    Class representing the table containing the teachers.

    '''

    __tablename__ = 'teachers'
    id = Column(Integer, primary_key=True)
    name = Column(String(1000), nullable=False)
    school_id = Column(Integer, ForeignKey('schools.id'))

    school = relationship('School', back_populates='teachers')
    students = relationship('Student', back_populates='teacher')

    def __init__(self, **kwargs):
        super(Teacher, self).__init__(**kwargs)

    def __repr__(self):
        return '<Teacher %r>' % self.name

class Unit(Base):
    '''
    Class representing the table containing the units.

    '''

    __tablename__ = 'units'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(1000), nullable=False)

    session = relationship('Session', back_populates='unit')

    def __init__(self, **kwargs):
        super(Unit, self).__init__(**kwargs)

    def __repr__(self):
        return '<Unit %r>' % self.name
