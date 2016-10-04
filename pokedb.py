import threading
import sqlite3
import logging
import collections
import time

DB_threadlocal = threading.local()

class DB(object):
    def __new__(cls, **kwargs):
        if getattr(DB_threadlocal, 'db_instance', None) is None:
            DB_threadlocal.db_instance = object.__new__(cls)
            DB_threadlocal.db_instance.conn = sqlite3.connect('poke.db')
            DB_threadlocal.db_instance.conn.row_factory = sqlite3.Row
            DB_threadlocal.db_instance.__createTables(drop_before=kwargs.get('wipe'))
        return DB_threadlocal.db_instance
        
    def __createTables(self, **kwargs):
        drop_before = kwargs.get('drop_before')
        cursor = self.conn.cursor()
        try:
            cursor.execute('SELECT `version` FROM `version` ORDER BY `version` DESC LIMIT 1')
            self.version = cursor.fetchone()['version']
        except sqlite3.OperationalError:
            self.version = 0

        try:
            if drop_before:
                logging.warning('Dropping the existing database')
                cursor.execute('DROP TABLE IF EXISTS `users`')
                cursor.execute('DROP TABLE IF EXISTS `user_positions`')
                cursor.execute('DROP TABLE IF EXISTS `location_groups`')
                cursor.execute('DROP TABLE IF EXISTS `locations`')
                cursor.execute('DROP TABLE IF EXISTS `pokemons`')
                cursor.execute('DROP TABLE IF EXISTS `user_filters`')
                cursor.execute('DROP TABLE IF EXISTS `spawns`')
                cursor.execute('DROP TABLE IF EXISTS `notifications`')
                cursor.execute('DROP TABLE IF EXISTS `version`')
                self.version = 0
                self.conn.commit()


            current_version = 20161002
            if self.version < current_version:
                cursor.execute('''CREATE TABLE `users` (
                    `id` INTEGER NOT NULL UNIQUE PRIMARY KEY,
                    `first_name` TEXT,
                    `last_name` TEXT,
                    `username` TEXT,
                    `chat_id` TEXT NOT NULL UNIQUE,
                    `distance` INTEGER) ''')

                cursor.execute('''CREATE TABLE `user_positions` (
                    `user_id` INTEGER,
                    `timestamp` INTEGER,
                    `latitude` REAL,
                    `longitude` REAL )''')

                cursor.execute('''CREATE TABLE `location_groups` (
                    `id` INTEGER NOT NULL UNIQUE PRIMARY KEY,
                    `name` TEXT )''')

                cursor.execute('''CREATE TABLE `locations` (
                    `id` INTEGER NOT NULL UNIQUE PRIMARY KEY,
                    `location_group_id` INTEGER NOT NULL,
                    `name` TEXT,
                    `latitude` REAL,
                    `longitude` REAL )''')

                cursor.execute('''CREATE TABLE `pokemons` (
                    `id` INTEGER NOT NULL UNIQUE PRIMARY KEY,
                    `name` TEXT,
                    `internal_name` TEXT,
                    `rarity` INTEGER )''')

                cursor.execute('''CREATE TABLE `user_filters` (
                    `user_id` INTEGER,
                    `pokemon_id` INTEGER,
                    PRIMARY KEY( `user_id`, `pokemon_id` ) )''')

                cursor.execute('''CREATE TABLE `spawns` (
                    `encounter_id` BIGINT UNIQUE,
                    `expiration_timestamp` INTEGER,
                    `latitude` REAL,
                    `longitude` REAL,
                    `name` TEXT,
                    `spawn_point_id` )''')

                cursor.execute('''CREATE TABLE `notifications` (
                    `encounter_id` BIGINT,
                    `user_id` INTEGER,
                    PRIMARY KEY( `encounter_id`, `user_id`) )''')

                cursor.execute('''CREATE TABLE IF NOT EXISTS `version` (
                    `version` UNSIGNED INTEGER NOT NULL )''')

                cursor.execute('''INSERT INTO `version` (`version`) values ( ? )''', (current_version,) )

                self.conn.commit()
                self.version = current_version
                logging.debug("Upgraded DB to version {}".format(current_version) )
        except Exception as e:
            self.conn.rollback()
            logging.error("Error creating DB: ({}) - {}".format(kwargs, e))

    @classmethod
    def connection(cls):
        return cls().conn

    @classmethod
    def cursor(cls):
        return cls().conn.cursor()

    @classmethod
    def commit(cls):
        return cls().conn.commit()

    @classmethod
    def rollback(cls):
        return cls().conn.rollback()

class UserFilter(object):
    __attrs = [ 'user_id', 'pokemon_id']
    
    def __init__(self, **kwargs):
        for a in UserFilter.__attrs:
            setattr(self, a, kwargs.get(a))

class Spawn(object):
    __attrs = [ 'encounter_id', 'expiration_timestamp', 'latitude', 'longitude', 'name', 'spawn_point_id']
    __update = '''UPDATE spawns set encounter_id=:encounter_id, 
                    expiration_timestamp=:expiration_timestamp, latitude=:latitude,
                    longitude=:longitude, name=:name, spawn_point_id=:spawn_point_id
                    where encounter_id=:encounter_id'''

    __insert = '''INSERT INTO spawns (encounter_id, expiration_timestamp, latitude,
                    longitude, name, spawn_point_id) SELECT  :encounter_id, 
                    :expiration_timestamp, :latitude, :longitude, :name, 
                    :spawn_point_id WHERE(SELECT CHANGES() = 0)'''

    def __init__(self, **kwargs):
        for a in Spawn.__attrs:
            setattr(self, a, kwargs.get(a))

    def save(self):
        try:
            c = DB.cursor()
            c.execute(Spawn.__update, self.__dict__)
            c.execute(Spawn.__insert, self.__dict__)
            DB.commit()
        except Excepion as e:
            DB.rollback()
            logging.warn("Error saving Spanw ({}) - {}".format(self.__dict__, e)) 


    @classmethod
    def register(cls, obj):
        Spawn(**obj).save()


    @classmethod
    def all_active(cls):
        c = DB.cursor()
        c.execute('''SELECT * FROM spawns 
                        WHERE datetime(expiration_timestamp, 'unixepoch') > CURRENT_TIMESTAMP 
                        ORDER BY expiration_timestamp ASC''')
        def creator(data):
            return cls(**dict(data))
        return map(creator, c.fetchall())

            
class User(object):
    __attrs = [ 'id', 'first_name', 'last_name', 'username', 'chat_id', 'distance' ]
    __update = '''UPDATE `users` set id=:id, first_name=:first_name, last_name=:last_name, 
                username=:username, chat_id=:chat_id, distance=:distance 
                WHERE chat_id=:chat_id'''
    __insert = '''INSERT INTO `users` 
                (id, first_name, last_name, username, chat_id, distance) 
                SELECT :id,:first_name,:last_name,:username,:chat_id,:distance
                WHERE(SELECT CHANGES() = 0)'''

    def __init__(self, **kwargs):
        self.last_pos = None
        for a in User.__attrs:
            setattr(self, a, kwargs.get(a))
        
 
    def save(self):
        try:
            cursor = DB.cursor()
            cursor.execute(User.__update, self.__dict__)
            cursor.execute(User.__insert, self.__dict__)
            self.id = cursor.lastrowid
            DB.commit()
            return self
        except Exception as e:
            DB.rollback()
            logging.error("Error saving User ({}) - {}".format(self.__dict__, e))
            raise

    def update_position(self, latitude, longitude):
        self.last_pos = UserPosition(user_id=self.id, timestamp=time.time(), 
                latitude=latitude, longitude=longitude)
        self.last_pos.save()

    def position(self):
        if self.last_pos == None:
            self.last_pos = UserPosition.get_last(self.id)
        return self.last_pos

    def add_filter(self, pokemon_id):
        try:
            c = DB.cursor()
            c.execute('INSERT INTO user_filters VALUES ( ? , ? )', (self.id, pokemon_id))
            DB.commit()
        except Exception as e:
            DB.rollback()
            logging.warning("Could not insert filter for user: {} - {} ({})".format(
                self.username, pokemon_id, e))
            pass
            

    def del_filter(self, pokemon_id):
        try:
            c = DB.cursor()
            c.execute('DELETE FROM user_filters WHERE user_id=? AND pokemon_id=?', (self.id, pokemon_id))
            DB.commit()
        except Exceptino as e:
            DB.rollback()
            logging.warning("Could not remove filter for user: {} - {} ({})".format(
                self.username, pokemon_id, e))

    def filters(self):
        c = DB.cursor()
        c.execute('''SELECT internal_name, name FROM user_filters AS f
                    LEFT JOIN pokemons AS p ON p.id = f.pokemon_id
                    WHERE user_id=?''', (self.id,) )
        Filter = collections.namedtuple('Filter', 'internal_name, name')
        return map(Filter._make, c.fetchall())

    def notify(self, encounter_id):
        c = DB.cursor()
        try:
            c.execute('''INSERT INTO notifications VALUES ( ?, ? ) ''', (encounter_id, self.id))
            DB.commit()
            return True
        except Exception as e:
            DB.rollback()
            return False


    @classmethod
    def new(cls, first, last, user, chat_id, distance=1000):
        return User( id=None, first_name=first, last_name=last, username=user, 
                chat_id=chat_id, distance=distance).save()

    @classmethod
    def all(cls):
        cursor = DB.cursor()
        cursor.execute('select * from `users`')
        def creator(data):
            return cls(**dict(data))
        return map(creator, cursor.fetchall())


    @classmethod
    def find(cls,chat_id):
        cursor = DB.cursor()
        cursor.execute('SELECT * from `users` where `chat_id`=?', (chat_id,))
        data = cursor.fetchone()
        if data == None:
            return None
        return User(**dict(data))

class UserPosition(object):
    __attrs = [ 'user_id', 'timestamp', 'latitude', 'longitude' ] 
    __insert = '''INSERT INTO `user_positions` (user_id, timestamp, latitude, longitude) 
                    VALUES (:user_id, :timestamp, :latitude, :longitude)'''

    def __init__(self, **kwargs):
        for a in UserPosition.__attrs:
            setattr(self, a, kwargs.get(a))

    def save(self):
        try:
            c = DB.cursor()
            c.execute(UserPosition.__insert, self.__dict__)
            DB.commit()
            return self
        except Exception as e:
            DB.rollback()
            logging.warn("Error saving user position: ({}) - {}".format(self.__dict__, e))

    @classmethod
    def get_last(cls, user_id):
        c = DB.cursor()
        c.execute('''SELECT * FROM user_positions WHERE user_id=:user_id 
                        ORDER BY timestamp DESC LIMIT 1''', (user_id,))
        data = c.fetchone()
        if data == None:
            return None
        return UserPosition(**dict(data))


class Pokemon(collections.namedtuple('Pokemon', 'id name internal_name rarity')):
    __update = '''UPDATE `pokemons` set id=:id, name=:name, internal_name=:internal_name, 
                    rarity=:rarity where id=:id'''
    __insert = '''INSERT INTO `pokemons` (id, name, internal_name, rarity) 
                    SELECT ?,?,?,? WHERE (SELECT CHANGES() = 0)'''
    
    def save(self):
        try:
            cursor = DB.cursor()
            cursor.execute(Pokemon.__update, self)
            cursor.execute(Pokemon.__insert, self)
            DB.commit()
        except Exception as e:
            DB.rollback()
            logging.warn("Error saving pokemon({}) - {}".format(self.__dict__, e))

    @classmethod
    def all(cls):
        c = DB.cursor()
        c.execute('SELECT * from `pokemons` ORDER BY id ASC')
        return map(Pokemon._make, c.fetchall())

    @classmethod
    def find(cls, pokeid):
        cursor = DB.cursor()
        cursor.execute('SELECT * from `pokemons` where id = ?', (pokeid,))
        return Pokemon._make(cursor.fetchone())

    @classmethod
    def by_name(cls,name):
        cursor = DB.cursor()
        cursor.execute('SELECT * from `pokemons` where internal_name LIKE ? or name LIKE ?', (name,name))
        return Pokemon._make(cursor.fetchone())


class LocationGroup(collections.namedtuple('LocationGroup', 'id name')):
    __update = '''UPDATE `location_groups` set id=:id, name=:name where id=:id'''
    __insert = '''INSERT INTO `location_groups` (id, name) 
                    SELECT ?,? WHERE (SELECT CHANGES() = 0)'''

    def save(self):
        cursor = DB.cursor()
        cursor.execute(LocationGroup.__update, self)
        cursor.execute(LocationGroup.__insert, self)
        DB.commit()

    def add_location(self, name, lat, lng):
        l = Location( None, self.id, name, lat, lng)
        l.save()

    def locations(self):
        return Location.by_group(self.id)
        

    @classmethod
    def new(cls, name):
        l = LocationGroup(None, name)
        l.save()
        return cls.find(name)

    @classmethod
    def all(cls):
        c = DB.cursor()
        c.execute("SELECT * from location_groups")
        return map(LocationGroup._make, c.fetchall())

    @classmethod
    def find(cls, name):
        cursor = DB.cursor()
        cursor.execute('SELECT * from `location_groups` where name = ? LIMIT 1', (name,))
        return LocationGroup._make(cursor.fetchone())

    @classmethod
    def by_id(cls, group_id):
        cursor = DB.cursor()
        cursor.execute('SELECT * from `location_groups` where id = ? LIMIT 1', (group_id,))
        return LocationGroup._make(cursor.fetchone())

 

class Location(collections.namedtuple('Location', 'id location_group_id name latitude longitude')):
    __update = '''UPDATE `locations` set id=:id, location_group_id=:location_group_id,
                    name=:name, latitude=:latitude, longitude=:longitude where id=:id'''
    __insert = '''INSERT INTO `locations` (id, location_group_id, name, latitude, longitude) 
                    SELECT ?,?,?,?,? WHERE (SELECT CHANGES() = 0)'''

    def save(self):
        cursor = DB.cursor()
        cursor.execute(Location.__update, self)
        cursor.execute(Location.__insert, self)
        self = cursor.lastrowid
        DB.commit()

    def group(self):
        if not hasattr(self, '__group'):
            self.__group = LocationGroup.by_id(self.location_group_id)
        return self.__group

    @classmethod
    def by_group(cls, group_id):
        c = DB.cursor()
        c.execute("SELECT * FROM locations WHERE location_group_id=?", (group_id,))
        return map(Location._make, c.fetchall())

    @classmethod
    def find(cls, loc_id):
        cursor = DB.cursor()
        cursor.execute('SELECT * from `location` where id = ? LIMIT 1', (loc_id,))
        return LocationGroup._make(cursor.fetchone())

         

