import threading
import sqlite3
import mysql.connector
import logging
import collections
import time
import json

DB_threadlocal = threading.local()

class DB(object):
    def __new__(cls, **kwargs):
        if getattr(DB_threadlocal, 'db_instance', None) is None:
            config = {}
            with open('poke.json') as config_file:
                config = json.load(config_file)

            db = config["database"]
                
            DB_threadlocal.db_instance = object.__new__(cls)
            DB_threadlocal.db_instance.conn = mysql.connector.connect(user=db["user"],
                    password=db["password"], host=db["host"], database=db["database"])
            DB_threadlocal.db_instance.cursor_param = {"dictionary": True}
            DB_threadlocal.db_instance.__createTables(drop_before=kwargs.get('wipe'))

        return DB_threadlocal.db_instance
        
    def __createTables(self, **kwargs):
        drop_before = kwargs.get('drop_before')
        cursor = self.cursor()
        try:
            cursor.execute('SELECT `version` FROM `version` ORDER BY `version` DESC LIMIT 1')
            v = cursor.fetchone()
            self.version = v['version']
        except Exception as e:
            logging.warn("Error reading DB version. ({})".format(e))
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
                    `first_name` VARCHAR(256),
                    `last_name` VARCHAR(256),
                    `username` VARCHAR(256),
                    `chat_id` VARCHAR(256) NOT NULL UNIQUE,
                    `distance` INTEGER) ''')

                cursor.execute('''CREATE TABLE `user_positions` (
                    `user_id` INTEGER,
                    `timestamp` INTEGER,
                    `latitude` REAL,
                    `longitude` REAL )''')

                cursor.execute('''CREATE TABLE `location_groups` (
                    `id` INTEGER NOT NULL UNIQUE PRIMARY KEY AUTO_INCREMENT,
                    `name` VARCHAR(256) )''')

                cursor.execute('''CREATE TABLE `locations` (
                    `id` INTEGER NOT NULL UNIQUE PRIMARY KEY AUTO_INCREMENT,
                    `location_group_id` INTEGER NOT NULL,
                    `name` VARCHAR(256),
                    `latitude` REAL,
                    `longitude` REAL )''')

                cursor.execute('''CREATE TABLE `pokemons` (
                    `id` INTEGER NOT NULL UNIQUE PRIMARY KEY,
                    `name` VARCHAR(64),
                    `internal_name` VARCHAR(64),
                    `rarity` INTEGER )''')

                cursor.execute('''CREATE TABLE `user_filters` (
                    `user_id` INTEGER,
                    `pokemon_id` INTEGER,
                    PRIMARY KEY( `user_id`, `pokemon_id` ) )''')

                cursor.execute('''CREATE TABLE `spawns` (
                    `encounter_id` VARCHAR(64) UNIQUE,
                    `expiration_timestamp` INTEGER,
                    `latitude` REAL,
                    `longitude` REAL,
                    `name` VARCHAR(64),
                    `spawn_point_id` VARCHAR(256) )''')

                cursor.execute('''CREATE TABLE `notifications` (
                    `encounter_id` VARCHAR(64),
                    `user_id` INTEGER,
                    PRIMARY KEY( `encounter_id`, `user_id`) )''')

                cursor.execute('''CREATE TABLE `version` (
                    `version` INTEGER UNSIGNED NOT NULL )''')

                cursor.execute('''INSERT INTO `version` (`version`) values ( %s )''', (current_version,) )

                self.conn.commit()
                self.version = current_version
                logging.debug("Upgraded DB to version {}".format(current_version) )
        except Exception as e:
            self.conn.rollback()
            logging.error("Error creating DB: ({}) - {}".format(kwargs, e))
            raise

    @classmethod
    def connection(cls):
        return cls().conn

    @classmethod
    def cursor(cls):
        return cls().conn.cursor( **cls().cursor_param )

    @classmethod
    def commit(cls):
        return cls().conn.commit()

    @classmethod
    def rollback(cls):
        return cls().conn.rollback()

class Data(object):
    def __init__(self, **kwargs):
        for a in self._attrs():
            setattr(self, a, kwargs.get(a))

    def save(self):
        try:
            c = DB.cursor()
            c.execute(self._insert(), self.__dict__)
            DB.commit()
        except Exception as e:
            DB.rollback()
            logging.warn("Error saving {} ({}) - {}".format(self.__class__.__name__, self.__dict__, e))

    @classmethod
    def _make(cls, args):
        return cls(**args)

    @classmethod 
    def _attrs(cls):
        raise NotImplementedError

    @classmethod 
    def _insert(cls):
        raise NotImplementedError



class UserFilter(Data):
    @classmethod
    def _attrs(cls):
        return [  'user_id', 'pokemon_id']
    

class Spawn(Data):
    @classmethod
    def _attrs(cls):
        return [ 'encounter_id', 'expiration_timestamp', 'latitude', 'longitude', 
                'name', 'spawn_point_id']

    @classmethod
    def _insert(cls):
        return '''INSERT INTO spawns (encounter_id, expiration_timestamp, latitude,
                    longitude, name, spawn_point_id) 
                    VALUES ( %(encounter_id)s, %(expiration_timestamp)s, 
                        %(latitude)s, %(longitude)s, %(name)s, %(spawn_point_id)s )'''

    @classmethod
    def register(cls, obj):
        Spawn(**obj).save()


    @classmethod
    def all_active(cls):
        c = DB.cursor()
        c.execute('''SELECT * FROM spawns 
                        WHERE expiration_timestamp > UNIX_TIMESTAMP( NOW() ) 
                        ORDER BY expiration_timestamp ASC''')
        def creator(data):
            return cls(**dict(data))
        return map(creator, c.fetchall())

class Filter(Data):
    @classmethod
    def _attrs(cls):
        return [ 'internal_name', 'name' ]
            
class User(Data):
    @classmethod
    def _attrs(cls):
        return  [ 'id', 'first_name', 'last_name', 'username', 'chat_id', 'distance' ]

    @classmethod
    def _insert(cls):
        return '''INSERT INTO `users` (id, first_name, last_name, username, chat_id, distance)
                    VALUES (%(id)s, %(first_name)s, %(last_name)s, %(username)s, %(chat_id)s, 
                        %(distance)s )
                    ON DUPLICATE KEY UPDATE
                        first_name = %(first_name)s,
                        last_name = %(last_name)s,
                        username = %(username)s,
                        chat_id = %(chat_id)s,
                        distance = %(distance)s'''

    def update_position(self, latitude, longitude):
        self.last_pos = UserPosition(user_id=self.id, timestamp=time.time(), 
                latitude=latitude, longitude=longitude)
        self.last_pos.save()

    def position(self):
        if not hasattr(self, 'last_pos') or self.last_pos == None:
            self.last_pos = UserPosition.get_last(self.id)
        return self.last_pos

    def add_filter(self, pokemon_id):
        try:
            c = DB.cursor()
            c.execute('INSERT INTO user_filters VALUES ( %s , %s )', (self.id, pokemon_id))
            DB.commit()
        except Exception as e:
            DB.rollback()
            logging.warning("Could not insert filter for user: {} - {} ({})".format(
                self.username, pokemon_id, e))
            pass
            

    def del_filter(self, pokemon_id):
        try:
            c = DB.cursor()
            c.execute('DELETE FROM user_filters WHERE user_id=%s AND pokemon_id=%s', 
                    (self.id, pokemon_id))
            DB.commit()
        except Exceptino as e:
            DB.rollback()
            logging.warning("Could not remove filter for user: {} - {} ({})".format(
                self.username, pokemon_id, e))

    def filters(self):
        c = DB.cursor()
        c.execute('''SELECT internal_name, name FROM user_filters AS f
                    LEFT JOIN pokemons AS p ON p.id = f.pokemon_id
                    WHERE user_id=%s''', (self.id,) )
        
        return map(Filter._make, c.fetchall())

    def notify(self, encounter_id):
        c = DB.cursor()
        try:
            c.execute('''INSERT INTO notifications VALUES ( %s, %s ) ''', (encounter_id, self.id))
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
        cursor.execute('SELECT * from `users` where `chat_id`=%s', (chat_id,))
        data = cursor.fetchone()
        if data == None:
            return None
        return User(**dict(data))


class UserPosition(Data):
    @classmethod
    def _attrs(cls):
        return [ 'user_id', 'timestamp', 'latitude', 'longitude' ] 

    @classmethod
    def _insert(cls):
        return '''INSERT INTO `user_positions` (user_id, timestamp, latitude, longitude) 
                    VALUES (%(user_id)s, %(timestamp)s, %(latitude)s, %(longitude)s)'''

    @classmethod
    def get_last(cls, user_id):
        c = DB.cursor()
        c.execute('''SELECT * FROM user_positions WHERE user_id=%(user_id)s
                        ORDER BY timestamp DESC LIMIT 1''', {'user_id': user_id} )
        data = c.fetchone()
        if data == None:
            return None
        return UserPosition(**dict(data))


class Pokemon(Data):
    @classmethod 
    def _attrs(cls):
        return [ 'id', 'name', 'internal_name', 'rarity']

    @classmethod 
    def _insert(cls):
        return '''INSERT INTO `pokemons` (id, name, internal_name, rarity) 
                    VALUES (%(id)s,%(name)s,%(internal_name)s,%(rarity)s) 
                    ON DUPLICATE KEY UPDATE 
                        name = %(name)s,
                        internal_name = %(internal_name)s,
                        rarity = %(rarity)s'''

    # def __init__(self, **kwargs):
    #     for a in Pokemon.__attrs:
    #         setattr(self, a, kwargs.get(a))
    # 
    # def save(self):
    #     try:
    #         cursor = DB.cursor()
    #         cursor.execute(Pokemon.__insert, self.__dict__)
    #         DB.commit()
    #     except Exception as e:
    #         DB.rollback()
    #         logging.warn("Error saving pokemon({}) - {}".format(self.__dict__, e))

    @classmethod
    def all(cls):
        c = DB.cursor()
        c.execute('SELECT * from `pokemons` ORDER BY id ASC')
        return map(Pokemon._make, c.fetchall())

    @classmethod
    def find(cls, pokeid):
        cursor = DB.cursor()
        cursor.execute('SELECT * from `pokemons` where id = %s', (pokeid,))
        return Pokemon._make(cursor.fetchone())

    @classmethod
    def by_name(cls,name):
        cursor = DB.cursor()
        cursor.execute('SELECT * from `pokemons` where internal_name LIKE %s or name LIKE %s', (name,name))
        return Pokemon._make(cursor.fetchone())


class LocationGroup(Data):
    @classmethod
    def _attrs(cls):
        return ['id', 'name']

    @classmethod
    def _insert(cls):
        return '''INSERT INTO `location_groups` (id, name) 
                    VALUES ( %(id)s, %(name)s )
                    ON DUPLICATE KEY UPDATE
                        name = %(name)s'''

    # def __init__(self, **kwargs):
    #     for a in Pokemon.__attrs:
    #         setattr(self, a, kwargs.get(a))

    # def save(self):
    #     cursor = DB.cursor()
    #     cursor.execute(LocationGroup.__insert, self.__dict__)
    #     DB.commit()

    def add_location(self, name, lat, lng):
        l = Location( location_group_id=self.id, name=name, latitude=lat, longitude=lng)
        l.save()

    def locations(self):
        return Location.by_group(self.id)
        

    @classmethod
    def new(cls, name):
        l = LocationGroup(name=name)
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
        cursor.execute('SELECT * from `location_groups` where name = %s LIMIT 1', (name,))
        return LocationGroup._make(cursor.fetchone())

    @classmethod
    def by_id(cls, group_id):
        cursor = DB.cursor()
        cursor.execute('SELECT * from `location_groups` where id = %s LIMIT 1', (group_id,))
        return LocationGroup._make(cursor.fetchone())

 

class Location(Data):
    @classmethod
    def _attrs(cls):
        return ['id', 'location_group_id', 'name', 'latitude','longitude']

    def _insert(cls):
        return '''INSERT INTO `locations` (id, location_group_id, name, latitude, longitude) 
                    VALUES (%(id)s, %(location_group_id)s, %(name)s, %(latitude)s, %(longitude)s )
                    ON DUPLICATE KEY UPDATE
                        location_group_id = %(location_group_id)s,
                        name = %(name)s,
                        latitude = %(latitude)s,
                        longitude = %(longitude)s'''

    # def save(self):
    #     cursor = DB.cursor()
    #     cursor.execute(Location.__update, self)
    #     cursor.execute(Location.__insert, self)
    #     self = cursor.lastrowid
    #     DB.commit()

    def group(self):
        if not hasattr(self, '__group'):
            self.__group = LocationGroup.by_id(self.location_group_id)
        return self.__group

    @classmethod
    def by_group(cls, group_id):
        c = DB.cursor()
        c.execute("SELECT * FROM locations WHERE location_group_id=%s", (group_id,))
        return map(Location._make, c.fetchall())

    @classmethod
    def find(cls, loc_id):
        cursor = DB.cursor()
        cursor.execute('SELECT * from `location` where id = %s LIMIT 1', (loc_id,))
        return LocationGroup._make(cursor.fetchone())

         

