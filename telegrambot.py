#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from telegram.ext import * #Updater, CommandHandler, MessageHandler, Filters, InlineQueryHandler
from telegram import *
import logging
import logging.handlers
import sqlite3
from pokedb import *
from datetime import *
import math
import json

FFORMAT='%(levelname)1.1s|%(asctime)s| %(message)s'


logger = logging.getLogger('poke.telegram')
emoji = {
    "map": '\U0001f5fa',
    "keyboard": '\u2328',
    "enabled": '\u2714',
    "disabled": '\u2716',
    "ruler": '\U0001F4CF'
}

def config_log(config):
    filename = "poke.log";
    if "log-file" in config:
        filename = config["log-file"]

    handler = logging.handlers.RotatingFileHandler(filename,
            maxBytes=100*1024*1024, backupCount=5)
    handler.setFormatter(logging.Formatter(FFORMAT))
    logger.addHandler(handler)
    handler.setLevel(logging.DEBUG)
    logger.setLevel(logging.DEBUG)



def get_user(update):
    return  User.find(update.message.chat_id)

def distance(pA, pB):
    R = 6371000.0 #raio da terra em metros
    aLat = math.radians(pA.latitude)
    aLng = math.radians(pA.longitude)
    bLat = math.radians(pB.latitude)
    bLng = math.radians(pB.longitude)
    distLat = bLat - aLat
    distLng = (bLng - aLng) * math.cos(0.5*(bLat+aLat))

    dist = R * math.sqrt(distLat*distLat + distLng*distLng)
    return dist

def cmd_help(bot, update):
    chat_id = update.message.chat_id
    bot.sendMessage(chat_id,text=
'''/help
/start
/add <pokemon_name>
/add <pokemon_name1> <pokemon_name2>...
/rem <pokemon_name>
/rem <pokemon_name1> <pokemon_name2>...
/list - List configured  pokemons
/keyboard - updates the keyboard
Send <location> - Update location
''')
 

def cmd_start(bot, update):
    chat_id = update.message.chat_id
    user = get_user(update)
    if user:
        user.chat_id = chat_id
        user.save()
        bot.sendMessage(chat_id, text="Welcome back {}, where are you? Please send me your updated position.".format(user.first_name))
    else:
        msgfrom = update.message.from_user
        user = User.new(msgfrom.first_name, msgfrom.last_name, msgfrom.username, chat_id)
        user.save()
        update.message.reply_text("""I'm the PokeBot, I'll let you know when monsters are nearby.
                Please send your localization and a /distance""")
        logger.info('New User: {} {} (@{}-{})'.format(user.first_name, user.last_name, 
            user.username, chat_id))

    cmd_keyboard(bot, update)

def cmd_list(bot,update):
    chat_id = update.message.chat_id
    user = get_user(update)
    
    resp = 'You will receive notifications for: '
    for f in user.filters():
        resp += f.name + ' '

    bot.sendMessage( chat_id,  text=resp )

def cmd_add(bot, update, args):
    chat_id = update.message.chat_id
    user = get_user(update)

    try:
        for p in args:
            poke = Pokemon.by_name(p)
            logger.debug("{}({}) add notify: {}".format(user.first_name, chat_id, poke))
            user.add_filter(poke.id)
        cmd_list(bot, update)
    except Exception as e:
        logger.error('{}({}) add error: {}'.format(user.username, chat_id, e))
        bot.sendMessage(chat_id,text="/add NAME or /add NAME1 NAME2")


def cmd_rem(bot, update, args):
    chat_id = update.message.chat_id
    user = get_user(update)

    try:
        for p in args:
            poke = Pokemon.by_name(p)
            logger.debug("{}({}) rem notify: {}".format(user.first_name, chat_id, poke))
        user.del_filter(poke.id)
    except Exception as e:
        logger.error('{}({}) rem Error: {}'.format(user.username, chat_id, e))
        bot.sendMessage(chat_id,text="/rem NAME or /rem NAME1 NAME2")

    cmd_list(bot,update)

def cmd_distance(bot, update, args):
    chat_id = update.message.chat_id
    user = get_user(update)

    if len(args) < 1:
        bot.sendMessage(chat_id, text="Current distance is {}m".format(user.distance))
        return

    user.distance = int(args[0])
    user.save()
    bot.sendMessage(chat_id, text="Distance set to {}m".format(user.distance))
    logger.debug("{}({}) Distance: {}m".format(user.first_name, chat_id, user.distance))

def cmd_location(bot, update):
    chat_id = update.message.chat_id
    user = get_user(update)

    try:
        loc = update.message.location
        user.update_position(loc.latitude, loc.longitude)
        bot.sendMessage(chat_id, text='Position set' )
        logger.debug("{}({}) Position set: lat={}, lng={}".format(user.first_name, chat_id,
            loc.latitude, loc.longitude))
    except Exception as e:
        bot.sendMessage(chat_id, text="Error setting position")
        logger.error("{}({}) error setting position (msg={}) - {}".format(
            user.first_name, update.message, e ))


def callback_periodic_check(bot, job):
    #print('.', end='', flush=True)
    all_users = User.all()
    all_spawns = list(Spawn.all_active())
    now = datetime.now()
    for u in all_users:
        if not u.position():
            continue
        filters = list(u.filters()) #must convert to list, maps only iterate once

        notified = False

        for s in all_spawns:
            exp = datetime.fromtimestamp(s.expiration_timestamp)
            secs = (exp - now).total_seconds()
            #print("  {:10s} - {:30s} - {:30s} - {}".format(s.name, str(now), str(exp), secs))
            if secs < 0:
                continue
            for f in filters:
                if s.name == f.internal_name:
                    if u.notify(s.encounter_id):
                        dist = distance(s, u.position())
                        if dist < u.distance:
                            if not notified:
                                logger.debug( "{}({}) Notifying:".format(u.first_name, u.chat_id))
                                notified = True

                            logger.debug( "    spawn: {} dist: {:1.1f}m - exp in {:02d}m{:02d}s".format(
                                f.name, dist, int(secs/60), int(secs%60)))
                            bot.sendVenue(u.chat_id, s.latitude, s.longitude, 
                                "{}".format(s.name),
                                "{:02d}m{:02d}s left ({:02d}:{:02d}) {:1.1f}m away".format(
                                    int(secs/60), int(secs%60), exp.hour, exp.minute, dist) )
                    break
        if not notified:
            pass

def cmd_text(bot, update):
    chat_id = update.message.chat_id
    user = get_user(update)
    cmd = update.message.text[0]
    if cmd == emoji["map"]:
        cmd_location(bot, update)
    elif cmd == emoji["keyboard"]:
        bot.sendMessage(chat_id=chat_id, text='Keyboard hidden', reply_markup=ReplyKeyboardHide() )
    elif cmd == emoji["enabled"]:
        pokename = update.message.text[2:]
        poke = Pokemon.by_name(pokename)
        user.del_filter(poke.id)
        logger.debug("{}({}) disable notify: {}".format(user.first_name, chat_id, pokename))
        bot.sendMessage(chat_id=chat_id, text='Disabled notifications for ' + pokename, 
                reply_markup=get_keyboard(user))
    elif cmd == emoji["disabled"]:
        pokename = update.message.text[2:]
        poke = Pokemon.by_name(pokename)
        user.add_filter(poke.id)
        logger.debug("{}({}) enable notify: {}".format(user.first_name, chat_id, pokename))
        bot.sendMessage(chat_id=chat_id, text='Enabled notifications for ' + pokename, 
                reply_markup=get_keyboard(user))
    elif cmd == emoji["ruler"]:
        dist = update.message.text[1:-1]
        cmd_distance(bot, update, [dist])
        #bot.sendMessage(chat_id=chat_id, text="int? = {}".format(dist) )


def cmd_keyboard(bot, update):
    chat_id = update.message.chat_id
    user = get_user(update)
    
    bot.sendMessage(chat_id=chat_id, text="Use keyboard to enable/disable notifications", reply_markup=get_keyboard(user))


def get_keyboard(user):
    filters = [f.name for f in user.filters() ]
    r = emoji["ruler"]
    custom_keyboard = [ [ KeyboardButton(text=emoji["map"] + ' Location', request_location=True)],
        [r+'100m', r+'300m'], [r+'500m', r+'1000m'] ]

    pokes = Pokemon.all()
    counter = 0
    row = []
    for p in pokes:
        name = emoji["enabled"] if (p.name in filters) else emoji["disabled"]
        name += ' ' + p.name
        row.append( name)
        if len(row) == 2:
            custom_keyboard.append(row)
            row = []
    if len(row) > 0:
        custom_keyboard.append(row)
    custom_keyboard.append([KeyboardButton(text=emoji["keyboard"] + ' Hide Keyboard')])

    return ReplyKeyboardMarkup(custom_keyboard)

def error(bot, update, error):
    logger.error('Update "{}" caused error"{}"'.format( update, error))

def main():
    config = {}
    with open('poke.json') as config_file:
        config = json.load(config_file)
    config_log(config)

    if 'telegram-token' not in config:
        print("Configuration file lacks Telegram Token")
        logger.error("Configuration file lacks Telegram Token")
        exit()

    try:
        updater = Updater(config['telegram-token'])

        dp = updater.dispatcher

        dp.add_handler(CommandHandler('help', cmd_help))
        dp.add_handler(CommandHandler('start', cmd_start))
        dp.add_handler(CommandHandler('add', cmd_add, pass_args=True))
        dp.add_handler(CommandHandler('rem', cmd_rem, pass_args=True))
        dp.add_handler(CommandHandler('distance', cmd_distance, pass_args=True))
        dp.add_handler(CommandHandler('list', cmd_list))
        dp.add_handler(CommandHandler('keyboard', cmd_keyboard ))
        dp.add_handler(MessageHandler([Filters.text], cmd_text))
        dp.add_handler(MessageHandler([Filters.location], cmd_location))

        jq = updater.job_queue
        jq.put(Job(callback_periodic_check, 30.0), next_t=0.0)

        dp.add_error_handler(error)

        logger.info("Starting PokeBot.")
        updater.start_polling()

        updater.idle()
    except Exception as e:
        logger.error("Error starting Bot: {}".format(e))


if __name__ == '__main__':
    main()

