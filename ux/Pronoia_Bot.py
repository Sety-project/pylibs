#!/usr/bin/env python
# -*- coding: utf-8 -*-
# This program is dedicated to the public domain under the CC0 license.
from utils.api_utils import extract_args_kwargs,api,MyModules
import importlib,logging

import subprocess
import shlex
from ux.docker_access import *
from telegram import ParseMode
import pandas as pd

#import dataframe_image as dfi
"""
Pronoia_Bot to reply to Telegram messages with all ftx basis

First, a few handler functions are defined. Then, those functions are passed to
the Dispatcher and registered at their respective places.
Then, the bot is started and runs until we press Ctrl-C on the command line.

Usage:
Basic Echobot example, repeats messages.
Press Ctrl-C on the command line or send a signal to the process to stop the
bot.
"""

from telegram.ext import Updater, CommandHandler, MessageHandler, Filters

# Define a few command handlers. These usually take the two arguments update and
# context. Error handlers also receive the raised TelegramError object in error.
def start(update, context):
    """Send a message when the command /start is issued."""
    update.message.reply_text('coucou')
    temp_dir = '/Runtime/logs/Pronoia_Bot'
    for file in os.listdir(temp_dir): os.remove(temp_dir + '/' + file)

def help(update, context):
    """Send a message when the command /help is issued."""
    update.message.reply_text('requests:\n')
    for mod_name,data in MyModules.current_module_list.items():
        if mod_name != 'ux':
            update.message.reply_text('\n'.join([mod_name]+data['testbed']))

def echo(update, context):
    try:
        caller = update.effective_message.chat['username']
        logging.getLogger('ux').info(f'{caller} called {update.effective_message.text} at {datetime.datetime.utcnow()}')

        split_message = update.effective_message.text.split()
        split_message[0] = split_message[0].lower()
        whitelist = ['daviidarr']
        if caller not in whitelist:
            update.message.reply_text("Hey " + update.effective_message.chat['first_name'] + ": get in touch for whitelisting.")
            log=pd.DataFrame({'first_name':[update.effective_message.chat['first_name']],
                              'date':[str(update.effective_message['date'])],
                              'message':[update.effective_message['text']]})
            log.to_csv(os.path.join(os.sep, "tmp", "ux", 'chathistory.csv'))
            return

        if split_message[0] in MyModules.current_module_list:
            module = importlib.import_module(f'{split_message[0]}.main')
            main_func = getattr(module, 'main')
            args, kwargs = extract_args_kwargs(split_message)
            data = main_func(*args,**kwargs)
        elif split_message[0] == 'docker':
            if split_message[1] == 'ps':
                response = docker_ps()
                update.message.reply_text(f'<pre>{response}</pre>', parse_mode=ParseMode.HTML)
                data = None
            elif split_message[1] == 'stop':
                if len(split_message) < 3:
                    raise Exception('need appname for docker stop')
                response = docker_stop(split_message[2])
                update.message.reply_text(f'<pre>{response}</pre>', parse_mode=ParseMode.HTML)
                data = None
            else:
                raise Exception('docker ps or docker stop ?')
        elif split_message[0] == 'exec':
            if len(split_message) < 4:
                raise Exception('need all arguments, eg: exec flatten ftx SysPerp')
            run_type = split_message[1]
            exchange_name = split_message[2]
            sub_account = split_message[3]
            command = f"docker run -d -e USERNAME=\"ec2-user\" --network host \"878533356457.dkr.ecr.eu-west-2.amazonaws.com/tradeexecutor:latest\" --restart=on-failure --name=tradeexecutor_worker -e RUN_TYPE=\"{run_type}\" -e EXCHANGE_NAME=\"{exchange_name}\" -e SUB_ACCOUNT=\"{sub_account}\" -v ~/.cache/setyvault:/home/ec2-user/.cache/setyvault -v ~/config/prod:/home/ec2-user/config -v /tmp:/tmp"
            response = bash_run(command)
            update.message.reply_text(''.join([f'{key} ----->\n {value}\n' for key, value in response.items()]))
            data = None
        elif split_message[0] == 'bash:':
            raise Exception('disabled')
            response = bash_run(''.join(split_message[1:]))
            update.message.reply_text(''.join([f'{key} ----->\n {value}\n' for key, value in response.items()]))
            data = None
        else:
            raise Exception('unknown command, type /help')

        dirname = os.path.join(os.sep, "tmp", "ux")
        filename = os.path.join(os.sep,dirname,'telegram_file.csv')
        if not os.path.exists(dirname):
            os.umask(000)
            os.makedirs(dirname, mode=0o777)
        if data is not None:
            data.to_csv(filename)
            with open(filename, "rb") as file:
                update.message.bot.sendDocument(update.message['chat']['id'], document=file)

#            msg = update.message.reply_text(docker_status(split_message[0]))
#            msg = docker_status(split_message[0])
#            msg = [[1,2,3,234,542,12],[1,2,3,234,542,12],[1,2,3,234,542,12],[1,2,3,234,542,12]]
#            update.message.reply_text(f'<pre>{msg}</pre>', parse_mode=ParseMode.HTML)
#            if len(msg) > 4096:
#                for x in range(0, len(msg), 4096):
#                    update.message.reply_text(msg[x:x + 4096], parse_mode=ParseMode.HTML)
#           else:
#               update.message.reply_text(msg, parse_mode=ParseMode.HTML)

    except Exception as e:
        logging.getLogger('ux').critical(str(e),stack_info=True)
        update.message.reply_text(str(e))

def bash_run(command):
    completed_process = subprocess.run(['/bin/bash', '-i', '-c'] + shlex.split(command), capture_output=True, timeout=5, encoding="utf-8")
    response = completed_process.stdout
    error_msg = completed_process.stderr
    return {'response':response,'error_msg':error_msg,'returncode':completed_process.returncode}

# def error(update, context):
#     """Log Errors caused by Updates."""
#     logging.getLogger('ux').critical('Update "%s" caused error "%s"', update, context.error)

@api
def main(*args,**kwargs):
    """Start the bot."""
    # Create the Updater and pass it your bot's token.
    # Make sure to set use_context=True to use the new context based callbacks
    # Post version 12 this will no longer be necessary
    logger = kwargs.pop('__logger')

    updater = Updater('1752990518:AAF0NpZBMgBzRTSfoaDDk69Zr5AdtoKtWGk', use_context=True)

    # Get the dispatcher to register handlers
    dp = updater.dispatcher

    # on different commands - answer in Telegram
    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CommandHandler("help", help))

    # on noncommand i.e message - echo the message on Telegram
    dp.add_handler(MessageHandler(Filters.text, echo))

    # log all errors
    #dp.add_error_handler(error)

    # Start the Bot
    updater.start_polling()

    # Run the bot until you press Ctrl-C or the process receives SIGINT,
    # SIGTERM or SIGABRT. This should be used most of the time, since
    # start_polling() is non-blocking and will stop the bot gracefully.
    updater.idle()

# if __name__ == '__main__':
#     main(*args)
