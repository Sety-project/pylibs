#!/usr/bin/env python
# -*- coding: utf-8 -*-
# This program is dedicated to the public domain under the CC0 license.

from histfeed.ftx_history import *
from pfoptimizer.portoflio_optimizer import strategy_wrapper,enricher_wrapper
from riskpnl.ftx_risk_pnl import ftx_portoflio_main
from tradeexecutor.ftx_ws_execute import *
from histfeed.ftx_history import ftx_history_main_wrapper
import logging
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

# Enable logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)

logger = logging.getLogger(__name__)


# Define a few command handlers. These usually take the two arguments update and
# context. Error handlers also receive the raised TelegramError object in error.
def start(update, context):
    """Send a message when the command /start is issued."""
    update.message.reply_text('coucou')
    temp_dir = '/Runtime/logs/Pronoia_Bot'
    for file in os.listdir(temp_dir): os.remove(temp_dir + '/' + file)

def help(update, context):
    """Send a message when the command /help is issued."""
    update.message.reply_text('example requests:')
    update.message.reply_text('* risk [exchange] [subaccount]-> live risk')
    update.message.reply_text('* plex [exchange] [subaccount]-> compute plex')
    update.message.reply_text('* hist [coin] [exchange] [days]-> history of BTC and related futures/borrow every 15m for past 7d')
    update.message.reply_text('* basis [future] [size] [exchange] -> futures basis on ftx in size 10000')
    update.message.reply_text('* sysperp [holding period] [signal horizon]: optimal perps')
    update.message.reply_text('* execute: executes latest sysperp run')
    update.message.reply_text('* fromOptimal: live portoflio vs target')

def echo(update, context):
    try:
        user_msg = update.effective_message.text.lower()
        split_message = update.effective_message.text.lower().split()
        whitelist = ['daviidarr','Stephan', 'Victor']
        if not update.effective_message.chat['first_name'] in whitelist:
            update.message.reply_text("Hey " + update.effective_message.chat['first_name'] + ": my code is so slow you have time to read that")
            log=pd.DataFrame({'first_name':[update.effective_message.chat['first_name']],
                              'date':[str(update.effective_message['date'])],
                              'message':[update.effective_message['text']]})
            log.to_excel(os.path.join(os.sep, "tmp", "ux", 'chathistory.xlsx'))

        if split_message[0] == 'hist':
            split_message[0] = 'get'
            exchange_name = 'ftx'
            run_type = split_message[0]
            universe = configLoader.get_bases(split_message[1])
            nb_of_days = int(split_message[2])
            data = asyncio.run(ftx_history_main_wrapper(exchange_name, run_type, universe, nb_of_days))
        elif split_message[0] == 'basis':
            type = 'perpetual' if len(split_message)<2 else str(split_message[1])
            depth = 0 if len(split_message)<3 else int(split_message[2])
            exchange_name = 'ftx' if len(split_message) < 4 else split_message[3]
            data = enricher_wrapper(exchange_name,type,depth)
#        elif update.effective_message.chat['first_name'] in whitelist:
        elif split_message[0] in ['risk','plex','fromoptimal']:
            # Call pyrun with the good params
            data = ftx_portoflio_main(*split_message)
        elif split_message[0] == 'sysperp':
            # Call pyrun with the good params
            #data = strategies_main(*split_message)
            pass
        elif split_message[0] == 'execute':
            # Call pyrun with the good params
            data = ftx_ws_spread_main(*split_message)[0]
        else:
            raise Exception('unknown command, type /help')

        dirname = os.path.join(os.sep, "tmp", "ux")
        filename = os.path.join(os.sep,dirname,'telegram_file.csv')
        if not os.path.exists(dirname):
            os.umask(000)
            os.makedirs(dirname, mode=0o777)
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
        if user_msg == 'docker ps':
            response = docker_ps()
            update.message.reply_text(f'<pre>{response}</pre>', parse_mode=ParseMode.HTML)

    except Exception as e:
        update.message.reply_text(str(e))

def error(update, context):
    """Log Errors caused by Updates."""
    logger.warning('Update "%s" caused error "%s"', update, context.error)

def main(*args):
    """Start the bot."""
    # Create the Updater and pass it your bot's token.
    # Make sure to set use_context=True to use the new context based callbacks
    # Post version 12 this will no longer be necessary
    updater = Updater('1752990518:AAF0NpZBMgBzRTSfoaDDk69Zr5AdtoKtWGk', use_context=True)

    # Get the dispatcher to register handlers
    dp = updater.dispatcher

    # on different commands - answer in Telegram
    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CommandHandler("help", help))

    # on noncommand i.e message - echo the message on Telegram
    dp.add_handler(MessageHandler(Filters.text, echo))

    # log all errors
    dp.add_error_handler(error)

    # Start the Bot
    updater.start_polling()

    # Run the bot until you press Ctrl-C or the process receives SIGINT,
    # SIGTERM or SIGABRT. This should be used most of the time, since
    # start_polling() is non-blocking and will stop the bot gracefully.
    updater.idle()

# if __name__ == '__main__':
#     main(*args)