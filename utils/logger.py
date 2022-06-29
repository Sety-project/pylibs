import os,logging

def build_logging(app_name,log_mapping={logging.INFO:'info.log',logging.WARNING:'warning.log',logging.CRITICAL:'program_flow.log'}):
    '''log_mapping={logging.DEBUG:'debug.log'...
    3 handlers: >=debug, ==info and >=warning'''

    class MyFilter(object):
        '''this is to restrict info logger to info only'''
        def __init__(self, level):
            self.__level = level
        def filter(self, logRecord):
            return logRecord.levelno <= self.__level

    # mkdir log repos if does not exist
    log_path = os.path.join(os.sep, "tmp", app_name)
    if not os.path.exists(log_path):
        os.umask(0)
        os.makedirs(log_path, mode=0o777)

    logging.basicConfig()
    logger = logging.getLogger(app_name)

    # logs
    for level,filename in log_mapping.items():
        handler = logging.FileHandler(os.path.join(os.sep,log_path,filename), mode='w')
        handler.setLevel(level)
        handler.setFormatter(logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s'))
        #handler.addFilter(MyFilter(level))
        logger.addHandler(handler)

    # handler_alert = logging.handlers.SMTPHandler(mailhost='smtp.google.com',
    #                                              fromaddr='david@pronoia.link',
    #                                              toaddrs=['david@pronoia.link'],
    #                                              subject='auto alert',
    #                                              credentials=('david@pronoia.link', ''),
    #                                              secure=None)
    # handler_alert.setLevel(logging.CRITICAL)
    # handler_alert.setFormatter(logging.Formatter(f"%(levelname)s: %(message)s"))
    # self.myLogger.addHandler(handler_alert)

    logger.setLevel(min(log_mapping.keys()))

    return logger