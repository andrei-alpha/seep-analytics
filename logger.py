import logging

class ColoredFormatter(logging.Formatter):
    def format(self, record):
        if record.levelno == logging.ERROR or record.levelno == logging.CRITICAL:
            record.levelname = '\033[1m\033[0;31m%s\033[0m' % record.levelname
        elif record.levelno == logging.WARNING:
            record.levelname = '\033[1m\033[1;31m%s\033[0m' % record.levelname
        elif record.levelno == logging.INFO:
            record.levelname = '\033[1m\033[0;34m%s\033[0m' % record.levelname
        elif record.levelno == logging.DEBUG:
        	record.levelname = '\033[1m\033[1;30m%s\033[0m' % record.levelname
        return logging.Formatter.format(self, record)

class Logger:
	def __init__(self, name='Default', level=logging.DEBUG):
		self.logger = logging.getLogger(name)
		self.logger.setLevel(level)
		handler = logging.StreamHandler()

		log_format = '%(asctime)s [%(name)s] %(levelname)-8s %(message)s'
		time_format = '%H:%M:%S'
		formatter = ColoredFormatter(log_format, datefmt=time_format)
		handler.setFormatter(formatter)
		handler.setLevel(level)
		self.logger.addHandler(handler)

	def warn(self, *msg):
		self.logger.warning(' '.join([str(x) for x in list(msg)]))

	def error(self, *msg):
		self.logger.error(' '.join([str(x) for x in list(msg)]))

	def info(self, *msg):
		self.logger.info(' '.join([str(x) for x in list(msg)]))

	def debug(self, *msg):
		self.logger.debug(' '.join([str(x) for x in list(msg)]))

	def critical(self, *msg):
		self.logger.critical(' '.join([str(x) for x in list(msg)]))
