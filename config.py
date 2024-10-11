import logging


# Configure logging
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'default': {
            'format': '%(asctime)s - %(levelname)s - %(message)s',
        },
    },
    'handlers': {
        'file': {
            'class': 'logging.FileHandler',
            'filename': 'sparse2json.log',
            'formatter': 'default',
        },
    },
    'loggers': {
        'sparse2json_logger': {
            'handlers': ['file'],
            'level': logging.INFO,
            'propagate': False,
        },
    },
}
