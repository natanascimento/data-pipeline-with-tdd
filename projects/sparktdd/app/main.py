import logging

LOG_FILENAME = 'project.log'

if __name__ == '__main__':
    logging.basicConfig(filename=LOG_FILENAME, level=logging.INFO)

    