"""
Logbook handlers using for sqlite3 logging and notify-send.
"""

import codecs
from subprocess import call

from logbook.base import NOTSET, ERROR, WARNING, NOTICE
from logbook.handlers import Handler, \
                             LimitingHandlerMixin, \
                             StringFormatterHandlerMixin

import pypb.flock as flock

EXPIRES_NEVER = 0
EXPIRES_DEFAULT = 5

def notify_send(summary, text, urgency="normal", expire_time=5):
    """
    Call notify send using the given parameters.
    """

    assert urgency in ("low", "normal", "critical")

    cmd = ["notify-send",
           "-u", urgency,
           "-t", str(int(expire_time) * 1000),
           summary,
           text]
    call(cmd)

class NotifySendHandler(Handler, StringFormatterHandlerMixin,
                        LimitingHandlerMixin):
    """
    Log using notify-send.
    """

    def __init__(self, format_string=None, record_limit=None, record_delta=None,
                 level=NOTSET, filter=None, bubble=False):

        Handler.__init__(self, level, filter, bubble)
        StringFormatterHandlerMixin.__init__(self, format_string)
        LimitingHandlerMixin.__init__(self, record_limit, record_delta)

    def emit(self, record):
        """
        Log the record.
        """

        if not self.check_delivery(record)[1]:
            return

        # Create summary
        summary = "{}: {}"
        summary = summary.format(record.channel, record.level_name.title())

        # Create text
        text = self.format(record)

        # Get expire time
        if record.level >= ERROR:
            expire_time = EXPIRES_NEVER
        else:
            expire_time = EXPIRES_DEFAULT

        # Get record level
        if record.level >= ERROR:
            urgency = "critical"
        elif record.level in (NOTICE, WARNING):
            urgency = "normal"
        else:
            urgency = "low"

        notify_send(summary, text, urgency, expire_time)

class LockedFileHandler(Handler, StringFormatterHandlerMixin):
    """
    Log to a file.
    """

    def __init__(self, filename, format_string=None,
                 level=NOTSET, filter=None, bubble=False):

        Handler.__init__(self, level, filter, bubble)
        StringFormatterHandlerMixin.__init__(self, format_string)

        self.filename = filename
        self.lock_filename = filename + ".lock"

    def emit(self, record):
        """
        Log the record.
        """

        with flock.flock(self.lock_filename):
            with codecs.open(self.filename, mode="ab", encoding="utf-8") as fobj:
                message = self.format(record)
                fobj.write(message)

                fobj.flush()

    def emit_batch(self, records, reason):
        """
        Log multiple records.
        """

        with flock.flock(self.lock_filename):
            with codecs.open(self.filename, mode="ab", encoding="utf-8") as fobj:
                for record in records:
                    message = self.format(record)
                    fobj.write(message)

                fobj.flush()

def main():
    import logbook

    log = logbook.Logger("logbook_notifier")
    hdl = NotifySendHandler()
    hdl.push_application()

    log.info("hello")
    log.notice("hello")
    log.warn("hello")
    log.error("hello")
    log.critical("hello")

if __name__ == '__main__':
    main()

