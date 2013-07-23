"""
Unescape xml entities to return unicode.

Source stolen from http://effbot.org/zone/re-sub.htm#unescape-html
"""

__all__    = ["escape", "unescape"]

import re
import cgi
from htmlentitydefs import name2codepoint

def fixup(m):
    """
    Part of unescpe below.
    """

    text = m.group(0)
    if text[:2] == "&#":
        # character reference
        try:
            if text[:3] == "&#x":
                return unichr(int(text[3:-1], 16))
            else:
                return unichr(int(text[2:-1]))
        except ValueError:
            pass
    else:
        # named entity
        try:
            text = unichr(name2codepoint[text[1:-1]])
        except KeyError:
            pass

    return text # leave as is

def unescape(text):
    """
    Removes HTML or XML character references and entities from a text string.
    Returns the plain text, as a unicode string, if necessary.

    text - The HTML (or XML) source text.
    """

    return re.sub("&#?\\w+;", fixup, text)

def escape(text):
    """
    Encode HTML special chars and non ascii stuff using XML entities.
    Returns plain ascii text.

    text - Unicode text to be encoded using HTML source.
    """

    return cgi.escape(text, True).encode("ascii", "xmlcharrefreplace")

