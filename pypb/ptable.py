"""
Pretty print Tables.
"""

import textwrap
from itertools import izip_longest

# Default value for the maximum column width
MAX_COL_WIDTH = 70

def str_tab(xss, fmts, maxws):
    """
    Convert the values in the 2d array to strings.
    """

    yss = []
    for xs in xss:
        ys = []
        for i, x in enumerate(xs):
            fmt  = fmts.get(i, u"")
            maxw = maxws.get(i, MAX_COL_WIDTH)
            y = u"{0:{fmt}}".format(x, fmt=fmt)
            y = textwrap.wrap(y, maxw)
            ys.append(y)
        for zs in izip_longest(*ys, fillvalue=u""):
            yss.append(zs)
    
    return yss

def max_col_widths(xss):
    """
    Return an array of maximum column widths for the 2d array.
    """

    cols = len(xss[0])

    ws = []
    for i in range(cols):
        w = max(len(xs[i]) for xs in xss)
        ws.append(w)
    
    return ws

def align_tab(xss, widths, aligns, fills):
    """
    Return 2d array with the strings correctly padded and aligned.
    """
    
    yss = []
    for xs in xss:
        ys = []
        for i, x in enumerate(xs):
            fill  = fills.get(i, u" ")
            align = aligns.get(i, u"<")
            width = widths[i]

            fmt = u"{0:{fill}{align}{width}}"
            y = fmt.format(x, fill=fill, align=align, width=width)
            ys.append(y)
        yss.append(ys)

    return yss

def fmt_tab(xss, fmts=None, aligns=None, fills=None, maxws=None):
    """
    Return 2d array of strings correcly formatted, padded, and aligned.
    """
    
    fmts   = {} if fmts is None else fmts
    aligns = {} if aligns is None else aligns
    fills  = {} if fills is None else fills
    maxws  = {} if maxws is None else maxws

    xss = str_tab(xss, fmts, maxws)
    widths = max_col_widths(xss)
    xss = align_tab(xss, widths, aligns, fills)

    return xss

def simple_fmt_tab(xss, fmts=None, aligns=None, fills=None, maxws=None,
                   cstart=u"| ", cend=u" |", csep=u" | "):
    """
    Create a text formatted table.
    """

    xss = fmt_tab(xss, fmts=fmts, aligns=aligns, fills=fills, maxws=maxws)
    xss = [cstart + csep.join(xs) + cend for xs in xss]
    xss = u"\n".join(xss)
    return xss

def wiki_fmt_tab(xss, fmts=None):
    """
    Create a wiki formatted table.
    """

    return simple_fmt_tab(xss, fmts=fmts,
                          cstart=u"|| ", cend=u" ||", csep=u" || ")

def latex_fmt_tab(xss, fmts=None):
    """
    Create a latex formatted table.
    """

    return simple_fmt_tab(xss, fmts=fmts,
                          cstart=u"", cend=u" \\\\ \\hline", csep=u" & ")

