"""
Base 36 encode and decode functions.

Copied from: http://stackoverflow.com/questions/1181919/python-base-36-encoding
"""

def encode(number, alphabet='0123456789abcdefghijklmnopqrstuvwxyz'):
    """
    Converts an integer to a Base36 string.
    """

    if not isinstance(number, (int, long)):
        raise TypeError('Input must be an integer')

    base36 = ''
    sign = ''

    if number < 0:
        sign = '-'
        number = -number

    if 0 <= number < len(alphabet):
        return sign + alphabet[number]

    while number != 0:
        number, i = divmod(number, len(alphabet))
        base36 = alphabet[i] + base36

    return sign + base36

def decode(number):
    """
    Convert an Base36 string to integer.
    """

    return int(number, 36)

