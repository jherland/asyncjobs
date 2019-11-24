#!/usr/bin/env python3

import functools
import itertools
import logging
import re
import sys


class AnsiColors:
    Codes = {
        'black': '\033[0;30m',
        'gray': '\033[1;30m',
        'maroon': '\033[0;31m',
        'red': '\033[1;31m',
        'green': '\033[0;32m',
        'lime': '\033[1;32m',
        'brown': '\033[0;33m',
        'yellow': '\033[1;33m',
        'navy': '\033[0;34m',
        'blue': '\033[1;34m',
        'purple': '\033[0;35m',
        'pink': '\033[1;35m',
        'teal': '\033[0;36m',
        'cyan': '\033[1;36m',
        'silver': '\033[0;37m',
        'white': '\033[1;37m',
        'none': '\033[0m',
        'reset': '\033[0m',
    }

    # List of colors that "stand out" from the default gray/silver
    VisibleColors = [
        'maroon',
        'green',
        'brown',
        'navy',
        'purple',
        'teal',
        'red',
        'lime',
        'yellow',
        'blue',
        'pink',
        'cyan',
    ]

    enabled = sys.stdout.isatty()

    def __init__(self, enabled=None):
        if enabled is not None:
            self.enabled = enabled

        # Provide no-op codes when coloring is disabled
        if not self.enabled:
            self.Codes = dict.fromkeys(self.__class__.Codes.keys(), '')

    @staticmethod
    def strip(s):
        """Return a copy of 's' without ANSI-codes."""
        # Collapse any character sequence that starts with '\033[' and ends
        # with 'm'.
        return re.sub(r'\033\[.*?m', '', s)

    @classmethod
    def display_len(cls, s):
        """Return number of printable/displayed characters in the given string.

        When outputting ANSI-colored strings, the len(s) of a string 's' does
        not correspond to the displayed length in the output. This method
        returns the total length of the string after removing any ANSI-codes
        found in 's', i.e. the number of characters that take up space in the
        displayed output.
        """
        return len(cls.strip(s))

    def embed(self, color, s, nostrip=False):
        assert color in self.Codes
        if self.enabled:
            if not nostrip:
                s = self.strip(s)
            return self.Codes[color] + s + self.Codes['reset']
        else:
            return s

    def roundrobin_color(self):
        """Generate callables for embedding strings in different colors.

        Each item returned from this iterator is a function that colorizes its
        input in one color from VisibleColors. The first item colors all input
        strings in 'maroon', the next item colors all input strings in 'green',
        and so on.

        This is useful when you want to group things by colorizing them, but
        the actual color used is not that important.
        """
        for color in itertools.cycle(self.VisibleColors):
            yield functools.partial(self.embed, color)


# Provide methods for each of the color names in Codes
for color in AnsiColors.Codes.keys():
    setattr(
        AnsiColors, color, functools.partialmethod(AnsiColors.embed, color)
    )


class AnsiColorFormatter(logging.Formatter):
    LevelColors = {
        logging.DEBUG: 'gray',
        logging.INFO: 'teal',
        logging.WARNING: 'yellow',
        logging.ERROR: 'red',
        logging.FATAL: 'maroon',
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ansi = AnsiColors()

    def format(self, r):
        color = self.LevelColors.get(r.levelno, 'none')
        return self.ansi.embed(color, super().format(r), nostrip=True)


if __name__ == '__main__':
    ansi = AnsiColors()
    print(ansi.gray('This is gray!'))
    print('regular before >>', ansi.red('This is red!'), '<< regular after')
    print(ansi.embed('green', 'This is green!'))
    print(ansi.reset('This is regular!'))
    print()

    for color in ansi.Codes:
        print(ansi.embed(color, 'This line is colored {!r}'.format(color)))
    print()

    logger = logging.getLogger('test')
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(AnsiColorFormatter(fmt=logging.BASIC_FORMAT))
    logger.addHandler(handler)

    logger.debug('This is debug')
    logger.info('This is info')
    logger.warning('This is warning')
    logger.error('This is error')
    logger.fatal('This is fatal')

    red = ansi.red('red')
    logger.info(
        '"{}" has len() == {}, but has display_len() == {}'.format(
            red, len(red), ansi.display_len(red)
        )
    )
