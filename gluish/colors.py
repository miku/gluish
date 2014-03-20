# coding: utf-8

"""
Color helpers.
"""

from colorama import Fore, Back, Style


def dim(text):
    """ Dim text. """
    return Back.WHITE + Fore.BLACK + text + Fore.RESET + Back.RESET

def green(text):
    """ Green text. """
    return Fore.GREEN + text + Fore.RESET

def red(text):
    """ Red text. """
    return Fore.RED + text + Fore.RESET

def yellow(text):
    """ Yellow text. """
    return Fore.YELLOW + text + Fore.RESET

def cyan(text):
    """ Cyan text. """
    return Fore.CYAN + text + Fore.RESET

def magenta(text):
    """ Magenta text. """
    return Fore.MAGENTA + text + Fore.RESET
