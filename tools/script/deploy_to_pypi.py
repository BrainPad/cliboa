#!/usr/bin/env python3
# -*- coding utf-8 -*-

import sys

import pexpect

args = sys.argv
pypi_username = args[1]
pypi_password = args[2]
is_test = args[3]
pypi_repository = "testpypi" if is_test else "pypi"
upload_command = "python3 -m twine upload --repository " + pypi_repository + " dist/*"
proc = pexpect.spawn(upload_command, encoding="utf-8")
proc.logfile = open("/tmp/debug", "w")  # debug
proc.expect("Enter your username: ")
proc.sendline(pypi_username)
proc.expect("Enter your password: ")
proc.sendline(pypi_password)
proc.expect(pexpect.EOF)
proc.close()
