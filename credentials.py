#!/bin/python

class Credentials():	
    def __init__(self, username="admin", password="admin"):
        self.username = username
        self.password = password

if __name__ == "__main__":
	print ("Checking if credential are correctly created ...")
	c = Credentials()
	assert c.username == "admin"
	assert c.password == "admin"

	d = Credentials("user","pass")
	assert d.username == "user"
	assert d.password == "pass"
	assert c.username == "admin"
	assert c.password == "admin"
	print ("Ok")
