# We need pymongo to interact with the MongoDB
from pymongo import MongoClient
from os.path import isfile, join, isdir, commonprefix, relpath
from shutil import copyfile
from os import makedirs
from urllib.request import urlretrieve
from urllib.parse import quote
#FTP Issues re: http://www.sami-lehtinen.net/blog/python-32-ms-ftps-ssl-tls-lockup-fix
from pathlib import Path
import hashlib, sys, os, configparser

#Method that downloads the files from the FTP or web server according to the config.
#Arguments: None
#Returns: Nothing
def download():
	#Need to read some information from the config, specifically the location of the file tree,
	#any loose files, and whether to use an FTP or web server to download the files.
	config = configparser.ConfigParser()
	config.read('config.ini')
	client = MongoClient(config['Database Credentials']['Host'], config['Database Credentials'].getint('Port'))
	#The table where the information is located has to be called 'skyrim' unless you change every instance of this.
	client.skyrim.authenticate(config['Database Credentials']['Login'], config['Database Credentials']['Password'])
	db = client[config['Database Variables']['Database Name']]
	collection = db[config['Database Variables']['Collection Name']]

	#Grabs the tree BSON object from the DB, if it can't it will create a BSON object that matches its search.
	#Probably not best case in the instance of wanting a user with only read-access to the database. Can likely
	#just remove push.
	treeFiles = collection.find_one({'type' : 'tree'})
	if treeFiles is None:
		collection.insert_one({'type' : 'tree'})
		treeFiles = {'type' : 'tree'}
	#Does the same as above except with the loose files BSON.
	looseFiles = collection.find_one({'type' : 'loose'})
	if looseFiles is None:
		collection.insert_one({'type' : 'loose'})
		looseFiles = {'type' : 'loose'}

	#Reads from the config to determine whether or not to use the web server settings or FTP server.
	preferHTTP = config['HTTP'].getboolean('Prefer')
	if preferHTTP:
		#Iterates through every pair in the dict that is the BSON object of treefiles and disregards the MongoDB
		#id key and the type key which I implemented as part of my BSON schema to determine whether the BSON
		#object was for loose files or tree files.
		for file in treeFiles:
			if (file != "_id") and (file != 'type'):
				#Obtains a relative file path for the pair. E.g. data/bears.txt
				#BSON objects can't have periods in their key so I replaced the period with a sequence of characters
				#that probably shouldn't ever occur. Not best practice I'm sure.
				relpath = file.replace("#$#", ".")
				#Joins the relative file path and the base path given in the config. E.g. C:/Skyrim/ + data/bears = C:/Skyrim/data/bears.txt
				joinPath = join(config['File System']['Tree'], relpath)
				#I allow for a 'USERNAME' keyword in the config that will replace an instance of 'USERNAME' in the path.
				#This allows for C:/Users/Dave/bears.txt to be translated to C:/Users/Ian/bears.txt on another client.
				joinPath = joinPath.replace('USERNAME', config['File System']['Username'])
				#Checks if the file exists or if the file hash does not match the file has from the pair, true would mean we need to changed
				#the destination file in some way.
				if not (isfile(joinPath)) or ((calculateFileHash(joinPath)) != treeFiles[file]):
					#0 is the hash for files that need to be deleted.
					if treeFiles[file] != '0':
						#If the file does not need to be deleted and either doesn't exist or doesn't match the hash, we need to download it.
						#First we create the path to where the file should be.
						#os.path.split(something)[0] takes the head of a path, e.g. C:/Skyrim/data/ where [1] would be bears.txt
						#Used to prevent creating a directory bears.txt in C:/Skyrim/data
						if not os.path.exists(os.path.split(joinPath)[0]):
							os.makedirs(os.path.split(joinPath)[0])
						#Downloads the actual file. Files in the tree subdirectory are part of the file tree.
						urlretrieve(config['HTTP']['url base'] + '/tree/' + quote(relpath), joinPath)
					else:
						#If the hash of the file is 0 and the file exists on the local system we delete it.
						if isfile(joinPath):
							os.remove(joinPath)
		#Iterates through every pair in the dict that is the BSON object of loosefiles and disregards the MongoDB
		#id key and the type key which I implemented as part of my BSON schema to determine whether the BSON
		#object was for loose files or tree files.
		for file in looseFiles:
			if (file != "_id") and (file != 'type'):
				#Obtains a full file path for the pair. E.g. C:/Skyrim/data/bears.txt
				#BSON objects can't have periods in their key so I replaced the period with a sequence of characters
				#that probably shouldn't ever occur. Not best practice I'm sure.
				formatted = file.replace("#$#", ".")
				remoteFormatted = formatted
				#I allow for a 'USERNAME' keyword in the config that will replace an instance of 'USERNAME' in the path.
				#This allows for C:/Users/Dave/bears.txt to be translated to C:/Users/Ian/bears.txt on another client.
				formatted = formatted.replace('USERNAME', config['File System']['Username'])
				#Checks if the file exists or if the file hash does not match the file has from the pair, true would mean we need to changed
				#the destination file in some way.
				if not (isfile(formatted)) or ((calculateFileHash(formatted)) != looseFiles[file]):
					#0 is the hash for files that need to be deleted.
					if looseFiles[file] != '0':
						#If the file does not need to be deleted and either doesn't exist or doesn't match the hash, we need to download it.
						#First we create the path to where the file should be.
						#os.path.split(something)[0] takes the head of a path, e.g. C:/Skyrim/data/ where [1] would be bears.txt
						#Used to prevent creating a directory bears.txt in C:/Skyrim/data
						if not os.path.exists(os.path.split(formatted)[0]):
							os.makedirs(os.path.split(formatted)[0])
						#Downloads the actual file. Files in the loose subdirectory are loose files.
						urlretrieve(config['HTTP']['url base'] + '/loose/' + quote(remoteFormatted.replace(':\\', '/')), formatted)
					else:
						#If the hash of the file is 0 and the file exists on the local system we delete it.
						if isfile(formatted):
							os.remove(formatted)

	else:
		#Determines whether or not FTPS will be attempted.
		ssl = config['FTP'].getboolean('ssl')

		#Constructs the FTP object accordingly.
		if ssl:
			#Constructs an FTP_TLS object if FTPS is required given the username and password from the config.
			ftp = FTP_TLS(config['FTP']['URL'])
			ftp.auth()
			ftp.login(config['FTP']['Login'], config['FTP']['Password'])
			ftp.prot_p()
		else:
			#Constructs an FTP object if FTP is required given the username and password from the config.
			ftp = FTP(config['FTP']['URL'])
			ftp.login(config['FTP']['Login'], config['FTP']['Password'])

		#The base directory of the FTP server is some directory that all the files will go in to, rather than clutter up the base FTP
		#directory. Not good if you don't have write access.
		baseDirectory = config['FTP']['Directory']
		#Creates the base directory if it doesn't exist.
		if baseDirectory not in ftp.nlst():
			ftp.mkd(baseDirectory)
		#Navigates to the base directory.
		ftp.cwd(baseDirectory)
		#Navigates to the tree directory or creates it if it does not exist to then download all the appropriate files.
		#Doesn't fail elegantly if you don't have write access.
		if 'tree' not in ftp.nlst():
			ftp.mkd('tree')
		ftp.cwd('tree')

		#Iterates through every pair in the dict that is the BSON object of treefiles and disregards the MongoDB
		#id key and the type key which I implemented as part of my BSON schema to determine whether the BSON
		#object was for loose files or tree files.
		for file in treeFiles:
			if (file != "_id") and (file != 'type'):
				#Obtains a relative file path for the pair. E.g. data/bears.txt
				#BSON objects can't have periods in their key so I replaced the period with a sequence of characters
				#that probably shouldn't ever occur. Not best practice I'm sure.
				relpath = file.replace("#$#", ".")
				#Joins the relative file path and the base path given in the config. E.g. C:/Skyrim/ + data/bears = C:/Skyrim/data/bears.txt
				joinPath = join(config['File System']['Tree'], relpath)
				#I allow for a 'USERNAME' keyword in the config that will replace an instance of 'USERNAME' in the path.
				#This allows for C:/Users/Dave/bears.txt to be translated to C:/Users/Ian/bears.txt on another client.
				joinPath = joinPath.replace('USERNAME', config['File System']['Username'])
				#Checks if the file exists or if the file hash does not match the file has from the pair, true would mean we need to changed
				#the destination file in some way.
				if not (isfile(joinPath)) or ((calculateFileHash(joinPath)) != treeFiles[file]):
					#0 is the hash for files that need to be deleted.
					if treeFiles[file] != '0':
						#If the file does not need to be deleted and either doesn't exist or doesn't match the hash, we need to download it.
						#First we create the path to where the file should be.
						#os.path.split(something)[0] takes the head of a path, e.g. C:/Skyrim/data/ where [1] would be bears.txt
						#Used to prevent creating a directory bears.txt in C:/Skyrim/data
						if not os.path.exists(os.path.split(joinPath)[0]):
							os.makedirs(os.path.split(joinPath)[0])
						#The path object allows us to iterate over a list of the directories that the relative path is comprised of.
						#E.g. Skyrim -> data -> Textures, etc. We use this ability to change our FTP directory to the next folder in
						#the chain. Creates the folder if it does not exist. Also, likely a problem if your FTP account only has read
						#access.
						p = Path(relpath)
						for d in p.parts[:-1]:
							if d not in ftp.nlst():
								ftp.mkd(d)
							ftp.cwd(d)
						#Retrieves the file from the FTP server to its location on the local system.
						ftp.retrbinary('RETR ' + p.parts[-1], open(joinPath, 'wb').write)
						#Navigates back to the tree directory in the base directory given in the config.
						for a in range(len(p.parts[:-1])):
							ftp.cwd("..")
					else:
						#If the hash of the file is 0 and the file exists on the local system we delete it.
						if isfile(joinPath):
							os.remove(joinPath)

		#We want to nagivate back up to the base directory given in the config to then navigate to loose where the loose
		#files are stored. Again, makes the folder if it does not exist. Probably not the best.
		ftp.cwd('..')
		if 'loose' not in ftp.nlst():
			ftp.mkd('loose')
		ftp.cwd('loose')

		#Iterates through every pair in the dict that is the BSON object of loosefiles and disregards the MongoDB
		#id key and the type key which I implemented as part of my BSON schema to determine whether the BSON
		#object was for loose files or tree files.
		for file in looseFiles:
			if (file != "_id") and (file != 'type'):
				#Obtains a relative file path for the pair. E.g. data/bears.txt
				#BSON objects can't have periods in their key so I replaced the period with a sequence of characters
				#that probably shouldn't ever occur. Not best practice I'm sure.
				path = file.replace("#$#", '.')
				remotepath = path
				#I allow for a 'USERNAME' keyword in the config that will replace an instance of 'USERNAME' in the path.
				#This allows for C:/Users/Dave/bears.txt to be translated to C:/Users/Ian/bears.txt on another client.
				path = path.replace('USERNAME', config['File System']['Username'])
				#Checks if the file exists or if the file hash does not match the file has from the pair, true would mean we need to changed
				#the destination file in some way.
				if not (isfile(path)) or ((calculateFileHash(path)) != looseFiles[file]):
					#0 is the hash for files that need to be deleted.
					if looseFiles[file] != '0':
						#If the file does not need to be deleted and either doesn't exist or doesn't match the hash, we need to download it.
						#First we create the path to where the file should be.
						#os.path.split(something)[0] takes the head of a path, e.g. C:/Skyrim/data/ where [1] would be bears.txt
						#Used to prevent creating a directory bears.txt in C:/Skyrim/data
						if not os.path.exists(os.path.split(path)[0]):
							os.makedirs(os.path.split(path)[0])
						#The path object allows us to iterate over a list of the directories that the relative path is comprised of.
						#E.g. Skyrim -> data -> Textures, etc. We use this ability to change our FTP directory to the next folder in
						#the chain. Creates the folder if it does not exist. Also, likely a problem if your FTP account only has read
						#access.
						p = Path(remotepath)
						for d in p.parts[:-1]:
							#Changes the Drive:\\ directory to just Drive, e.g. C:\\ to C
							d = d.replace(':\\', '')
							if d not in ftp.nlst():
								ftp.mkd(d)
							ftp.cwd(d)
						#Retrieves the file from the FTP server to its location on the local system.
						ftp.retrbinary('RETR ' + p.parts[-1], open(path, 'wb').write)
						#Navigates back to the loose directory in the base directory given in the config.
						for a in range(len(p.parts[:-1])):
							ftp.cwd('..')
					else:
						#If the hash of the file is 0 and the file exists on the local system we delete it.
						if isfile(path):
							os.remove(path)

#Arguments: Tree navigation location and a document.
#Returns: Nothing
#Navigates through the walk location and all its subfolders adding each file it encounters to
#the document to be converted into a BSON object and uploaded.
def addTreeFilesToDocument(walk_location, document):
	for root, subdirs, files in os.walk(walk_location):
		for file in files:
			#Removes the walk location part from the absolute path of the file location. E.g. removing C:\Skyrim\ from C:\Skyrim\data\bears.txt
			#Leaving data\bears.txt, our relative path.
			relative_path = relpath(join(root, file), walk_location)
			#Assigns that formatted key the bears.txt SHA1 hash.
			document[relative_path.replace(".", "#$#")] = calculateFileHash(join(root, file))

#Arguments: A document
#Returns: Nothing
#For every loose file in the config adds its location and hash to the document to be converted to BSON
#and uploaded.
def addLooseFilesToDocument(document):
	#Read the config for loose files
	config = configparser.ConfigParser()
	config.read('config.ini')
	#Iterates through the loose files in the config
	for file in config['File System']['Loose'].split(','):
		#Formats the key
		formatted = file.replace('.', '#$#')
		#Replaces USERNAME in loose file config with local username
		file = file.replace('USERNAME', config['File System']['Username'])
		#Sets the value for the loose file path key to the hash of the loose file.
		document[formatted] = calculateFileHash(file)

#TODO: Comment this
def pushDocument(treeDocument, looseDocument):
	config = configparser.ConfigParser()
	config.read('config.ini')
	client = MongoClient(config['Database Credentials']['Host'], config['Database Credentials'].getint('Port'))
	client.skyrim.authenticate(config['Database Credentials']['Login'], config['Database Credentials']['Password'])
	db = client[config['Database Variables']['Database Name']]
	collection = db[config['Database Variables']['Collection Name']]

	serverTreeDocument = collection.find_one({'type' : 'tree'})
	if serverTreeDocument is None:
		collection.insert_one({'type' : 'tree'})
		serverTreeDocument = {'type' : 'tree'}
	serverLooseDocument = collection.find_one({'type' : 'loose'})
	if serverLooseDocument is None:
		collection.insert_one({'type' : 'loose'})
		serverLooseDocument = {'type' : 'loose'}

	#Want to create a document based off of what we've calculated and what we've
	#already got on the server. From this we want to upload the file if there is
	#a change or delete the file if the new hash is 0.

	treeAdditions = {}
	for key in treeDocument:
		if (key not in serverTreeDocument) or (serverTreeDocument[key] != treeDocument[key]):
			treeAdditions[key] = treeDocument[key]

	treeRemovals = {}
	for key in serverTreeDocument:
		if (key not in treeDocument) and (key != "_id") and (key != 'type'):
			treeRemovals[key] = '0'

	serverTreeDocument.update(treeAdditions)
	serverTreeDocument.update(treeRemovals)

	looseAdditions = {}
	for key in looseDocument:
		if (key not in serverLooseDocument) or (serverLooseDocument[key] != looseDocument[key]):
			looseAdditions[key] = looseDocument[key]

	looseRemovals = {}
	for key in serverLooseDocument:
		if (key not in looseDocument) and (key != "_id") and (key != 'type'):
			looseRemovals[key] = '0'

	serverLooseDocument.update(looseAdditions)
	serverLooseDocument.update(looseRemovals)

	ssl = config['FTP'].getboolean('ssl')

	if ssl:
		ftp = FTP_TLS(config['FTP']['URL'])
		ftp.auth()
		ftp.login(config['FTP']['Login'], config['FTP']['Password'])
		ftp.prot_p()

	else:
		ftp = FTP(config['FTP']['URL'])
		ftp.login(config['FTP']['Login'], config['FTP']['Password'])

	baseDirectory = config['FTP']['Directory']
	if baseDirectory not in ftp.nlst():
		ftp.mkd(baseDirectory)
	ftp.cwd(baseDirectory)
	if 'tree' not in ftp.nlst():
		ftp.mkd('tree')
	ftp.cwd('tree')

	for key in treeAdditions:
		formatted = key.replace('#$#', '.')
		p = Path(formatted)
		for d in p.parts[:-1]:
			if d not in ftp.nlst():
				ftp.mkd(d)
			ftp.cwd(d)
		ftp.storbinary('STOR ' + p.parts[-1], open(os.path.join(config['File System']['Tree'], formatted), 'rb'))
		for a in range(len(p.parts[:-1])):
			ftp.cwd("..")

	for key in treeRemovals:
		formatted = key.replace('#$#', '.')
		p = Path(formatted)
		for d in p.parts[:-1]:
			if d not in ftp.nlst():
				ftp.mkd(d)
			ftp.cwd(d)
		if p.parts[-1] in ftp.nlst():
			ftp.delete(p.parts[-1])
		for a in range(len(p.parts[:-1])):
			ftp.cwd("..")

	ftp.cwd('..')
	if 'loose' not in ftp.nlst():
		ftp.mkd('loose')
	ftp.cwd('loose')

	for key in looseAdditions:
		formatted = key.replace('#$#', '.')
		localFormatted = formatted.replace('USERNAME', config['File System']['Username'])
		p = Path(formatted)
		for d in p.parts[:-1]:
			d = d.replace(':\\', '')
			if d not in ftp.nlst():
				ftp.mkd(d)
			ftp.cwd(d)
		ftp.storbinary('STOR ' + p.parts[-1], open(localFormatted, 'rb'))
		for a in range(len(p.parts[:-1])):
			ftp.cwd("..")

	for key in looseRemovals:
		formatted = key.replace('#$#', '.')
		p = Path(formatted)
		for d in p.parts[:-1]:
			d = d.replace(':\\', '')
			if d not in ftp.nlst():
				ftp.mkd(d)
			ftp.cwd(d)
		if p.parts[-1] in ftp.nlst():
			ftp.delete(p.parts[-1])
		for a in range(len(p.parts[:-1])):
			ftp.cwd("..")

	collection.replace_one({'type' : 'tree'}, serverTreeDocument)
	collection.replace_one({'type' : 'loose'}, serverLooseDocument)

	client.close()
	ftp.close()

#Calculutes the SHA1 hash for a file. I'm pretty sure I got this off Stack Overflow.
def calculateFileHash(file):
	BLOCKSIZE = 65536
	hasher = hashlib.sha1()
	with open(file, 'rb') as afile:
		buf = afile.read(BLOCKSIZE)
		while len(buf) > 0:
			hasher.update(buf)
			buf = afile.read(BLOCKSIZE)
	return(hasher.hexdigest())


def main():
	config = configparser.ConfigParser(allow_no_value = True)
	if not isfile('config.ini'):
		config['HTTP'] = {'URL Base' : 'http://example.com/', 'Prefer' : 'true', '# Set prefer to false if you\'d like to download files via FTP' : None}
		config['FTP'] = {'URL' : 'http://ftp.example.com/', 'Login' : 'test', 'Password' : 'test', 'Directory' : 'files', 'SSL' : 'true'}
		config['Database Credentials'] = {'Host' : 'http://example.com/', 'Port' : '9000', 'Login' : 'test', 'Password' : 'test'}
		config['Database Variables'] = {'Database Name' : 'skyrim', 'Collection Name' : 'mods'}
		config['File System'] = {'Tree' : 'C:\\test', 'Loose' : 'C:\test.txt, C:\test2.txt', 'Username' : 'test'}
		with open('config.ini', 'w') as configfile:
			config.write(configfile)
	config.read('config.ini')
	if config['HTTP']['URL Base'] == 'http://example.com/':
		print('Update the config.ini file in the current directory!')
		sys.exit(0)

	treeDocument = {'type' : 'tree'}
	looseDocument = {'type' : 'loose'}
	if len(sys.argv) == 1 or (sys.argv[1].lower() != '-update'):
		download()
	else:
		addTreeFilesToDocument(config['File System']['Tree'], treeDocument)
		addLooseFilesToDocument(looseDocument)
		pushDocument(treeDocument, looseDocument)

if __name__ == "__main__":
	main()
