from pymongo import MongoClient
from os.path import isfile, join, isdir, commonprefix, relpath
from shutil import copyfile
from os import makedirs
from urllib.request import urlretrieve
from urllib.parse import quote
from myftplib import FTP, FTP_TLS
from pathlib import Path
import hashlib, sys, os, configparser

def download(destination):
	config = configparser.ConfigParser()
	config.read('config.ini')
	client = MongoClient(config['Database Credentials']['Host'], config['Database Credentials'].getint('Port'))
	client.skyrim.authenticate(config['Database Credentials']['Login'], config['Database Credentials']['Password'])
	db = client[config['Database Variables']['Database Name']]
	collection = db[config['Database Variables']['Collection Name']]

	treeFiles = collection.find_one({'type' : 'tree'})
	if treeFiles is None:
		collection.insert_one({'type' : 'tree'})
		treeFiles = {'type' : 'tree'}
	looseFiles = collection.find_one({'type' : 'loose'})
	if looseFiles is None:
		collection.insert_one({'type' : 'loose'})
		looseFiles = {'type' : 'loose'}

	preferHTTP = config['HTTP'].getboolean('Prefer')
	if preferHTTP:
		for file in treeFiles:
			if (file != "_id") and (file != 'type'):
				relpath = file.replace("#$#", ".")
				joinPath = join(config['File System']['Tree'], relpath)
				joinPath = joinPath.replace('USERNAME', config['File System']['Username'])
				if not (isfile(joinPath)) or ((calculateFileHash(joinPath)) != treeFiles[file]):
					if treeFiles[file] != '0':
						if not os.path.exists(os.path.split(joinPath)[0]):
							os.makedirs(os.path.split(joinPath)[0])
						urlretrieve(config['HTTP']['url base'] + '/tree/' + quote(relpath), joinPath)
					else:
						if isfile(joinPath):
							os.remove(joinPath)
		for file in looseFiles:
			if (file != "_id") and (file != 'type'):
				formatted = file.replace("#$#", ".")
				remoteFormatted = formatted
				formatted = formatted.replace('USERNAME', config['File System']['Username'])
				if not (isfile(formatted)) or ((calculateFileHash(formatted)) != looseFiles[file]):
					if looseFiles[file] != '0':
						if not os.path.exists(os.path.split(formatted)[0]):
							os.makedirs(os.path.split(formatted)[0])
						urlretrieve(config['HTTP']['url base'] + '/loose/' + quote(remoteFormatted.replace(':\\', '/')), formatted)
					else:
						if isfile(formatted):
							os.remove(formatted)

	else:
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

		#Iterate through tree files
		for file in treeFiles:
			if (file != "_id") and (file != 'type'):
				relpath = file.replace("#$#", ".")
				joinPath = join(config['File System']['Tree'], relpath)
				if not (isfile(joinPath)) or ((calculateFileHash(joinPath)) != treeFiles[file]):
					if treeFiles[file] != '0':
						p = Path(relpath)
						if not os.path.exists(os.path.split(joinPath)[0]):
							os.makedirs(os.path.split(joinPath)[0])
						for d in p.parts[:-1]:
							if d not in ftp.nlst():
								ftp.mkd(d)
							ftp.cwd(d)
						ftp.retrbinary('RETR ' + p.parts[-1], open(joinPath, 'wb').write)
						for a in range(len(p.parts[:-1])):
							ftp.cwd("..")
					else:
						if isfile(joinPath):
							os.remove(joinPath)

		ftp.cwd('..')
		if 'loose' not in ftp.nlst():
			ftp.mkd('loose')
		ftp.cwd('loose')

		#Iterate through loose files
		for file in looseFiles:
			if (file != "_id") and (file != 'type'):
				path = file.replace("#$#", '.')
				remotepath = path
				path = path.replace('USERNAME', config['File System']['Username'])
				if not (isfile(path)) or ((calculateFileHash(path)) != looseFiles[file]):
					if looseFiles[file] != '0':
						p = Path(remotepath)
						if not os.path.exists(os.path.split(path)[0]):
							os.makedirs(os.path.split(path)[0])
						for d in p.parts[:-1]:
							d = d.replace(':\\', '')
							if d not in ftp.nlst():
								ftp.mkd(d)
							ftp.cwd(d)
						ftp.retrbinary('RETR ' + p.parts[-1], open(path, 'wb').write)
						for a in range(len(p.parts[:-1])):
							ftp.cwd('..')
					else:
						if isfile(path):
							os.remove(path)
def addTreeFilesToDocument(walk_location, document):
	for root, subdirs, files in os.walk(walk_location):
		for file in files:
			relative_path = relpath(join(root, file), walk_location)
			document[relative_path.replace(".", "#$#")] = calculateFileHash(join(root, file))

def addLooseFilesToDocument(document):
	config = configparser.ConfigParser()
	config.read('config.ini')
	for file in config['File System']['Loose'].split(','):
		formatted = file.replace('.', '#$#')
		file = file.replace('USERNAME', config['File System']['Username'])
		document[formatted] = calculateFileHash(file)

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
		download(config['File System']['Tree'])
	else:
		addTreeFilesToDocument(config['File System']['Tree'], treeDocument)
		addLooseFilesToDocument(looseDocument)
		pushDocument(treeDocument, looseDocument)

if __name__ == "__main__":
	main()
