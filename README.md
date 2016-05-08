# ModSync
***Reinventing the wheel, poorly***

Because of the now defunct Tamriel Online, I got the idea in my head to create a utility to sync Skyrim mod configurations between computers. I wrote this utility that makes use (poorly) of a MongoDB database and an FTP server to sync files/folders.

## To Use

Requires pymongo

Running the program once creates a 'config.ini' file. In there you can set a bunch of settings. It should all be straightforward.

To update the server with your current configuration, run with the -update command.

## How does it work?
I see there are no comments in your code, what does it do?

When run with the update flag the program will scan through the supplied folder tree and create a BSON object with an entry for each file where the key is the file location and the value is the SHA1 hash of the file. It will then do this for the loose files in a new BSON object. The program will then attempt to upload the files that have been changed, if any, to the FTP server. Then the program will push the two BSON objects to the database.

When run normally the program will grab the two BSON objects from the database and check each entry against the local files and download the files from the FTP server if necessary. There is also an option to download the files from a website.

## Does it work?
Sort've. Now that I think about it it probably doesn't work if you don't supply any loose files to download. Nor will the program work if the uploader stores their file tree in say, C:\Mods, and another user stores their file tree in F:\MegaMods. My first inclination to fix this is having some sort've user-defined name for the file tree\loose files so the uploader could say, upload "Main Mod Tree" starting from C:\Mods and the second user could download the files by saying, download "Main Mod Tree" starting from F:\MegaMods. If I had my druthers I would also add support for multiple file trees. I probably am also not effectively using the MongoDB. I should check what the best use cases are for it, for example fetching many objects from the database versus one large object.

## Isn't this basically Dropbox/Google Drive/ownCloud, but worse?
Yes.

## Why are you including some strange FTP library?
Because there is a bug in the python provided FTP library where it wont work with Microsoft FTP servers with SSL. [See](http://www.sami-lehtinen.net/blog/python-32-ms-ftps-ssl-tls-lockup-fix), I basically just implemented that guy's fix.
