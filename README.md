DESCRIPTION
-----------
**WindowsAzureBlobFS** is a [Dokan](http://dokan-dev.net/en) file system provider for Windows Azure blob storage accounts.

USAGE
-----
To use it, install [Dokan](http://dokan-dev.net/en/download/#dokan) and run
`BlobMount <mount-point> <account> <key>`. E.g. `BlobMount w:\ myaccount EbhC5+NTN...==`

Top-level directories under the mount point are containers in blob storage. Lower-level directories are blob prefixes ('/'-delimited).
Files are blobs.

**NOTE:** Because Windows Azure blob storage doesn't have a notion of explicit "directories," empty directories are currently
ephemeral. They don't exist in blob storage, but only in memory for the duration of the mounted drive. That means any empty directories
will vanish when the drive is unmounted. It may be better to do something like persist the "empty directories" via blobs with metadata
(IsEmptyDirectory=true).

SCARY STATUS
------------
Plenty of work left. See all the TODOs in the code. :-) This is alpha code and probably works for a single user editing some blobs. Don't hold me
responsible for data loss... have a backup for sure. Lots of places in this code only implement the paths I've tested. Things like
seeking within a file during a write may totally destroy data. Consider yourself warned.