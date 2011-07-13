// DESCRIPTION
// -----------
// **WindowsAzureBlobFS** is a [Dokan](http://dokan-dev.net/en) file system provider for Windows Azure blob storage accounts.
// Full source code is at [http://github.com/smarx/BlobMount](http://github.com/smarx/BlobMount).

// USAGE
// -----
// To use it, install [Dokan](http://dokan-dev.net/en/download/#dokan) and run
// `BlobMount <mount-point> <account> <key>`. E.g. `BlobMount w:\ myaccount EbhC5+NTN...==`

// Top-level directories under the mount point are containers in blob storage. Lower-level directories are blob prefixes ('/'-delimited).
// Files are blobs.

// **NOTE:** Because Windows Azure blob storage doesn't have a notion of explicit "directories," empty directories are currently
// ephemeral. They don't exist in blob storage, but only in memory for the duration of the mounted drive. That means any empty directories
// will vanish when the drive is unmounted. It may be better to do something like persist the "empty directories" via blobs with metadata
// (IsEmptyDirectory=true).

// SCARY STATUS
// ------------
// Plenty of work left. See all the TODOs in the code. :-) This is alpha code and probably works for a single user editing some blobs. Don't hold me
// responsible for data loss... have a backup for sure. Lots of places in this code only implement the paths I've tested. Things like
// seeking within a file during a write may totally destroy data. Consider yourself warned.
using System;
using System.Collections.Generic;
using System.Collections;
using System.Linq;
using System.Text;
using Dokan;
using System.IO;
using Microsoft.WindowsAzure.StorageClient;
using Microsoft.WindowsAzure;

namespace BlobMount
{
    public class WindowsAzureBlobFS : DokanOperations
    {
        // Mount takes the following arguments:
        //
        // * Mount point
        // * Storage account
        // * Storage key
        //
        // Translates Dokan error messages to friendly text.
        public static void Mount(string[] args)
        {
            switch (DokanNet.DokanMain(new DokanOptions
            {
                MountPoint = args[0],
                DebugMode = true,
                UseStdErr = true,
                VolumeLabel = "WindowsAzureBlobDrive"
            }, new WindowsAzureBlobFS(args[1], args[2])))
            {
                case DokanNet.DOKAN_DRIVE_LETTER_ERROR:
                    Console.WriteLine("Drive letter error");
                    break;
                case DokanNet.DOKAN_DRIVER_INSTALL_ERROR:
                    Console.WriteLine("Driver install error");
                    break;
                case DokanNet.DOKAN_MOUNT_ERROR:
                    Console.WriteLine("Mount error");
                    break;
                case DokanNet.DOKAN_START_ERROR:
                    Console.WriteLine("Start error");
                    break;
                case DokanNet.DOKAN_ERROR:
                    Console.WriteLine("Unknown error");
                    break;
                case DokanNet.DOKAN_SUCCESS:
                    Console.WriteLine("Success");
                    break;
                default:
                    Console.WriteLine("Unknown status");
                    break;
            }
        }

        private CloudBlobClient blobs;

        // Initialize with a cloud storage account and key.
        public WindowsAzureBlobFS(string account, string key)
        {
            blobs = new CloudStorageAccount(new StorageCredentialsAccountAndKey(account, key), false).CreateCloudBlobClient();
        }

        // Close the corresponding BlobStream, if any, to commit the changes.
        public int Cleanup(string filename, DokanFileInfo info)
        {
            if (readBlobs.ContainsKey(filename)) readBlobs[filename].Close();
            if (writeBlobs.ContainsKey(filename)) writeBlobs[filename].Close();
            return 0;
        }

        // Call close on the underlying BlobStream (if any) so writes are committed and buffered reads are discarded.
        public int CloseFile(string filename, DokanFileInfo info)
        {
            if (writeBlobs.ContainsKey(filename))
            {
                writeBlobs[filename].Close();
                writeBlobs.Remove(filename);
            }
            if (readBlobs.ContainsKey(filename))
            {
                readBlobs[filename].Close();
                readBlobs.Remove(filename);
            }
            return 0;
        }

        // Keep track of empty directories (in memory).
        private HashSet<string> emptyDirectories = new HashSet<string>();
        private IEnumerable<string> EnumeratePathAndParents(string directory)
        {
            var split = directory.Split(new [] { '\\' }, StringSplitOptions.RemoveEmptyEntries);
            var path = "\\" + split.FirstOrDefault() ?? "";
            foreach (var segment in split.Skip(1))
            {
                path += "\\" + segment;
                yield return path;
            }
        }
        private bool AddEmptyDirectories(string directory)
        {
            bool alreadyExisted = false;
            foreach (var path in EnumeratePathAndParents(directory))
            {
                alreadyExisted |= emptyDirectories.Add(path);
            }
            Console.WriteLine("emptyDirectories:\n\t" + string.Join("\n\t", emptyDirectories));
            return alreadyExisted;
        }
        private void RemoveEmptyDirectories(string directory)
        {
            foreach (var path in EnumeratePathAndParents(directory))
            {
                emptyDirectories.Remove(path);
            }
            Console.WriteLine("emptyDirectories:\n\t" + string.Join("\n\t", emptyDirectories));
        }

        // CreateDirectory creates a container if at the top level and otherwise creates an empty directory (tracked in memory only).
        public int CreateDirectory(string filename, DokanFileInfo info)
        {
            var split = filename.Split(new[] { Path.DirectorySeparatorChar }, StringSplitOptions.RemoveEmptyEntries);

            if (split.Length == 1)
            {
                try
                {
                    blobs.GetContainerReference(split[0]).Create();
                }
                catch (StorageClientException e)
                {
                    if (e.ErrorCode == StorageErrorCode.ContainerAlreadyExists)
                    {
                        // TODO: This doesn't seem to give the right error message when I try to "md" a container that already exists.
                        return -DokanNet.ERROR_ALREADY_EXISTS;
                    }
                    throw;
                }
                return 0;
            }
            else
            {
                // Use OpenDirectory as a way to test for existence of a directory.
                if (OpenDirectory(filename, info) == 0)
                {
                    return -DokanNet.ERROR_ALREADY_EXISTS;
                }
                else
                {
                    // Track the empty directory in memory.
                    if (!AddEmptyDirectories(filename))
                    {
                        return -DokanNet.ERROR_ALREADY_EXISTS;
                    }
                    else
                    {
                        return 0;
                    }
                }
            }
        }

        // Create file does nothing except validate that the file requested is okay to create.
        // Actual creation of a corresponding blob in cloud storage is done when the file is actually written.
        public int CreateFile(string filename, FileAccess access, FileShare share, FileMode mode, FileOptions options, DokanFileInfo info)
        {
            // When trying to open a file for reading, succeed only if the file already exists.
            if (mode == FileMode.Open && (access == FileAccess.Read || access == FileAccess.ReadWrite))
            {
                if (GetFileInformation(filename, new FileInformation(), new DokanFileInfo(0)) == 0)
                {
                    return 0;
                }
                else
                {
                    return -DokanNet.ERROR_FILE_NOT_FOUND;
                }
            }
            // When creating a file, always succeed. (Empty directories will be implicitly created as needed.)
            else if (mode == FileMode.Create || mode == FileMode.OpenOrCreate)
            {
                // Since we're creating a file, we don't need to track the parents (up the tree) as empty directories any longer.
                RemoveEmptyDirectories(Path.GetDirectoryName(filename));
                return 0;
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        // DeleteDirectory removes a container if it's at the root level, fails if a directory is not empty,
        // and removes a tracked empty directory if one exists.
        public int DeleteDirectory(string filename, DokanFileInfo info)
        {
            var split = filename.Trim('\\').Split(Path.DirectorySeparatorChar);
            if (split.Length == 1)
            {
                try
                {
                    blobs.GetContainerReference(split[0]).Delete();
                    return 0;
                }
                catch { return -1; }
            }
            if (blobs.ListBlobsWithPrefix(filename.Trim('\\').Replace('\\', '/')).Any())
            {
                // TODO: Revisit what a better error code might be.
                return -1;
            }
            else if (emptyDirectories.Any(f => f.StartsWith(filename + "\\")))
            {
                // TODO: Revisit what a better error code might be.
                return -1;
            }
            else
            {
                emptyDirectories.Remove(filename);
                return 0;
            }
        }

        // DeleteFile tries to delete the corresponding blob and ensures the parent directory is tracked as empty.
        public int DeleteFile(string filename, DokanFileInfo info)
        {
            try
            {
                blobs.GetBlobReference(filename.Trim('\\')).Delete();
                if (!blobs.ListBlobsWithPrefix(Path.GetDirectoryName(filename).Trim('\\').Replace('\\', '/')).Any())
                {
                    AddEmptyDirectories(Path.GetDirectoryName(filename));
                }
                return 0;
            }
            catch { return -1; }
        }

        // FindFiles enumerates blobs and blob prefixes and represents them as files and directories.
        public int FindFiles(string filename, ArrayList files, DokanFileInfo info)
        {
            if (filename == "\\")
            {
                files.AddRange(blobs.ListContainers().Select(c => new FileInformation
                {
                    FileName = c.Name,
                    Attributes = FileAttributes.Directory,
                    CreationTime = c.Properties.LastModifiedUtc,
                    LastAccessTime = c.Properties.LastModifiedUtc,
                    LastWriteTime = c.Properties.LastModifiedUtc
                }).ToList());
            }
            else
            {
                var split = filename.Trim('\\').Split(Path.DirectorySeparatorChar);
                var container = blobs.GetContainerReference(split[0]);
                IEnumerable<IListBlobItem> items =
                    split.Length > 1
                    ? container.GetDirectoryReference(string.Join("/", split.Skip(1).Take(split.Length - 1))).ListBlobs()
                    : container.ListBlobs();
                files.AddRange(items.Select(c => new FileInformation
                {
                    FileName = c.Uri.AbsolutePath.Substring(filename.Length + 1).TrimEnd('/'),
                    Attributes = (c is CloudBlobDirectory) ? FileAttributes.Directory : FileAttributes.Normal,
                    CreationTime = (c is CloudBlob) ? ((CloudBlob)c).Properties.LastModifiedUtc : DateTime.UtcNow,
                    LastAccessTime = (c is CloudBlob) ? ((CloudBlob)c).Properties.LastModifiedUtc : DateTime.UtcNow,
                    LastWriteTime = (c is CloudBlob) ? ((CloudBlob)c).Properties.LastModifiedUtc : DateTime.UtcNow,
                    Length = (c is CloudBlob) ? ((CloudBlob)c).Properties.Length : 0
                }).ToList());
                files.AddRange(emptyDirectories.Where(f => f.StartsWith(filename + "\\") && !f.Substring(filename.Length + 1).Contains("\\")).Select(f => new FileInformation
                {
                    FileName = f.Substring(filename.Length + 1),
                    Attributes = FileAttributes.Directory,
                    CreationTime = DateTime.UtcNow,
                    LastAccessTime = DateTime.UtcNow,
                    LastWriteTime = DateTime.UtcNow,
                    Length = 0
                }).ToList());
            }
            return 0;
        }

        public int FlushFileBuffers(string filename, DokanFileInfo info)
        {
            return 0;
        }

        // GetDiskFreeSpace returns hardcoded values.
        public int GetDiskFreeSpace(ref ulong freeBytesAvailable, ref ulong totalBytes, ref ulong totalFreeBytes, DokanFileInfo info)
        {
            freeBytesAvailable = 512 * 1024 * 1024;
            totalBytes = 1024 * 1024 * 1024;
            totalFreeBytes = 512 * 1024 * 1024;
            return 0;
        }

        // GetFileInformation returns information about a container at the top level, blob prefixes at lower levels,
        // and empty directories (tracked in memory). File times are all specified as `DateTime.UtcNow`.
        public int GetFileInformation(string filename, FileInformation fileinfo, DokanFileInfo info)
        {
            var split = filename.Split(new[] { Path.DirectorySeparatorChar }, StringSplitOptions.RemoveEmptyEntries);
            if (split.Length == 0)
            {
                fileinfo.Attributes = FileAttributes.Directory;
                fileinfo.CreationTime = DateTime.UtcNow;
                fileinfo.LastAccessTime = DateTime.UtcNow;
                fileinfo.LastWriteTime = DateTime.UtcNow;
                return 0;
            }
            if (split.Length == 1)
            {
                var container = blobs.ListContainers(split[0]).FirstOrDefault();
                if (container != null && container.Name == split[0])
                {
                    fileinfo.Attributes = FileAttributes.Directory;
                    fileinfo.CreationTime = DateTime.UtcNow;
                    fileinfo.LastAccessTime = DateTime.UtcNow;
                    fileinfo.LastWriteTime = DateTime.UtcNow;
                    return 0;
                }
                else
                {
                    return -1;
                }
            }
            var blob = blobs.GetBlobReference(filename.Trim('\\'));
            try
            {
                blob.FetchAttributes();
                fileinfo.CreationTime = blob.Properties.LastModifiedUtc;
                fileinfo.LastWriteTime = blob.Properties.LastModifiedUtc;
                fileinfo.LastAccessTime = DateTime.UtcNow;
                fileinfo.Length = blob.Properties.Length;
                return 0;
            }
            catch
            {
                if (emptyDirectories.Contains(filename) || blobs.ListBlobsWithPrefix(filename.Trim('\\').Replace('\\', '/')).Any())
                {
                    fileinfo.Attributes = FileAttributes.Directory;
                    fileinfo.CreationTime = DateTime.UtcNow;
                    fileinfo.LastAccessTime = DateTime.UtcNow;
                    fileinfo.LastWriteTime = DateTime.UtcNow;
                    return 0;
                }
                else
                {
                    return -DokanNet.ERROR_FILE_NOT_FOUND;
                }
            }
        }

        // TODO: Perhaps use leases on blobs to do locking?
        public int LockFile(string filename, long offset, long length, DokanFileInfo info)
        {
            return 0;
        }

        // TODO: Use copy and then delete to do an efficient move where possible.
        public int MoveFile(string filename, string newname, bool replace, DokanFileInfo info)
        {
            throw new NotImplementedException();
        }

        // OpenDirectory succeeds as long as the specified path is an empty directory (tracked in memory)
        // or a blob prefix containing blobs (discovered via GetFileInformation returning a success code with a directory).
        public int OpenDirectory(string filename, DokanFileInfo info)
        {
            if (emptyDirectories.Contains(filename)) return 0;
            var fileinfo = new FileInformation();
            var status = GetFileInformation(filename, fileinfo, info);
            if ((status == 0) && (fileinfo.Attributes == FileAttributes.Directory)) return 0;
            return -DokanNet.ERROR_FILE_NOT_FOUND;
        }

        // Keep track of open streams for reading blobs. (Done this way instead of making an HTTP call for each read
        // operation so that we can do read-ahead (significant perf gain for normal operations like reading an entire file).
        private Dictionary<string, BlobStream> readBlobs = new Dictionary<string, BlobStream>();
        public int ReadFile(string filename, byte[] buffer, ref uint readBytes, long offset, DokanFileInfo info)
        {
            if (!readBlobs.ContainsKey(filename))
            {
                readBlobs[filename] = blobs.GetBlobReference(filename.Trim('\\')).OpenRead();
            }
            readBlobs[filename].Position = offset;
            readBytes = (uint)readBlobs[filename].Read(buffer, 0, buffer.Length);
            return 0;
        }

        // TODO: Figure out what this is supposed to do and maybe do it. :)
        public int SetAllocationSize(string filename, long length, DokanFileInfo info)
        {
            return 0;
        }

        // TODO: This is presumably to truncate a file? This could be implemented in the future.
        public int SetEndOfFile(string filename, long length, DokanFileInfo info)
        {
            throw new NotImplementedException();
        }

        // TODO: Consider implementing this.
        public int SetFileAttributes(string filename, FileAttributes attr, DokanFileInfo info)
        {
            throw new NotImplementedException();
        }

        // SetFileTime isn't supported, since we don't actually track meaningful times for most of this.
        public int SetFileTime(string filename, DateTime ctime, DateTime atime, DateTime mtime, DokanFileInfo info)
        {
            throw new NotImplementedException();
        }

        // TODO: Perhaps use leases on blobs to do locking?
        public int UnlockFile(string filename, long offset, long length, DokanFileInfo info)
        {
            return 0;
        }

        // Close everything.
        public int Unmount(DokanFileInfo info)
        {
            foreach (var filename in readBlobs.Keys) CloseFile(filename, info);
            foreach (var filename in writeBlobs.Keys) CloseFile(filename, info);
            return 0;
        }

        private Dictionary<string, Stream> writeBlobs = new Dictionary<string, Stream>();
        public int WriteFile(string filename, byte[] buffer, ref uint writtenBytes, long offset, DokanFileInfo info)
        {
            if (!writeBlobs.ContainsKey(filename))
            {
                var blob = blobs.GetBlockBlobReference(filename.TrimStart('\\'));
                writeBlobs[filename] = blob.OpenWrite();
                if (offset != 0)
                {
                    blob.FetchAttributes();
                    if (offset == blob.Properties.Length)
                    {
                        // TODO: This is a really inefficient way to do this.
                        // The right thing to do is to start writing new blocks and then commit *old blocks* + *new blocks*.
                        // This method is okay for small files and "works."
                        var previousBytes = blob.DownloadByteArray();
                        writeBlobs[filename].Write(previousBytes, 0, previousBytes.Length);
                    }
                    else
                    {
                        // TODO: Handle arbitrary seeks during writing.
                        throw new NotImplementedException();
                    }
                }
            }
            writeBlobs[filename].Write(buffer, 0, buffer.Length);
            writtenBytes = (uint)buffer.Length;
            return 0;
        }
    }

}
