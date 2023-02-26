# DuplicateFileFinder

Simple Python utility to scan a subtree in your OS and find duplicate file candidates.
The utility scans the tree to catalog all files by path, modification date, and size.
Once done with this scan the md5 checksum for all files in groups with the same size are processed.
Duplicate candidates are files with the same md5 checksum and size.

Requirements

The utility uses
* SQLite3 to store data.
* Python3 to run the code.


