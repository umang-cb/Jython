
rm -rf /Applications/Couchbase\ Server.app
rm -rf ~/Library/Application\ Support/Couchbase
rm -rf ~/Library/Application\ Support/Membase

system_profiler SPSoftwareDataType

#echo "couchbase" | sudo shutdown -r now