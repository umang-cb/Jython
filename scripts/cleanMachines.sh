

kill -9 $(ps aux | grep 'couchdb' |  awk '{print $2}')
dpkg -r couchbase-server;dpkg --purge couchbase-server;
kill -9 $(ps aux | grep 'epmd' |  awk '{print $2}')
kill -9 $(ps aux | grep 'beam.smp' |  awk '{print $2}')
kill -9 $(ps aux | grep 'memsup' |  awk '{print $2}')
kill -9 $(ps aux | grep 'memcached' |  awk '{print $2}') 
kill -9 $(ps aux | grep 'cpu_sup' |  awk '{print $2}')
kill -9 $(ps aux | grep 'moxi' |  awk '{print $2}') 
kill -9 $(ps aux | grep 'goxdcr' |  awk '{print $2}')
kill -9 $(ps aux | grep 'vbucketmigrator' |  awk '{print $2}') 
kill -9 $(ps aux | grep 'erlang' |  awk '{print $2}')
kill -9 $(ps aux | grep 'cpu_sup' |  awk '{print $2}') 
rm -rf /var/opt/membase
rm -rf /opt/membase
rm -rf /etc/opt/membase
rm -rf /var/membase/data/*
rm -rf /opt/membase/var/lib/membase/*
rm -rf /opt/couchbase
rm -rf /data/*
ipcrm



kill -9 $(ps aux | grep 'couchdb' |  awk '{print $2}')
rpm -e couchbase-server;
kill -9 $(ps aux | grep 'epmd' |  awk '{print $2}')
kill -9 $(ps aux | grep 'beam.smp' |  awk '{print $2}')
kill -9 $(ps aux | grep 'memsup' |  awk '{print $2}')
kill -9 $(ps aux | grep 'memcached' |  awk '{print $2}') 
kill -9 $(ps aux | grep 'cpu_sup' |  awk '{print $2}')
kill -9 $(ps aux | grep 'moxi' |  awk '{print $2}') 
kill -9 $(ps aux | grep 'goxdcr' |  awk '{print $2}')
kill -9 $(ps aux | grep 'vbucketmigrator' |  awk '{print $2}') 
kill -9 $(ps aux | grep 'erlang' |  awk '{print $2}')
kill -9 $(ps aux | grep 'cpu_sup' |  awk '{print $2}') 
rm -rf /var/opt/membase
rm -rf /opt/membase
rm -rf /etc/opt/membase
rm -rf /var/membase/data/*
rm -rf /opt/membase/var/lib/membase/*
rm -rf /opt/couchbase
rm -rf /data/*
ipcrm
systemctl stop firewalld
systemctl disable firewalld
