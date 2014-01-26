# Columby Worker

## About
The background workers for Columby (used for data-processing). 


## Installation 
Installation using the default Debian supervisor package: 
    
    apt-get install supervisor
    service supervisor start

Stopping the service does not seem to kill the process. To do it manually: 

    ps auxww | grep supervisor | grep -v grep
    kill -9 a[PID]

Update the configuration file and add the reference to the columby-worker process
    chmod=0777 ;???
  chown=columby:www-data ;???
  logfile=/var/log/supervisor/supervisord.log
  logfile_maxbytes=20MB
  logfile_backups=50
      
  [inet_http_server]
  port=*:9001
  username = user
  password = pass
      
  [include]
  files = /etc/supervisor/conf.d/*.conf

Copy the columby-worker configuration file to /etc/supervisor/conf.d/columby-worker.conf

Add a settings file to the config folder
    


Start the service
    
    service supervisor start
