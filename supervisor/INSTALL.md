# Installation

## Server setup
  * Operating system: Debaian 7 Wheezy
  * Packages: ufw

## Installation
The supervisor package from the debian repository does not seem to work properly. Better to use easy_install. Reference: http://edvanbeinum.com/how-to-install-and-configure-supervisord/

    $ sudo apt-get install python-setuptools
    $ sudo easy_install pip
    $ sudo pip install supervisor
    $ sudo su root
    $ echo_supervisord_conf > /etc/supervisord.conf
    $ exit
    $ sudo nano /etc/supervisord.conf

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
	    
	$ sudo mkdir /var/log/supervisor
	$ sudo mkdir /var/log/supervisor/columby_worker
	$ sudo mkdir /etc/supervisor
	$ sudo mkdir /etc/supervisor/conf.d
	$ sudo nano /etc/supervisor/conf.d/columby_worker.conf
	    Copy columby_worker.conf file contents
		  
	

Open the Firewall for this port

    $ sudo ufw allow 9001/tcp

Init.d script for starting and stopping: 
    
    $ sudo nano /etc/init.d/supervisord

    
    #! /bin/bash -e
  
    SUPERVISORD=/usr/local/bin/supervisord
    PIDFILE=/tmp/supervisord.pid
    OPTS="-c /etc/supervisord.conf"
  
    test -x $SUPERVISORD || exit 0
  
    . /lib/lsb/init-functions
  
    export PATH="${PATH:+$PATH:}/usr/local/bin:/usr/sbin:/sbin"
  
    case "$1" in
      start)
        log_begin_msg "Starting Supervisor daemon manager..."
        start-stop-daemon --start --quiet --pidfile $PIDFILE --exec $SUPERVISORD -- $OPTS || log_end_msg 1
        log_end_msg 0
        ;;
      stop)
        log_begin_msg "Stopping Supervisor daemon manager..."
        start-stop-daemon --stop --quiet --oknodo --pidfile $PIDFILE || log_end_msg 1
        log_end_msg 0
        ;;

    restart|reload|force-reload)
      log_begin_msg "Restarting Supervisor daemon manager..."
      start-stop-daemon --stop --quiet --oknodo --retry 30 --pidfile $PIDFILE
      start-stop-daemon --start --quiet --pidfile /var/run/sshd.pid --exec $SUPERVISORD -- $OPTS || log_end_msg 1
      log_end_msg 0
      ;;

    *)
      log_success_msg "Usage: /etc/init.d/supervisor
    {start|stop|reload|force-reload|restart}"
        exit 1
    esac
  
    exit 0
    
 Update file

    $ sudo chmod +x /etc/init.d/supervisord  
    $ sudo update-rc.d supervisord defaults
    $ sudo service supervisord start

## Configuration
  1. Copy the configuration file:  
    /etc/supervisor/conf.d/columby_worker.conf
  2. Copy the worker files (all files inside the supervisor folder): 
    cp /home/columby/hub/extras/columby_worker_supervisor/* /home/columby/columby_worker/ -r

## Command references
list of open ports

    sudo netstat -tulpn
f