#!/bin/bash
(
    flock -n 200 || exit 1

    rsync -rlptH --safe-links --delete-delay --delay-updates rsync://arch.mirror.constant.com/archlinux/ /var/www/html

    bash /root/Scripts/footer.sh

) 200>/tmp/rsync-footer.lock
