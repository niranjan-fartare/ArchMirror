#!/bin/bash

# Path to the footer file
#FOOTER_FILE="/var/www/html/footer.html"
STATS_FILE="/var/www/html/stats"
# Fetch vnstat output
VNSTAT_OUTPUT=$(vnstat)
#SUCCESS_COUNTER=$(cat /home/niranjan/logs/sync_success_count)
#FAILURE_COUNTER=$(cat /home/niranjan/logs/sync_failure_count)
#CPU_USAGE=$(top -bn1 | grep "Cpu(s)" | sed "s/.*, *\([0-9.]*\)%* id.*/\1/" | awk '{print 100 - $1"%"}')
#RAM_USAGE=$(free -m | awk 'NR==2{printf "%.2f%%", $3*100/$2 }')
# Generate the footer content in HTML
cat <<EOF > $STATS_FILE

$(echo "$VNSTAT_OUTPUT")


EOF

cp /root/favicon.ico /var/www/html
cp /root/fancy /var/www/html -r
