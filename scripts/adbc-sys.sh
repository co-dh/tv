#!/bin/bash
# Helper to run system commands via ADBC SQLite
# Usage: source scripts/adbc-sys.sh
#        tv_ps, tv_tcp, tv_env, etc.

TV_DB="${TV_DB:-/dev/shm/tv_sys.db}"

# Import tab-separated data into SQLite table
_tv_import() {
    local table=$1
    shift
    sqlite3 "$TV_DB" "DROP TABLE IF EXISTS $table;"
    sqlite3 "$TV_DB" ".mode tabs" ".import /dev/stdin $table" <<< "$("$@")"
}

# ps - process list
tv_ps() {
    _tv_import ps bash -c 'printf "user\tpid\tcpu\tmem\tcmd\n"; ps aux --no-headers | awk "{printf \"%s\t%s\t%s\t%s\t%s\\n\",\$1,\$2,\$3,\$4,\$11}"'
    tabv "adbc:sqlite://$TV_DB?table=ps"
}

# tcp - TCP connections
tv_tcp() {
    _tv_import tcp bash -c 'printf "proto\tlocal\tremote\tstate\n"; ss -tn | tail -n+2 | awk "{printf \"tcp\t%s\t%s\t%s\\n\",\$4,\$5,\$1}"'
    tabv "adbc:sqlite://$TV_DB?table=tcp"
}

# udp - UDP connections
tv_udp() {
    _tv_import udp bash -c 'printf "proto\tlocal\tremote\tstate\n"; ss -un | tail -n+2 | awk "{printf \"udp\t%s\t%s\t%s\\n\",\$4,\$5,\$1}"'
    tabv "adbc:sqlite://$TV_DB?table=udp"
}

# env - environment variables
tv_env() {
    _tv_import env bash -c 'printf "name\tvalue\n"; env | sed "s/=/\t/"'
    tabv "adbc:sqlite://$TV_DB?table=env"
}

# df - disk usage
tv_df() {
    _tv_import df bash -c 'df -h | awk "NR==1{printf \"fs\\tsize\\tused\\tavail\\tpct\\tmount\\n\"} NR>1{printf \"%s\\t%s\\t%s\\t%s\\t%s\\t%s\\n\",\$1,\$2,\$3,\$4,\$5,\$6}"'
    tabv "adbc:sqlite://$TV_DB?table=df"
}

# mounts
tv_mounts() {
    _tv_import mounts bash -c 'printf "dev\tmount\ttype\topts\n"; mount | awk "{printf \"%s\\t%s\\t%s\\t%s\\n\",\$1,\$3,\$5,\$6}"'
    tabv "adbc:sqlite://$TV_DB?table=mounts"
}

echo "ADBC system commands: tv_ps, tv_tcp, tv_udp, tv_env, tv_df, tv_mounts"
