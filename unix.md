# Unix Commands with Tabular Output

Commands suitable for tv table viewer implementation.

| # | Command | Columns | Notes |
|---|---------|---------|-------|
| **Process/Memory** |
| 1 | `ps` | PID,TTY,TIME,CMD | done |
| 2 | `top -bn1` | PID,USER,%CPU,%MEM,CMD | snapshot |
| 3 | `free` | total,used,free,shared,cache | memory |
| 4 | `pmap <pid>` | Address,Kbytes,Mode,Mapping | process memory map |
| 5 | `vmstat` | procs,memory,swap,io,system,cpu | virtual memory |
| 6 | `iostat` | Device,tps,kB_read/s,kB_wrtn/s | disk I/O |
| **Filesystem** |
| 7 | `df` | Filesystem,Size,Used,Avail,Use%,Mount | done |
| 8 | `du` | Size,Path | disk usage |
| 9 | `mount` | Device,Mount,Type,Options | done |
| 10 | `findmnt` | TARGET,SOURCE,FSTYPE,OPTIONS | better mount |
| 11 | `lsblk` | NAME,SIZE,TYPE,MOUNTPOINT | done |
| 12 | `blkid` | Device,UUID,TYPE,LABEL | block device IDs |
| 13 | `fdisk -l` | Device,Start,End,Sectors,Size,Type | partitions |
| 14 | `swapon` | NAME,TYPE,SIZE,USED,PRIO | swap areas |
| **Network** |
| 15 | `ss` | State,Recv-Q,Send-Q,Local,Peer | sockets |
| 16 | `netstat` | Proto,Recv-Q,Send-Q,Local,Foreign,State | legacy sockets |
| 17 | `ip addr` | Index,Name,State,MAC,IP | interfaces |
| 18 | `ip route` | Dest,Gateway,Dev,Metric | routing |
| 19 | `arp` | Address,HWtype,HWaddress,Iface | ARP cache |
| 20 | `nft list` | Chain,Hook,Prio,Policy,Rules | firewall |
| 21 | `tc qdisc` | Qdisc,Handle,Dev,Stats | traffic control |
| 22 | `iwconfig` | Interface,ESSID,Mode,Freq,Signal | wifi |
| 23 | `hostnamectl` | Property,Value | hostname info |
| **Hardware** |
| 24 | `lspci` | Slot,Class,Vendor,Device | PCI devices |
| 25 | `lsusb` | Bus,Device,ID,Description | USB devices |
| 26 | `lscpu` | Property,Value | CPU info |
| 27 | `lsmem` | Range,Size,State,Removable,Block | memory blocks |
| 28 | `sensors` | Sensor,Temp,High,Crit | temperatures |
| 29 | `acpi` | Battery,State,Percent | power |
| 30 | `dmidecode` | Handle,Type,Bytes,Description | BIOS/hardware |
| **Users/Auth** |
| 31 | `who` | User,TTY,Login,From | done |
| 32 | `w` | User,TTY,From,Login,Idle,JCPU,PCPU,What | detailed who |
| 33 | `last` | User,TTY,From,Login,Logout,Duration | login history |
| 34 | `lastlog` | Username,Port,From,Latest | last login each user |
| 35 | `id` | UID,GID,Groups | user identity |
| 36 | `getent passwd` | User,x,UID,GID,Name,Home,Shell | /etc/passwd |
| 37 | `getent group` | Group,x,GID,Members | /etc/group |
| **Services/Systemd** |
| 38 | `systemctl` | Unit,Load,Active,Sub,Description | done |
| 39 | `journalctl` | Time,Host,Unit,Message | done |
| 40 | `timedatectl` | Property,Value | time/timezone |
| 41 | `loginctl` | Session,UID,User,Seat,TTY | sessions |
| **Files/Search** |
| 42 | `ls -l` | Mode,Links,Owner,Group,Size,Date,Name | done |
| 43 | `find` | Path | file search |
| 44 | `locate` | Path | indexed search |
| 45 | `stat` | Property,Value | file metadata |
| **Kernel/Modules** |
| 46 | `lsmod` | Module,Size,Used,By | kernel modules |
| 47 | `dmesg` | Time,Facility,Level,Message | kernel ring buffer |
| 48 | `sysctl -a` | Key,Value | kernel params |
| **Package** |
| 49 | `dpkg -l` | Status,Name,Version,Arch,Description | debian pkgs |
| 50 | `pacman -Q` | Name,Version | done |

## Priority Implementation
1. `ss -tuln` - listening ports (security audit)
2. `lsmod` - kernel modules
3. `systemctl --type=service` - services
4. `lspci`/`lsusb` - hardware inventory
5. `du -sh *` - disk usage by dir
6. `last` - login audit trail
