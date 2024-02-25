达梦数据库驱动
==================


https://gitee.com/chunanyong/dm


初始化要求：UTF-8字符编码,不区分大小写,建表语句的字段名不要带""双引号

./dminit path=/opt/dmdbms/data page_size=16 extent_size=16 log_size=2048 port_num=5236 charset=1 case_sensitive=0 LENGTH_IN_CHAR=1 db_name=DAMENG instance_name=DMSERVER

注册服务
cd /opt/dmdbms/script/root
./dm_service_installer.sh -t dmserver -dm_ini /opt/dmdbms/data/DAMENG/dm.ini -p DMSERVER

修改 /opt/dmdbms/data/DAMENG/dm.ini 的 COMPATIBLE_MODE 为4 (0:none, 1:SQL92, 2:Oracle, 3:MS SQL Server, 4:MySQL, 5:DM6, 6:Teradata)。

启停服务

systemctl enable DmServiceDMSERVER
systemctl start DmServiceDMSERVER
systemctl stop DmServiceDMSERVER

卸载：

/opt/dmdbms/script/root/dm_service_uninstaller.sh -n DmServiceDMSERVER

访问数据库：

./disql
用户名：sysdba 密码：SYSDBA


创建用户

create user "yunioncloud" identified by "passw0rd";
grant resource,public to yunioncloud;

创建用户的同时会自动创建一个同名的 schema (模式)，达梦的模式类似其他数据库的database


数据迁移：

找一台Windows主机，下载达梦windows版本，仅安装客户端，则会包含达梦的数据库迁移工具DTS，使用DTS将mysql的数据迁移到达梦。

