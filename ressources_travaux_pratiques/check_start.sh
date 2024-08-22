#!/bin/bash

usage="$0 <hdfs|yarn|hbase|spark>"

if [ $# -ne 1 ]
then
	echo $usage
	exit 1
fi

platform=$1




hget() {
    eval echo '${'"$1$2"'}'
}

hput() {
	eval "$1""$2"='$3'
	if [ "$1" != "keys" ]
	then
		k=`hget keys $1`
		if [ "$k" = "" ]
		then
			k=$2
		else
			present=`echo $k| tr '-' '\n' |grep $2`
			if [ "$present" = "" ]
			then
				k="$k"-"$2"
			fi
		fi
		hput keys $1 $k
	fi
}

hgetkeys() {
	echo `hget keys $1` | tr '-' ' '
}





case $platform in
	hdfs)
		var_platform_home="MY_HADOOP_HOME"
		daemons="namenode datanode secondarynamenode"
		hput namenode pattern "org.apache.hadoop.hdfs.server.namenode.NameNode"
		hput namenode log "`eval echo '$'$var_platform_home`/logs/*-namenode-*.log"
		
		hput datanode pattern "org.apache.hadoop.hdfs.server.datanode.DataNode"
		hput datanode log "`eval echo '$'$var_platform_home`/logs/*-datanode-*.log"
		
		hput secondarynamenode pattern "org.apache.hadoop.hdfs.server.namenode.SecondaryNameNode"
		hput secondarynamenode log "`eval echo '$'$var_platform_home`/logs/*-secondarynamenode-*.log"
		
		;;
	yarn)
		var_platform_home="MY_HADOOP_HOME"
		daemons="resourcemanager nodemanager"
		
		hput resourcemanager pattern "org.apache.hadoop.yarn.server.resourcemanager.ResourceManager"
		hput resourcemanager log "`eval echo '$'$var_platform_home`/logs/*-resourcemanager-*.log"
		
		hput nodemanager pattern "org.apache.hadoop.yarn.server.nodemanager.NodeManager"
		hput nodemanager log "`eval echo '$'$var_platform_home`/logs/*-nodemanager-*.log"
		;;
	hbase)
		var_platform_home="MY_HBASE_HOME"
		daemons="hmaster regionserver zookeeper"
		
		hput hmaster pattern "org.apache.hadoop.hbase.master.HMaster"
		hput hmaster log "`eval echo '$'$var_platform_home`/logs/*-master-*.log"
		
		hput regionserver pattern "org.apache.hadoop.hbase.regionserver.HRegionServer"
		hput regionserver log "`eval echo '$'$var_platform_home`/logs/*-regionserver-*.log"
		
		hput zookeeper pattern "org.apache.hadoop.hbase.zookeeper.HQuorumPeer"
		hput zookeeper log "`eval echo '$'$var_platform_home`/logs/*-zookeeper-*.log"
		;;
	spark)
		var_platform_home="MY_SPARK_HOME"
		daemons="sparkMaster sparkWorker"
		
		hput sparkMaster pattern "org.apache.spark.deploy.master.Master"
		hput sparkMaster log "`eval echo '$'$var_platform_home`/logs/*master*.out"
		
		hput sparkWorker pattern "org.apache.spark.deploy.worker.Worker"
		hput sparkWorker log "`eval echo '$'$var_platform_home`/logs/*worker*.out"
		;;
	*)
		echo $usage 
		exit 1
		;;
esac 


if [ -z "`env | grep $var_platform_home`" ]
then
	echo "La variable d'environnement $var_platform_home n'est pas définie"
	exit 1
fi

bad=0
for daemon in $daemons
do
	pattern_daemon=`hget $daemon pattern`
	tmp=`ps aux | grep $pattern_daemon | sed -e "s/\(^.*grep.*$\)//" | sed '/^$/d'`
	nb=`ps aux | grep $pattern_daemon | sed -e "s/\(^.*grep.*$\)//" | sed '/^$/d' | wc -l`
	echo -n "$daemon : "
	
	if [ "$tmp" = "" ]
	then
		echo "NE FONCTIONE PAS !"
		echo "Pour plus d'informations : cat `hget $daemon log` "
		bad=1
	else
		
		echo -n "OK"
		if [ $nb -gt 1 ]
		then
			echo  " ($nb running)"
		else
			echo ""
		fi
	fi
	echo ""
done

if [ "$bad" = "0" ]
then
	echo "Bravo, tout a l'air de fonctionner pour $platform :)"
fi
exit 0
